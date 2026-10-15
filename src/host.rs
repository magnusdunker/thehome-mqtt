//! [`HubApi`] over the plugin protocol. The MQTT core is synchronous and
//! must not wait for TheHome, so calls are queued and sent in order by one
//! task. Ids come back asynchronously; the core uses device keys and entity
//! unique ids, and this task translates them to TheHome's ids.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::{json, Map, Value};
use thehome_plugin::Host;
use tokio::sync::mpsc;

use crate::hub::{device_key, HubApi};
use crate::types::{DeviceSpec, EntitySpec, Issue};

enum Op {
    Device(String, DeviceSpec),
    Entity(EntitySpec),
    RemoveEntity(String),
    State(String, String, Map<String, Value>),
    Event(String, Value),
    Issue(Issue),
    DeleteIssue(String),
}

#[derive(Default)]
struct Ids {
    /// device key -> TheHome device id
    devices: HashMap<String, String>,
    /// entity unique id -> TheHome entity id (None when disabled)
    entities: HashMap<String, Option<String>>,
}

pub struct HostHub {
    tx: mpsc::UnboundedSender<Op>,
    ids: Arc<Mutex<Ids>>,
}

impl HostHub {
    /// Start the sending task. It ends when the hub is dropped.
    pub fn start(host: Host, entry_id: String) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let ids: Arc<Mutex<Ids>> = Arc::default();
        tokio::spawn(worker(host, entry_id, rx, ids.clone()));
        Arc::new(Self { tx, ids })
    }

    /// The unique id of one of our entities, by TheHome's entity id.
    pub fn unique_id_of(&self, entity_id: &str) -> Option<String> {
        self.ids
            .lock()
            .entities
            .iter()
            .find(|(_, e)| e.as_deref() == Some(entity_id))
            .map(|(u, _)| u.clone())
    }
}

impl HubApi for HostHub {
    fn register_device(&self, spec: DeviceSpec) -> String {
        let key = device_key(&spec).unwrap_or_default();
        let _ = self.tx.send(Op::Device(key.clone(), spec));
        key
    }

    fn remove_device(&self, _key: &str) {
        // The plugin protocol cannot remove devices; the user can delete it.
    }

    fn add_entity(&self, spec: EntitySpec) -> Option<String> {
        let uid = spec.unique_id.clone();
        let _ = self.tx.send(Op::Entity(spec));
        Some(uid)
    }

    fn remove_entity(&self, handle: &str) {
        let _ = self.tx.send(Op::RemoveEntity(handle.to_string()));
    }

    fn set_state(&self, handle: &str, state: String, attributes: Map<String, Value>) {
        let _ = self.tx.send(Op::State(handle.to_string(), state, attributes));
    }

    fn fire_event(&self, event_type: &str, data: Value) {
        let _ = self.tx.send(Op::Event(event_type.to_string(), data));
    }

    fn create_issue(&self, issue: Issue) {
        let _ = self.tx.send(Op::Issue(issue));
    }

    fn delete_issue(&self, issue_id: &str) {
        let _ = self.tx.send(Op::DeleteIssue(issue_id.to_string()));
    }

    fn device_key(&self, device_id: &str) -> Option<String> {
        self.ids.lock().devices.iter().find(|(_, id)| *id == device_id).map(|(k, _)| k.clone())
    }

    fn device_id(&self, key: &str) -> Option<String> {
        self.ids.lock().devices.get(key).cloned()
    }
}

async fn worker(host: Host, entry_id: String, mut rx: mpsc::UnboundedReceiver<Op>, ids: Arc<Mutex<Ids>>) {
    // devices announced before the device they are connected through
    let mut pending_via: Vec<(String, DeviceSpec)> = vec![];
    while let Some(op) = rx.recv().await {
        match op {
            Op::Device(key, spec) => {
                let mut queue = vec![(key, spec)];
                while let Some((key, spec)) = queue.pop() {
                    let Some(reply) = register(&host, &entry_id, &spec).await else { continue };
                    if let Some(id) = reply.get("id").and_then(Value::as_str) {
                        ids.lock().devices.insert(key.clone(), id.to_string());
                    }
                    pending_via.retain(|(k, _)| *k != key);
                    let linked = reply.get("via_device_id").is_some_and(|v| !v.is_null());
                    if spec.via_device.is_some() && !linked {
                        pending_via.push((key.clone(), spec.clone()));
                    }
                    // link the devices waiting for this one
                    let (ready, rest): (Vec<_>, Vec<_>) = pending_via
                        .drain(..)
                        .partition(|(_, p)| p.via_device.as_ref().is_some_and(|v| spec.identifiers.contains(v)));
                    pending_via = rest;
                    queue.extend(ready);
                }
            }
            Op::Entity(mut spec) => {
                let uid = spec.unique_id.clone();
                spec.device_id = spec.device_id.and_then(|k| ids.lock().devices.get(&k).cloned());
                match host.request("entity/add", json!({ "entry_id": entry_id, "entity": spec, "handler": true })).await {
                    Ok(v) => {
                        let eid = v.get("entity_id").and_then(Value::as_str).map(str::to_string);
                        ids.lock().entities.insert(uid, eid);
                    }
                    Err(e) => crate::warn!("kunne ikke tilføje {uid}: {e}"),
                }
            }
            Op::RemoveEntity(uid) => {
                // The plugin protocol cannot remove entities, so it goes
                // unavailable; the user can delete it in TheHome.
                let eid = ids.lock().entities.get(&uid).cloned().flatten();
                if let Some(eid) = eid {
                    if let Err(e) = host.set_unavailable(&eid).await {
                        crate::debug!("{eid}: {e}");
                    }
                }
            }
            Op::State(uid, state, attributes) => {
                let eid = ids.lock().entities.get(&uid).cloned().flatten();
                if let Some(eid) = eid {
                    if let Err(e) = host.set_state(&eid, state, attributes).await {
                        crate::debug!("{eid}: {e}");
                    }
                }
            }
            Op::Event(event_type, data) => {
                if let Err(e) = host.fire_event(&event_type, data).await {
                    crate::debug!("{event_type}: {e}");
                }
            }
            Op::Issue(i) => {
                if let Err(e) = host.create_issue(&entry_id, &i.issue_id, &i.severity, &i.title, &i.description).await {
                    crate::debug!("{}: {e}", i.issue_id);
                }
            }
            Op::DeleteIssue(id) => {
                if let Err(e) = host.delete_issue(&entry_id, &id).await {
                    crate::debug!("{id}: {e}");
                }
            }
        }
    }
}

async fn register(host: &Host, entry_id: &str, spec: &DeviceSpec) -> Option<Value> {
    match host.request("device/register", json!({ "entry_id": entry_id, "device": spec })).await {
        Ok(v) => Some(v),
        Err(e) => {
            crate::warn!("kunne ikke registrere enheden {}: {e}", spec.name);
            None
        }
    }
}
