//! Plays TheHome over a pipe: answers the integration's requests the way
//! TheHome does and records devices, entities, states, events, services and
//! issues.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use thehome_plugin::Integration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};

#[derive(Default)]
pub struct Recorded {
    /// TheHome device id -> spec
    pub devices: Vec<(String, Value)>,
    /// entity_id -> spec
    pub entities: HashMap<String, Value>,
    pub states: HashMap<String, (String, Value)>,
    pub events: Vec<(String, Value)>,
    pub services: Vec<String>,
    pub issues: Vec<String>,
}

pub struct FakeHost {
    tx: mpsc::UnboundedSender<String>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>,
    next: AtomicU64,
    pub rec: Arc<Mutex<Recorded>>,
}

impl FakeHost {
    pub fn start<I: Integration>(integration: I) -> Self {
        let (plugin_io, host_io) = tokio::io::duplex(1 << 20);
        let (pr, pw) = tokio::io::split(plugin_io);
        tokio::spawn(thehome_plugin::serve_io(integration, pr, pw));
        let (hr, mut hw) = tokio::io::split(host_io);

        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            while let Some(line) = rx.recv().await {
                if hw.write_all(format!("{line}\n").as_bytes()).await.is_err() {
                    break;
                }
            }
        });

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>> = Arc::default();
        let rec: Arc<Mutex<Recorded>> = Arc::default();
        let (p, r, t) = (pending.clone(), rec.clone(), tx.clone());
        tokio::spawn(async move {
            let mut lines = BufReader::new(hr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let msg: Value = serde_json::from_str(&line).unwrap();
                match msg.get("method").and_then(Value::as_str) {
                    Some(method) => {
                        let result = answer(&r, method, &msg["params"]);
                        if let Some(id) = msg.get("id") {
                            let _ = t.send(json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string());
                        }
                    }
                    None => {
                        let id = msg["id"].as_u64().unwrap();
                        if let Some(s) = p.lock().unwrap().remove(&id) {
                            let _ = s.send(msg);
                        }
                    }
                }
            }
        });
        Self { tx, pending, next: AtomicU64::new(1), rec }
    }

    /// Call the integration; `Err` holds the error object.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, Value> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (s, r) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, s);
        self.tx.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string()).unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(30), r).await.expect("no answer").unwrap();
        match reply.get("error") {
            Some(e) if !e.is_null() => Err(e.clone()),
            _ => Ok(reply["result"].clone()),
        }
    }

    pub fn state(&self, entity_id: &str) -> Option<(String, Value)> {
        self.rec.lock().unwrap().states.get(entity_id).cloned()
    }

    /// Wait until `entity_id` has the given state.
    pub async fn wait_state(&self, entity_id: &str, want: &str) -> Value {
        for _ in 0..200 {
            if let Some((s, attrs)) = self.state(entity_id) {
                if s == want {
                    return attrs;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("{entity_id} never became {want}, it is {:?}", self.state(entity_id));
    }

    pub async fn wait_entity(&self, entity_id: &str) -> Value {
        for _ in 0..200 {
            if let Some(e) = self.rec.lock().unwrap().entities.get(entity_id) {
                return e.clone();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("{entity_id} missing; have {:?}", self.rec.lock().unwrap().entities.keys().collect::<Vec<_>>());
    }

    pub async fn wait_event(&self, event_type: &str) -> Value {
        for _ in 0..200 {
            {
                let mut rec = self.rec.lock().unwrap();
                if let Some(i) = rec.events.iter().position(|(t, _)| t == event_type) {
                    return rec.events.remove(i).1;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no {event_type} event");
    }
}

/// TheHome's entity ids: the suggested object id, else device name plus
/// entity name.
fn slug(s: &str) -> String {
    let s = s.to_lowercase().replace('æ', "ae").replace('ø', "oe").replace('å', "aa");
    let s: String = s.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '_' }).collect();
    s.split('_').filter(|p| !p.is_empty()).collect::<Vec<_>>().join("_")
}

fn answer(rec: &Mutex<Recorded>, method: &str, p: &Value) -> Value {
    let mut rec = rec.lock().unwrap();
    match method {
        "device/register" => {
            let spec = p["device"].clone();
            let ids = spec["identifiers"].clone();
            let id = match rec.devices.iter().position(|(_, d)| d["identifiers"] == ids) {
                Some(i) => {
                    rec.devices[i].1 = spec.clone();
                    rec.devices[i].0.clone()
                }
                None => {
                    let id = format!("dev{}", rec.devices.len() + 1);
                    rec.devices.push((id.clone(), spec.clone()));
                    id
                }
            };
            let via = spec["via_device"]
                .as_array()
                .and_then(|v| rec.devices.iter().find(|(_, d)| d["identifiers"].as_array().is_some_and(|a| a.contains(&Value::Array(v.clone())))))
                .map(|(id, _)| id.clone());
            json!({ "id": id, "name": spec["name"], "via_device_id": via })
        }
        "entity/add" => {
            let e = &p["entity"];
            let dev_name = e["device_id"]
                .as_str()
                .and_then(|d| rec.devices.iter().find(|(id, _)| id == d))
                .and_then(|(_, d)| d["name"].as_str())
                .unwrap_or_default()
                .to_string();
            let object = match e["suggested_object_id"].as_str() {
                Some(o) => o.to_string(),
                None => [dev_name.as_str(), e["name"].as_str().unwrap_or("")].iter().filter(|s| !s.is_empty()).cloned().collect::<Vec<_>>().join(" "),
            };
            let id = format!("{}.{}", e["domain"].as_str().unwrap(), slug(&object));
            rec.entities.insert(id.clone(), e.clone());
            json!({ "entity_id": id })
        }
        "state/set" => {
            let id = p["entity_id"].as_str().unwrap().to_string();
            rec.states.insert(id, (p["state"].as_str().unwrap_or("").to_string(), p["attributes"].clone()));
            Value::Null
        }
        "state/unavailable" => {
            let id = p["entity_id"].as_str().unwrap().to_string();
            rec.states.insert(id, ("unavailable".into(), Value::Null));
            Value::Null
        }
        "event/fire" => {
            rec.events.push((p["event_type"].as_str().unwrap_or("").to_string(), p["data"].clone()));
            Value::Null
        }
        "service/register" => {
            rec.services.push(p["service"].as_str().unwrap_or("").to_string());
            Value::Null
        }
        "issue/create" => {
            rec.issues.push(p["issue_id"].as_str().unwrap_or("").to_string());
            Value::Null
        }
        "issue/delete" => {
            let id = p["issue_id"].as_str().unwrap_or("").to_string();
            rec.issues.retain(|i| *i != id);
            Value::Null
        }
        _ => Value::Null,
    }
}
