//! Test doubles for the MQTT core: a transport that records what would go
//! to the broker, and a hub that keeps devices, entities and states the way
//! TheHome does (entity ids, registry attributes, friendly names).

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::{json, Map, Value};
use thehome_mqtt::entity::Publish;
use thehome_mqtt::hub::{device_key, HubApi};
use thehome_mqtt::types::{DeviceSpec, EntitySpec, Issue, ServiceError};
use thehome_mqtt::{Mqtt, Settings, Transport};

#[derive(Default)]
pub struct Fake {
    pub published: Mutex<Vec<Publish>>,
    pub subscribed: Mutex<Vec<String>>,
    pub unsubscribed: Mutex<Vec<String>>,
}

impl Transport for Fake {
    fn publish(&self, p: Publish) {
        self.published.lock().push(p);
    }
    fn subscribe(&self, filter: &str, _qos: u8) {
        self.subscribed.lock().push(filter.to_string());
    }
    fn unsubscribe(&self, filter: &str) {
        self.unsubscribed.lock().push(filter.to_string());
    }
}

impl Fake {
    pub fn take(&self) -> Vec<Publish> {
        std::mem::take(&mut *self.published.lock())
    }
    pub fn take_pairs(&self) -> Vec<(String, String)> {
        self.take().into_iter().map(|p| (p.topic, p.payload)).collect()
    }
}

#[derive(Clone, Debug)]
pub struct State {
    pub state: String,
    pub attributes: Map<String, Value>,
}

#[derive(Clone, Debug)]
pub struct Device {
    pub id: String,
    pub key: String,
    pub spec: DeviceSpec,
    pub via_device_id: Option<String>,
}

impl std::ops::Deref for Device {
    type Target = DeviceSpec;
    fn deref(&self) -> &DeviceSpec {
        &self.spec
    }
}

#[derive(Clone, Debug)]
pub struct Entity {
    pub entity_id: String,
    pub spec: EntitySpec,
    /// TheHome's device id.
    pub device_id: Option<String>,
}

#[derive(Default)]
struct Inner {
    devices: Vec<Device>,
    entities: HashMap<String, Entity>,
    states: HashMap<String, State>,
    events: Vec<(String, Value)>,
    issues: Vec<Issue>,
}

#[derive(Default)]
pub struct FakeHub {
    inner: Mutex<Inner>,
}

/// TheHome's slugify.
pub fn slugify(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars().flat_map(|c| c.to_lowercase()) {
        let rep: &str = match c {
            'æ' | 'ä' => "ae",
            'ø' | 'ö' => "oe",
            'å' => "aa",
            'ü' => "ue",
            'é' | 'è' | 'ê' | 'ë' => "e",
            c if c.is_ascii_alphanumeric() => {
                out.push(c);
                continue;
            }
            _ => "_",
        };
        out.push_str(rep);
    }
    let mut collapsed = String::new();
    for c in out.chars() {
        if !(c == '_' && collapsed.ends_with('_')) {
            collapsed.push(c);
        }
    }
    collapsed.trim_matches('_').to_string()
}

impl Inner {
    fn device_by_key(&self, key: &str) -> Option<&Device> {
        self.devices.iter().find(|d| d.key == key)
    }

    fn friendly_name(&self, e: &Entity) -> Option<String> {
        let dev = e.device_id.as_ref().and_then(|d| self.devices.iter().find(|x| &x.id == d));
        match (dev, &e.spec.name) {
            (Some(d), Some(n)) => Some(format!("{} {n}", d.spec.name)),
            (Some(d), None) => Some(d.spec.name.clone()),
            (None, n) => n.clone(),
        }
    }
}

impl FakeHub {
    pub fn state(&self, id: &str) -> Option<State> {
        self.inner.lock().states.get(id).cloned()
    }
    pub fn entity(&self, id: &str) -> Option<Entity> {
        self.inner.lock().entities.get(id).cloned()
    }
    pub fn entity_ids(&self) -> Vec<String> {
        let mut v: Vec<String> = self.inner.lock().entities.keys().cloned().collect();
        v.sort();
        v
    }
    pub fn devices(&self) -> Vec<Device> {
        let inner = self.inner.lock();
        inner
            .devices
            .iter()
            .map(|d| {
                let mut d = d.clone();
                // linked once the parent exists, like TheHome's registry
                d.via_device_id = d
                    .spec
                    .via_device
                    .as_ref()
                    .and_then(|v| inner.devices.iter().find(|p| p.spec.identifiers.contains(v)).map(|p| p.id.clone()));
                d
            })
            .collect()
    }
    pub fn device_named(&self, name: &str) -> Device {
        self.devices().into_iter().find(|d| d.spec.name == name).unwrap_or_else(|| panic!("no device {name}"))
    }
    pub fn take_events(&self) -> Vec<(String, Value)> {
        std::mem::take(&mut self.inner.lock().events)
    }
    pub fn issues(&self) -> Vec<Issue> {
        self.inner.lock().issues.clone()
    }

    /// A service call the way TheHome routes it to the integration: generic
    /// services translated for the entity's domain, then the entity handler.
    pub fn call(&self, mqtt: &Mqtt, domain: &str, service: &str, data: Value) -> Result<(), ServiceError> {
        let mut data = match data {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        let entity_id = data.remove("entity_id").and_then(|v| v.as_str().map(str::to_string)).expect("entity_id");
        assert_eq!(entity_id.split('.').next(), Some(domain), "{entity_id} is not a {domain}");
        let (uid, state) = {
            let inner = self.inner.lock();
            let e = inner.entities.get(&entity_id).ok_or_else(|| ServiceError::NotFound(entity_id.clone()))?;
            (e.spec.unique_id.clone(), inner.states.get(&entity_id).map(|s| s.state.clone()).unwrap_or_default())
        };
        let service = translate(domain, service, &state);
        mqtt.command_entity(&uid, &service, &data)
    }
}

/// TheHome's mapping of generic services to what a domain understands.
fn translate(domain: &str, service: &str, state: &str) -> String {
    let on = |s: &str| -> String {
        match (domain, s) {
            ("cover", "turn_on") => "open_cover".into(),
            ("cover", "turn_off") => "close_cover".into(),
            ("valve", "turn_on") => "open_valve".into(),
            ("valve", "turn_off") => "close_valve".into(),
            ("lock", "turn_on") => "lock".into(),
            ("lock", "turn_off") => "unlock".into(),
            ("button", "turn_on") => "press".into(),
            _ => s.into(),
        }
    };
    match service {
        "toggle" => match domain {
            "cover" => if state == "open" || state == "opening" { "close_cover".into() } else { "open_cover".into() },
            "valve" => if state == "open" { "close_valve".into() } else { "open_valve".into() },
            "lock" => if state == "locked" { "unlock".into() } else { "lock".into() },
            _ => {
                let is_on = !matches!(state, "off" | "unavailable" | "unknown" | "idle" | "standby" | "closed" | "");
                if is_on { on("turn_off") } else { on("turn_on") }
            }
        },
        s => on(s),
    }
}

impl HubApi for FakeHub {
    fn register_device(&self, spec: DeviceSpec) -> String {
        let key = device_key(&spec).unwrap();
        let mut inner = self.inner.lock();
        match inner.devices.iter_mut().find(|d| d.key == key) {
            Some(d) => d.spec = spec,
            None => {
                let id = format!("dev{}", inner.devices.len() + 1);
                inner.devices.push(Device { id, key: key.clone(), spec, via_device_id: None });
            }
        }
        key
    }

    fn remove_device(&self, key: &str) {
        self.inner.lock().devices.retain(|d| d.key != key);
    }

    fn add_entity(&self, mut spec: EntitySpec) -> Option<String> {
        let mut inner = self.inner.lock();
        let uid = spec.unique_id.clone();
        let device = spec.device_id.as_ref().and_then(|k| inner.device_by_key(k)).cloned();
        spec.device_id = None;
        let existing = inner.entities.values().find(|e| e.spec.unique_id == uid && e.spec.domain == spec.domain).map(|e| e.entity_id.clone());
        let entity_id = existing.unwrap_or_else(|| {
            let object_id = spec.suggested_object_id.clone().unwrap_or_else(|| {
                let parts: Vec<String> = [device.as_ref().map(|d| d.spec.name.clone()), spec.name.clone()].into_iter().flatten().collect();
                if parts.is_empty() { format!("mqtt_{uid}") } else { parts.join(" ") }
            });
            let base = format!("{}.{}", spec.domain, slugify(&object_id));
            let mut candidate = base.clone();
            let mut n = 2;
            while inner.entities.contains_key(&candidate) {
                candidate = format!("{base}_{n}");
                n += 1;
            }
            candidate
        });
        inner.entities.insert(
            entity_id.clone(),
            Entity { entity_id: entity_id.clone(), spec, device_id: device.map(|d| d.id) },
        );
        Some(uid)
    }

    fn remove_entity(&self, handle: &str) {
        let mut inner = self.inner.lock();
        let ids: Vec<String> = inner.entities.values().filter(|e| e.spec.unique_id == handle).map(|e| e.entity_id.clone()).collect();
        for id in ids {
            inner.entities.remove(&id);
            inner.states.remove(&id);
        }
    }

    fn set_state(&self, handle: &str, state: String, attributes: Map<String, Value>) {
        let mut inner = self.inner.lock();
        let Some(e) = inner.entities.values().find(|e| e.spec.unique_id == handle).cloned() else { return };
        let mut attrs = Map::new();
        if let Value::Object(caps) = &e.spec.capabilities {
            attrs.extend(caps.clone());
        }
        attrs.extend(attributes);
        for (k, v) in [
            ("unit_of_measurement", &e.spec.unit_of_measurement),
            ("device_class", &e.spec.device_class),
            ("state_class", &e.spec.state_class),
            ("icon", &e.spec.icon),
        ] {
            if let Some(v) = v {
                attrs.insert(k.into(), json!(v));
            }
        }
        if let Some(n) = inner.friendly_name(&e) {
            attrs.insert("friendly_name".into(), json!(n));
        }
        inner.states.insert(e.entity_id.clone(), State { state, attributes: attrs });
    }

    fn fire_event(&self, event_type: &str, data: Value) {
        self.inner.lock().events.push((event_type.to_string(), data));
    }

    fn create_issue(&self, issue: Issue) {
        let mut inner = self.inner.lock();
        inner.issues.retain(|i| i.issue_id != issue.issue_id);
        inner.issues.push(issue);
    }

    fn delete_issue(&self, issue_id: &str) {
        self.inner.lock().issues.retain(|i| i.issue_id != issue_id);
    }

    fn device_key(&self, device_id: &str) -> Option<String> {
        self.inner.lock().devices.iter().find(|d| d.id == device_id).map(|d| d.key.clone())
    }

    fn device_id(&self, key: &str) -> Option<String> {
        self.inner.lock().device_by_key(key).map(|d| d.id.clone())
    }
}

/// A connected core with a fake transport and hub.
pub fn setup() -> (Arc<FakeHub>, Arc<Fake>, Arc<Mqtt>) {
    let hub = Arc::new(FakeHub::default());
    let fake = Arc::new(Fake::default());
    let mqtt = Mqtt::new(hub.clone(), fake.clone(), Settings::default());
    mqtt.on_connected();
    (hub, fake, mqtt)
}

pub fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
}
