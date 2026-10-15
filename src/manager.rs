//! The protocol-independent MQTT core: subscriptions, discovery, entity
//! state and command routing. The network client feeds messages into
//! [`Mqtt::handle_message`] and publishes through a [`Transport`]; TheHome
//! is reached through [`HubApi`], so tests can drive it without a broker
//! or TheHome.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde_json::{json, Map, Value};
use tokio::sync::oneshot;

use crate::discovery;
use crate::entity::{payload_str, Cfg, Entity, Publish, Trigger};
use crate::hub::{self, HubApi};
use crate::topic;
use crate::types::{DeviceSpec, Issue, ServiceError, STATE_UNAVAILABLE};
use crate::zigbee2mqtt::{self as z2m, Bridge};

/// Where publishes and subscriptions go. Calls must not block.
pub trait Transport: Send + Sync {
    fn publish(&self, p: Publish);
    fn subscribe(&self, filter: &str, qos: u8);
    fn unsubscribe(&self, filter: &str);
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub discovery: bool,
    pub prefix: String,
    /// Publish `<prefix>/status` = `online` after connecting.
    pub birth: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            discovery: true,
            prefix: "homeassistant".into(),
            birth: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Target {
    Discovery,
    Entity { id: String, role: String },
    Trigger { id: String },
    User,
    BridgeInfo,
    BridgeResponse,
}

struct Sub {
    qos: u8,
    targets: Vec<Target>,
}

const DEBUG_MESSAGES: usize = 10;
/// How long to wait for a Zigbee2MQTT bridge to answer a request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Default)]
struct Inner {
    connected: bool,
    subs: HashMap<String, Sub>,
    entities: HashMap<String, Entity>,
    triggers: HashMap<String, Trigger>,
    /// Discovery topic -> discovery ids announced on it.
    topics: HashMap<String, Vec<String>>,
    /// Discovery id -> recent messages for debug info.
    recent: HashMap<String, VecDeque<Value>>,
    /// Device key -> its identifiers.
    devices: HashMap<String, Vec<(String, String)>>,
    /// Zigbee2MQTT bridges by base topic.
    bridges: BTreeMap<String, Bridge>,
    /// Open `bridge/request/*` calls by transaction id.
    waiters: HashMap<String, oneshot::Sender<Value>>,
}

pub struct Mqtt {
    hub: Arc<dyn HubApi>,
    transport: Arc<dyn Transport>,
    settings: Settings,
    inner: Mutex<Inner>,
}

impl Mqtt {
    pub fn new(hub: Arc<dyn HubApi>, transport: Arc<dyn Transport>, settings: Settings) -> Arc<Self> {
        let mqtt = Arc::new(Self {
            hub,
            transport,
            settings,
            inner: Mutex::new(Inner::default()),
        });
        {
            let mut inner = mqtt.inner.lock();
            if mqtt.settings.discovery {
                let filter = format!("{}/#", mqtt.settings.prefix);
                mqtt.add_sub(&mut inner, &filter, 0, Target::Discovery);
            }
            mqtt.add_sub(&mut inner, z2m::INFO_FILTER, 0, Target::BridgeInfo);
            mqtt.add_sub(&mut inner, z2m::RESPONSE_FILTER, 0, Target::BridgeResponse);
        }
        mqtt
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    pub fn is_connected(&self) -> bool {
        self.inner.lock().connected
    }

    // ------------------------------------------------------------------
    // connection lifecycle

    pub fn on_connected(&self) {
        let mut inner = self.inner.lock();
        inner.connected = true;
        for (filter, sub) in &inner.subs {
            self.transport.subscribe(filter, sub.qos);
        }
        if self.settings.birth {
            self.transport.publish(Publish {
                topic: format!("{}/status", self.settings.prefix),
                payload: "online".into(),
                qos: 0,
                retain: false,
            });
        }
        let ids: Vec<String> = inner.entities.keys().cloned().collect();
        for id in ids {
            self.push(&inner, &id);
        }
    }

    pub fn on_disconnected(&self) {
        let mut inner = self.inner.lock();
        if !inner.connected {
            return;
        }
        inner.connected = false;
        let ids: Vec<String> = inner.entities.keys().cloned().collect();
        for id in ids {
            self.push(&inner, &id);
        }
    }

    /// Check `expire_after` and `off_delay` timers.
    pub fn tick(&self) {
        let mut inner = self.inner.lock();
        let now = Instant::now();
        let changed: Vec<String> = inner
            .entities
            .iter_mut()
            .filter_map(|(id, e)| e.tick(now).then(|| id.clone()))
            .collect();
        for id in changed {
            self.push(&inner, &id);
        }
    }

    // ------------------------------------------------------------------
    // subscriptions

    fn add_sub(&self, inner: &mut Inner, filter: &str, qos: u8, target: Target) {
        if !topic::valid_filter(filter) {
            crate::warn!("ugyldigt MQTT-topicfilter: {filter}");
            return;
        }
        let connected = inner.connected;
        let sub = inner.subs.entry(filter.to_string()).or_insert_with(|| {
            if connected {
                self.transport.subscribe(filter, qos);
            }
            Sub { qos, targets: vec![] }
        });
        if qos > sub.qos {
            sub.qos = qos;
            if connected {
                self.transport.subscribe(filter, qos);
            }
        }
        if !sub.targets.contains(&target) {
            sub.targets.push(target);
        }
    }

    fn remove_targets<F: Fn(&Target) -> bool>(&self, inner: &mut Inner, f: F) {
        let connected = inner.connected;
        inner.subs.retain(|filter, sub| {
            sub.targets.retain(|t| !f(t));
            if sub.targets.is_empty() {
                if connected {
                    self.transport.unsubscribe(filter);
                }
                false
            } else {
                true
            }
        });
    }

    /// Subscribe on behalf of a user/automation; matches fire `mqtt_message`.
    pub fn subscribe_user(&self, filter: &str, qos: u8) -> Result<(), ServiceError> {
        if !topic::valid_filter(filter) {
            return Err(ServiceError::InvalidArgument(format!("ugyldigt topic: {filter}")));
        }
        let mut inner = self.inner.lock();
        self.add_sub(&mut inner, filter, qos, Target::User);
        Ok(())
    }

    pub fn unsubscribe_user(&self, filter: &str) {
        let mut inner = self.inner.lock();
        let connected = inner.connected;
        if let Some(sub) = inner.subs.get_mut(filter) {
            sub.targets.retain(|t| *t != Target::User);
            if sub.targets.is_empty() {
                inner.subs.remove(filter);
                if connected {
                    self.transport.unsubscribe(filter);
                }
            }
        }
    }

    /// Topics subscribed on behalf of users and automations.
    pub fn user_subscriptions(&self) -> Vec<String> {
        let inner = self.inner.lock();
        let mut v: Vec<String> = inner
            .subs
            .iter()
            .filter(|(_, s)| s.targets.contains(&Target::User))
            .map(|(f, _)| f.clone())
            .collect();
        v.sort();
        v
    }

    pub fn publish(&self, topic_name: &str, payload: String, qos: u8, retain: bool) -> Result<(), ServiceError> {
        if !topic::valid_publish_topic(topic_name) {
            return Err(ServiceError::InvalidArgument(format!("ugyldigt topic: {topic_name}")));
        }
        if qos > 2 {
            return Err(ServiceError::InvalidArgument("qos skal være 0, 1 eller 2".into()));
        }
        self.transport.publish(Publish {
            topic: topic_name.to_string(),
            payload,
            qos,
            retain,
        });
        Ok(())
    }

    // ------------------------------------------------------------------
    // incoming

    pub fn handle_message(&self, topic_name: &str, payload: &[u8], qos: u8, retain: bool) {
        let text = String::from_utf8_lossy(payload);
        let mut inner = self.inner.lock();
        let mut targets: Vec<Target> = vec![];
        for (filter, sub) in &inner.subs {
            if topic::matches(filter, topic_name) {
                for t in &sub.targets {
                    if !targets.contains(t) {
                        targets.push(t.clone());
                    }
                }
            }
        }
        if targets.is_empty() {
            return;
        }
        let mut changed: Vec<String> = vec![];
        let mut user_fired = false;
        for t in targets {
            match t {
                Target::Discovery => {
                    if let Some(msg) = discovery::parse(&self.settings.prefix, topic_name, payload) {
                        self.apply_discovery(&mut inner, msg);
                    }
                }
                Target::Entity { id, role } => {
                    let recent = inner.recent.entry(id.clone()).or_default();
                    recent.push_back(json!({
                        "topic": topic_name, "payload": text, "qos": qos, "retain": retain,
                        "time": chrono::Utc::now().to_rfc3339(),
                    }));
                    while recent.len() > DEBUG_MESSAGES {
                        recent.pop_front();
                    }
                    if let Some(e) = inner.entities.get_mut(&id) {
                        if e.handle(&role, &text) && !changed.contains(&id) {
                            changed.push(id);
                        }
                    }
                }
                Target::Trigger { id } => {
                    if let Some(mut data) = inner.triggers.get(&id).and_then(|t| t.fire(&text)) {
                        // the trigger knows its device by key; automations by TheHome's id
                        let key = data["device_id"].as_str().map(str::to_string);
                        data["device_id"] = json!(key.and_then(|k| self.hub.device_id(&k)));
                        self.hub.fire_event("mqtt_device_trigger", data);
                    }
                }
                Target::User => {
                    if !user_fired {
                        user_fired = true;
                        self.hub.fire_event(
                            "mqtt_message",
                            json!({ "topic": topic_name, "payload": text, "qos": qos, "retain": retain }),
                        );
                    }
                }
                Target::BridgeInfo => self.on_bridge_info(&mut inner, topic_name, payload),
                Target::BridgeResponse => {
                    let tx = serde_json::from_slice::<Value>(payload).ok().and_then(|v| {
                        let t = v.get("transaction").and_then(payload_str)?;
                        Some((inner.waiters.remove(&t)?, v))
                    });
                    if let Some((tx, v)) = tx {
                        let _ = tx.send(v);
                    }
                }
            }
        }
        for id in changed {
            self.push(&inner, &id);
        }
    }

    fn apply_discovery(&self, inner: &mut Inner, msg: discovery::Message) {
        let keep: Vec<String> = msg
            .items
            .iter()
            .filter(|i| i.config.is_some())
            .map(|i| i.discovery_id.clone())
            .collect();
        let previous = inner.topics.remove(&msg.topic).unwrap_or_default();
        for id in previous.iter().filter(|id| !keep.contains(id)) {
            self.remove(inner, id);
        }
        for item in msg.items {
            match item.config {
                None => self.remove(inner, &item.discovery_id),
                Some(cfg) if item.component == "device_automation" => {
                    self.add_trigger(inner, &item.discovery_id, &msg.topic, cfg);
                }
                Some(cfg) => self.add_entity(inner, &item.component, &item.discovery_id, &msg.topic, cfg),
            }
        }
        if !keep.is_empty() {
            inner.topics.insert(msg.topic, keep);
        }
    }

    /// Register the config's device. Returns its key and name.
    fn register_device(&self, inner: &mut Inner, cfg: &Cfg) -> Option<(String, String)> {
        let dev = cfg.get("device")?.as_object()?;
        let ids: Vec<String> = match dev.get("identifiers") {
            Some(Value::Array(a)) => a.iter().filter_map(payload_str).collect(),
            Some(v) => payload_str(v).into_iter().collect(),
            None => vec![],
        };
        let connections: Vec<(String, String)> = dev
            .get("connections")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|c| {
                        let c = c.as_array()?;
                        Some((payload_str(c.first()?)?.to_lowercase(), payload_str(c.get(1)?)?.to_lowercase()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        if ids.is_empty() && connections.is_empty() {
            return None;
        }
        let s = |k: &str| dev.get(k).and_then(payload_str).filter(|s| !s.is_empty());
        let name = s("name")
            .or_else(|| ids.first().cloned())
            .or_else(|| connections.first().map(|c| c.1.clone()))
            .unwrap_or_default();
        let spec = DeviceSpec {
            identifiers: ids.into_iter().map(|i| (crate::ID_DOMAIN.to_string(), i)).collect(),
            connections,
            manufacturer: s("manufacturer"),
            model: s("model"),
            model_id: s("model_id"),
            sw_version: s("sw_version"),
            hw_version: s("hw_version"),
            serial_number: s("serial_number"),
            name: name.clone(),
            via_device: s("via_device").map(|v| (crate::ID_DOMAIN.to_string(), v)),
            suggested_area: s("suggested_area"),
            entry_type: None,
            configuration_url: s("configuration_url"),
        };
        let key = hub::device_key(&spec)?;
        inner.devices.insert(key.clone(), spec.identifiers.clone());
        let key = self.hub.register_device(spec);
        Some((key, name))
    }

    fn add_entity(&self, inner: &mut Inner, component: &str, id: &str, discovery_topic: &str, cfg: Map<String, Value>) {
        if let Some(old) = inner.entities.get(id) {
            if old.cfg.0 == cfg && old.component == component {
                return;
            }
        }
        let mut entity = Entity::new(component, id, discovery_topic, cfg);
        if let Some(old) = inner.entities.remove(id) {
            // Keep the last known state across config updates; the broker
            // does not resend retained messages for topics still subscribed.
            if old.component == entity.component {
                entity.state = old.state;
                entity.attrs = old.attrs;
                entity.json_attrs = old.json_attrs;
            }
            self.remove_targets(inner, |t| matches!(t, Target::Entity { id: i, .. } if i == id));
        }
        let device = self.register_device(inner, &entity.cfg);
        entity.device_id = device.as_ref().map(|d| d.0.clone());
        let spec = entity.spec(entity.device_id.clone(), device.as_ref().map(|d| d.1.as_str()));
        entity.entity_id = self.hub.add_entity(spec);
        crate::debug!("MQTT-entity fundet: {id} -> {:?}", entity.entity_id);
        let subs = entity.subscriptions();
        let qos = entity.cfg.qos();
        inner.entities.insert(id.to_string(), entity);
        for (t, role) in subs {
            self.add_sub(inner, &t, qos, Target::Entity { id: id.to_string(), role });
        }
        self.push(inner, id);
    }

    fn add_trigger(&self, inner: &mut Inner, id: &str, discovery_topic: &str, cfg: Map<String, Value>) {
        let cfg = Cfg(cfg);
        if inner.triggers.get(id).is_some_and(|t| t.cfg == cfg) {
            return;
        }
        self.remove_targets(inner, |t| matches!(t, Target::Trigger { id: i } if i == id));
        let device_id = self.register_device(inner, &cfg).map(|d| d.0);
        let trigger = Trigger {
            discovery_id: id.to_string(),
            discovery_topic: discovery_topic.to_string(),
            cfg,
            device_id,
        };
        let topic = trigger.topic().map(str::to_string);
        let qos = trigger.cfg.qos();
        inner.triggers.insert(id.to_string(), trigger);
        if let Some(t) = topic {
            self.add_sub(inner, &t, qos, Target::Trigger { id: id.to_string() });
        }
    }

    fn remove(&self, inner: &mut Inner, id: &str) {
        let mut device_id = None;
        if let Some(e) = inner.entities.remove(id) {
            self.remove_targets(inner, |t| matches!(t, Target::Entity { id: i, .. } if i == id));
            if let Some(eid) = &e.entity_id {
                self.hub.remove_entity(eid);
            }
            device_id = e.device_id;
            crate::debug!("MQTT-entity fjernet: {id}");
        }
        if let Some(t) = inner.triggers.remove(id) {
            self.remove_targets(inner, |x| matches!(x, Target::Trigger { id: i } if i == id));
            device_id = t.device_id;
        }
        inner.recent.remove(id);
        // Drop the device when nothing references it anymore.
        if let Some(d) = device_id {
            let used = inner.entities.values().any(|e| e.device_id.as_ref() == Some(&d))
                || inner.triggers.values().any(|t| t.device_id.as_ref() == Some(&d));
            if !used {
                inner.devices.remove(&d);
                self.hub.remove_device(&d);
            }
        }
    }

    /// Write one entity's state to the hub.
    fn push(&self, inner: &Inner, id: &str) {
        let Some(e) = inner.entities.get(id) else { return };
        let Some(eid) = &e.entity_id else { return };
        if !inner.connected || !e.is_available() {
            self.hub.set_state(eid, STATE_UNAVAILABLE.into(), Map::new());
            return;
        }
        let (state, attrs) = e.render();
        self.hub.set_state(eid, state, attrs);
    }

    // ------------------------------------------------------------------
    // commands

    /// A service call on the entity with this hub handle.
    pub fn command_entity(&self, handle: &str, service: &str, data: &Map<String, Value>) -> Result<(), ServiceError> {
        let id = {
            let inner = self.inner.lock();
            inner
                .entities
                .iter()
                .find(|(_, e)| e.entity_id.as_deref() == Some(handle))
                .map(|(id, _)| id.clone())
                .ok_or_else(|| ServiceError::NotFound(handle.to_string()))?
        };
        self.command(&id, service, data)
    }

    pub fn command(&self, id: &str, service: &str, data: &Map<String, Value>) -> Result<(), ServiceError> {
        let mut inner = self.inner.lock();
        if !inner.connected {
            return Err(ServiceError::Unavailable("MQTT er ikke forbundet".into()));
        }
        let e = inner
            .entities
            .get_mut(id)
            .ok_or_else(|| ServiceError::NotFound(id.to_string()))?;
        let pubs = e.command(service, data)?;
        for p in pubs {
            self.transport.publish(p);
        }
        self.push(&inner, id);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Zigbee2MQTT bridges

    fn on_bridge_info(&self, inner: &mut Inner, topic_name: &str, payload: &[u8]) {
        let Some(base) = z2m::base_of(topic_name, "info") else { return };
        let issue_id = format!("zigbee2mqtt_{base}");
        let Some(bridge) = z2m::parse_info(topic_name, payload) else {
            if payload.is_empty() && inner.bridges.remove(base).is_some() {
                self.hub.delete_issue(&issue_id);
            }
            return;
        };
        if inner.bridges.get(base) == Some(&bridge) {
            return;
        }
        if !inner.bridges.contains_key(base) {
            crate::info!("fandt Zigbee2MQTT-bro på {base} (version {})", bridge.version.as_deref().unwrap_or("?"));
        }
        match z2m::problem(&bridge, self.settings.discovery, &self.settings.prefix) {
            Some((title, description)) => {
                crate::warn!("{base}: {title}");
                self.hub.create_issue(Issue {
                    issue_id,
                    severity: "warning".into(),
                    title,
                    description,
                });
            }
            None => self.hub.delete_issue(&issue_id),
        }
        inner.bridges.insert(base.to_string(), bridge);
    }

    /// Known Zigbee2MQTT bridges.
    pub fn bridges(&self) -> Vec<Bridge> {
        self.inner.lock().bridges.values().cloned().collect()
    }

    /// The bridge to talk to: the given base topic, the only known bridge,
    /// or the Zigbee2MQTT default.
    pub fn bridge(&self, base: Option<&str>) -> Result<Bridge, ServiceError> {
        let inner = self.inner.lock();
        if let Some(b) = base {
            return Ok(inner.bridges.get(b).cloned().unwrap_or_else(|| Bridge {
                base_topic: b.to_string(),
                ..Default::default()
            }));
        }
        match inner.bridges.len() {
            0 => Ok(Bridge {
                base_topic: z2m::DEFAULT_BASE.into(),
                ..Default::default()
            }),
            1 => Ok(inner.bridges.values().next().cloned().unwrap_or_default()),
            _ => Err(ServiceError::InvalidArgument("flere Zigbee2MQTT-broer fundet; angiv base_topic".into())),
        }
    }

    /// Base topic and IEEE address of a Zigbee2MQTT device, by TheHome's
    /// device id.
    pub fn zigbee2mqtt_device(&self, device_id: &str) -> Option<(String, String)> {
        let key = self.hub.device_key(device_id)?;
        let inner = self.inner.lock();
        let ieee = inner
            .devices
            .get(&key)?
            .iter()
            .filter(|(d, _)| d == crate::ID_DOMAIN)
            .find_map(|(_, i)| z2m::ieee_of_identifier(i))?
            .to_string();
        let topics: Vec<String> = inner
            .entities
            .values()
            .filter(|e| e.device_id.as_deref() == Some(key.as_str()))
            .flat_map(|e| ["state_topic", "command_topic"].map(|k| e.cfg.str(k).map(str::to_string)))
            .chain(
                inner
                    .triggers
                    .values()
                    .filter(|t| t.device_id.as_deref() == Some(key.as_str()))
                    .map(|t| t.topic().map(str::to_string)),
            )
            .flatten()
            .collect();
        let base = inner
            .bridges
            .keys()
            .find(|b| topics.iter().any(|t| t.starts_with(&format!("{b}/"))))
            .cloned()
            .or_else(|| topics.first().and_then(|t| t.split('/').next()).map(str::to_string))?;
        Some((base, ieee))
    }

    /// Send `<base>/bridge/request/<name>` and wait for the bridge's reply.
    pub async fn bridge_request(&self, base: &str, name: &str, payload: Value) -> Result<Value, ServiceError> {
        let transaction = format!("thehome-{:08x}", rand::random::<u32>());
        let mut body = match payload {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        body.insert("transaction".into(), json!(transaction));
        let (tx, rx) = oneshot::channel();
        {
            let mut inner = self.inner.lock();
            if !inner.connected {
                return Err(ServiceError::Unavailable("MQTT er ikke forbundet".into()));
            }
            inner.waiters.insert(transaction.clone(), tx);
        }
        self.publish(&format!("{base}/bridge/request/{name}"), Value::Object(body).to_string(), 1, false)?;
        let reply = tokio::time::timeout(REQUEST_TIMEOUT, rx).await;
        self.inner.lock().waiters.remove(&transaction);
        match reply {
            Ok(Ok(v)) => z2m::response_result(v).map_err(ServiceError::Failed),
            _ => Err(ServiceError::Unavailable(format!("intet svar fra Zigbee2MQTT på «{base}»"))),
        }
    }

    /// Mark everything unavailable (entry unload).
    pub fn shutdown(&self) {
        self.on_disconnected();
    }

    pub fn debug_info(&self) -> Value {
        let inner = self.inner.lock();
        let mut entities: Vec<Value> = inner
            .entities
            .values()
            .map(|e| {
                let subs: Vec<Value> = e
                    .subscriptions()
                    .into_iter()
                    .map(|(t, r)| json!({ "topic": t, "role": r }))
                    .collect();
                json!({
                    "discovery_id": e.discovery_id,
                    "component": e.component,
                    "unique_id": e.entity_id,
                    "device": e.device_id,
                    "discovery_topic": e.discovery_topic,
                    "config": redact(&e.cfg.0),
                    "subscriptions": subs,
                    "available": e.is_available(),
                    "messages": inner.recent.get(&e.discovery_id).cloned().unwrap_or_default(),
                })
            })
            .collect();
        entities.sort_by(|a, b| a["discovery_id"].as_str().cmp(&b["discovery_id"].as_str()));
        let triggers: Vec<Value> = inner
            .triggers
            .values()
            .map(|t| {
                json!({
                    "discovery_id": t.discovery_id,
                    "device": t.device_id,
                    "discovery_topic": t.discovery_topic,
                    "config": t.cfg.0,
                })
            })
            .collect();
        let mut subs: Vec<&String> = inner.subs.keys().collect();
        subs.sort();
        json!({
            "connected": inner.connected,
            "zigbee2mqtt": inner.bridges.values().collect::<Vec<_>>(),
            "subscriptions": subs,
            "entities": entities,
            "triggers": triggers,
        })
    }
}

fn redact(cfg: &Map<String, Value>) -> Value {
    let mut c = cfg.clone();
    for k in ["password", "code"] {
        if c.contains_key(k) {
            c.insert(k.into(), json!("**REDACTED**"));
        }
    }
    Value::Object(c)
}
