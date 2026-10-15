//! MQTT as an external TheHome integration: a client (or a built-in broker)
//! plus Home Assistant MQTT discovery, so Zigbee2MQTT, Z-Wave JS UI,
//! Tasmota, ESPHome (MQTT), Frigate and OpenMQTTGateway devices show up as
//! entities. Ported from TheHome's built-in `mqtt` integration.

pub mod abbrev;
pub mod broker;
pub mod client;
pub mod color;
pub mod discovery;
pub mod entity;
pub mod host;
pub mod hub;
pub mod manager;
pub mod template;
pub mod topic;
pub mod types;
pub mod zigbee2mqtt;

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use thehome_plugin::{async_trait, json, Entry, Error, Flow, FlowReply, Host, Integration, Map, Value};
use tokio_util::sync::CancellationToken;

pub use manager::{Mqtt, Settings, Transport};

pub const DOMAIN: &str = "mqtt";
/// The domain in device identifiers.
pub const ID_DOMAIN: &str = DOMAIN;

/// Logging to TheHome's log through stderr.
#[doc(hidden)]
pub fn emit(level: &str, message: String) {
    thehome_plugin::log(level, message);
}

#[macro_export]
macro_rules! debug {
    ($($t:tt)*) => { $crate::emit("DEBUG", format!($($t)*)) };
}
#[macro_export]
macro_rules! info {
    ($($t:tt)*) => { $crate::emit("INFO", format!($($t)*)) };
}
#[macro_export]
macro_rules! warn {
    ($($t:tt)*) => { $crate::emit("WARN", format!($($t)*)) };
}
#[macro_export]
macro_rules! error {
    ($($t:tt)*) => { $crate::emit("ERROR", format!($($t)*)) };
}

struct Running {
    mqtt: Arc<Mqtt>,
    hub: Arc<host::HostHub>,
    client: rumqttc::AsyncClient,
    cancel: CancellationToken,
}

#[derive(Default)]
pub struct MqttIntegration {
    running: Mutex<HashMap<String, Running>>,
}

impl MqttIntegration {
    fn get(&self, entry_id: &str) -> Result<(Arc<Mqtt>, Arc<host::HostHub>), Error> {
        self.running
            .lock()
            .get(entry_id)
            .map(|r| (r.mqtt.clone(), r.hub.clone()))
            .ok_or_else(|| Error::unavailable("MQTT er ikke sat op"))
    }
}

// ---------------------------------------------------------------------------
// forms

fn user_schema(defaults: &Value) -> Value {
    let d = |k: &str, v: Value| defaults.get(k).cloned().filter(|v| !v.is_null()).unwrap_or(v);
    let tls_default = match defaults.get("tls") {
        Some(Value::Bool(true)) => json!("on"),
        Some(Value::Bool(false)) => json!("off"),
        Some(Value::String(s)) => json!(s),
        _ => json!("auto"),
    };
    let mut port = json!({
        "type": "integer", "title": "Port", "minimum": 1, "maximum": 65535,
        "description": "Tom = 1883, eller 8883 med TLS"
    });
    if let Some(p) = defaults.get("port").filter(|p| p.is_u64()) {
        port["default"] = p.clone();
    }
    json!({
        "type": "object",
        "properties": {
            "builtin": {
                "type": "boolean", "title": "Start indbygget broker",
                "description": "Kør en MQTT-broker i integrationen på port 1883. Feltet Broker ignoreres så.",
                "default": d("builtin", json!(false))
            },
            "broker": { "type": "string", "title": "Broker", "description": "Værtsnavn eller IP-adresse, f.eks. 192.168.1.10 eller xxxx.s2.eu.hivemq.cloud", "default": d("broker", json!("")) },
            "port": port,
            "username": { "type": "string", "title": "Brugernavn", "default": d("username", json!("")) },
            "password": { "type": "string", "format": "password", "title": "Adgangskode" },
            "tls": {
                "type": "string", "title": "TLS",
                "description": "Automatisk slår TLS til på port 8883 og for brokere uden for dit netværk, f.eks. HiveMQ Cloud.",
                "default": tls_default,
                "oneOf": [
                    { "const": "auto", "title": "Automatisk" },
                    { "const": "on", "title": "Til" },
                    { "const": "off", "title": "Fra" }
                ]
            },
            "certificate": {
                "type": "string", "format": "textarea", "title": "CA-certifikat (PEM)",
                "description": "Kun ved TLS med eget certifikat. Tom = de almindelige udstedere, som cloud-brokere bruger.",
                "x-advanced": true
            },
            "tls_insecure": {
                "type": "boolean", "title": "Spring certifikattjek over",
                "description": "Kun til en lokal broker med selvsigneret certifikat. Forbindelsen er stadig krypteret, men ikke beskyttet mod en mellemmand.",
                "default": d("tls_insecure", json!(false)), "x-advanced": true
            },
            "client_cert": { "type": "string", "format": "textarea", "title": "Klientcertifikat (PEM)", "description": "Kun hvis brokeren kræver det", "x-advanced": true },
            "client_key": { "type": "string", "format": "textarea", "title": "Klientnøgle (PEM)", "x-advanced": true },
            "transport": {
                "type": "string", "title": "Forbindelse", "default": d("transport", json!("tcp")), "x-advanced": true,
                "oneOf": [
                    { "const": "tcp", "title": "MQTT (TCP)" },
                    { "const": "websockets", "title": "MQTT over WebSocket" }
                ]
            },
            "ws_path": { "type": "string", "title": "WebSocket-sti", "default": d("ws_path", json!("/mqtt")), "x-advanced": true },
            "keepalive": { "type": "integer", "title": "Keep-alive (sekunder)", "minimum": 5, "maximum": 3600, "default": d("keepalive", json!(30)), "x-advanced": true },
            "client_id": { "type": "string", "title": "Klient-id", "description": "Tom = genereres automatisk", "default": d("client_id", json!("")), "x-advanced": true }
        }
    })
}

fn options_schema(cur: &Value) -> Value {
    json!({
        "type": "object",
        "properties": {
            "discovery": {
                "type": "boolean", "title": "Aktivér MQTT discovery",
                "default": cur.get("discovery").cloned().unwrap_or(json!(true))
            },
            "discovery_prefix": {
                "type": "string", "title": "Discovery-præfiks",
                "default": cur.get("discovery_prefix").cloned().unwrap_or(json!("homeassistant"))
            },
            "birth_message": {
                "type": "boolean", "title": "Send online/offline-status",
                "description": "Publicér <præfiks>/status, så enheder sender deres konfiguration igen.",
                "default": cur.get("birth_message").cloned().unwrap_or(json!(true))
            },
            "subscriptions": {
                "type": "array", "items": { "type": "string" }, "title": "Abonnér på topics",
                "description": "Beskeder på disse topics sendes som hændelsen mqtt_message, som automationer med en MQTT-trigger lytter efter. F.eks. zigbee2mqtt/+/action.",
                "default": cur.get("subscriptions").cloned().unwrap_or(json!([]))
            }
        }
    })
}

fn str_field(v: &Value, k: &str) -> Option<String> {
    v.get(k)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn int_field(v: &Value, k: &str) -> Option<u64> {
    v.get(k).and_then(|p| p.as_u64().or_else(|| p.as_str()?.trim().parse().ok()))
}

// ---------------------------------------------------------------------------
// services

fn payload_value(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

fn publish_args(data: &Map<String, Value>) -> Result<(String, String, u8, bool), String> {
    let topic = data
        .get("topic")
        .and_then(Value::as_str)
        .ok_or("topic mangler")?
        .to_string();
    let mut payload = payload_value(data.get("payload"));
    if let Some(t) = data.get("payload_template").and_then(Value::as_str) {
        payload = template::render(t, &Map::new())?;
    }
    if data.get("evaluate_payload").and_then(Value::as_bool).unwrap_or(false) {
        payload = template::render(&payload, &Map::new())?;
    }
    let qos = data.get("qos").and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok())).unwrap_or(0);
    if qos > 2 {
        return Err("qos skal være 0, 1 eller 2".into());
    }
    let retain = data.get("retain").and_then(Value::as_bool).unwrap_or(false);
    Ok((topic, payload, qos as u8, retain))
}

fn field(name: &str, required: bool, selector: Value) -> Value {
    json!({ "name": name, "required": required, "selector": selector })
}

/// The domain's services with their descriptions.
fn services() -> Vec<(&'static str, Value)> {
    let text = || json!({ "text": {} });
    let z2m_base = field("Base-topic", false, text());
    vec![
        (
            "publish",
            json!({ "name": "Publicér", "description": "Send en besked til et MQTT-topic.", "fields": {
                "topic": field("Topic", true, text()),
                "payload": field("Payload", false, json!({ "text": { "multiline": true } })),
                "qos": field("QoS", false, json!({ "select": { "options": ["0", "1", "2"] } })),
                "retain": field("Retain", false, json!({ "boolean": {} })),
            }}),
        ),
        (
            "subscribe",
            json!({ "name": "Abonnér", "description": "Lyt på et topic. Beskeder sendes som hændelsen mqtt_message, indtil integrationen genindlæses. Brug integrationens indstillinger til faste abonnementer.", "fields": {
                "topic": field("Topic", true, text()),
                "qos": field("QoS", false, json!({ "select": { "options": ["0", "1", "2"] } })),
            }}),
        ),
        (
            "unsubscribe",
            json!({ "name": "Stop abonnement", "description": "Hold op med at lytte på et topic.", "fields": {
                "topic": field("Topic", true, text()),
            }}),
        ),
        (
            "debug_info",
            json!({ "name": "Fejlsøgning", "description": "Forbindelse, abonnementer, entities og de seneste beskeder.", "response": "only", "fields": {} }),
        ),
        (
            "zigbee2mqtt_bridges",
            json!({ "name": "Zigbee2MQTT-broer", "description": "De Zigbee2MQTT-broer, integrationen har set.", "response": "only", "fields": {} }),
        ),
        (
            "zigbee2mqtt_enable_discovery",
            json!({ "name": "Slå discovery til i Zigbee2MQTT", "description": "Slå Home Assistant-integrationen til i Zigbee2MQTT med TheHomes præfiks, og genstart Zigbee2MQTT, hvis den beder om det.", "response": "optional", "fields": {
                "base_topic": z2m_base.clone(),
            }}),
        ),
        (
            "zigbee2mqtt_permit_join",
            json!({ "name": "Tillad nye Zigbee-enheder", "description": "Åbn Zigbee2MQTT-netværket for nye enheder.", "response": "optional", "fields": {
                "time": field("Sekunder", false, json!({ "number": { "min": 0, "max": 254 } })),
                "device": field("Kun via enhed", false, text()),
                "base_topic": z2m_base.clone(),
            }}),
        ),
        (
            "zigbee2mqtt_rename_device",
            json!({ "name": "Omdøb Zigbee2MQTT-enhed", "description": "Omdøb en enhed i Zigbee2MQTT.", "response": "optional", "fields": {
                "device_id": field("Enhed", false, json!({ "device": {} })),
                "from": field("Nuværende navn eller IEEE", false, text()),
                "to": field("Nyt navn", true, text()),
                "base_topic": z2m_base.clone(),
            }}),
        ),
        (
            "zigbee2mqtt_remove_device",
            json!({ "name": "Fjern Zigbee2MQTT-enhed", "description": "Fjern en enhed fra Zigbee-netværket.", "response": "optional", "fields": {
                "device_id": field("Enhed", false, json!({ "device": {} })),
                "id": field("Navn eller IEEE", false, text()),
                "force": field("Tving", false, json!({ "boolean": {} })),
                "block": field("Bloker", false, json!({ "boolean": {} })),
                "base_topic": z2m_base,
            }}),
        ),
    ]
}

/// Base topic and device id for a Zigbee2MQTT device command: either
/// `device_id` (a TheHome device) or the Zigbee2MQTT id/name in `id_key`.
fn z2m_target(m: &Mqtt, data: &Value, id_key: &str) -> Result<(String, String), Error> {
    let base = data.get("base_topic").and_then(Value::as_str).filter(|s| !s.is_empty());
    if let Some(dev) = data.get("device_id").and_then(Value::as_str).filter(|s| !s.is_empty()) {
        let (found_base, ieee) = m
            .zigbee2mqtt_device(dev)
            .ok_or_else(|| Error::invalid("enheden kommer ikke fra Zigbee2MQTT"))?;
        return Ok((base.map(str::to_string).unwrap_or(found_base), ieee));
    }
    let id = str_field(data, id_key).ok_or_else(|| Error::invalid(format!("{id_key} eller device_id mangler")))?;
    let bridge = m.bridge(base)?;
    Ok((bridge.base_topic, id))
}

async fn domain_call(m: &Mqtt, service: &str, data: Map<String, Value>) -> Result<Value, Error> {
    let v = Value::Object(data.clone());
    let base = v.get("base_topic").and_then(Value::as_str).filter(|s| !s.is_empty());
    match service {
        "publish" => {
            let (topic, payload, qos, retain) = publish_args(&data).map_err(Error::invalid)?;
            m.publish(&topic, payload, qos, retain)?;
            Ok(Value::Null)
        }
        "subscribe" => {
            let topic = str_field(&v, "topic").ok_or_else(|| Error::invalid("topic mangler"))?;
            let qos = int_field(&v, "qos").unwrap_or(0).min(2) as u8;
            m.subscribe_user(&topic, qos)?;
            Ok(Value::Null)
        }
        "unsubscribe" => {
            let topic = str_field(&v, "topic").ok_or_else(|| Error::invalid("topic mangler"))?;
            m.unsubscribe_user(&topic);
            Ok(Value::Null)
        }
        "debug_info" => Ok(m.debug_info()),
        "zigbee2mqtt_bridges" => Ok(json!({ "bridges": m.bridges() })),
        "zigbee2mqtt_enable_discovery" => {
            if !m.settings().discovery {
                return Err(Error::invalid("MQTT discovery er slået fra i integrationens indstillinger"));
            }
            let bridge = m.bridge(base)?;
            let base = bridge.base_topic.clone();
            let opts = zigbee2mqtt::enable_discovery_options(&bridge, &m.settings().prefix);
            let res = m.bridge_request(&base, "options", opts).await?;
            let restart = res.get("restart_required").and_then(Value::as_bool).unwrap_or(false);
            if restart {
                m.bridge_request(&base, "restart", json!({})).await?;
            }
            Ok(json!({ "base_topic": base, "restarted": restart }))
        }
        "zigbee2mqtt_permit_join" => {
            let bridge = m.bridge(base)?;
            let time = int_field(&v, "time").unwrap_or(254);
            let body = zigbee2mqtt::permit_join_payload(time, v.get("device").and_then(Value::as_str).filter(|s| !s.is_empty()));
            Ok(m.bridge_request(&bridge.base_topic, "permit_join", body).await?)
        }
        "zigbee2mqtt_rename_device" => {
            let (base, from) = z2m_target(m, &v, "from")?;
            let to = str_field(&v, "to").ok_or_else(|| Error::invalid("to mangler"))?;
            let body = json!({ "from": from, "to": to, "homeassistant_rename": true });
            Ok(m.bridge_request(&base, "device/rename", body).await?)
        }
        "zigbee2mqtt_remove_device" => {
            let (base, id) = z2m_target(m, &v, "id")?;
            let flag = |k: &str| v.get(k).and_then(Value::as_bool).unwrap_or(false);
            let body = json!({ "id": id, "force": flag("force"), "block": flag("block") });
            Ok(m.bridge_request(&base, "device/remove", body).await?)
        }
        other => Err(Error::not_supported(other)),
    }
}

// ---------------------------------------------------------------------------
// the integration

#[async_trait]
impl Integration for MqttIntegration {
    async fn config_flow(&self, _host: &Host, flow: Flow, step: &str, input: Option<Value>) -> Result<FlowReply, Error> {
        match (step, input) {
            ("zeroconf", _) => {
                let info = flow.discovery.clone().unwrap_or(Value::Null);
                let host = info
                    .get("addresses")
                    .and_then(Value::as_array)
                    .and_then(|a| a.first())
                    .and_then(Value::as_str)
                    .or_else(|| info.get("host").and_then(Value::as_str))
                    .unwrap_or_default()
                    .trim_end_matches('.')
                    .to_string();
                let port = info.get("port").and_then(Value::as_u64).unwrap_or(1883);
                let defaults = json!({ "broker": host, "port": port });
                let mut data = Map::new();
                data.insert("defaults".into(), defaults.clone());
                Ok(FlowReply::form("user", user_schema(&defaults)).with_data(data))
            }
            ("user", None) => {
                let defaults = flow.data.get("defaults").cloned().unwrap_or(Value::Null);
                Ok(FlowReply::form("user", user_schema(&defaults)))
            }
            ("user", Some(v)) => {
                let builtin = v.get("builtin").and_then(Value::as_bool).unwrap_or(false);
                let broker = if builtin { Some("127.0.0.1".to_string()) } else { str_field(&v, "broker") };
                let schema = user_schema(&v);
                let Some(host) = broker.clone() else {
                    return Ok(FlowReply::form_with_errors("user", schema, json!({ "broker": "required" })));
                };
                let port = int_field(&v, "port").filter(|p| (1..=65535).contains(p)).map(|p| p as u16);
                let tls = match v.get("tls").and_then(Value::as_str).unwrap_or("auto") {
                    _ if builtin => false,
                    "on" => true,
                    "off" => false,
                    _ => client::auto_tls(&host, port),
                };
                let mut data = json!({
                    "builtin": builtin,
                    "broker": broker,
                    "username": str_field(&v, "username"),
                    "password": v.get("password").and_then(Value::as_str).filter(|p| !p.is_empty()),
                    "tls": tls,
                    "certificate": if tls { str_field(&v, "certificate") } else { None },
                    "tls_insecure": tls && v.get("tls_insecure").and_then(Value::as_bool).unwrap_or(false),
                    "client_cert": if tls { str_field(&v, "client_cert") } else { None },
                    "client_key": if tls { str_field(&v, "client_key") } else { None },
                    "transport": str_field(&v, "transport").filter(|t| t == "websockets"),
                    "ws_path": str_field(&v, "ws_path").filter(|p| p != "/mqtt"),
                    "keepalive": int_field(&v, "keepalive").filter(|k| *k != 30),
                    "client_id": str_field(&v, "client_id"),
                });
                // the port the connection settings pick when left empty
                let mut cfg = client::ConnConfig::from_entry(&data, &Value::Null);
                if let Some(p) = port {
                    cfg.port = p;
                }
                data["port"] = json!(cfg.port);
                if builtin {
                    if let Err(e) = broker::ensure_started(cfg.port, cfg.username.clone(), cfg.password.clone()).await {
                        crate::warn!("den indbyggede broker kunne ikke starte: {e}");
                        return Ok(FlowReply::form_with_errors("user", schema, json!({ "base": "Den indbyggede broker kunne ikke starte, er porten optaget" })));
                    }
                }
                if let Err(f) = client::test_connection(&cfg).await {
                    return Ok(FlowReply::form_with_errors("user", schema, json!({ "base": f.code() })));
                }
                if let Value::Object(m) = &mut data {
                    m.retain(|_, v| !v.is_null());
                }
                let title = if builtin { "Indbygget MQTT-broker".to_string() } else { cfg.address() };
                Ok(FlowReply::create(&title, data))
            }
            (s, _) => Err(Error::invalid(format!("ukendt trin {s}"))),
        }
    }

    async fn options_flow(&self, _host: &Host, flow: Flow, _step: &str, input: Option<Value>) -> Result<FlowReply, Error> {
        let cur = flow.entry.as_ref().map(|e| e.options.clone()).unwrap_or(Value::Null);
        match input {
            None => Ok(FlowReply::form("options", options_schema(&cur))),
            Some(v) => {
                let mut subs: Vec<String> = match v.get("subscriptions") {
                    Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).map(str::trim).filter(|s| !s.is_empty()).map(str::to_string).collect(),
                    Some(Value::String(s)) => s.split([',', '\n']).map(str::trim).filter(|s| !s.is_empty()).map(str::to_string).collect(),
                    _ => vec![],
                };
                subs.dedup();
                if let Some(bad) = subs.iter().find(|s| !topic::valid_filter(s)) {
                    let mut schema = options_schema(&v);
                    schema["properties"]["subscriptions"]["default"] = json!(subs);
                    return Ok(FlowReply::form_with_errors("options", schema, json!({ "subscriptions": format!("Ugyldigt topic «{bad}»") })));
                }
                let opts = json!({
                    "discovery": v.get("discovery").and_then(Value::as_bool).unwrap_or(true),
                    "discovery_prefix": str_field(&v, "discovery_prefix").unwrap_or_else(|| "homeassistant".into()),
                    "birth_message": v.get("birth_message").and_then(Value::as_bool).unwrap_or(true),
                    "subscriptions": subs,
                });
                Ok(FlowReply::create("", opts))
            }
        }
    }

    async fn setup_entry(&self, host: &Host, entry: Entry) -> Result<(), Error> {
        let data = entry.data.clone();
        let options = entry.options.clone();
        let cfg = client::ConnConfig::from_entry(&data, &options);
        if data.get("builtin").and_then(Value::as_bool).unwrap_or(false) {
            broker::ensure_started(cfg.port, cfg.username.clone(), cfg.password.clone())
                .await
                .map_err(Error::not_ready)?;
        }
        let settings = Settings {
            discovery: options.get("discovery").and_then(Value::as_bool).unwrap_or(true),
            prefix: cfg.prefix.clone(),
            birth: cfg.birth,
        };
        let (transport, mqtt_client, eventloop, forward) = client::connect(&cfg).map_err(Error::failed)?;
        let hub = host::HostHub::start(host.clone(), entry.entry_id.clone());
        let mqtt = Mqtt::new(hub.clone(), transport, settings);
        for topic in options.get("subscriptions").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str) {
            if let Err(e) = mqtt.subscribe_user(topic, 0) {
                crate::warn!("{e}");
            }
        }
        for (name, desc) in services() {
            host.register_service(&entry.entry_id, name, Some(desc)).await?;
        }
        let cancel = CancellationToken::new();
        tokio::spawn(forward);
        tokio::spawn(client::run(mqtt.clone(), eventloop, cancel.clone()));
        crate::info!("forbinder til {}", cfg.address());
        self.running.lock().insert(
            entry.entry_id.clone(),
            Running { mqtt, hub, client: mqtt_client, cancel },
        );
        Ok(())
    }

    async fn unload_entry(&self, _host: &Host, entry_id: &str) -> Result<(), Error> {
        let r = self.running.lock().remove(entry_id);
        if let Some(r) = r {
            if r.mqtt.settings().birth && r.mqtt.is_connected() {
                let _ = r.mqtt.publish(&format!("{}/status", r.mqtt.settings().prefix), "offline".into(), 1, false);
                // give the queued message a moment to go out
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            let _ = r.client.try_disconnect();
            r.cancel.cancel();
        }
        Ok(())
    }

    async fn entity_service(&self, _host: &Host, entry_id: &str, entity_id: &str, service: &str, data: Map<String, Value>) -> Result<(), Error> {
        let (mqtt, hub) = self.get(entry_id)?;
        let uid = hub.unique_id_of(entity_id).ok_or_else(|| Error::new("not_found", entity_id))?;
        Ok(mqtt.command_entity(&uid, service, &data)?)
    }

    async fn domain_service(&self, _host: &Host, entry_id: &str, service: &str, data: Map<String, Value>, _entity_ids: Vec<String>) -> Result<Value, Error> {
        let (mqtt, _) = self.get(entry_id)?;
        domain_call(&mqtt, service, data).await
    }

    async fn diagnostics(&self, _host: &Host, entry: Entry) -> Value {
        let mut data = entry.data.clone();
        if let Value::Object(m) = &mut data {
            for k in ["password", "certificate", "client_cert", "client_key"] {
                if m.contains_key(k) {
                    m.insert(k.into(), json!("**REDACTED**"));
                }
            }
        }
        json!({
            "config": data,
            "options": entry.options,
            "state": self.get(&entry.entry_id).ok().map(|(m, _)| m.debug_info()),
        })
    }
}
