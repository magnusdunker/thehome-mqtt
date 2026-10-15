//! Zigbee2MQTT bridge support.
//!
//! Zigbee2MQTT devices become entities through Home Assistant discovery,
//! which Zigbee2MQTT only publishes when its `homeassistant` option is on.
//! This module recognises a bridge from its retained `<base>/bridge/info`,
//! works out whether its devices will reach TheHome, and builds the
//! `<base>/bridge/request/*` messages for permit join, rename and remove.

use serde::Serialize;
use serde_json::{json, Map, Value};

/// Every bridge publishes its info retained here.
pub const INFO_FILTER: &str = "+/bridge/info";
/// Replies to `bridge/request/*`.
pub const RESPONSE_FILTER: &str = "+/bridge/response/#";
pub const DEFAULT_BASE: &str = "zigbee2mqtt";

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Bridge {
    pub base_topic: String,
    pub version: Option<String>,
    pub coordinator_type: Option<String>,
    pub coordinator_ieee: Option<String>,
    pub permit_join: bool,
    /// When permit join ends (ms since epoch), if Zigbee2MQTT says.
    pub permit_join_end: Option<i64>,
    /// `homeassistant` option; None when the bridge does not say.
    pub discovery: Option<bool>,
    pub discovery_prefix: Option<String>,
    /// Zigbee2MQTT 1.x, where the options and payloads look different.
    pub legacy: bool,
}

/// `<base>` of a `<base>/bridge/<rest>` topic.
pub fn base_of<'a>(topic: &'a str, rest: &str) -> Option<&'a str> {
    let base = topic.strip_suffix(rest)?.strip_suffix("/bridge/")?;
    (!base.is_empty()).then_some(base)
}

/// Parse a `<base>/bridge/info` payload. None when it does not look like
/// Zigbee2MQTT.
pub fn parse_info(topic: &str, payload: &[u8]) -> Option<Bridge> {
    let base = base_of(topic, "info")?;
    let Ok(Value::Object(info)) = serde_json::from_slice::<Value>(payload) else { return None };
    let version = info.get("version").and_then(Value::as_str)?.to_string();
    if !info.contains_key("coordinator") && !info.contains_key("config") {
        return None;
    }
    let legacy = version.split('.').next().and_then(|m| m.parse::<u32>().ok()).is_some_and(|m| m < 2);
    let coordinator = info.get("coordinator");
    let config = info.get("config");
    let ha = config.and_then(|c| c.get("homeassistant"));
    let advanced = config.and_then(|c| c.get("advanced"));
    let (discovery, prefix) = match ha {
        Some(Value::Bool(b)) => (
            Some(*b),
            advanced.and_then(|a| a.get("homeassistant_discovery_topic")).and_then(Value::as_str),
        ),
        Some(Value::Object(h)) => (
            Some(h.get("enabled").and_then(Value::as_bool).unwrap_or(true)),
            h.get("discovery_topic").and_then(Value::as_str),
        ),
        _ => (None, None),
    };
    let discovery = discovery.or_else(|| config.is_some().then_some(false));
    Some(Bridge {
        base_topic: base.to_string(),
        version: Some(version),
        coordinator_type: coordinator.and_then(|c| c.get("type")).and_then(Value::as_str).map(str::to_string),
        coordinator_ieee: coordinator.and_then(|c| c.get("ieee_address")).and_then(Value::as_str).map(str::to_string),
        permit_join: info.get("permit_join").and_then(Value::as_bool).unwrap_or(false),
        permit_join_end: info.get("permit_join_end").and_then(Value::as_i64),
        discovery,
        discovery_prefix: if discovery == Some(true) {
            Some(prefix.unwrap_or("homeassistant").to_string())
        } else {
            None
        },
        legacy,
    })
}

/// Why this bridge's devices will not show up, as (title, description).
pub fn problem(b: &Bridge, discovery_on: bool, prefix: &str) -> Option<(String, String)> {
    let base = &b.base_topic;
    if !discovery_on {
        return Some((
            "Zigbee2MQTT-enheder bliver ikke oprettet".into(),
            format!(
                "Der er fundet en Zigbee2MQTT-bro på «{base}», men MQTT discovery er slået fra i TheHome. \
                 Slå «Aktivér MQTT discovery» til under MQTT-integrationens indstillinger."
            ),
        ));
    }
    match (b.discovery, &b.discovery_prefix) {
        (Some(false), _) => Some((
            "Zigbee2MQTT sender ikke sine enheder til TheHome".into(),
            format!(
                "Zigbee2MQTT på «{base}» har Home Assistant-integrationen slået fra, så dens enheder ikke \
                 bliver oprettet her. Slå den til i Zigbee2MQTT under Indstillinger > Home Assistant-integration \
                 og genstart Zigbee2MQTT, eller kør tjenesten mqtt_ext.zigbee2mqtt_enable_discovery under \
                 Udviklerværktøjer > Tjenester, så gør TheHome det for dig."
            ),
        )),
        (Some(true), Some(p)) if p != prefix => Some((
            "Zigbee2MQTT bruger et andet discovery-præfiks".into(),
            format!(
                "Zigbee2MQTT på «{base}» sender sine enheder til «{p}», men TheHome lytter på «{prefix}». \
                 Ret discovery-præfikset i MQTT-integrationens indstillinger til «{p}», eller kør tjenesten \
                 mqtt_ext.zigbee2mqtt_enable_discovery under Udviklerværktøjer > Tjenester."
            ),
        )),
        _ => None,
    }
}

/// Options that make the bridge publish discovery to `prefix`.
pub fn enable_discovery_options(b: &Bridge, prefix: &str) -> Value {
    if b.legacy {
        json!({ "options": {
            "homeassistant": true,
            "advanced": {
                "homeassistant_discovery_topic": prefix,
                "homeassistant_status_topic": format!("{prefix}/status"),
            }
        }})
    } else {
        json!({ "options": { "homeassistant": {
            "enabled": true,
            "discovery_topic": prefix,
            "status_topic": format!("{prefix}/status"),
        }}})
    }
}

/// Permit join payload. `time` 0 closes the network. `device` is the
/// friendly name of a router to join through. 1.x reads `value`, 2.x
/// reads `time`, so both are sent.
pub fn permit_join_payload(time: u64, device: Option<&str>) -> Value {
    let mut m = Map::new();
    m.insert("value".into(), json!(time > 0));
    m.insert("time".into(), json!(time.min(254)));
    if let Some(d) = device {
        m.insert("device".into(), json!(d));
    }
    Value::Object(m)
}

/// The IEEE address in a Zigbee2MQTT device identifier.
pub fn ieee_of_identifier(id: &str) -> Option<&str> {
    let ieee = id.strip_prefix("zigbee2mqtt_")?;
    (ieee.starts_with("0x") && !ieee.starts_with("bridge_")).then_some(ieee)
}

/// Turn a bridge response into the command result.
pub fn response_result(v: Value) -> Result<Value, String> {
    if v.get("status").and_then(Value::as_str) == Some("error") {
        let e = v.get("error").and_then(Value::as_str).unwrap_or("ukendt fejl");
        return Err(format!("Zigbee2MQTT: {e}"));
    }
    Ok(v.get("data").cloned().unwrap_or(Value::Null))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_v2() {
        let p = br#"{"version":"2.1.1","commit":"abc","coordinator":{"type":"zStack3x0","ieee_address":"0x00124b0022ef1234","meta":{}},
            "permit_join":true,"permit_join_end":1760000000000,
            "config":{"homeassistant":{"enabled":false,"discovery_topic":"homeassistant","status_topic":"homeassistant/status"},"mqtt":{"base_topic":"zigbee2mqtt"}}}"#;
        let b = parse_info("zigbee2mqtt/bridge/info", p).unwrap();
        assert_eq!(b.base_topic, "zigbee2mqtt");
        assert_eq!(b.coordinator_type.as_deref(), Some("zStack3x0"));
        assert!(b.permit_join && !b.legacy);
        assert_eq!(b.discovery, Some(false));
        let (title, _) = problem(&b, true, "homeassistant").unwrap();
        assert!(title.contains("sender ikke"));
        assert_eq!(
            enable_discovery_options(&b, "homeassistant"),
            json!({"options":{"homeassistant":{"enabled":true,"discovery_topic":"homeassistant","status_topic":"homeassistant/status"}}})
        );

        let p = br#"{"version":"2.1.1","coordinator":{},"config":{"homeassistant":{"enabled":true,"discovery_topic":"ha"}}}"#;
        let b = parse_info("hus/bridge/info", p).unwrap();
        assert_eq!(b.base_topic, "hus");
        assert_eq!(b.discovery_prefix.as_deref(), Some("ha"));
        assert!(problem(&b, true, "homeassistant").unwrap().0.contains("præfiks"));
        assert!(problem(&b, true, "ha").is_none());
        assert!(problem(&b, false, "ha").is_some());
    }

    #[test]
    fn info_v1_and_noise() {
        let p = br#"{"version":"1.40.2","coordinator":{"type":"EZSP v8"},"permit_join":false,
            "config":{"homeassistant":true,"advanced":{"homeassistant_discovery_topic":"homeassistant"}}}"#;
        let b = parse_info("zigbee2mqtt/bridge/info", p).unwrap();
        assert!(b.legacy);
        assert_eq!(b.discovery, Some(true));
        assert!(problem(&b, true, "homeassistant").is_none());
        assert_eq!(enable_discovery_options(&b, "homeassistant")["options"]["homeassistant"], true);

        assert!(parse_info("other/bridge/info", br#"{"name":"not z2m"}"#).is_none());
        assert!(parse_info("other/bridge/info", b"online").is_none());
        assert!(parse_info("/bridge/info", br#"{"version":"2.0.0","config":{}}"#).is_none());
    }

    #[test]
    fn helpers() {
        assert_eq!(permit_join_payload(254, None), json!({"value": true, "time": 254}));
        assert_eq!(permit_join_payload(0, Some("Router")), json!({"value": false, "time": 0, "device": "Router"}));
        assert_eq!(ieee_of_identifier("zigbee2mqtt_0x0017880108a1b2c3"), Some("0x0017880108a1b2c3"));
        assert_eq!(ieee_of_identifier("zigbee2mqtt_bridge_0x00124b0022ef1234"), None);
        assert_eq!(base_of("zigbee2mqtt/bridge/response/device/rename", "response/device/rename"), Some("zigbee2mqtt"));
        assert_eq!(response_result(json!({"status":"ok","data":{"from":"a","to":"b"}})).unwrap(), json!({"from":"a","to":"b"}));
        assert!(response_result(json!({"status":"error","error":"Device 'x' does not exist"})).is_err());
    }
}
