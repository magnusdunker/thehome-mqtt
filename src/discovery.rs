//! Parsing of Home Assistant MQTT discovery messages.
//!
//! Topics are `<prefix>/<component>/[<node_id>/]<object_id>/config` or the
//! device-based form `<prefix>/device/<object_id>/config` whose payload has a
//! `cmps` map of components sharing `device`, `origin` and topics.

use serde_json::{Map, Value};

use crate::abbrev;

pub const COMPONENTS: &[&str] = &[
    "sensor",
    "binary_sensor",
    "switch",
    "light",
    "cover",
    "climate",
    "fan",
    "lock",
    "number",
    "select",
    "button",
    "text",
    "device_automation",
    "event",
    "siren",
    "valve",
    "update",
    "scene",
];

/// One component config. `config` is None when it is being removed.
#[derive(Clone, Debug, PartialEq)]
pub struct Item {
    pub component: String,
    pub discovery_id: String,
    pub config: Option<Map<String, Value>>,
}

/// Everything announced on one discovery topic.
#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    /// The discovery topic. Components announced earlier on the same topic
    /// that are missing now are removed.
    pub topic: String,
    pub items: Vec<Item>,
}

/// Parse a message on a discovery topic. Returns None if the topic is not a
/// discovery config topic or the payload is invalid.
pub fn parse(prefix: &str, topic: &str, payload: &[u8]) -> Option<Message> {
    let rest = topic.strip_prefix(prefix)?.strip_prefix('/')?;
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.last() != Some(&"config") || !(3..=4).contains(&parts.len()) {
        return None;
    }
    let component = parts[0];
    let ids = &parts[1..parts.len() - 1];
    let payload = std::str::from_utf8(payload).ok()?.trim();

    if component == "device" {
        if ids.len() != 1 {
            return None;
        }
        return parse_device(topic, ids[0], payload);
    }
    if !COMPONENTS.contains(&component) {
        crate::debug!("{topic}: unsupported MQTT discovery component");
        return None;
    }
    let discovery_id = format!("{component}/{}", ids.join("/"));
    let config = if payload.is_empty() {
        None
    } else {
        match serde_json::from_str::<Value>(payload) {
            Ok(Value::Object(m)) => Some(abbrev::expand(m)),
            _ => {
                crate::warn!("{topic}: invalid MQTT discovery payload");
                return None;
            }
        }
    };
    let items = match config {
        Some(c) => vec![Item { component: component.into(), discovery_id, config: Some(c) }],
        None => vec![],
    };
    Some(Message { topic: topic.into(), items })
}

const SHARED_SKIP: &[&str] = &["cmps", "components", "device", "dev", "origin", "o"];

fn parse_device(topic: &str, object_id: &str, payload: &str) -> Option<Message> {
    if payload.is_empty() {
        return Some(Message { topic: topic.into(), items: vec![] });
    }
    let Ok(Value::Object(raw)) = serde_json::from_str::<Value>(payload) else {
        crate::warn!("{topic}: invalid MQTT device discovery payload");
        return None;
    };
    let device = raw.get("dev").or_else(|| raw.get("device")).cloned();
    let origin = raw.get("o").or_else(|| raw.get("origin")).cloned();
    let cmps = raw.get("cmps").or_else(|| raw.get("components"))?.as_object()?.clone();
    let mut shared = Map::new();
    for (k, v) in &raw {
        if !SHARED_SKIP.contains(&k.as_str()) {
            shared.insert(k.clone(), v.clone());
        }
    }
    let mut items = vec![];
    for (cmp_id, cfg) in cmps {
        let Value::Object(cfg) = cfg else { continue };
        let platform = cfg
            .get("p")
            .or_else(|| cfg.get("platform"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if !COMPONENTS.contains(&platform.as_str()) {
            continue;
        }
        let discovery_id = format!("{platform}/{object_id}/{cmp_id}");
        // A component with only the platform key is a removal.
        if cfg.len() <= 1 {
            items.push(Item { component: platform, discovery_id, config: None });
            continue;
        }
        let mut merged = shared.clone();
        for (k, v) in cfg {
            if k != "p" && k != "platform" {
                merged.insert(k, v);
            }
        }
        if let Some(d) = &device {
            merged.insert("dev".into(), d.clone());
        }
        if let Some(o) = &origin {
            merged.insert("o".into(), o.clone());
        }
        items.push(Item { component: platform, discovery_id, config: Some(abbrev::expand(merged)) });
    }
    Some(Message { topic: topic.into(), items })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_component() {
        let m = parse(
            "homeassistant",
            "homeassistant/sensor/0x1234/temperature/config",
            br#"{"stat_t":"z2m/x","uniq_id":"u1"}"#,
        )
        .unwrap();
        assert_eq!(m.items[0].discovery_id, "sensor/0x1234/temperature");
        assert_eq!(m.items[0].config.as_ref().unwrap()["state_topic"], "z2m/x");
        let m = parse("homeassistant", "homeassistant/sensor/0x1234/temperature/config", b"").unwrap();
        assert!(m.items.is_empty());
        assert!(parse("homeassistant", "homeassistant/status", b"online").is_none());
        assert!(parse("homeassistant", "homeassistant/vacuum/x/config", b"{}").is_none());
    }

    #[test]
    fn device_based() {
        let payload = br#"{
            "dev": {"ids": "dev1", "name": "Plug"},
            "o": {"name": "test"},
            "stat_t": "plug/state",
            "cmps": {
                "power": {"p": "sensor", "val_tpl": "{{ value_json.power }}", "uniq_id": "p1"},
                "relay": {"p": "switch", "cmd_t": "plug/set", "uniq_id": "r1"},
                "old": {"p": "sensor"}
            }
        }"#;
        let m = parse("homeassistant", "homeassistant/device/plug1/config", payload).unwrap();
        assert_eq!(m.items.len(), 3);
        let power = m.items.iter().find(|i| i.discovery_id == "sensor/plug1/power").unwrap();
        let cfg = power.config.as_ref().unwrap();
        assert_eq!(cfg["state_topic"], "plug/state");
        assert_eq!(cfg["device"]["identifiers"], "dev1");
        let old = m.items.iter().find(|i| i.discovery_id == "sensor/plug1/old").unwrap();
        assert!(old.config.is_none());
    }
}
