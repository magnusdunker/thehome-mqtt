//! Mapping between MQTT discovery configs and TheHome entities.
//!
//! An [`Entity`] holds the expanded discovery config and the current state.
//! Incoming payloads are applied with [`Entity::handle`] and service calls
//! are turned into MQTT publishes with [`Entity::command`]. Nothing here
//! touches the network, which keeps it testable.

use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};
use crate::types::{EntitySpec, ServiceError};

use crate::color;
use crate::template;

#[derive(Clone, Debug, PartialEq)]
pub struct Publish {
    pub topic: String,
    pub payload: String,
    pub qos: u8,
    pub retain: bool,
}

/// Config payloads may be strings, numbers or booleans. `null` disables.
pub fn payload_str(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(if *b { "True" } else { "False" }.into()),
        other => Some(other.to_string()),
    }
}

/// Typed access to an expanded discovery config.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Cfg(pub Map<String, Value>);

impl Cfg {
    pub fn get(&self, k: &str) -> Option<&Value> {
        self.0.get(k)
    }
    pub fn has(&self, k: &str) -> bool {
        self.str(k).is_some()
    }
    pub fn str(&self, k: &str) -> Option<&str> {
        self.0.get(k).and_then(Value::as_str).filter(|s| !s.is_empty())
    }
    /// A payload option: absent gives the default, `null` gives None.
    pub fn payload(&self, k: &str, default: &str) -> Option<String> {
        match self.0.get(k) {
            None => Some(default.to_string()),
            Some(v) => payload_str(v),
        }
    }
    pub fn opt_f64(&self, k: &str) -> Option<f64> {
        self.0
            .get(k)
            .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
    }
    pub fn f64(&self, k: &str, d: f64) -> f64 {
        self.opt_f64(k).unwrap_or(d)
    }
    pub fn bool(&self, k: &str, d: bool) -> bool {
        match self.0.get(k) {
            Some(Value::Bool(b)) => *b,
            Some(Value::String(s)) => matches!(s.to_ascii_lowercase().as_str(), "true" | "yes" | "on" | "1"),
            Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0) != 0.0,
            _ => d,
        }
    }
    pub fn list(&self, k: &str) -> Vec<String> {
        match self.0.get(k) {
            Some(Value::Array(a)) => a.iter().filter_map(payload_str).collect(),
            Some(Value::String(s)) => vec![s.clone()],
            _ => vec![],
        }
    }
    pub fn qos(&self) -> u8 {
        self.f64("qos", 0.0).clamp(0.0, 2.0) as u8
    }
    pub fn retain(&self) -> bool {
        self.bool("retain", false)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AvailCfg {
    pub topic: String,
    pub on: Option<String>,
    pub off: Option<String>,
    pub template: Option<String>,
}

pub const LIGHT_EFFECT: u64 = 4;
pub const LIGHT_FLASH: u64 = 8;
pub const LIGHT_TRANSITION: u64 = 32;

// Keys that json_attributes must not override.
const RESERVED_ATTRS: &[&str] = &[
    "assumed_state",
    "available",
    "device_class",
    "entity_picture",
    "friendly_name",
    "icon",
    "supported_features",
    "unit_of_measurement",
    "state",
];

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn fmt_num(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

fn num_value(v: f64) -> Value {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        json!(v as i64)
    } else {
        json!(v)
    }
}

fn parse_f64(s: &str) -> Option<f64> {
    s.trim().parse::<f64>().ok().filter(|f| f.is_finite())
}

fn parse_list(s: &str) -> Option<Vec<f64>> {
    s.split(',').map(parse_f64).collect()
}

fn data_f64(data: &Map<String, Value>, k: &str) -> Option<f64> {
    data.get(k)
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(parse_f64)))
}

fn data_list(data: &Map<String, Value>, k: &str) -> Option<Vec<f64>> {
    data.get(k)?.as_array()?.iter().map(Value::as_f64).collect()
}

fn invalid(msg: impl Into<String>) -> ServiceError {
    ServiceError::InvalidArgument(msg.into())
}

/// Requested color in a `light.turn_on` call.
#[derive(Clone, Copy, Debug, PartialEq)]
enum ColorReq {
    Hs(f64, f64),
    Rgb(f64, f64, f64),
    Xy(f64, f64),
    Kelvin(f64),
}

fn color_req(data: &Map<String, Value>) -> Option<ColorReq> {
    if let Some(hs) = data_list(data, "hs_color").filter(|v| v.len() == 2) {
        return Some(ColorReq::Hs(hs[0], hs[1]));
    }
    if let Some(c) = data_list(data, "rgb_color").filter(|v| v.len() >= 3) {
        return Some(ColorReq::Rgb(c[0], c[1], c[2]));
    }
    if let Some(c) = data_list(data, "xy_color").filter(|v| v.len() == 2) {
        return Some(ColorReq::Xy(c[0], c[1]));
    }
    if let Some(k) = data_f64(data, "color_temp_kelvin").or_else(|| data_f64(data, "kelvin")) {
        return Some(ColorReq::Kelvin(k));
    }
    if let Some(m) = data_f64(data, "color_temp") {
        return Some(ColorReq::Kelvin(color::mired_to_kelvin(m)));
    }
    None
}

fn brightness_req(data: &Map<String, Value>, current: Option<f64>) -> Option<f64> {
    if let Some(b) = data_f64(data, "brightness") {
        return Some(b.clamp(0.0, 255.0));
    }
    if let Some(p) = data_f64(data, "brightness_pct") {
        return Some((p.clamp(0.0, 100.0) * 255.0 / 100.0).round());
    }
    if let Some(s) = data_f64(data, "brightness_step") {
        return Some((current.unwrap_or(0.0) + s).clamp(0.0, 255.0));
    }
    if let Some(s) = data_f64(data, "brightness_step_pct") {
        return Some((current.unwrap_or(0.0) + s * 255.0 / 100.0).clamp(0.0, 255.0).round());
    }
    None
}

#[derive(Debug)]
pub struct Entity {
    pub component: String,
    pub discovery_id: String,
    pub discovery_topic: String,
    pub cfg: Cfg,
    pub entity_id: Option<String>,
    pub device_id: Option<String>,
    /// None = unknown.
    pub state: Option<String>,
    pub attrs: Map<String, Value>,
    pub json_attrs: Map<String, Value>,
    avail: Vec<Option<bool>>,
    latest_avail: Option<bool>,
    last_update: Option<Instant>,
    expired: bool,
    off_at: Option<Instant>,
}

impl Entity {
    pub fn new(component: &str, discovery_id: &str, discovery_topic: &str, cfg: Map<String, Value>) -> Self {
        let cfg = Cfg(cfg);
        let n_avail = availability_of(&cfg).len();
        let mut e = Self {
            component: component.into(),
            discovery_id: discovery_id.into(),
            discovery_topic: discovery_topic.into(),
            cfg,
            entity_id: None,
            device_id: None,
            state: None,
            attrs: Map::new(),
            json_attrs: Map::new(),
            avail: vec![None; n_avail],
            latest_avail: None,
            last_update: None,
            expired: false,
            off_at: None,
        };
        if e.component == "climate" && !e.cfg.has("mode_state_topic") {
            e.state = Some("off".into());
        }
        e
    }

    pub fn unique_id(&self) -> String {
        match self.cfg.str("unique_id") {
            Some(u) => u.to_string(),
            None => format!("mqtt_{}", self.discovery_id.replace('/', "_")),
        }
    }

    pub fn light_schema(&self) -> &str {
        match self.cfg.str("schema") {
            Some("json") => "json",
            Some("template") => "template",
            _ => "basic",
        }
    }

    pub fn availability(&self) -> Vec<AvailCfg> {
        availability_of(&self.cfg)
    }

    /// Topics to subscribe to and the role of each.
    pub fn subscriptions(&self) -> Vec<(String, String)> {
        let mut out = vec![];
        for (i, a) in self.availability().into_iter().enumerate() {
            out.push((a.topic, format!("avty:{i}")));
        }
        let keys: &[&str] = match self.component.as_str() {
            "light" if self.light_schema() == "basic" => &[
                "state_topic",
                "brightness_state_topic",
                "color_temp_state_topic",
                "rgb_state_topic",
                "hs_state_topic",
                "xy_state_topic",
                "effect_state_topic",
                "color_mode_state_topic",
            ],
            "cover" => &["state_topic", "position_topic", "tilt_status_topic"],
            "climate" => &[
                "mode_state_topic",
                "current_temperature_topic",
                "temperature_state_topic",
                "temperature_low_state_topic",
                "temperature_high_state_topic",
                "action_topic",
                "fan_mode_state_topic",
                "swing_mode_state_topic",
                "preset_mode_state_topic",
                "current_humidity_topic",
                "target_humidity_state_topic",
            ],
            "fan" => &[
                "state_topic",
                "percentage_state_topic",
                "preset_mode_state_topic",
                "oscillation_state_topic",
                "direction_state_topic",
            ],
            "update" => &["state_topic", "latest_version_topic"],
            "button" | "scene" => &[],
            _ => &["state_topic"],
        };
        for k in keys.iter().chain(&["json_attributes_topic"]) {
            if let Some(t) = self.cfg.str(k) {
                out.push((t.to_string(), k.to_string()));
            }
        }
        out
    }

    /// The registry description. `device_name` is used to strip a
    /// redundant device-name prefix from the entity name.
    pub fn spec(&self, device_id: Option<String>, device_name: Option<&str>) -> EntitySpec {
        let mut name = match self.cfg.get("name") {
            Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
            _ => None,
        };
        if let (Some(n), Some(d)) = (&name, device_name) {
            if let Some(rest) = n.strip_prefix(d) {
                let rest = rest.trim();
                name = if rest.is_empty() { None } else { Some(rest.to_string()) };
            }
        }
        if device_id.is_none() && name.is_none() {
            name = Some(self.discovery_id.rsplit('/').next().unwrap_or("mqtt").replace('_', " "));
        }
        let suggested_object_id = self
            .cfg
            .str("default_entity_id")
            .map(|s| s.split_once('.').map(|(_, o)| o).unwrap_or(s).to_string())
            .or_else(|| self.cfg.str("object_id").map(str::to_string));
        let unit = match self.component.as_str() {
            "sensor" | "number" => self.cfg.str("unit_of_measurement").map(str::to_string),
            _ => None,
        };
        EntitySpec {
            domain: self.component.clone(),
            unique_id: self.unique_id(),
            name,
            device_id,
            icon: self.cfg.str("icon").map(str::to_string),
            device_class: self.cfg.str("device_class").map(str::to_string),
            unit_of_measurement: unit,
            state_class: self.cfg.str("state_class").map(str::to_string),
            entity_category: self.cfg.str("entity_category").map(str::to_string),
            suggested_object_id,
            disabled_by_default: !self.cfg.bool("enabled_by_default", true),
            capabilities: Value::Object(self.capabilities()),
        }
    }

    /// Static attributes (supported features, ranges, option lists).
    pub fn capabilities(&self) -> Map<String, Value> {
        let c = &self.cfg;
        let mut m = Map::new();
        match self.component.as_str() {
            "light" => {
                let modes = self.color_modes();
                let mut f = 0;
                if c.has("effect_command_topic") || c.bool("effect", false) || !c.list("effect_list").is_empty() {
                    f |= LIGHT_EFFECT;
                    m.insert("effect_list".into(), json!(c.list("effect_list")));
                }
                if self.light_schema() != "basic" {
                    f |= LIGHT_FLASH | LIGHT_TRANSITION;
                }
                if modes.iter().any(|m| m == "color_temp") {
                    let (lo, hi) = self.kelvin_range();
                    m.insert("min_color_temp_kelvin".into(), num_value(lo));
                    m.insert("max_color_temp_kelvin".into(), num_value(hi));
                }
                m.insert("supported_color_modes".into(), json!(modes));
                m.insert("supported_features".into(), json!(f));
            }
            "cover" => {
                let mut f = 0u64;
                if c.has("command_topic") {
                    if c.payload("payload_open", "OPEN").is_some() {
                        f |= 1;
                    }
                    if c.payload("payload_close", "CLOSE").is_some() {
                        f |= 2;
                    }
                    if c.payload("payload_stop", "STOP").is_some() {
                        f |= 8;
                    }
                }
                if c.has("set_position_topic") {
                    f |= 4;
                }
                if c.has("tilt_command_topic") {
                    f |= 16 | 32 | 128;
                }
                m.insert("supported_features".into(), json!(f));
            }
            "climate" => {
                let mut f = 0u64;
                if c.has("temperature_command_topic") {
                    f |= 1;
                }
                if c.has("temperature_low_command_topic") || c.has("temperature_high_command_topic") {
                    f |= 2;
                }
                if c.has("target_humidity_command_topic") {
                    f |= 4;
                }
                if c.has("fan_mode_command_topic") {
                    f |= 8;
                    m.insert("fan_modes".into(), json!(self.list_or("fan_modes", &["auto", "low", "medium", "high"])));
                }
                if c.has("preset_mode_command_topic") {
                    f |= 16;
                    let presets: Vec<String> = c.list("preset_modes").into_iter().filter(|p| p != "none").collect();
                    m.insert("preset_modes".into(), json!(presets));
                }
                if c.has("swing_mode_command_topic") {
                    f |= 32;
                    m.insert("swing_modes".into(), json!(self.list_or("swing_modes", &["on", "off"])));
                }
                let modes = self.hvac_modes();
                if c.has("power_command_topic") || modes.len() > 1 {
                    f |= 128 | 256;
                }
                let celsius = c.str("temperature_unit").map(|u| u.contains('C')).unwrap_or(true);
                let (dmin, dmax) = if celsius { (7.0, 35.0) } else { (45.0, 95.0) };
                m.insert("hvac_modes".into(), json!(modes));
                m.insert("min_temp".into(), num_value(c.f64("min_temp", dmin)));
                m.insert("max_temp".into(), num_value(c.f64("max_temp", dmax)));
                m.insert(
                    "target_temp_step".into(),
                    num_value(c.opt_f64("temp_step").or_else(|| c.opt_f64("precision")).unwrap_or(if celsius { 0.5 } else { 1.0 })),
                );
                if c.has("target_humidity_command_topic") {
                    m.insert("min_humidity".into(), num_value(c.f64("min_humidity", 30.0)));
                    m.insert("max_humidity".into(), num_value(c.f64("max_humidity", 99.0)));
                }
                m.insert("supported_features".into(), json!(f));
            }
            "fan" => {
                let mut f = 16u64 | 32;
                if c.has("percentage_command_topic") {
                    f |= 1;
                    let (lo, hi) = self.speed_range();
                    m.insert("percentage_step".into(), json!(100.0 / (hi - lo + 1.0)));
                }
                if c.has("oscillation_command_topic") {
                    f |= 2;
                }
                if c.has("direction_command_topic") {
                    f |= 4;
                }
                if c.has("preset_mode_command_topic") {
                    f |= 8;
                    m.insert("preset_modes".into(), json!(c.list("preset_modes")));
                }
                m.insert("supported_features".into(), json!(f));
            }
            "lock" => {
                let f = if c.get("payload_open").is_some_and(|v| !v.is_null()) { 1 } else { 0 };
                m.insert("supported_features".into(), json!(f));
            }
            "number" => {
                m.insert("min".into(), num_value(c.f64("min", 1.0)));
                m.insert("max".into(), num_value(c.f64("max", 100.0)));
                m.insert("step".into(), num_value(c.f64("step", 1.0)));
                m.insert("mode".into(), json!(c.str("mode").unwrap_or("auto")));
            }
            "select" => {
                m.insert("options".into(), json!(c.list("options")));
            }
            "sensor" => {
                if c.str("device_class") == Some("enum") {
                    m.insert("options".into(), json!(c.list("options")));
                }
            }
            "text" => {
                m.insert("min".into(), num_value(c.f64("min", 0.0)));
                m.insert("max".into(), num_value(c.f64("max", 255.0)));
                m.insert("mode".into(), json!(c.str("mode").unwrap_or("text")));
                if let Some(p) = c.str("pattern") {
                    m.insert("pattern".into(), json!(p));
                }
            }
            "siren" => {
                let mut f = 1u64 | 2;
                let tones = c.list("available_tones");
                if !tones.is_empty() {
                    f |= 4;
                    m.insert("available_tones".into(), json!(tones));
                }
                if c.bool("support_volume_set", true) {
                    f |= 8;
                }
                if c.bool("support_duration", true) {
                    f |= 16;
                }
                m.insert("supported_features".into(), json!(f));
            }
            "valve" => {
                let mut f = 1u64 | 2;
                if c.bool("reports_position", false) {
                    f |= 4;
                }
                if c.get("payload_stop").is_some_and(|v| !v.is_null()) {
                    f |= 8;
                }
                m.insert("supported_features".into(), json!(f));
            }
            "update" => {
                let mut f = 4u64;
                if c.has("command_topic") {
                    f |= 1;
                }
                m.insert("supported_features".into(), json!(f));
            }
            "event" => {
                m.insert("event_types".into(), json!(c.list("event_types")));
            }
            _ => {}
        }
        m
    }

    fn list_or(&self, k: &str, d: &[&str]) -> Vec<String> {
        let l = self.cfg.list(k);
        if l.is_empty() && self.cfg.get(k).is_none() {
            d.iter().map(|s| s.to_string()).collect()
        } else {
            l
        }
    }

    fn hvac_modes(&self) -> Vec<String> {
        self.list_or("modes", &["auto", "off", "cool", "heat", "dry", "fan_only"])
    }

    fn speed_range(&self) -> (f64, f64) {
        (self.cfg.f64("speed_range_min", 1.0), self.cfg.f64("speed_range_max", 100.0))
    }

    fn kelvin_range(&self) -> (f64, f64) {
        let c = &self.cfg;
        let lo = c
            .opt_f64("min_kelvin")
            .or_else(|| c.opt_f64("max_mireds").map(color::mired_to_kelvin))
            .unwrap_or(2000.0);
        let hi = c
            .opt_f64("max_kelvin")
            .or_else(|| c.opt_f64("min_mireds").map(color::mired_to_kelvin))
            .unwrap_or(6535.0);
        (lo, hi)
    }

    pub fn color_modes(&self) -> Vec<String> {
        let c = &self.cfg;
        let mut modes: Vec<String> = vec![];
        match self.light_schema() {
            "json" => {
                modes = c.list("supported_color_modes");
                if modes.is_empty() {
                    for (flag, mode) in [("hs", "hs"), ("rgb", "rgb"), ("xy", "xy"), ("color_temp", "color_temp")] {
                        if c.bool(flag, false) {
                            modes.push(mode.into());
                        }
                    }
                }
                if modes.is_empty() && c.bool("brightness", false) {
                    modes.push("brightness".into());
                }
            }
            "template" => {
                if c.has("red_template") && c.has("green_template") && c.has("blue_template") {
                    modes.push("rgb".into());
                }
                if c.has("color_temp_template") {
                    modes.push("color_temp".into());
                }
                if modes.is_empty() && c.has("brightness_template") {
                    modes.push("brightness".into());
                }
            }
            _ => {
                for (topic, mode) in [
                    ("rgb_command_topic", "rgb"),
                    ("rgbw_command_topic", "rgbw"),
                    ("rgbww_command_topic", "rgbww"),
                    ("hs_command_topic", "hs"),
                    ("xy_command_topic", "xy"),
                    ("color_temp_command_topic", "color_temp"),
                    ("white_command_topic", "white"),
                ] {
                    if c.has(topic) {
                        modes.push(mode.into());
                    }
                }
                if modes.is_empty() && c.has("brightness_command_topic") {
                    modes.push("brightness".into());
                }
            }
        }
        if modes.is_empty() {
            modes.push("onoff".into());
        }
        modes
    }

    fn optimistic(&self, state_key: &str) -> bool {
        self.cfg.bool("optimistic", false) || !self.cfg.has(state_key)
    }

    /// Apply a value template, or pass the payload through.
    fn tpl(&self, key: &str, payload: &str) -> Option<String> {
        match self.cfg.str(key) {
            Some(t) => match template::render_value(t, payload) {
                Ok(v) => Some(v),
                Err(e) => {
                    crate::debug!("{}: template {key} failed: {e}", self.discovery_id);
                    None
                }
            },
            None => Some(payload.trim().to_string()),
        }
    }

    fn cmd_tpl(&self, key: &str, value: Value, extra: &Map<String, Value>) -> Result<String, ServiceError> {
        match self.cfg.str(key) {
            Some(t) => template::render_command(t, value, extra).map_err(ServiceError::Failed),
            None => Ok(match value {
                Value::String(s) => s,
                Value::Number(n) => n.as_f64().map(fmt_num).unwrap_or_else(|| n.to_string()),
                other => other.to_string(),
            }),
        }
    }

    fn publish(&self, topic_key: &str, payload: String) -> Option<Publish> {
        self.cfg.str(topic_key).map(|t| Publish {
            topic: t.to_string(),
            payload,
            qos: self.cfg.qos(),
            retain: self.cfg.retain(),
        })
    }

    fn need_publish(&self, topic_key: &str, payload: String) -> Result<Publish, ServiceError> {
        self.publish(topic_key, payload)
            .ok_or_else(|| ServiceError::NotSupported(format!("{} has no {topic_key}", self.discovery_id)))
    }

    // ------------------------------------------------------------------
    // availability

    /// Whether the entity is available given the broker connection.
    pub fn is_available(&self) -> bool {
        if self.expired {
            return false;
        }
        if self.avail.is_empty() {
            return true;
        }
        match self.cfg.str("availability_mode").unwrap_or("latest") {
            "all" => self.avail.iter().all(|a| *a == Some(true)),
            "any" => self.avail.contains(&Some(true)),
            _ => self.latest_avail == Some(true),
        }
    }

    // ------------------------------------------------------------------
    // incoming

    /// Apply a message for one of this entity's subscriptions. Returns
    /// true if the visible state may have changed.
    pub fn handle(&mut self, role: &str, payload: &str) -> bool {
        if let Some(i) = role.strip_prefix("avty:") {
            let Ok(i) = i.parse::<usize>() else { return false };
            let Some(a) = self.availability().into_iter().nth(i) else { return false };
            let value = match &a.template {
                Some(t) => template::render_value(t, payload).unwrap_or_default(),
                None => payload.trim().to_string(),
            };
            let v = if Some(&value) == a.on.as_ref() {
                Some(true)
            } else if Some(&value) == a.off.as_ref() {
                Some(false)
            } else {
                return false;
            };
            if let Some(slot) = self.avail.get_mut(i) {
                *slot = v;
            }
            self.latest_avail = v;
            return true;
        }
        if role == "json_attributes_topic" {
            let Some(v) = self.tpl("json_attributes_template", payload) else { return false };
            if let Ok(Value::Object(m)) = serde_json::from_str::<Value>(&v) {
                self.json_attrs = m
                    .into_iter()
                    .filter(|(k, _)| !RESERVED_ATTRS.contains(&k.as_str()))
                    .collect();
                return true;
            }
            return false;
        }
        let changed = match self.component.as_str() {
            "sensor" => self.on_sensor(payload),
            "binary_sensor" => self.on_binary_sensor(payload),
            "switch" => self.on_switch(payload),
            "light" => match self.light_schema() {
                "json" => self.on_light_json(payload),
                "template" => self.on_light_template(payload),
                _ => self.on_light_basic(role, payload),
            },
            "cover" => self.on_cover(role, payload),
            "climate" => self.on_climate(role, payload),
            "fan" => self.on_fan(role, payload),
            "lock" => self.on_lock(payload),
            "number" => self.on_number(payload),
            "select" => self.on_select(payload),
            "text" => self.on_text(payload),
            "siren" => self.on_siren(payload),
            "valve" => self.on_valve(payload),
            "update" => self.on_update(role, payload),
            "event" => self.on_event(payload),
            _ => false,
        };
        if changed {
            self.last_update = Some(Instant::now());
            self.expired = false;
        }
        changed
    }

    /// Periodic check for `expire_after` and `off_delay`.
    pub fn tick(&mut self, now: Instant) -> bool {
        let mut changed = false;
        if let (Some(secs), Some(last)) = (self.cfg.opt_f64("expire_after"), self.last_update) {
            if secs > 0.0 && !self.expired && now.duration_since(last) >= Duration::from_secs_f64(secs) {
                self.expired = true;
                changed = true;
            }
        }
        if let Some(at) = self.off_at {
            if now >= at {
                self.off_at = None;
                self.state = Some("off".into());
                changed = true;
            }
        }
        changed
    }

    fn set_state(&mut self, s: Option<String>) -> bool {
        if self.state != s {
            self.state = s;
        }
        true
    }

    fn set_attr(&mut self, k: &str, v: Value) {
        self.attrs.insert(k.into(), v);
    }

    fn on_sensor(&mut self, payload: &str) -> bool {
        let Some(v) = self.tpl("value_template", payload) else { return false };
        let numeric = self.cfg.has("unit_of_measurement") || self.cfg.has("state_class");
        if v == "None" || (v.is_empty() && numeric) {
            return self.set_state(None);
        }
        self.set_state(Some(v))
    }

    fn on_binary_sensor(&mut self, payload: &str) -> bool {
        let Some(v) = self.tpl("value_template", payload) else { return false };
        let on = self.cfg.payload("payload_on", "ON");
        let off = self.cfg.payload("payload_off", "OFF");
        if Some(&v) == on.as_ref() {
            self.off_at = self
                .cfg
                .opt_f64("off_delay")
                .filter(|d| *d > 0.0)
                .map(|d| Instant::now() + Duration::from_secs_f64(d));
            self.set_state(Some("on".into()))
        } else if Some(&v) == off.as_ref() {
            self.off_at = None;
            self.set_state(Some("off".into()))
        } else if v == "None" || v.is_empty() {
            self.set_state(None)
        } else {
            crate::debug!("{}: no matching payload for binary_sensor: {v}", self.discovery_id);
            false
        }
    }

    fn on_switch(&mut self, payload: &str) -> bool {
        let Some(v) = self.tpl("value_template", payload) else { return false };
        let on = self.cfg.payload("state_on", &self.cfg.payload("payload_on", "ON").unwrap_or_default());
        let off = self.cfg.payload("state_off", &self.cfg.payload("payload_off", "OFF").unwrap_or_default());
        if Some(&v) == on.as_ref() {
            self.set_state(Some("on".into()))
        } else if Some(&v) == off.as_ref() {
            self.set_state(Some("off".into()))
        } else if v == "None" {
            self.set_state(None)
        } else {
            false
        }
    }

    fn brightness_scale(&self) -> f64 {
        self.cfg.f64("brightness_scale", 255.0).max(1.0)
    }

    fn on_light_basic(&mut self, role: &str, payload: &str) -> bool {
        match role {
            "state_topic" => {
                let Some(v) = self.tpl("state_value_template", payload) else { return false };
                let on = self.cfg.payload("payload_on", "ON");
                let off = self.cfg.payload("payload_off", "OFF");
                if Some(&v) == on.as_ref() {
                    self.set_state(Some("on".into()))
                } else if Some(&v) == off.as_ref() {
                    self.set_state(Some("off".into()))
                } else if v == "None" {
                    self.set_state(None)
                } else {
                    false
                }
            }
            "brightness_state_topic" => {
                let Some(b) = self.tpl("brightness_value_template", payload).as_deref().and_then(parse_f64) else {
                    return false;
                };
                let b = (b / self.brightness_scale() * 255.0).round().clamp(0.0, 255.0);
                self.set_attr("brightness", num_value(b));
                true
            }
            "color_temp_state_topic" => {
                let Some(v) = self.tpl("color_temp_value_template", payload).as_deref().and_then(parse_f64) else {
                    return false;
                };
                let k = if self.cfg.bool("color_temp_kelvin", false) { v } else { color::mired_to_kelvin(v) };
                self.set_attr("color_temp_kelvin", num_value(k));
                self.auto_mode("color_temp");
                true
            }
            "rgb_state_topic" => {
                let Some(c) = self.tpl("rgb_value_template", payload).as_deref().and_then(parse_list) else {
                    return false;
                };
                if c.len() < 3 {
                    return false;
                }
                self.set_attr("rgb_color", json!([c[0] as u8, c[1] as u8, c[2] as u8]));
                let (h, s) = color::rgb_to_hs(c[0], c[1], c[2]);
                self.set_attr("hs_color", json!([h, s]));
                self.auto_mode("rgb");
                true
            }
            "hs_state_topic" => {
                let Some(c) = self.tpl("hs_value_template", payload).as_deref().and_then(parse_list) else {
                    return false;
                };
                if c.len() != 2 {
                    return false;
                }
                self.set_attr("hs_color", json!([c[0], c[1]]));
                let (r, g, b) = color::hs_to_rgb(c[0], c[1]);
                self.set_attr("rgb_color", json!([r, g, b]));
                self.auto_mode("hs");
                true
            }
            "xy_state_topic" => {
                let Some(c) = self.tpl("xy_value_template", payload).as_deref().and_then(parse_list) else {
                    return false;
                };
                if c.len() != 2 {
                    return false;
                }
                self.set_attr("xy_color", json!([c[0], c[1]]));
                self.auto_mode("xy");
                true
            }
            "effect_state_topic" => {
                let Some(v) = self.tpl("effect_value_template", payload) else { return false };
                self.set_attr("effect", json!(v));
                true
            }
            "color_mode_state_topic" => {
                let Some(v) = self.tpl("color_mode_value_template", payload) else { return false };
                self.set_attr("color_mode", json!(v));
                true
            }
            _ => false,
        }
    }

    /// Set color_mode from the last color update unless the device reports it.
    fn auto_mode(&mut self, mode: &str) {
        if !self.cfg.has("color_mode_state_topic") && self.color_modes().iter().any(|m| m == mode) {
            self.set_attr("color_mode", json!(mode));
        }
    }

    fn on_light_json(&mut self, payload: &str) -> bool {
        let Ok(Value::Object(j)) = serde_json::from_str::<Value>(payload) else { return false };
        let mut changed = false;
        match j.get("state").and_then(Value::as_str) {
            Some("ON") => changed |= self.set_state(Some("on".into())),
            Some("OFF") => changed |= self.set_state(Some("off".into())),
            _ if j.get("state").is_some_and(Value::is_null) => changed |= self.set_state(None),
            _ => {}
        }
        if let Some(b) = j.get("brightness").and_then(Value::as_f64) {
            let b = (b / self.brightness_scale() * 255.0).round().clamp(0.0, 255.0);
            self.set_attr("brightness", num_value(b));
            changed = true;
        }
        let mut inferred = None;
        if let Some(c) = j.get("color").and_then(Value::as_object) {
            let g = |k: &str| c.get(k).and_then(Value::as_f64);
            if let (Some(h), Some(s)) = (g("h"), g("s")) {
                self.set_attr("hs_color", json!([h, s]));
                inferred = Some("hs");
            }
            if let (Some(r), Some(gr), Some(b)) = (g("r"), g("g"), g("b")) {
                self.set_attr("rgb_color", json!([r as u8, gr as u8, b as u8]));
                if inferred.is_none() {
                    let (h, s) = color::rgb_to_hs(r, gr, b);
                    self.set_attr("hs_color", json!([h, s]));
                }
                inferred = Some("rgb");
            }
            if let (Some(x), Some(y)) = (g("x"), g("y")) {
                self.set_attr("xy_color", json!([x, y]));
                inferred.get_or_insert("xy");
            }
            changed = true;
        }
        if let Some(ct) = j.get("color_temp").and_then(Value::as_f64) {
            let k = if self.cfg.bool("color_temp_kelvin", false) { ct } else { color::mired_to_kelvin(ct) };
            self.set_attr("color_temp_kelvin", num_value(k));
            if inferred.is_none() {
                inferred = Some("color_temp");
            }
            changed = true;
        }
        if let Some(m) = j.get("color_mode").and_then(Value::as_str) {
            self.set_attr("color_mode", json!(m));
            changed = true;
        } else if let Some(m) = inferred {
            self.auto_mode(m);
        }
        if let Some(e) = j.get("effect") {
            self.set_attr("effect", e.clone());
            changed = true;
        }
        changed
    }

    fn on_light_template(&mut self, payload: &str) -> bool {
        let mut changed = false;
        if let Some(v) = self.cfg.str("state_template").and_then(|t| template::render_value(t, payload).ok()) {
            match v.as_str() {
                "on" => changed |= self.set_state(Some("on".into())),
                "off" => changed |= self.set_state(Some("off".into())),
                _ => {}
            }
        }
        let num = |e: &Self, k: &str| {
            e.cfg
                .str(k)
                .and_then(|t| template::render_value(t, payload).ok())
                .and_then(|v| parse_f64(&v))
        };
        if let Some(b) = num(self, "brightness_template") {
            self.set_attr("brightness", num_value(b.clamp(0.0, 255.0)));
            changed = true;
        }
        if let Some(m) = num(self, "color_temp_template") {
            self.set_attr("color_temp_kelvin", num_value(color::mired_to_kelvin(m)));
            self.auto_mode("color_temp");
            changed = true;
        }
        if let (Some(r), Some(g), Some(b)) =
            (num(self, "red_template"), num(self, "green_template"), num(self, "blue_template"))
        {
            self.set_attr("rgb_color", json!([r as u8, g as u8, b as u8]));
            self.auto_mode("rgb");
            changed = true;
        }
        if let Some(e) = self.cfg.str("effect_template").and_then(|t| template::render_value(t, payload).ok()) {
            self.set_attr("effect", json!(e));
            changed = true;
        }
        changed
    }

    fn scale_pos(&self, v: f64, closed_key: &str, open_key: &str, dc: f64, dopen: f64) -> f64 {
        let closed = self.cfg.f64(closed_key, dc);
        let open = self.cfg.f64(open_key, dopen);
        if open == closed {
            return 0.0;
        }
        ((v - closed) / (open - closed) * 100.0).round().clamp(0.0, 100.0)
    }

    fn unscale_pos(&self, pct: f64, closed_key: &str, open_key: &str, dc: f64, dopen: f64) -> f64 {
        let closed = self.cfg.f64(closed_key, dc);
        let open = self.cfg.f64(open_key, dopen);
        (closed + pct.clamp(0.0, 100.0) / 100.0 * (open - closed)).round()
    }

    fn on_cover(&mut self, role: &str, payload: &str) -> bool {
        match role {
            "state_topic" => {
                let Some(v) = self.tpl("value_template", payload) else { return false };
                let c = &self.cfg;
                let s = if Some(&v) == c.payload("state_open", "open").as_ref() {
                    "open"
                } else if Some(&v) == c.payload("state_opening", "opening").as_ref() {
                    "opening"
                } else if Some(&v) == c.payload("state_closed", "closed").as_ref() {
                    "closed"
                } else if Some(&v) == c.payload("state_closing", "closing").as_ref() {
                    "closing"
                } else if Some(&v) == c.payload("state_stopped", "stopped").as_ref() {
                    match self.attrs.get("current_position").and_then(Value::as_f64) {
                        Some(p) if p <= 0.0 => "closed",
                        _ => "open",
                    }
                } else if v == "None" {
                    return self.set_state(None);
                } else {
                    return false;
                };
                self.set_state(Some(s.into()))
            }
            "position_topic" => {
                let Some(v) = self.tpl("position_template", payload).as_deref().and_then(parse_f64) else {
                    return false;
                };
                let pct = self.scale_pos(v, "position_closed", "position_open", 0.0, 100.0);
                self.set_attr("current_position", num_value(pct));
                if !self.cfg.has("state_topic") {
                    self.state = Some(if pct <= 0.0 { "closed" } else { "open" }.into());
                }
                true
            }
            "tilt_status_topic" => {
                let Some(v) = self.tpl("tilt_status_template", payload).as_deref().and_then(parse_f64) else {
                    return false;
                };
                let pct = self.scale_pos(v, "tilt_min", "tilt_max", 0.0, 100.0);
                self.set_attr("current_tilt_position", num_value(pct));
                true
            }
            _ => false,
        }
    }

    fn on_climate(&mut self, role: &str, payload: &str) -> bool {
        let (tpl_key, attr) = match role {
            "mode_state_topic" => {
                let Some(v) = self.tpl("mode_state_template", payload) else { return false };
                if v.is_empty() || v == "None" {
                    return false;
                }
                return self.set_state(Some(v));
            }
            "current_temperature_topic" => ("current_temperature_template", "current_temperature"),
            "temperature_state_topic" => ("temperature_state_template", "temperature"),
            "temperature_low_state_topic" => ("temperature_low_state_template", "target_temp_low"),
            "temperature_high_state_topic" => ("temperature_high_state_template", "target_temp_high"),
            "current_humidity_topic" => ("current_humidity_template", "current_humidity"),
            "target_humidity_state_topic" => ("target_humidity_state_template", "humidity"),
            "action_topic" => {
                let Some(v) = self.tpl("action_template", payload) else { return false };
                self.set_attr("hvac_action", if v.is_empty() || v == "None" { Value::Null } else { json!(v) });
                return true;
            }
            "fan_mode_state_topic" | "swing_mode_state_topic" | "preset_mode_state_topic" => {
                let (t, a) = match role {
                    "fan_mode_state_topic" => ("fan_mode_state_template", "fan_mode"),
                    "swing_mode_state_topic" => ("swing_mode_state_template", "swing_mode"),
                    _ => ("preset_mode_value_template", "preset_mode"),
                };
                let Some(v) = self.tpl(t, payload) else { return false };
                let val = if v.is_empty() || v == "None" || (a == "preset_mode" && v == "none") {
                    Value::Null
                } else {
                    json!(v)
                };
                self.set_attr(a, val);
                return true;
            }
            _ => return false,
        };
        let Some(v) = self.tpl(tpl_key, payload) else { return false };
        match parse_f64(&v) {
            Some(f) => self.set_attr(attr, num_value(f)),
            None if v == "None" || v.is_empty() => self.set_attr(attr, Value::Null),
            None => return false,
        }
        true
    }

    fn on_fan(&mut self, role: &str, payload: &str) -> bool {
        let c = self.cfg.clone();
        match role {
            "state_topic" => {
                let Some(v) = self.tpl("state_value_template", payload) else { return false };
                if Some(&v) == c.payload("payload_on", "ON").as_ref() {
                    self.set_state(Some("on".into()))
                } else if Some(&v) == c.payload("payload_off", "OFF").as_ref() {
                    self.set_state(Some("off".into()))
                } else if v == "None" {
                    self.set_state(None)
                } else {
                    false
                }
            }
            "percentage_state_topic" => {
                let Some(v) = self.tpl("percentage_value_template", payload) else { return false };
                if Some(&v) == c.payload("payload_reset_percentage", "None").as_ref() {
                    self.set_attr("percentage", Value::Null);
                    return true;
                }
                let Some(speed) = parse_f64(&v) else { return false };
                let (lo, hi) = self.speed_range();
                let pct = if speed <= 0.0 {
                    0.0
                } else {
                    ((speed - (lo - 1.0)) * 100.0 / (hi - lo + 1.0)).floor().clamp(0.0, 100.0)
                };
                self.set_attr("percentage", num_value(pct));
                true
            }
            "preset_mode_state_topic" => {
                let Some(v) = self.tpl("preset_mode_value_template", payload) else { return false };
                if Some(&v) == c.payload("payload_reset_preset_mode", "None").as_ref() {
                    self.set_attr("preset_mode", Value::Null);
                    return true;
                }
                if !c.list("preset_modes").contains(&v) {
                    return false;
                }
                self.set_attr("preset_mode", json!(v));
                true
            }
            "oscillation_state_topic" => {
                let Some(v) = self.tpl("oscillation_value_template", payload) else { return false };
                if Some(&v) == c.payload("payload_oscillation_on", "oscillate_on").as_ref() {
                    self.set_attr("oscillating", json!(true));
                } else if Some(&v) == c.payload("payload_oscillation_off", "oscillate_off").as_ref() {
                    self.set_attr("oscillating", json!(false));
                } else {
                    return false;
                }
                true
            }
            "direction_state_topic" => {
                let Some(v) = self.tpl("direction_value_template", payload) else { return false };
                self.set_attr("direction", json!(v));
                true
            }
            _ => false,
        }
    }

    fn on_lock(&mut self, payload: &str) -> bool {
        let Some(v) = self.tpl("value_template", payload) else { return false };
        let c = &self.cfg;
        let table = [
            ("state_locked", "LOCKED", "locked"),
            ("state_unlocked", "UNLOCKED", "unlocked"),
            ("state_locking", "LOCKING", "locking"),
            ("state_unlocking", "UNLOCKING", "unlocking"),
            ("state_jammed", "JAMMED", "jammed"),
            ("state_open", "OPEN", "open"),
            ("state_opening", "OPENING", "opening"),
        ];
        for (k, d, s) in table {
            if Some(&v) == c.payload(k, d).as_ref() {
                return self.set_state(Some(s.into()));
            }
        }
        if v == "None" {
            return self.set_state(None);
        }
        false
    }

    fn on_number(&mut self, payload: &str) -> bool {
        let Some(v) = self.tpl("value_template", payload) else { return false };
        if Some(&v) == self.cfg.payload("payload_reset", "None").as_ref() {
            return self.set_state(None);
        }
        match parse_f64(&v) {
            Some(f) => self.set_state(Some(fmt_num(f))),
            None => false,
        }
    }

    fn on_select(&mut self, payload: &str) -> bool {
        let Some(v) = self.tpl("value_template", payload) else { return false };
        if v == "None" || v.is_empty() {
            return self.set_state(None);
        }
        if !self.cfg.list("options").contains(&v) {
            crate::debug!("{}: invalid select option {v}", self.discovery_id);
            return false;
        }
        self.set_state(Some(v))
    }

    fn on_text(&mut self, payload: &str) -> bool {
        let Some(v) = self.tpl("value_template", payload) else { return false };
        self.set_state(Some(v))
    }

    fn on_siren(&mut self, payload: &str) -> bool {
        let Some(mut v) = self.tpl("state_value_template", payload) else { return false };
        if let Ok(Value::Object(j)) = serde_json::from_str::<Value>(&v) {
            for k in ["tone", "duration", "volume_level"] {
                if let Some(x) = j.get(k) {
                    self.set_attr(k, x.clone());
                }
            }
            match j.get("state").and_then(payload_str) {
                Some(s) => v = s,
                None => return true,
            }
        }
        let on_p = self.cfg.payload("payload_on", "ON").unwrap_or_default();
        let off_p = self.cfg.payload("payload_off", "OFF").unwrap_or_default();
        if Some(&v) == self.cfg.payload("state_on", &on_p).as_ref() {
            self.set_state(Some("on".into()))
        } else if Some(&v) == self.cfg.payload("state_off", &off_p).as_ref() {
            self.set_state(Some("off".into()))
        } else if v == "None" {
            self.set_state(None)
        } else {
            false
        }
    }

    fn on_valve(&mut self, payload: &str) -> bool {
        let Some(v) = self.tpl("value_template", payload) else { return false };
        let reports_position = self.cfg.bool("reports_position", false);
        let (mut state_str, mut pos) = (Some(v.clone()), None);
        if let Ok(Value::Object(j)) = serde_json::from_str::<Value>(&v) {
            state_str = j.get("state").and_then(payload_str);
            pos = j.get("position").and_then(Value::as_f64);
        } else if reports_position {
            if let Some(p) = parse_f64(&v) {
                pos = Some(p);
                state_str = None;
            }
        }
        let mut changed = false;
        if let Some(p) = pos {
            let pct = self.scale_pos(p, "position_closed", "position_open", 0.0, 100.0);
            self.set_attr("current_position", num_value(pct));
            if state_str.is_none() {
                self.state = Some(if pct <= 0.0 { "closed" } else { "open" }.into());
            }
            changed = true;
        }
        if let Some(s) = state_str {
            let c = &self.cfg;
            let mapped = [
                ("state_open", "open"),
                ("state_opening", "opening"),
                ("state_closed", "closed"),
                ("state_closing", "closing"),
            ]
            .into_iter()
            .find(|(k, d)| Some(&s) == c.payload(k, d).as_ref())
            .map(|(_, d)| d);
            if let Some(m) = mapped {
                self.state = Some(m.into());
                changed = true;
            }
        }
        changed
    }

    fn on_update(&mut self, role: &str, payload: &str) -> bool {
        if role == "latest_version_topic" {
            let Some(v) = self.tpl("latest_version_template", payload) else { return false };
            self.set_attr("latest_version", json!(v));
        } else {
            let Some(v) = self.tpl("value_template", payload) else { return false };
            match serde_json::from_str::<Value>(&v) {
                Ok(Value::Object(j)) => {
                    for k in [
                        "installed_version",
                        "latest_version",
                        "title",
                        "release_summary",
                        "release_url",
                        "entity_picture",
                        "in_progress",
                        "update_percentage",
                    ] {
                        if let Some(x) = j.get(k) {
                            self.set_attr(k, x.clone());
                        }
                    }
                }
                _ => self.set_attr("installed_version", json!(v)),
            }
        }
        let inst = self.attrs.get("installed_version").and_then(Value::as_str).map(str::to_string);
        let latest = self.attrs.get("latest_version").and_then(Value::as_str).map(str::to_string);
        self.state = match (inst, latest) {
            (Some(i), Some(l)) => Some(if i != l { "on" } else { "off" }.into()),
            (Some(_), None) => Some("off".into()),
            _ => None,
        };
        true
    }

    fn on_event(&mut self, payload: &str) -> bool {
        let Some(v) = self.tpl("value_template", payload) else { return false };
        let Ok(Value::Object(mut j)) = serde_json::from_str::<Value>(&v) else {
            crate::debug!("{}: event payload is not a JSON object", self.discovery_id);
            return false;
        };
        let Some(t) = j.remove("event_type").and_then(|v| payload_str(&v)) else { return false };
        let types = self.cfg.list("event_types");
        if !types.is_empty() && !types.contains(&t) {
            crate::debug!("{}: unknown event type {t}", self.discovery_id);
            return false;
        }
        self.attrs = j;
        self.set_attr("event_type", json!(t));
        self.set_state(Some(now_iso()))
    }

    // ------------------------------------------------------------------
    // state for the hub

    /// Current state and dynamic attributes as shown in the hub.
    pub fn render(&self) -> (String, Map<String, Value>) {
        let mut attrs = self.json_attrs.clone();
        let state = self.state.clone().unwrap_or_else(|| crate::types::STATE_UNKNOWN.into());
        let on = state == "on";
        for (k, v) in &self.attrs {
            // Light color attributes are only meaningful while on.
            if self.component == "light" && !on {
                continue;
            }
            attrs.insert(k.clone(), v.clone());
        }
        if self.component == "light" && on && !attrs.contains_key("color_mode") {
            let modes = self.color_modes();
            let m = if modes.len() == 1 {
                modes[0].clone()
            } else {
                modes.into_iter().find(|m| m != "onoff" && m != "brightness").unwrap_or_else(|| "onoff".into())
            };
            attrs.insert("color_mode".into(), json!(m));
        }
        (state, attrs)
    }

    // ------------------------------------------------------------------
    // commands

    /// Turn a service call into publishes. Optimistic entities update
    /// their own state.
    pub fn command(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        match self.component.as_str() {
            "switch" => self.cmd_switch(service),
            "light" => match self.light_schema() {
                "json" => self.cmd_light_json(service, data),
                "template" => self.cmd_light_template(service, data),
                _ => self.cmd_light_basic(service, data),
            },
            "cover" => self.cmd_cover(service, data),
            "climate" => self.cmd_climate(service, data),
            "fan" => self.cmd_fan(service, data),
            "lock" => self.cmd_lock(service),
            "number" => self.cmd_number(service, data),
            "select" => self.cmd_select(service, data),
            "button" => self.cmd_button(service),
            "scene" => self.cmd_scene(service),
            "text" => self.cmd_text(service, data),
            "siren" => self.cmd_siren(service, data),
            "valve" => self.cmd_valve(service, data),
            "update" => self.cmd_update(service),
            other => Err(ServiceError::NotSupported(format!("{other}.{service}"))),
        }
    }

    fn unsupported(&self, service: &str) -> ServiceError {
        ServiceError::NotSupported(format!("{}.{service}", self.component))
    }

    fn cmd_switch(&mut self, service: &str) -> Result<Vec<Publish>, ServiceError> {
        let (payload_key, default, state) = match service {
            "turn_on" => ("payload_on", "ON", "on"),
            "turn_off" => ("payload_off", "OFF", "off"),
            _ => return Err(self.unsupported(service)),
        };
        let payload = self.cfg.payload(payload_key, default).unwrap_or_default();
        let payload = self.cmd_tpl("command_template", json!(payload), &Map::new())?;
        let p = self.need_publish("command_topic", payload)?;
        if self.optimistic("state_topic") {
            self.state = Some(state.into());
        }
        Ok(vec![p])
    }

    fn cmd_light_basic(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        let optimistic = self.optimistic("state_topic");
        if service == "turn_off" {
            let p = self.need_publish("command_topic", self.cfg.payload("payload_off", "OFF").unwrap_or_default())?;
            if optimistic {
                self.state = Some("off".into());
            }
            return Ok(vec![p]);
        }
        if service != "turn_on" {
            return Err(self.unsupported(service));
        }
        let c = self.cfg.clone();
        let mut attrs_pubs = vec![];
        let current_b = self.attrs.get("brightness").and_then(Value::as_f64);
        let brightness = brightness_req(data, current_b);
        let mut sent_brightness = false;
        if let Some(b) = brightness {
            if c.has("brightness_command_topic") {
                let v = (b / 255.0 * self.brightness_scale()).round();
                let payload = self.cmd_tpl("brightness_command_template", num_value(v), &Map::new())?;
                attrs_pubs.extend(self.publish("brightness_command_topic", payload));
                sent_brightness = true;
            }
            if optimistic || !c.has("brightness_state_topic") {
                self.set_attr("brightness", num_value(b));
            }
        }
        if let Some(req) = color_req(data) {
            let bscale = match (brightness, c.has("brightness_command_topic")) {
                (Some(b), false) => b / 255.0,
                _ => 1.0,
            };
            let rgb_pub = |e: &Self, r: f64, g: f64, b: f64| -> Result<Option<Publish>, ServiceError> {
                let (r, g, b) = ((r * bscale).round(), (g * bscale).round(), (b * bscale).round());
                let mut vars = Map::new();
                vars.insert("red".into(), num_value(r));
                vars.insert("green".into(), num_value(g));
                vars.insert("blue".into(), num_value(b));
                let payload = match c.str("rgb_command_template") {
                    Some(t) => template::render_command(t, json!(format!("{r},{g},{b}")), &vars).map_err(ServiceError::Failed)?,
                    None => format!("{},{},{}", fmt_num(r), fmt_num(g), fmt_num(b)),
                };
                Ok(e.publish("rgb_command_topic", payload))
            };
            let hs_pub = |e: &Self, h: f64, s: f64| -> Result<Option<Publish>, ServiceError> {
                let mut vars = Map::new();
                vars.insert("hue".into(), json!(h));
                vars.insert("sat".into(), json!(s));
                let payload = match c.str("hs_command_template") {
                    Some(t) => template::render_command(t, json!(format!("{h},{s}")), &vars).map_err(ServiceError::Failed)?,
                    None => format!("{},{}", fmt_num(h), fmt_num(s)),
                };
                Ok(e.publish("hs_command_topic", payload))
            };
            let xy_pub = |e: &Self, x: f64, y: f64| -> Result<Option<Publish>, ServiceError> {
                let mut vars = Map::new();
                vars.insert("x".into(), json!(x));
                vars.insert("y".into(), json!(y));
                let payload = match c.str("xy_command_template") {
                    Some(t) => template::render_command(t, json!(format!("{x},{y}")), &vars).map_err(ServiceError::Failed)?,
                    None => format!("{x},{y}"),
                };
                Ok(e.publish("xy_command_topic", payload))
            };
            let (p, mode) = match req {
                ColorReq::Kelvin(k) => {
                    let v = if c.bool("color_temp_kelvin", false) { k } else { color::kelvin_to_mired(k) };
                    let payload = self.cmd_tpl("color_temp_command_template", num_value(v), &Map::new())?;
                    self.set_attr("color_temp_kelvin", num_value(k.round()));
                    (self.publish("color_temp_command_topic", payload), "color_temp")
                }
                ColorReq::Hs(h, s) => {
                    self.set_attr("hs_color", json!([h, s]));
                    if c.has("hs_command_topic") {
                        (hs_pub(self, h, s)?, "hs")
                    } else if c.has("rgb_command_topic") {
                        let (r, g, b) = color::hs_to_rgb(h, s);
                        (rgb_pub(self, r as f64, g as f64, b as f64)?, "rgb")
                    } else {
                        let (r, g, b) = color::hs_to_rgb(h, s);
                        let (x, y) = color::rgb_to_xy(r as f64, g as f64, b as f64);
                        (xy_pub(self, x, y)?, "xy")
                    }
                }
                ColorReq::Rgb(r, g, b) => {
                    self.set_attr("rgb_color", json!([r as u8, g as u8, b as u8]));
                    if c.has("rgb_command_topic") {
                        (rgb_pub(self, r, g, b)?, "rgb")
                    } else if c.has("hs_command_topic") {
                        let (h, s) = color::rgb_to_hs(r, g, b);
                        (hs_pub(self, h, s)?, "hs")
                    } else {
                        let (x, y) = color::rgb_to_xy(r, g, b);
                        (xy_pub(self, x, y)?, "xy")
                    }
                }
                ColorReq::Xy(x, y) => {
                    self.set_attr("xy_color", json!([x, y]));
                    if c.has("xy_command_topic") {
                        (xy_pub(self, x, y)?, "xy")
                    } else {
                        let (r, g, b) = color::xy_to_rgb(x, y);
                        if c.has("rgb_command_topic") {
                            (rgb_pub(self, r as f64, g as f64, b as f64)?, "rgb")
                        } else {
                            let (h, s) = color::rgb_to_hs(r as f64, g as f64, b as f64);
                            (hs_pub(self, h, s)?, "hs")
                        }
                    }
                }
            };
            if let Some(p) = p {
                attrs_pubs.push(p);
                self.auto_mode(mode);
            }
        }
        if let Some(e) = data.get("effect").and_then(Value::as_str) {
            let payload = self.cmd_tpl("effect_command_template", json!(e), &Map::new())?;
            attrs_pubs.extend(self.publish("effect_command_topic", payload));
            self.set_attr("effect", json!(e));
        }
        let on = self.publish("command_topic", c.payload("payload_on", "ON").unwrap_or_default());
        let mut out = vec![];
        match c.str("on_command_type").unwrap_or("last") {
            "first" => {
                out.extend(on);
                out.extend(attrs_pubs);
            }
            "brightness" => {
                if !sent_brightness {
                    if c.has("brightness_command_topic") && self.state.as_deref() != Some("on") {
                        let v = self.brightness_scale();
                        let payload = self.cmd_tpl("brightness_command_template", num_value(v), &Map::new())?;
                        out.extend(self.publish("brightness_command_topic", payload));
                    } else {
                        out.extend(on);
                    }
                }
                out.extend(attrs_pubs);
            }
            _ => {
                out.extend(attrs_pubs);
                out.extend(on);
            }
        }
        if out.is_empty() {
            return Err(ServiceError::NotSupported("light has no command topic".into()));
        }
        if optimistic {
            self.state = Some("on".into());
        }
        Ok(out)
    }

    fn cmd_light_json(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        let optimistic = self.optimistic("state_topic");
        let mut msg = Map::new();
        if let Some(t) = data_f64(data, "transition") {
            msg.insert("transition".into(), num_value(t));
        }
        if let Some(f) = data.get("flash").and_then(Value::as_str) {
            let secs = if f == "long" {
                self.cfg.f64("flash_time_long", 10.0)
            } else {
                self.cfg.f64("flash_time_short", 2.0)
            };
            msg.insert("flash".into(), num_value(secs));
        }
        match service {
            "turn_off" => {
                msg.insert("state".into(), json!("OFF"));
                if optimistic {
                    self.state = Some("off".into());
                }
            }
            "turn_on" => {
                msg.insert("state".into(), json!("ON"));
                let modes = self.color_modes();
                let has = |m: &str| modes.iter().any(|x| x == m);
                let current_b = self.attrs.get("brightness").and_then(Value::as_f64);
                if let Some(b) = brightness_req(data, current_b) {
                    if modes.iter().any(|m| m != "onoff") || self.cfg.bool("brightness", false) {
                        let v = (b / 255.0 * self.brightness_scale()).round();
                        msg.insert("brightness".into(), num_value(v));
                    }
                    self.set_attr("brightness", num_value(b));
                }
                if let Some(req) = color_req(data) {
                    let (rgb, hs, xy) = match req {
                        ColorReq::Hs(h, s) => {
                            let (r, g, b) = color::hs_to_rgb(h, s);
                            let rgb = (r as f64, g as f64, b as f64);
                            (rgb, (h, s), color::rgb_to_xy(rgb.0, rgb.1, rgb.2))
                        }
                        ColorReq::Rgb(r, g, b) => ((r, g, b), color::rgb_to_hs(r, g, b), color::rgb_to_xy(r, g, b)),
                        ColorReq::Xy(x, y) => {
                            let (r, g, b) = color::xy_to_rgb(x, y);
                            let rgb = (r as f64, g as f64, b as f64);
                            (rgb, color::rgb_to_hs(rgb.0, rgb.1, rgb.2), (x, y))
                        }
                        ColorReq::Kelvin(k) => {
                            let v = if self.cfg.bool("color_temp_kelvin", false) { k } else { color::kelvin_to_mired(k) };
                            msg.insert("color_temp".into(), num_value(v));
                            self.set_attr("color_temp_kelvin", num_value(k.round()));
                            ((0.0, 0.0, 0.0), (0.0, 0.0), (0.0, 0.0))
                        }
                    };
                    if !matches!(req, ColorReq::Kelvin(_)) {
                        let prefer = match req {
                            ColorReq::Hs(..) => ["hs", "rgb", "xy"],
                            ColorReq::Xy(..) => ["xy", "hs", "rgb"],
                            _ => ["rgb", "hs", "xy"],
                        };
                        let pick = prefer.into_iter().find(|m| has(m)).or_else(|| {
                            ["rgbw", "rgbww"].into_iter().find(|m| has(m)).map(|_| "rgb")
                        });
                        let color = match pick {
                            Some("hs") => json!({ "h": hs.0, "s": hs.1 }),
                            Some("xy") => json!({ "x": xy.0, "y": xy.1 }),
                            Some(_) => json!({ "r": rgb.0 as u8, "g": rgb.1 as u8, "b": rgb.2 as u8 }),
                            None => Value::Null,
                        };
                        if !color.is_null() {
                            msg.insert("color".into(), color);
                            self.set_attr("hs_color", json!([hs.0, hs.1]));
                            self.set_attr("rgb_color", json!([rgb.0 as u8, rgb.1 as u8, rgb.2 as u8]));
                            self.set_attr("xy_color", json!([xy.0, xy.1]));
                            self.auto_mode(pick.unwrap_or("rgb"));
                        }
                    } else {
                        self.auto_mode("color_temp");
                    }
                }
                if let Some(e) = data.get("effect") {
                    msg.insert("effect".into(), e.clone());
                    self.set_attr("effect", e.clone());
                }
                if optimistic {
                    self.state = Some("on".into());
                }
            }
            _ => return Err(self.unsupported(service)),
        }
        Ok(vec![self.need_publish("command_topic", Value::Object(msg).to_string())?])
    }

    fn cmd_light_template(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        let mut vars = Map::new();
        if let Some(t) = data_f64(data, "transition") {
            vars.insert("transition".into(), num_value(t));
        }
        let key = match service {
            "turn_on" => {
                vars.insert("state".into(), json!("on"));
                let current_b = self.attrs.get("brightness").and_then(Value::as_f64);
                if let Some(b) = brightness_req(data, current_b) {
                    vars.insert("brightness".into(), num_value(b));
                }
                match color_req(data) {
                    Some(ColorReq::Kelvin(k)) => {
                        vars.insert("color_temp".into(), num_value(color::kelvin_to_mired(k)));
                    }
                    Some(req) => {
                        let (r, g, b) = match req {
                            ColorReq::Hs(h, s) => color::hs_to_rgb(h, s),
                            ColorReq::Xy(x, y) => color::xy_to_rgb(x, y),
                            ColorReq::Rgb(r, g, b) => (r as u8, g as u8, b as u8),
                            ColorReq::Kelvin(_) => unreachable!(),
                        };
                        let (h, s) = color::rgb_to_hs(r as f64, g as f64, b as f64);
                        vars.insert("red".into(), json!(r));
                        vars.insert("green".into(), json!(g));
                        vars.insert("blue".into(), json!(b));
                        vars.insert("hue".into(), json!(h));
                        vars.insert("sat".into(), json!(s));
                    }
                    None => {}
                }
                if let Some(e) = data.get("effect") {
                    vars.insert("effect".into(), e.clone());
                }
                "command_on_template"
            }
            "turn_off" => {
                vars.insert("state".into(), json!("off"));
                "command_off_template"
            }
            _ => return Err(self.unsupported(service)),
        };
        let t = self
            .cfg
            .str(key)
            .ok_or_else(|| ServiceError::NotSupported(format!("missing {key}")))?;
        let payload = template::render(t, &vars).map_err(ServiceError::Failed)?;
        if self.optimistic("state_topic") {
            self.state = Some(if service == "turn_on" { "on" } else { "off" }.into());
        }
        Ok(vec![self.need_publish("command_topic", payload)?])
    }

    fn cmd_cover(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        let c = self.cfg.clone();
        let optimistic = c.bool("optimistic", false) || (!c.has("state_topic") && !c.has("position_topic"));
        let simple = |e: &mut Self, key: &str, default: &str, state: Option<(&str, f64)>| -> Result<Vec<Publish>, ServiceError> {
            let payload = c
                .payload(key, default)
                .ok_or_else(|| ServiceError::NotSupported(format!("{key} disabled")))?;
            let p = e.need_publish("command_topic", payload)?;
            if let (true, Some((s, pos))) = (optimistic, state) {
                e.state = Some(s.into());
                if c.has("set_position_topic") || c.has("position_topic") {
                    e.set_attr("current_position", num_value(pos));
                }
            }
            Ok(vec![p])
        };
        match service {
            "open_cover" => simple(self, "payload_open", "OPEN", Some(("open", 100.0))),
            "close_cover" => simple(self, "payload_close", "CLOSE", Some(("closed", 0.0))),
            "stop_cover" => simple(self, "payload_stop", "STOP", None),
            "set_cover_position" => {
                let pos = data_f64(data, "position").ok_or_else(|| invalid("position required"))?;
                if !c.has("set_position_topic") {
                    return match pos {
                        p if p >= 100.0 => simple(self, "payload_open", "OPEN", Some(("open", 100.0))),
                        p if p <= 0.0 => simple(self, "payload_close", "CLOSE", Some(("closed", 0.0))),
                        _ => Err(self.unsupported(service)),
                    };
                }
                let v = self.unscale_pos(pos, "position_closed", "position_open", 0.0, 100.0);
                let mut vars = Map::new();
                vars.insert("position".into(), num_value(v));
                let payload = self.cmd_tpl("set_position_template", num_value(v), &vars)?;
                let p = self.need_publish("set_position_topic", payload)?;
                if optimistic || !c.has("position_topic") {
                    self.set_attr("current_position", num_value(pos));
                    self.state = Some(if pos <= 0.0 { "closed" } else { "open" }.into());
                }
                Ok(vec![p])
            }
            "open_cover_tilt" | "close_cover_tilt" | "set_cover_tilt_position" => {
                let pct = match service {
                    "open_cover_tilt" => self.scale_pos(c.f64("tilt_opened_value", 100.0), "tilt_min", "tilt_max", 0.0, 100.0),
                    "close_cover_tilt" => self.scale_pos(c.f64("tilt_closed_value", 0.0), "tilt_min", "tilt_max", 0.0, 100.0),
                    _ => data_f64(data, "tilt_position").ok_or_else(|| invalid("tilt_position required"))?,
                };
                let v = match service {
                    "open_cover_tilt" => c.f64("tilt_opened_value", 100.0),
                    "close_cover_tilt" => c.f64("tilt_closed_value", 0.0),
                    _ => self.unscale_pos(pct, "tilt_min", "tilt_max", 0.0, 100.0),
                };
                let payload = self.cmd_tpl("tilt_command_template", num_value(v), &Map::new())?;
                let p = self.need_publish("tilt_command_topic", payload)?;
                if c.bool("tilt_optimistic", !c.has("tilt_status_topic")) {
                    self.set_attr("current_tilt_position", num_value(pct));
                }
                Ok(vec![p])
            }
            _ => Err(self.unsupported(service)),
        }
    }

    fn cmd_climate(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        let mut out = vec![];
        match service {
            "set_hvac_mode" => {
                let mode = data.get("hvac_mode").and_then(Value::as_str).ok_or_else(|| invalid("hvac_mode required"))?;
                out.push(self.set_climate_mode(mode)?);
            }
            "turn_on" | "turn_off" => {
                let on = service == "turn_on";
                if self.cfg.has("power_command_topic") {
                    let payload = self.cfg.payload(if on { "payload_on" } else { "payload_off" }, if on { "ON" } else { "OFF" }).unwrap_or_default();
                    let payload = self.cmd_tpl("power_command_template", json!(payload), &Map::new())?;
                    out.push(self.need_publish("power_command_topic", payload)?);
                } else if on {
                    let modes = self.hvac_modes();
                    let mode = ["heat_cool", "heat", "cool", "auto"]
                        .into_iter()
                        .find(|m| modes.iter().any(|x| x == m))
                        .map(str::to_string)
                        .or_else(|| modes.into_iter().find(|m| m != "off"))
                        .ok_or_else(|| self.unsupported(service))?;
                    out.push(self.set_climate_mode(&mode)?);
                } else {
                    out.push(self.set_climate_mode("off")?);
                }
            }
            "set_temperature" => {
                if let Some(mode) = data.get("hvac_mode").and_then(Value::as_str) {
                    out.push(self.set_climate_mode(mode)?);
                }
                for (field, topic, tpl, state_topic, attr) in [
                    ("temperature", "temperature_command_topic", "temperature_command_template", "temperature_state_topic", "temperature"),
                    ("target_temp_low", "temperature_low_command_topic", "temperature_low_command_template", "temperature_low_state_topic", "target_temp_low"),
                    ("target_temp_high", "temperature_high_command_topic", "temperature_high_command_template", "temperature_high_state_topic", "target_temp_high"),
                ] {
                    if let Some(t) = data_f64(data, field) {
                        let payload = self.cmd_tpl(tpl, num_value(t), &Map::new())?;
                        out.push(self.need_publish(topic, payload)?);
                        if self.optimistic(state_topic) {
                            self.set_attr(attr, num_value(t));
                        }
                    }
                }
                if out.is_empty() {
                    return Err(invalid("temperature required"));
                }
            }
            "set_fan_mode" | "set_swing_mode" | "set_preset_mode" | "set_humidity" => {
                let (field, topic, tpl, state_topic, attr) = match service {
                    "set_fan_mode" => ("fan_mode", "fan_mode_command_topic", "fan_mode_command_template", "fan_mode_state_topic", "fan_mode"),
                    "set_swing_mode" => ("swing_mode", "swing_mode_command_topic", "swing_mode_command_template", "swing_mode_state_topic", "swing_mode"),
                    "set_preset_mode" => ("preset_mode", "preset_mode_command_topic", "preset_mode_command_template", "preset_mode_state_topic", "preset_mode"),
                    _ => ("humidity", "target_humidity_command_topic", "target_humidity_command_template", "target_humidity_state_topic", "humidity"),
                };
                let v = data.get(field).cloned().ok_or_else(|| invalid(format!("{field} required")))?;
                let payload = self.cmd_tpl(tpl, v.clone(), &Map::new())?;
                out.push(self.need_publish(topic, payload)?);
                if self.optimistic(state_topic) {
                    self.set_attr(attr, v);
                }
            }
            _ => return Err(self.unsupported(service)),
        }
        Ok(out)
    }

    fn set_climate_mode(&mut self, mode: &str) -> Result<Publish, ServiceError> {
        if !self.hvac_modes().iter().any(|m| m == mode) {
            return Err(invalid(format!("unsupported hvac_mode {mode}")));
        }
        let payload = self.cmd_tpl("mode_command_template", json!(mode), &Map::new())?;
        let p = self.need_publish("mode_command_topic", payload)?;
        if self.optimistic("mode_state_topic") {
            self.state = Some(mode.into());
        }
        Ok(p)
    }

    fn cmd_fan(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        let c = self.cfg.clone();
        let mut out = vec![];
        match service {
            "turn_on" => {
                let payload = c.payload("payload_on", "ON").unwrap_or_default();
                let payload = self.cmd_tpl("command_template", json!(payload), &Map::new())?;
                out.push(self.need_publish("command_topic", payload)?);
                if self.optimistic("state_topic") {
                    self.state = Some("on".into());
                }
                if let Some(p) = data_f64(data, "percentage") {
                    out.extend(self.fan_percentage(p)?);
                }
                if let Some(m) = data.get("preset_mode").and_then(Value::as_str) {
                    out.push(self.fan_preset(m)?);
                }
            }
            "turn_off" => {
                let payload = c.payload("payload_off", "OFF").unwrap_or_default();
                let payload = self.cmd_tpl("command_template", json!(payload), &Map::new())?;
                out.push(self.need_publish("command_topic", payload)?);
                if self.optimistic("state_topic") {
                    self.state = Some("off".into());
                }
            }
            "set_percentage" => {
                let p = data_f64(data, "percentage").ok_or_else(|| invalid("percentage required"))?;
                if p <= 0.0 {
                    return self.cmd_fan("turn_off", &Map::new());
                }
                out.extend(self.fan_percentage(p)?);
            }
            "increase_speed" | "decrease_speed" => {
                let (lo, hi) = self.speed_range();
                let step = data_f64(data, "percentage_step").unwrap_or(100.0 / (hi - lo + 1.0));
                let cur = self.attrs.get("percentage").and_then(Value::as_f64).unwrap_or(0.0);
                let p = if service == "increase_speed" { cur + step } else { cur - step }.clamp(0.0, 100.0);
                if p <= 0.0 {
                    return self.cmd_fan("turn_off", &Map::new());
                }
                out.extend(self.fan_percentage(p)?);
            }
            "set_preset_mode" => {
                let m = data.get("preset_mode").and_then(Value::as_str).ok_or_else(|| invalid("preset_mode required"))?;
                out.push(self.fan_preset(m)?);
            }
            "oscillate" => {
                let osc = data.get("oscillating").and_then(Value::as_bool).ok_or_else(|| invalid("oscillating required"))?;
                let payload = if osc {
                    c.payload("payload_oscillation_on", "oscillate_on")
                } else {
                    c.payload("payload_oscillation_off", "oscillate_off")
                }
                .unwrap_or_default();
                let payload = self.cmd_tpl("oscillation_command_template", json!(payload), &Map::new())?;
                out.push(self.need_publish("oscillation_command_topic", payload)?);
                if self.optimistic("oscillation_state_topic") {
                    self.set_attr("oscillating", json!(osc));
                }
            }
            "set_direction" => {
                let d = data.get("direction").and_then(Value::as_str).ok_or_else(|| invalid("direction required"))?;
                let payload = self.cmd_tpl("direction_command_template", json!(d), &Map::new())?;
                out.push(self.need_publish("direction_command_topic", payload)?);
                if self.optimistic("direction_state_topic") {
                    self.set_attr("direction", json!(d));
                }
            }
            _ => return Err(self.unsupported(service)),
        }
        Ok(out)
    }

    fn fan_percentage(&mut self, pct: f64) -> Result<Vec<Publish>, ServiceError> {
        let (lo, hi) = self.speed_range();
        let speed = ((pct.clamp(0.0, 100.0) * (hi - lo + 1.0) / 100.0) + (lo - 1.0)).ceil();
        let payload = self.cmd_tpl("percentage_command_template", num_value(speed), &Map::new())?;
        let p = self.need_publish("percentage_command_topic", payload)?;
        if self.optimistic("percentage_state_topic") {
            self.set_attr("percentage", num_value(pct.round()));
        }
        Ok(vec![p])
    }

    fn fan_preset(&mut self, mode: &str) -> Result<Publish, ServiceError> {
        if !self.cfg.list("preset_modes").iter().any(|m| m == mode) {
            return Err(invalid(format!("unknown preset_mode {mode}")));
        }
        let payload = self.cmd_tpl("preset_mode_command_template", json!(mode), &Map::new())?;
        let p = self.need_publish("preset_mode_command_topic", payload)?;
        if self.optimistic("preset_mode_state_topic") {
            self.set_attr("preset_mode", json!(mode));
        }
        Ok(p)
    }

    fn cmd_lock(&mut self, service: &str) -> Result<Vec<Publish>, ServiceError> {
        let (key, default, state) = match service {
            "lock" => ("payload_lock", "LOCK", "locked"),
            "unlock" => ("payload_unlock", "UNLOCK", "unlocked"),
            "open" => ("payload_open", "OPEN", "open"),
            _ => return Err(self.unsupported(service)),
        };
        if service == "open" && !self.cfg.get("payload_open").is_some_and(|v| !v.is_null()) {
            return Err(self.unsupported(service));
        }
        let payload = self.cfg.payload(key, default).unwrap_or_default();
        let payload = self.cmd_tpl("command_template", json!(payload), &Map::new())?;
        let p = self.need_publish("command_topic", payload)?;
        if self.optimistic("state_topic") {
            self.state = Some(state.into());
        }
        Ok(vec![p])
    }

    fn cmd_number(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        if service != "set_value" {
            return Err(self.unsupported(service));
        }
        let v = data_f64(data, "value").ok_or_else(|| invalid("value required"))?;
        let (min, max) = (self.cfg.f64("min", 1.0), self.cfg.f64("max", 100.0));
        if v < min || v > max {
            return Err(invalid(format!("value {v} outside {min}..{max}")));
        }
        let payload = self.cmd_tpl("command_template", num_value(v), &Map::new())?;
        let p = self.need_publish("command_topic", payload)?;
        if self.optimistic("state_topic") {
            self.state = Some(fmt_num(v));
        }
        Ok(vec![p])
    }

    fn cmd_select(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        let options = self.cfg.list("options");
        let current = self.state.as_ref().and_then(|s| options.iter().position(|o| o == s));
        let option = match service {
            "select_option" => data
                .get("option")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("option required"))?
                .to_string(),
            "select_first" => options.first().cloned().ok_or_else(|| invalid("no options"))?,
            "select_last" => options.last().cloned().ok_or_else(|| invalid("no options"))?,
            "select_next" | "select_previous" => {
                if options.is_empty() {
                    return Err(invalid("no options"));
                }
                let n = options.len();
                let cycle = data.get("cycle").and_then(Value::as_bool).unwrap_or(true);
                let i = match (current, service) {
                    (None, _) => 0,
                    (Some(i), "select_next") if i + 1 < n => i + 1,
                    (Some(i), "select_next") => if cycle { 0 } else { i },
                    (Some(0), _) => if cycle { n - 1 } else { 0 },
                    (Some(i), _) => i - 1,
                };
                options[i].clone()
            }
            _ => return Err(self.unsupported(service)),
        };
        if !options.contains(&option) {
            return Err(invalid(format!("invalid option {option}")));
        }
        let payload = self.cmd_tpl("command_template", json!(option), &Map::new())?;
        let p = self.need_publish("command_topic", payload)?;
        if self.optimistic("state_topic") {
            self.state = Some(option);
        }
        Ok(vec![p])
    }

    fn cmd_button(&mut self, service: &str) -> Result<Vec<Publish>, ServiceError> {
        if service != "press" {
            return Err(self.unsupported(service));
        }
        let payload = self.cfg.payload("payload_press", "PRESS").unwrap_or_default();
        let payload = self.cmd_tpl("command_template", json!(payload), &Map::new())?;
        let p = self.need_publish("command_topic", payload)?;
        self.state = Some(now_iso());
        Ok(vec![p])
    }

    fn cmd_scene(&mut self, service: &str) -> Result<Vec<Publish>, ServiceError> {
        if service != "turn_on" {
            return Err(self.unsupported(service));
        }
        let payload = self.cfg.payload("payload_on", "ON").unwrap_or_default();
        let p = self.need_publish("command_topic", payload)?;
        self.state = Some(now_iso());
        Ok(vec![p])
    }

    fn cmd_text(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        if service != "set_value" {
            return Err(self.unsupported(service));
        }
        let v = data
            .get("value")
            .and_then(payload_str)
            .ok_or_else(|| invalid("value required"))?;
        let len = v.chars().count() as f64;
        if len < self.cfg.f64("min", 0.0) || len > self.cfg.f64("max", 255.0) {
            return Err(invalid("text length out of range"));
        }
        let payload = self.cmd_tpl("command_template", json!(v), &Map::new())?;
        let p = self.need_publish("command_topic", payload)?;
        if self.optimistic("state_topic") {
            self.state = Some(v);
        }
        Ok(vec![p])
    }

    fn cmd_siren(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        let on = match service {
            "turn_on" => true,
            "turn_off" => false,
            _ => return Err(self.unsupported(service)),
        };
        let payload = self
            .cfg
            .payload(if on { "payload_on" } else { "payload_off" }, if on { "ON" } else { "OFF" })
            .unwrap_or_default();
        let mut vars = Map::new();
        for k in ["tone", "duration", "volume_level"] {
            if let Some(v) = data.get(k) {
                vars.insert(k.into(), v.clone());
            }
        }
        let tpl_key = if !on && self.cfg.has("command_off_template") { "command_off_template" } else { "command_template" };
        let out = if let Some(t) = self.cfg.str(tpl_key) {
            template::render_command(t, json!(payload), &vars).map_err(ServiceError::Failed)?
        } else if on && !vars.is_empty() {
            let mut m = vars.clone();
            m.insert("state".into(), json!(payload));
            Value::Object(m).to_string()
        } else {
            payload
        };
        let p = self.need_publish("command_topic", out)?;
        if self.optimistic("state_topic") {
            self.state = Some(if on { "on" } else { "off" }.into());
        }
        Ok(vec![p])
    }

    fn cmd_valve(&mut self, service: &str, data: &Map<String, Value>) -> Result<Vec<Publish>, ServiceError> {
        let reports_position = self.cfg.bool("reports_position", false);
        let (value, state, pos) = match service {
            "open_valve" if reports_position => (num_value(self.cfg.f64("position_open", 100.0)), Some("open"), Some(100.0)),
            "close_valve" if reports_position => (num_value(self.cfg.f64("position_closed", 0.0)), Some("closed"), Some(0.0)),
            "open_valve" => (json!(self.cfg.payload("payload_open", "OPEN").unwrap_or_default()), Some("open"), None),
            "close_valve" => (json!(self.cfg.payload("payload_close", "CLOSE").unwrap_or_default()), Some("closed"), None),
            "stop_valve" => {
                let p = self
                    .cfg
                    .get("payload_stop")
                    .and_then(payload_str)
                    .ok_or_else(|| self.unsupported(service))?;
                (json!(p), None, None)
            }
            "set_valve_position" if reports_position => {
                let pct = data_f64(data, "position").ok_or_else(|| invalid("position required"))?;
                let v = self.unscale_pos(pct, "position_closed", "position_open", 0.0, 100.0);
                (num_value(v), Some(if pct <= 0.0 { "closed" } else { "open" }), Some(pct))
            }
            _ => return Err(self.unsupported(service)),
        };
        let payload = self.cmd_tpl("command_template", value, &Map::new())?;
        let p = self.need_publish("command_topic", payload)?;
        if self.optimistic("state_topic") {
            if let Some(s) = state {
                self.state = Some(s.into());
            }
            if let Some(pos) = pos {
                self.set_attr("current_position", num_value(pos));
            }
        }
        Ok(vec![p])
    }

    fn cmd_update(&mut self, service: &str) -> Result<Vec<Publish>, ServiceError> {
        if service != "install" {
            return Err(self.unsupported(service));
        }
        let payload = self
            .cfg
            .get("payload_install")
            .and_then(payload_str)
            .unwrap_or_else(|| "install".into());
        Ok(vec![self.need_publish("command_topic", payload)?])
    }
}

fn availability_of(cfg: &Cfg) -> Vec<AvailCfg> {
    let mut v = vec![];
    if let Some(t) = cfg.str("availability_topic") {
        v.push(AvailCfg {
            topic: t.to_string(),
            on: cfg.payload("payload_available", "online"),
            off: cfg.payload("payload_not_available", "offline"),
            template: cfg.str("availability_template").map(str::to_string),
        });
    }
    if let Some(Value::Array(items)) = cfg.get("availability") {
        for i in items {
            let c = Cfg(i.as_object().cloned().unwrap_or_default());
            let topic = match i {
                Value::String(s) => s.clone(),
                _ => match c.str("topic") {
                    Some(t) => t.to_string(),
                    None => continue,
                },
            };
            v.push(AvailCfg {
                topic,
                on: c.payload("payload_available", "online"),
                off: c.payload("payload_not_available", "offline"),
                template: c.str("value_template").map(str::to_string),
            });
        }
    }
    v
}

/// A `device_automation` trigger: fires an event when its topic receives
/// a matching payload.
#[derive(Debug, Clone)]
pub struct Trigger {
    pub discovery_id: String,
    pub discovery_topic: String,
    pub cfg: Cfg,
    pub device_id: Option<String>,
}

impl Trigger {
    pub fn topic(&self) -> Option<&str> {
        self.cfg.str("topic")
    }

    /// Event data if the payload fires this trigger.
    pub fn fire(&self, payload: &str) -> Option<Value> {
        let value = match self.cfg.str("value_template") {
            Some(t) => template::render_value(t, payload).ok()?,
            None => payload.trim().to_string(),
        };
        if let Some(expected) = self.cfg.get("payload").and_then(payload_str) {
            if expected != value {
                return None;
            }
        }
        Some(json!({
            "device_id": self.device_id,
            "discovery_id": self.discovery_id,
            "type": self.cfg.str("type"),
            "subtype": self.cfg.str("subtype"),
            "payload": value,
            "topic": self.topic(),
        }))
    }
}
