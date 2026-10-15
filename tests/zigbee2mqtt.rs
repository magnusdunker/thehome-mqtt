//! Zigbee2MQTT end to end through the MQTT core: discovery configs as
//! Zigbee2MQTT 2.x publishes them, device state messages and the commands
//! that go back to `zigbee2mqtt/<friendly_name>/set`.

mod support;

use serde_json::{json, Value};
use std::sync::Arc;

use support::{Fake, FakeHub};
use thehome_mqtt::Mqtt;

/// A connected core, with the birth message already taken.
fn setup() -> (Arc<FakeHub>, Arc<Fake>, Arc<Mqtt>) {
    let (hub, fake, mqtt) = support::setup();
    fake.take();
    (hub, fake, mqtt)
}

fn msg(m: &Mqtt, topic: &str, payload: &str) {
    m.handle_message(topic, payload.as_bytes(), 0, true);
}

fn state(hub: &FakeHub, id: &str) -> String {
    hub.state(id).unwrap_or_else(|| panic!("no state for {id}")).state
}

fn attr(hub: &FakeHub, id: &str, k: &str) -> Value {
    hub.state(id).unwrap_or_else(|| panic!("no state for {id}")).attributes.get(k).cloned().unwrap_or(Value::Null)
}

const IEEE_BRIDGE: &str = "0x00124b0022ef1234";

fn origin() -> Value {
    json!({"name": "Zigbee2MQTT", "sw": "2.1.1", "url": "https://www.zigbee2mqtt.io"})
}

fn bridge_device() -> Value {
    json!({
        "hw_version": "zStack3x0 20230507",
        "identifiers": [format!("zigbee2mqtt_bridge_{IEEE_BRIDGE}")],
        "manufacturer": "Zigbee2MQTT",
        "model": "Bridge",
        "name": "Zigbee2MQTT Bridge",
        "sw_version": "2.1.1"
    })
}

fn device(ieee: &str, name: &str, model: &str) -> Value {
    json!({
        "identifiers": [format!("zigbee2mqtt_{ieee}")],
        "manufacturer": "Test",
        "model": model,
        "model_id": model,
        "name": name,
        "via_device": format!("zigbee2mqtt_bridge_{IEEE_BRIDGE}")
    })
}

fn avail(name: &str) -> Value {
    json!([
        {"topic": "zigbee2mqtt/bridge/state", "value_template": "{{ value_json.state }}"},
        {"topic": format!("zigbee2mqtt/{name}/availability"), "value_template": "{{ value_json.state }}"}
    ])
}

/// Publish a discovery config, adding the fields every Z2M device entity has.
fn discover(m: &Mqtt, component: &str, ieee: &str, object: &str, dev: &Value, name: &str, mut cfg: Value) {
    let c = cfg.as_object_mut().unwrap();
    c.insert("availability".into(), avail(name));
    c.insert("availability_mode".into(), json!("all"));
    c.insert("device".into(), dev.clone());
    c.insert("origin".into(), origin());
    c.insert("unique_id".into(), json!(format!("{ieee}_{object}_zigbee2mqtt")));
    msg(m, &format!("homeassistant/{component}/{ieee}/{object}/config"), &cfg.to_string());
}

#[tokio::test]
async fn bridge_entities() {
    let (hub, fake, mqtt) = setup();
    let dev = bridge_device();
    let node = format!("1221051039810110150109113116116_{IEEE_BRIDGE}");
    let base = |extra: Value| {
        let mut c = json!({"device": dev.clone(), "origin": origin()});
        c.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        c.to_string()
    };
    msg(&mqtt, &format!("homeassistant/binary_sensor/{node}/connection_state/config"), &base(json!({
        "device_class": "connectivity", "entity_category": "diagnostic", "name": "Connection state",
        "object_id": "zigbee2mqtt_bridge_connection_state", "payload_off": "offline", "payload_on": "online",
        "state_topic": "zigbee2mqtt/bridge/state", "value_template": "{{ value_json.state }}",
        "unique_id": format!("bridge_{IEEE_BRIDGE}_connection_state_zigbee2mqtt")
    })));
    let bavail = json!([{"topic": "zigbee2mqtt/bridge/state", "value_template": "{{ value_json.state }}"}]);
    msg(&mqtt, &format!("homeassistant/switch/{node}/permit_join/config"), &base(json!({
        "availability": bavail, "entity_category": "config", "icon": "mdi:human-greeting-proximity",
        "name": "Permit join", "object_id": "zigbee2mqtt_bridge_permit_join",
        "command_topic": "zigbee2mqtt/bridge/request/permit_join",
        "payload_off": "{\"time\": 0}", "payload_on": "{\"time\": 254}",
        "state_topic": "zigbee2mqtt/bridge/info", "value_template": "{{ value_json.permit_join | lower }}",
        "state_off": "false", "state_on": "true",
        "unique_id": format!("bridge_{IEEE_BRIDGE}_permit_join_zigbee2mqtt")
    })));
    msg(&mqtt, &format!("homeassistant/sensor/{node}/version/config"), &base(json!({
        "availability": bavail, "entity_category": "diagnostic", "icon": "mdi:zigbee", "name": "Version",
        "object_id": "zigbee2mqtt_bridge_version", "state_topic": "zigbee2mqtt/bridge/info",
        "value_template": "{{ value_json.version }}",
        "unique_id": format!("bridge_{IEEE_BRIDGE}_version_zigbee2mqtt")
    })));
    msg(&mqtt, &format!("homeassistant/select/{node}/log_level/config"), &base(json!({
        "availability": bavail, "entity_category": "config", "name": "Log level",
        "object_id": "zigbee2mqtt_bridge_log_level", "state_topic": "zigbee2mqtt/bridge/info",
        "value_template": "{{ value_json.log_level | lower }}",
        "command_topic": "zigbee2mqtt/bridge/request/options",
        "command_template": "{\"options\": {\"advanced\": {\"log_level\": \"{{ value }}\" } } }",
        "options": ["error", "warning", "info", "debug"],
        "unique_id": format!("bridge_{IEEE_BRIDGE}_log_level_zigbee2mqtt")
    })));
    msg(&mqtt, &format!("homeassistant/button/{node}/restart/config"), &base(json!({
        "availability": bavail, "device_class": "restart", "entity_category": "config", "name": "Restart",
        "object_id": "zigbee2mqtt_bridge_restart", "command_topic": "zigbee2mqtt/bridge/request/restart",
        "payload_press": "", "unique_id": format!("bridge_{IEEE_BRIDGE}_restart_zigbee2mqtt")
    })));

    msg(&mqtt, "zigbee2mqtt/bridge/state", r#"{"state":"online"}"#);
    msg(&mqtt, "zigbee2mqtt/bridge/info", r#"{"version":"2.1.1","permit_join":false,"log_level":"info","coordinator":{"type":"zStack3x0","ieee_address":"0x00124b0022ef1234"},"config":{"homeassistant":{"enabled":true}}}"#);

    assert_eq!(state(&hub, "binary_sensor.zigbee2mqtt_bridge_connection_state"), "on");
    assert_eq!(state(&hub, "switch.zigbee2mqtt_bridge_permit_join"), "off");
    assert_eq!(state(&hub, "sensor.zigbee2mqtt_bridge_version"), "2.1.1");
    assert_eq!(state(&hub, "select.zigbee2mqtt_bridge_log_level"), "info");

    hub.call(&mqtt, "switch", "turn_on", json!({"entity_id": "switch.zigbee2mqtt_bridge_permit_join"})).unwrap();
    hub.call(&mqtt, "select", "select_option", json!({"entity_id": "select.zigbee2mqtt_bridge_log_level", "option": "debug"})).unwrap();
    hub.call(&mqtt, "button", "press", json!({"entity_id": "button.zigbee2mqtt_bridge_restart"})).unwrap();
    let p = fake.take_pairs();
    assert_eq!(p[0], ("zigbee2mqtt/bridge/request/permit_join".into(), r#"{"time": 254}"#.into()));
    let opts: Value = serde_json::from_str(&p[1].1).unwrap();
    assert_eq!(opts, json!({"options": {"advanced": {"log_level": "debug"}}}));
    assert_eq!(p[2], ("zigbee2mqtt/bridge/request/restart".into(), String::new()));

    msg(&mqtt, "zigbee2mqtt/bridge/info", r#"{"version":"2.1.1","permit_join":true,"log_level":"debug"}"#);
    assert_eq!(state(&hub, "switch.zigbee2mqtt_bridge_permit_join"), "on");

    let bridge = hub.device_named("Zigbee2MQTT Bridge");
    assert_eq!(bridge.hw_version.as_deref(), Some("zStack3x0 20230507"));

    // Devices announced after the bridge hang off it.
    let (ieee, name) = ("0x00158d00067a1b2c", "Gang PIR");
    discover(&mqtt, "sensor", ieee, "battery", &device(ieee, name, "RTCGQ11LM"), name, json!({
        "device_class": "battery", "name": null, "object_id": "gang_pir_battery",
        "state_topic": "zigbee2mqtt/Gang PIR", "unit_of_measurement": "%", "value_template": "{{ value_json.battery }}"
    }));
    let pir = hub.device_named("Gang PIR");
    assert_eq!(pir.via_device_id.as_deref(), Some(bridge.id.as_str()));
}

#[tokio::test]
async fn sensors_with_boolean_payloads() {
    let (hub, _fake, mqtt) = setup();
    let (ieee, name) = ("0x00158d00067a1b2c", "Gang PIR");
    let dev = device(ieee, name, "RTCGQ11LM");
    discover(&mqtt, "binary_sensor", ieee, "occupancy", &dev, name, json!({
        "device_class": "occupancy", "name": null, "object_id": "gang_pir_occupancy",
        "payload_off": false, "payload_on": true, "state_topic": "zigbee2mqtt/Gang PIR",
        "value_template": "{{ value_json.occupancy }}"
    }));
    discover(&mqtt, "sensor", ieee, "illuminance", &dev, name, json!({
        "device_class": "illuminance", "name": null, "object_id": "gang_pir_illuminance",
        "state_class": "measurement", "state_topic": "zigbee2mqtt/Gang PIR", "unit_of_measurement": "lx",
        "value_template": "{{ value_json.illuminance }}"
    }));
    discover(&mqtt, "sensor", ieee, "battery", &dev, name, json!({
        "device_class": "battery", "entity_category": "diagnostic", "name": null, "object_id": "gang_pir_battery",
        "state_class": "measurement", "state_topic": "zigbee2mqtt/Gang PIR", "unit_of_measurement": "%",
        "value_template": "{{ value_json.battery }}"
    }));
    discover(&mqtt, "sensor", ieee, "linkquality", &dev, name, json!({
        "enabled_by_default": false, "entity_category": "diagnostic", "icon": "mdi:signal", "name": "Linkquality",
        "object_id": "gang_pir_linkquality", "state_class": "measurement", "state_topic": "zigbee2mqtt/Gang PIR",
        "unit_of_measurement": "lqi", "value_template": "{{ value_json.linkquality }}"
    }));
    let (ieee2, name2) = ("0x00158d0001d2e3f4", "Hoveddør kontakt");
    let dev2 = device(ieee2, name2, "MCCGQ11LM");
    discover(&mqtt, "binary_sensor", ieee2, "contact", &dev2, name2, json!({
        "device_class": "door", "name": null, "object_id": "hoveddoer_kontakt_contact",
        "payload_off": true, "payload_on": false, "state_topic": "zigbee2mqtt/Hoveddør kontakt",
        "value_template": "{{ value_json.contact }}"
    }));

    for n in [name, name2] {
        msg(&mqtt, &format!("zigbee2mqtt/{n}/availability"), r#"{"state":"online"}"#);
    }
    msg(&mqtt, "zigbee2mqtt/bridge/state", r#"{"state":"online"}"#);
    msg(&mqtt, "zigbee2mqtt/Gang PIR", r#"{"occupancy":true,"illuminance":31,"battery":100,"linkquality":87,"voltage":3005}"#);
    msg(&mqtt, "zigbee2mqtt/Hoveddør kontakt", r#"{"contact":true,"battery":91}"#);

    let occ = "binary_sensor.gang_pir_occupancy";
    assert_eq!(state(&hub, occ), "on");
    assert_eq!(state(&hub, "sensor.gang_pir_illuminance"), "31");
    assert_eq!(state(&hub, "sensor.gang_pir_battery"), "100");
    assert_eq!(state(&hub, "binary_sensor.hoveddoer_kontakt_contact"), "off");
    msg(&mqtt, "zigbee2mqtt/Hoveddør kontakt", r#"{"contact":false,"battery":91}"#);
    assert_eq!(state(&hub, "binary_sensor.hoveddoer_kontakt_contact"), "on");
    msg(&mqtt, "zigbee2mqtt/Gang PIR", r#"{"occupancy":false,"illuminance":30,"battery":100,"linkquality":87}"#);
    assert_eq!(state(&hub, occ), "off");

    let lq = hub.entity("sensor.gang_pir_linkquality").unwrap();
    assert!(lq.spec.disabled_by_default, "linkquality is disabled by default");
    let bat = hub.entity("sensor.gang_pir_battery").unwrap();
    assert_eq!(bat.spec.entity_category.as_deref(), Some("diagnostic"));

    // Device offline -> unavailable.
    msg(&mqtt, "zigbee2mqtt/Gang PIR/availability", r#"{"state":"offline"}"#);
    assert_eq!(state(&hub, occ), "unavailable");
}

#[tokio::test]
async fn plug_with_config_entities_and_update() {
    let (hub, fake, mqtt) = setup();
    let (ieee, name) = ("0xa4c138d2e1f0b0c1", "Stik");
    let dev = device(ieee, name, "TS011F_plug_1");
    discover(&mqtt, "switch", ieee, "switch", &dev, name, json!({
        "command_topic": "zigbee2mqtt/Stik/set", "name": null, "object_id": "stik",
        "payload_off": "OFF", "payload_on": "ON", "state_topic": "zigbee2mqtt/Stik",
        "value_template": "{{ value_json.state }}"
    }));
    discover(&mqtt, "switch", ieee, "child_lock", &dev, name, json!({
        "command_topic": "zigbee2mqtt/Stik/set/child_lock", "entity_category": "config", "icon": "mdi:account-lock",
        "name": "Child lock", "object_id": "stik_child_lock", "payload_off": "UNLOCK", "payload_on": "LOCK",
        "state_off": "UNLOCK", "state_on": "LOCK", "state_topic": "zigbee2mqtt/Stik",
        "value_template": "{{ value_json.child_lock }}"
    }));
    discover(&mqtt, "select", ieee, "power_outage_memory", &dev, name, json!({
        "command_topic": "zigbee2mqtt/Stik/set/power_outage_memory", "entity_category": "config",
        "name": "Power outage memory", "object_id": "stik_power_outage_memory",
        "options": ["on", "off", "restore"], "state_topic": "zigbee2mqtt/Stik",
        "value_template": "{{ value_json.power_outage_memory }}"
    }));
    discover(&mqtt, "number", ieee, "countdown", &dev, name, json!({
        "command_topic": "zigbee2mqtt/Stik/set/countdown", "entity_category": "config", "name": "Countdown",
        "object_id": "stik_countdown", "max": 43200, "min": 0, "step": 1, "state_topic": "zigbee2mqtt/Stik",
        "unit_of_measurement": "s", "value_template": "{{ value_json.countdown }}"
    }));
    discover(&mqtt, "sensor", ieee, "power", &dev, name, json!({
        "device_class": "power", "name": null, "object_id": "stik_power", "state_class": "measurement",
        "state_topic": "zigbee2mqtt/Stik", "unit_of_measurement": "W", "value_template": "{{ value_json.power }}"
    }));
    discover(&mqtt, "sensor", ieee, "last_seen", &dev, name, json!({
        "device_class": "timestamp", "enabled_by_default": false, "entity_category": "diagnostic",
        "icon": "mdi:clock", "name": "Last seen", "object_id": "stik_last_seen", "state_topic": "zigbee2mqtt/Stik",
        "value_template": "{{ value_json.last_seen }}"
    }));
    discover(&mqtt, "button", ieee, "identify", &dev, name, json!({
        "command_topic": "zigbee2mqtt/Stik/set/identify", "device_class": "identify", "entity_category": "diagnostic",
        "name": "Identify", "object_id": "stik_identify", "payload_press": "identify"
    }));
    discover(&mqtt, "update", ieee, "update", &dev, name, json!({
        "command_topic": "zigbee2mqtt/bridge/request/device/ota_update/update", "device_class": "firmware",
        "entity_category": "config", "entity_picture": "https://github.com/Koenkk/zigbee2mqtt/raw/master/images/logo.png",
        "json_attributes_template": "{\"in_progress\": {{ iif(value_json['update']['state'] == 'updating', 'true', 'false') }} }",
        "json_attributes_topic": "zigbee2mqtt/Stik",
        "latest_version_template": "{{ value_json['update']['latest_version'] }}",
        "latest_version_topic": "zigbee2mqtt/Stik", "name": null, "object_id": "stik",
        "payload_install": format!("{{\"id\": \"{ieee}\"}}"), "state_topic": "zigbee2mqtt/Stik",
        "value_template": "{{ value_json['update']['installed_version'] }}"
    }));

    msg(&mqtt, "zigbee2mqtt/bridge/state", r#"{"state":"online"}"#);
    msg(&mqtt, "zigbee2mqtt/Stik/availability", r#"{"state":"online"}"#);
    msg(&mqtt, "zigbee2mqtt/Stik", &json!({
        "state": "ON", "child_lock": "UNLOCK", "power_outage_memory": "restore", "countdown": 0, "power": 41.2,
        "last_seen": "2026-10-12T08:15:00+02:00", "linkquality": 160,
        "update": {"state": "available", "installed_version": 192, "latest_version": 200}
    }).to_string());

    assert_eq!(state(&hub, "switch.stik"), "on");
    assert_eq!(state(&hub, "switch.stik_child_lock"), "off");
    assert_eq!(state(&hub, "select.stik_power_outage_memory"), "restore");
    assert_eq!(state(&hub, "number.stik_countdown"), "0");
    assert_eq!(state(&hub, "sensor.stik_power"), "41.2");
    assert_eq!(state(&hub, "update.stik"), "on");
    assert_eq!(attr(&hub, "update.stik", "installed_version"), "192");
    assert_eq!(attr(&hub, "update.stik", "latest_version"), "200");
    assert_eq!(attr(&hub, "update.stik", "in_progress"), false);

    let calls = [
        ("switch", "turn_off", json!({"entity_id": "switch.stik"})),
        ("switch", "turn_on", json!({"entity_id": "switch.stik_child_lock"})),
        ("select", "select_option", json!({"entity_id": "select.stik_power_outage_memory", "option": "off"})),
        ("number", "set_value", json!({"entity_id": "number.stik_countdown", "value": 600})),
        ("button", "press", json!({"entity_id": "button.stik_identify"})),
        ("update", "install", json!({"entity_id": "update.stik"})),
    ];
    for (d, s, data) in calls {
        hub.call(&mqtt, d, s, data).unwrap_or_else(|e| panic!("{d}.{s}: {e}"));
    }
    let got = fake.take_pairs();
    let got: Vec<(&str, &str)> = got.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
    let install = format!("{{\"id\": \"{ieee}\"}}");
    assert_eq!(
        got,
        vec![
            ("zigbee2mqtt/Stik/set", "OFF"),
            ("zigbee2mqtt/Stik/set/child_lock", "LOCK"),
            ("zigbee2mqtt/Stik/set/power_outage_memory", "off"),
            ("zigbee2mqtt/Stik/set/countdown", "600"),
            ("zigbee2mqtt/Stik/set/identify", "identify"),
            ("zigbee2mqtt/bridge/request/device/ota_update/update", install.as_str()),
        ]
    );

    msg(&mqtt, "zigbee2mqtt/Stik", r#"{"state":"OFF","update":{"state":"updating","installed_version":192,"latest_version":200,"progress":12}}"#);
    assert_eq!(attr(&hub, "update.stik", "in_progress"), true);
}

#[tokio::test]
async fn thermostat_cover_lock_fan() {
    let (hub, fake, mqtt) = setup();

    let (ieee, name) = ("0x847127fffe1a2b3c", "Radiator");
    let dev = device(ieee, name, "TS0601_thermostat");
    discover(&mqtt, "climate", ieee, "climate", &dev, name, json!({
        "action_template": "{% set values = {None:None,'idle':'idle','heat':'heating','cool':'cooling','fan_only':'fan'} %}{{ values[value_json.running_state] }}",
        "action_topic": "zigbee2mqtt/Radiator",
        "current_temperature_template": "{{ value_json.local_temperature }}",
        "current_temperature_topic": "zigbee2mqtt/Radiator",
        "max_temp": "35", "min_temp": "5",
        "mode_command_topic": "zigbee2mqtt/Radiator/set/system_mode",
        "mode_state_template": "{{ value_json.system_mode }}",
        "mode_state_topic": "zigbee2mqtt/Radiator",
        "modes": ["off", "heat", "auto"],
        "name": null, "object_id": "radiator",
        "preset_mode_command_topic": "zigbee2mqtt/Radiator/set/preset",
        "preset_mode_state_topic": "zigbee2mqtt/Radiator",
        "preset_mode_value_template": "{{ value_json.preset }}",
        "preset_modes": ["manual", "boost", "eco"],
        "temp_step": 0.5,
        "temperature_command_topic": "zigbee2mqtt/Radiator/set/current_heating_setpoint",
        "temperature_state_template": "{{ value_json.current_heating_setpoint }}",
        "temperature_state_topic": "zigbee2mqtt/Radiator",
        "temperature_unit": "C"
    }));

    let (ieee, cname) = ("0x5c0272fffe9a8b7c", "Gardin");
    let dev = device(ieee, cname, "E1757");
    discover(&mqtt, "cover", ieee, "cover", &dev, cname, json!({
        "command_topic": "zigbee2mqtt/Gardin/set", "name": null, "object_id": "gardin",
        "position_template": "{{ value_json.position }}", "position_topic": "zigbee2mqtt/Gardin",
        "set_position_template": "{ \"position\": {{ position }} }", "set_position_topic": "zigbee2mqtt/Gardin/set",
        "state_closed": "CLOSE", "state_open": "OPEN", "state_stopped": "STOP",
        "state_topic": "zigbee2mqtt/Gardin", "value_template": "{{ value_json.state }}"
    }));

    let (ieee, lname) = ("0x000d6f0011223344", "Dørlås");
    let dev = device(ieee, lname, "YRD256HA20BP");
    discover(&mqtt, "lock", ieee, "lock", &dev, lname, json!({
        "command_topic": "zigbee2mqtt/Dørlås/set", "name": null, "object_id": "doerlaas",
        "payload_lock": "LOCK", "payload_unlock": "UNLOCK", "state_locked": "LOCK", "state_unlocked": "UNLOCK",
        "state_topic": "zigbee2mqtt/Dørlås", "value_template": "{{ value_json.state }}"
    }));

    let (ieee, fname) = ("0x000d6ffffe556677", "Loftsventilator");
    let dev = device(ieee, fname, "99432");
    discover(&mqtt, "fan", ieee, "fan", &dev, fname, json!({
        "command_topic": "zigbee2mqtt/Loftsventilator/set/fan_state", "name": null, "object_id": "loftsventilator",
        "percentage_command_template": "{{ {0: 'off', 1: 'low', 2: 'medium', 3: 'high', 4: 'on'}[value] | default('') }}",
        "percentage_command_topic": "zigbee2mqtt/Loftsventilator/set/fan_mode",
        "percentage_state_topic": "zigbee2mqtt/Loftsventilator",
        "percentage_value_template": "{{ {'off': 0, 'low': 1, 'medium': 2, 'high': 3, 'on': 4}[value_json.fan_mode] | default('None') }}",
        "preset_mode_command_topic": "zigbee2mqtt/Loftsventilator/set/fan_mode",
        "preset_mode_state_topic": "zigbee2mqtt/Loftsventilator",
        "preset_mode_value_template": "{{ value_json.fan_mode if value_json.fan_mode in ['smart'] else 'None' | default('None') }}",
        "preset_modes": ["smart"], "speed_range_max": 4, "speed_range_min": 1,
        "state_topic": "zigbee2mqtt/Loftsventilator", "state_value_template": "{{ value_json.fan_state }}"
    }));

    msg(&mqtt, "zigbee2mqtt/bridge/state", r#"{"state":"online"}"#);
    for n in [name, cname, lname, fname] {
        msg(&mqtt, &format!("zigbee2mqtt/{n}/availability"), r#"{"state":"online"}"#);
    }
    msg(&mqtt, "zigbee2mqtt/Radiator", r#"{"system_mode":"heat","local_temperature":19.5,"current_heating_setpoint":21,"preset":"manual","running_state":"heat","battery":80}"#);
    msg(&mqtt, "zigbee2mqtt/Gardin", r#"{"state":"OPEN","position":60,"battery":70}"#);
    msg(&mqtt, "zigbee2mqtt/Dørlås", r#"{"state":"LOCK","lock_state":"locked","battery":90}"#);
    msg(&mqtt, "zigbee2mqtt/Loftsventilator", r#"{"fan_state":"ON","fan_mode":"medium"}"#);

    let c = "climate.radiator";
    assert_eq!(state(&hub, c), "heat");
    assert_eq!(attr(&hub, c, "current_temperature"), 19.5);
    assert_eq!(attr(&hub, c, "temperature"), 21);
    assert_eq!(attr(&hub, c, "hvac_action"), "heating");
    assert_eq!(attr(&hub, c, "preset_mode"), "manual");
    assert_eq!(attr(&hub, c, "min_temp"), 5);
    assert_eq!(attr(&hub, c, "max_temp"), 35);
    assert_eq!(attr(&hub, c, "target_temp_step"), 0.5);
    msg(&mqtt, "zigbee2mqtt/Radiator", r#"{"system_mode":"heat","local_temperature":19.5,"current_heating_setpoint":21,"preset":"manual","running_state":"idle"}"#);
    assert_eq!(attr(&hub, c, "hvac_action"), "idle");

    assert_eq!(state(&hub, "cover.gardin"), "open");
    assert_eq!(attr(&hub, "cover.gardin", "current_position"), 60);
    assert_eq!(state(&hub, "lock.doerlaas"), "locked");
    assert_eq!(state(&hub, "fan.loftsventilator"), "on");
    assert_eq!(attr(&hub, "fan.loftsventilator", "percentage"), 50);
    assert_eq!(attr(&hub, "fan.loftsventilator", "preset_mode"), Value::Null);

    let calls = [
        ("climate", "set_temperature", json!({"entity_id": c, "temperature": 22.5})),
        ("climate", "set_hvac_mode", json!({"entity_id": c, "hvac_mode": "off"})),
        ("climate", "set_preset_mode", json!({"entity_id": c, "preset_mode": "eco"})),
        ("cover", "set_cover_position", json!({"entity_id": "cover.gardin", "position": 25})),
        ("cover", "close_cover", json!({"entity_id": "cover.gardin"})),
        ("lock", "unlock", json!({"entity_id": "lock.doerlaas"})),
        ("fan", "set_percentage", json!({"entity_id": "fan.loftsventilator", "percentage": 100})),
        ("fan", "set_preset_mode", json!({"entity_id": "fan.loftsventilator", "preset_mode": "smart"})),
        ("fan", "turn_off", json!({"entity_id": "fan.loftsventilator"})),
    ];
    for (d, s, data) in calls {
        hub.call(&mqtt, d, s, data).unwrap_or_else(|e| panic!("{d}.{s}: {e}"));
    }
    let got = fake.take_pairs();
    let got: Vec<(&str, &str)> = got.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
    assert_eq!(
        got,
        vec![
            ("zigbee2mqtt/Radiator/set/current_heating_setpoint", "22.5"),
            ("zigbee2mqtt/Radiator/set/system_mode", "off"),
            ("zigbee2mqtt/Radiator/set/preset", "eco"),
            ("zigbee2mqtt/Gardin/set", "{ \"position\": 25 }"),
            ("zigbee2mqtt/Gardin/set", "CLOSE"),
            ("zigbee2mqtt/Dørlås/set", "UNLOCK"),
            ("zigbee2mqtt/Loftsventilator/set/fan_mode", "on"),
            ("zigbee2mqtt/Loftsventilator/set/fan_mode", "smart"),
            ("zigbee2mqtt/Loftsventilator/set/fan_state", "OFF"),
        ]
    );
}

#[tokio::test]
async fn hue_light_xy_and_device_trigger() {
    let (hub, fake, mqtt) = setup();
    let (ieee, name) = ("0x0017880108a1b2c3", "Køkken spot");
    let dev = device(ieee, name, "929002335001");
    discover(&mqtt, "light", ieee, "light", &dev, name, json!({
        "brightness": true, "brightness_scale": 254, "command_topic": "zigbee2mqtt/Køkken spot/set",
        "effect": true, "effect_list": ["blink", "breathe", "okay", "channel_change", "finish_effect", "stop_effect"],
        "max_mireds": 500, "min_mireds": 153, "name": null, "object_id": "koekken_spot", "schema": "json",
        "state_topic": "zigbee2mqtt/Køkken spot", "supported_color_modes": ["xy", "color_temp"]
    }));
    msg(&mqtt, "zigbee2mqtt/bridge/state", r#"{"state":"online"}"#);
    msg(&mqtt, "zigbee2mqtt/Køkken spot/availability", r#"{"state":"online"}"#);
    msg(&mqtt, "zigbee2mqtt/Køkken spot", r#"{"state":"ON","brightness":127,"color_mode":"xy","color":{"x":0.4,"y":0.4,"hue":40,"saturation":60},"color_temp":300}"#);
    let id = "light.koekken_spot";
    assert_eq!(state(&hub, id), "on");
    assert_eq!(attr(&hub, id, "color_mode"), "xy");
    assert_eq!(attr(&hub, id, "xy_color"), json!([0.4, 0.4]));
    hub.call(&mqtt, "light", "turn_on", json!({"entity_id": id, "xy_color": [0.3, 0.3]})).unwrap();
    let p = fake.take_pairs();
    let body: Value = serde_json::from_str(&p[0].1).unwrap();
    assert_eq!(body, json!({"state": "ON", "color": {"x": 0.3, "y": 0.3}}));

    // Remote: Z2M 2.x publishes device triggers per action.
    let (ieee, rname) = ("0x001788010c4d5e6f", "Fjernbetjening");
    hub.take_events();
    msg(&mqtt, &format!("homeassistant/device_automation/{ieee}/action_on_press/config"), &json!({
        "automation_type": "trigger", "device": device(ieee, rname, "324131092621"), "origin": origin(),
        "payload": "on_press", "subtype": "on_press", "topic": "zigbee2mqtt/Fjernbetjening/action", "type": "action"
    }).to_string());
    msg(&mqtt, "zigbee2mqtt/Fjernbetjening/action", "on_press");
    let events = hub.take_events();
    assert!(events.iter().any(|(t, d)| t == "mqtt_device_trigger" && d["subtype"] == "on_press"), "{events:?}");
}

#[tokio::test]
async fn device_before_bridge_is_linked_later() {
    let (hub, _fake, mqtt) = setup();
    let (ieee, name) = ("0x00158d00067a1b2c", "Gang PIR");
    discover(&mqtt, "sensor", ieee, "battery", &device(ieee, name, "RTCGQ11LM"), name, json!({
        "device_class": "battery", "name": null, "object_id": "gang_pir_battery",
        "state_topic": "zigbee2mqtt/Gang PIR", "unit_of_measurement": "%", "value_template": "{{ value_json.battery }}"
    }));
    let pir = || hub.device_named("Gang PIR");
    assert!(pir().via_device_id.is_none());
    let node = format!("1221051039810110150109113116116_{IEEE_BRIDGE}");
    msg(&mqtt, &format!("homeassistant/sensor/{node}/version/config"), &json!({
        "device": bridge_device(), "origin": origin(), "name": "Version", "state_topic": "zigbee2mqtt/bridge/info",
        "value_template": "{{ value_json.version }}", "unique_id": format!("bridge_{IEEE_BRIDGE}_version_zigbee2mqtt")
    }).to_string());
    let bridge = hub.device_named("Zigbee2MQTT Bridge");
    assert_eq!(pir().via_device_id, Some(bridge.id));
}

/// Wait until the core has published something.
async fn next_publish(fake: &Fake) -> (String, String) {
    for _ in 0..100 {
        if let Some(p) = fake.take_pairs().pop() {
            return p;
        }
        tokio::task::yield_now().await;
    }
    panic!("nothing published");
}

#[tokio::test]
async fn bridge_detection_and_requests() {
    let (hub, fake, mqtt) = setup();
    let issue = "zigbee2mqtt_zigbee2mqtt";
    let has_issue = || hub.issues().iter().any(|i| i.issue_id == issue);

    // Zigbee2MQTT 2.x with the Home Assistant option off: no entities, so
    // tell the user.
    let off = r#"{"version":"2.1.1","coordinator":{"type":"zStack3x0","ieee_address":"0x00124b0022ef1234"},"permit_join":false,
        "config":{"homeassistant":{"enabled":false,"discovery_topic":"homeassistant","status_topic":"homeassistant/status"}}}"#;
    msg(&mqtt, "zigbee2mqtt/bridge/info", off);
    assert!(has_issue());
    let bridges = mqtt.bridges();
    assert_eq!(bridges.len(), 1);
    assert_eq!(bridges[0].discovery, Some(false));
    assert_eq!(mqtt.debug_info()["zigbee2mqtt"][0]["base_topic"], "zigbee2mqtt");

    // Requests carry a transaction id and resolve with the bridge's reply.
    let m = mqtt.clone();
    let req = tokio::spawn(async move {
        m.bridge_request("zigbee2mqtt", "options", json!({"options": {"homeassistant": {"enabled": true}}})).await
    });
    let (topic, body) = next_publish(&fake).await;
    assert_eq!(topic, "zigbee2mqtt/bridge/request/options");
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["options"]["homeassistant"]["enabled"], true);
    let tr = body["transaction"].as_str().unwrap().to_string();
    // A reply to someone else's request is ignored.
    msg(&mqtt, "zigbee2mqtt/bridge/response/options", r#"{"data":{},"status":"ok","transaction":"other"}"#);
    msg(
        &mqtt,
        "zigbee2mqtt/bridge/response/options",
        &json!({"data": {"restart_required": true}, "status": "ok", "transaction": tr}).to_string(),
    );
    assert_eq!(req.await.unwrap().unwrap(), json!({"restart_required": true}));

    let m = mqtt.clone();
    let req = tokio::spawn(async move {
        m.bridge_request("zigbee2mqtt", "device/remove", json!({"id": "Findes ikke"})).await
    });
    let (_, body) = next_publish(&fake).await;
    let tr = serde_json::from_str::<Value>(&body).unwrap()["transaction"].clone();
    msg(
        &mqtt,
        "zigbee2mqtt/bridge/response/device/remove",
        &json!({"data": {"id": "Findes ikke"}, "status": "error", "error": "Device 'Findes ikke' does not exist", "transaction": tr}).to_string(),
    );
    let err = req.await.unwrap().unwrap_err().to_string();
    assert!(err.contains("does not exist"), "{err}");

    // Discovery on again: the issue goes away.
    msg(&mqtt, "zigbee2mqtt/bridge/info", &off.replace(r#""enabled":false"#, r#""enabled":true"#));
    assert!(!has_issue());
    // Discovery to another prefix is a problem too.
    msg(&mqtt, "zigbee2mqtt/bridge/info", &off.replace(r#""enabled":false,"discovery_topic":"homeassistant""#, r#""enabled":true,"discovery_topic":"ha""#));
    assert!(hub.issues().iter().any(|i| i.issue_id == issue && i.description.contains("«ha»")));

    // A TheHome device resolves to the bridge and IEEE address.
    let (ieee, name) = ("0xa4c138d2e1f0b0c1", "Stik");
    discover(&mqtt, "switch", ieee, "switch", &device(ieee, name, "TS011F"), name, json!({
        "command_topic": "zigbee2mqtt/Stik/set", "name": null, "state_topic": "zigbee2mqtt/Stik",
        "value_template": "{{ value_json.state }}"
    }));
    let dev = hub.device_named("Stik");
    assert_eq!(mqtt.zigbee2mqtt_device(&dev.id), Some(("zigbee2mqtt".into(), ieee.into())));

    // Not connected: requests fail straight away.
    mqtt.on_disconnected();
    assert!(mqtt.bridge_request("zigbee2mqtt", "permit_join", json!({"time": 254})).await.is_err());
}
