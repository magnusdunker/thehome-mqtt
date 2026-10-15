//! Feed discovery and state messages into the MQTT core with a fake
//! transport and hub, and check the resulting states and published commands.

mod support;

use serde_json::{json, Value};
use support::{fixture, setup, FakeHub, State};
use thehome_mqtt::Mqtt;

fn msg(m: &Mqtt, topic: &str, payload: &str) {
    m.handle_message(topic, payload.as_bytes(), 0, false);
}

fn state(hub: &FakeHub, id: &str) -> State {
    hub.state(id).unwrap_or_else(|| panic!("no state for {id}"))
}

#[tokio::test]
async fn z2m_light_json_schema() {
    let (hub, fake, mqtt) = setup();
    assert!(fake.subscribed.lock().contains(&"homeassistant/#".to_string()));
    let birth = fake.take();
    assert_eq!(birth[0].topic, "homeassistant/status");
    assert_eq!(birth[0].payload, "online");

    msg(&mqtt, "homeassistant/light/0x000b57fffec6a5b2/light/config", &fixture("z2m_bulb_config.json"));
    let id = "light.stue_lampe";
    // Unavailable until both availability topics report online.
    assert_eq!(state(&hub, id).state, "unavailable");
    let dev = hub.device_named("Stue lampe");
    assert_eq!(dev.manufacturer.as_deref(), Some("IKEA"));

    msg(&mqtt, "zigbee2mqtt/bridge/state", r#"{"state":"online"}"#);
    assert_eq!(state(&hub, id).state, "unavailable");
    msg(&mqtt, "zigbee2mqtt/Stue lampe/availability", r#"{"state":"online"}"#);
    assert_eq!(state(&hub, id).state, "unknown");

    msg(
        &mqtt,
        "zigbee2mqtt/Stue lampe",
        r#"{"state":"ON","brightness":254,"color_temp":370,"color_mode":"color_temp","linkquality":120}"#,
    );
    let s = state(&hub, id);
    assert_eq!(s.state, "on");
    assert_eq!(s.attributes["brightness"], 255);
    assert_eq!(s.attributes["color_temp_kelvin"], 2703);
    assert_eq!(s.attributes["color_mode"], "color_temp");
    assert_eq!(s.attributes["supported_color_modes"], json!(["color_temp"]));
    assert_eq!(s.attributes["min_color_temp_kelvin"], 2203);
    assert_eq!(s.attributes["max_color_temp_kelvin"], 4000);
    assert_eq!(s.attributes["effect_list"], json!(["blink", "breathe", "okay"]));

    hub.call(&mqtt, "light", "turn_on", json!({ "entity_id": id, "brightness_pct": 50, "color_temp_kelvin": 4000, "transition": 2 })).unwrap();
    let p = fake.take();
    assert_eq!(p.len(), 1);
    assert_eq!(p[0].topic, "zigbee2mqtt/Stue lampe/set");
    let body: Value = serde_json::from_str(&p[0].payload).unwrap();
    assert_eq!(body, json!({ "transition": 2, "state": "ON", "brightness": 127, "color_temp": 250 }));

    hub.call(&mqtt, "light", "toggle", json!({ "entity_id": id })).unwrap();
    let body: Value = serde_json::from_str(&fake.take()[0].payload).unwrap();
    assert_eq!(body, json!({ "state": "OFF" }));

    // Bridge offline -> unavailable again.
    msg(&mqtt, "zigbee2mqtt/bridge/state", r#"{"state":"offline"}"#);
    assert_eq!(state(&hub, id).state, "unavailable");

    // Broker disconnect makes everything unavailable; reconnect resubscribes.
    msg(&mqtt, "zigbee2mqtt/bridge/state", r#"{"state":"online"}"#);
    mqtt.on_disconnected();
    assert_eq!(state(&hub, id).state, "unavailable");
    fake.subscribed.lock().clear();
    mqtt.on_connected();
    assert!(fake.subscribed.lock().contains(&"zigbee2mqtt/Stue lampe".to_string()));
    assert_eq!(state(&hub, id).state, "on");

    // Empty config removes the entity.
    msg(&mqtt, "homeassistant/light/0x000b57fffec6a5b2/light/config", "");
    assert!(hub.state(id).is_none());
    assert!(hub.entity(id).is_none());
}

#[tokio::test]
async fn z2m_sensor_and_attributes() {
    let (hub, _fake, mqtt) = setup();
    msg(&mqtt, "zigbee2mqtt/bridge/state", r#"{"state":"online"}"#);
    msg(&mqtt, "homeassistant/sensor/0x00158d0001a2b3c4/temperature/config", &fixture("z2m_temp_config.json"));
    // Availability message arrived before discovery, so it is still unknown.
    msg(&mqtt, "zigbee2mqtt/bridge/state", r#"{"state":"online"}"#);
    msg(&mqtt, "zigbee2mqtt/Bad sensor", r#"{"temperature":21.37,"humidity":48.2,"battery":97}"#);
    let s = state(&hub, "sensor.bad_sensor_temperature");
    assert_eq!(s.state, "21.37");
    assert_eq!(s.attributes["unit_of_measurement"], "°C");
    assert_eq!(s.attributes["device_class"], "temperature");
    assert_eq!(s.attributes["state_class"], "measurement");
    assert_eq!(s.attributes["humidity"], 48.2);
    assert_eq!(s.attributes["friendly_name"], "Bad sensor Temperature");
}

#[tokio::test]
async fn tasmota_switch_abbreviated() {
    let (hub, fake, mqtt) = setup();
    msg(&mqtt, "homeassistant/switch/ABC123_RL_1/config", &fixture("tasmota_switch_config.json"));
    let id = "switch.tasmota_stik_relay";
    assert_eq!(state(&hub, id).state, "unavailable");
    msg(&mqtt, "tasmota_ABC123/tele/LWT", "Online");
    msg(&mqtt, "tasmota_ABC123/stat/POWER1", "ON");
    assert_eq!(state(&hub, id).state, "on");
    let dev = hub.device_named("Tasmota stik");
    assert_eq!(dev.connections, vec![("mac".to_string(), "aa:bb:cc:dd:ee:ff".to_string())]);

    fake.take();
    hub.call(&mqtt, "switch", "turn_off", json!({ "entity_id": id })).unwrap();
    let p = fake.take();
    assert_eq!((p[0].topic.as_str(), p[0].payload.as_str()), ("tasmota_ABC123/cmnd/POWER1", "OFF"));
    // Not optimistic: state waits for the device.
    assert_eq!(state(&hub, id).state, "on");
}

#[tokio::test]
async fn cover_and_climate() {
    let (hub, fake, mqtt) = setup();
    fake.take();
    msg(&mqtt, "homeassistant/cover/c1/config", &fixture("cover_config.json"));
    msg(&mqtt, "home/cover/position", "40");
    msg(&mqtt, "home/cover/state", "open");
    let s = state(&hub, "cover.gardin");
    assert_eq!(s.state, "open");
    assert_eq!(s.attributes["current_position"], 40);
    assert_eq!(s.attributes["supported_features"], 15);
    hub.call(&mqtt, "cover", "set_cover_position", json!({ "entity_id": "cover.gardin", "position": 75 })).unwrap();
    hub.call(&mqtt, "cover", "toggle", json!({ "entity_id": "cover.gardin" })).unwrap();
    let p = fake.take();
    assert_eq!((p[0].topic.as_str(), p[0].payload.as_str()), ("home/cover/set_position", "75"));
    assert_eq!((p[1].topic.as_str(), p[1].payload.as_str()), ("home/cover/set", "CLOSE"));

    msg(&mqtt, "homeassistant/climate/trv/config", &fixture("climate_config.json"));
    msg(&mqtt, "trv/state", r#"{"mode":"heat","target":21.5,"current":19.8}"#);
    let s = state(&hub, "climate.termostat");
    assert_eq!(s.state, "heat");
    assert_eq!(s.attributes["temperature"], 21.5);
    assert_eq!(s.attributes["current_temperature"], 19.8);
    assert_eq!(s.attributes["hvac_modes"], json!(["off", "heat"]));
    assert_eq!(s.attributes["min_temp"], 5);
    hub.call(&mqtt, "climate", "set_temperature", json!({ "entity_id": "climate.termostat", "temperature": 22 })).unwrap();
    hub.call(&mqtt, "climate", "turn_off", json!({ "entity_id": "climate.termostat" })).unwrap();
    let p = fake.take();
    assert_eq!((p[0].topic.as_str(), p[0].payload.as_str()), ("trv/temp/set", "22"));
    assert_eq!((p[1].topic.as_str(), p[1].payload.as_str()), ("trv/mode/set", "off"));
}

#[tokio::test]
async fn device_trigger_and_user_subscription() {
    let (hub, fake, mqtt) = setup();
    msg(&mqtt, "homeassistant/device_automation/0x00158d00045e6f70/action_single/config", &fixture("z2m_action_trigger.json"));
    assert!(fake.subscribed.lock().contains(&"zigbee2mqtt/Kontakt/action".to_string()));
    msg(&mqtt, "zigbee2mqtt/Kontakt/action", "double");
    msg(&mqtt, "zigbee2mqtt/Kontakt/action", "single");

    mqtt.subscribe_user("garage/+/door", 0).unwrap();
    msg(&mqtt, "garage/left/door", "open");

    let seen = hub.take_events();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert_eq!(seen[0].0, "mqtt_device_trigger");
    assert_eq!(seen[0].1["type"], "action");
    assert_eq!(seen[0].1["subtype"], "single");
    // TheHome's device id, not the core's device key
    assert_eq!(seen[0].1["device_id"], json!(hub.device_named("Kontakt").id));
    assert_eq!(seen[1].0, "mqtt_message");
    assert_eq!(seen[1].1, json!({ "topic": "garage/left/door", "payload": "open", "qos": 0, "retain": false }));

    mqtt.unsubscribe_user("garage/+/door");
    assert!(fake.unsubscribed.lock().contains(&"garage/+/door".to_string()));
}

#[tokio::test]
async fn misc_components() {
    let (hub, fake, mqtt) = setup();
    fake.take();
    let cfgs = [
        ("homeassistant/binary_sensor/door/config", json!({"name":"Dør","uniq_id":"bs1","stat_t":"door/state","pl_on":"open","pl_off":"closed","dev_cla":"door"})),
        ("homeassistant/fan/f1/config", json!({"name":"Ventilator","uniq_id":"f1","cmd_t":"fan/set","stat_t":"fan/state","pct_cmd_t":"fan/speed/set","pct_stat_t":"fan/speed","spd_rng_min":1,"spd_rng_max":3})),
        ("homeassistant/lock/l1/config", json!({"name":"Hoveddør","uniq_id":"l1","cmd_t":"lock/set","stat_t":"lock/state"})),
        ("homeassistant/number/n1/config", json!({"name":"Niveau","uniq_id":"n1","cmd_t":"num/set","stat_t":"num/state","min":0,"max":10,"step":0.5})),
        ("homeassistant/select/s1/config", json!({"name":"Tilstand","uniq_id":"s1","cmd_t":"sel/set","stat_t":"sel/state","ops":["a","b","c"]})),
        ("homeassistant/button/b1/config", json!({"name":"Genstart","uniq_id":"b1","cmd_t":"btn/set","pl_prs":"restart"})),
        ("homeassistant/event/e1/config", json!({"name":"Knap","uniq_id":"e1","stat_t":"evt/state","evt_typ":["press","hold"]})),
        ("homeassistant/update/u1/config", json!({"name":"Firmware","uniq_id":"u1","stat_t":"upd/state","cmd_t":"upd/set","pl_inst":"go"})),
        ("homeassistant/valve/v1/config", json!({"name":"Ventil","uniq_id":"v1","cmd_t":"valve/set","stat_t":"valve/state"})),
        ("homeassistant/siren/si1/config", json!({"name":"Sirene","uniq_id":"si1","cmd_t":"siren/set","stat_t":"siren/state","available_tones":["ding","alarm"]})),
        ("homeassistant/text/t1/config", json!({"name":"Besked","uniq_id":"t1","cmd_t":"text/set","stat_t":"text/state","max":10})),
        ("homeassistant/light/rgb/config", json!({"name":"Strip","uniq_id":"rgb1","cmd_t":"strip/set","stat_t":"strip/state","bri_cmd_t":"strip/bri/set","bri_stat_t":"strip/bri","rgb_cmd_t":"strip/rgb/set","rgb_stat_t":"strip/rgb"})),
    ];
    for (t, c) in &cfgs {
        msg(&mqtt, t, &c.to_string());
    }
    msg(&mqtt, "door/state", "open");
    msg(&mqtt, "fan/state", "ON");
    msg(&mqtt, "fan/speed", "2");
    msg(&mqtt, "lock/state", "LOCKED");
    msg(&mqtt, "num/state", "7.5");
    msg(&mqtt, "sel/state", "b");
    msg(&mqtt, "evt/state", r#"{"event_type":"hold","duration":3}"#);
    msg(&mqtt, "upd/state", r#"{"installed_version":"1.0","latest_version":"1.1"}"#);
    msg(&mqtt, "valve/state", "closed");
    msg(&mqtt, "siren/state", "OFF");
    msg(&mqtt, "text/state", "hej");
    msg(&mqtt, "strip/state", "ON");
    msg(&mqtt, "strip/bri", "128");
    msg(&mqtt, "strip/rgb", "255,0,0");

    assert_eq!(state(&hub, "binary_sensor.doer").state, "on");
    let fan = state(&hub, "fan.ventilator");
    assert_eq!((fan.state.as_str(), &fan.attributes["percentage"]), ("on", &json!(66)));
    assert_eq!(state(&hub, "lock.hoveddoer").state, "locked");
    assert_eq!(state(&hub, "number.niveau").state, "7.5");
    assert_eq!(state(&hub, "select.tilstand").state, "b");
    let ev = state(&hub, "event.knap");
    assert_eq!(ev.attributes["event_type"], "hold");
    assert_eq!(ev.attributes["duration"], 3);
    assert_eq!(ev.attributes["event_types"], json!(["press", "hold"]));
    assert_eq!(state(&hub, "update.firmware").state, "on");
    assert_eq!(state(&hub, "valve.ventil").state, "closed");
    assert_eq!(state(&hub, "siren.sirene").state, "off");
    assert_eq!(state(&hub, "text.besked").state, "hej");
    let strip = state(&hub, "light.strip");
    assert_eq!(strip.attributes["brightness"], 128);
    assert_eq!(strip.attributes["rgb_color"], json!([255, 0, 0]));
    assert_eq!(strip.attributes["color_mode"], "rgb");

    let calls = [
        ("fan", "set_percentage", json!({"entity_id":"fan.ventilator","percentage":100})),
        ("lock", "unlock", json!({"entity_id":"lock.hoveddoer"})),
        ("number", "set_value", json!({"entity_id":"number.niveau","value":2})),
        ("select", "select_next", json!({"entity_id":"select.tilstand"})),
        ("button", "press", json!({"entity_id":"button.genstart"})),
        ("update", "install", json!({"entity_id":"update.firmware"})),
        ("valve", "open_valve", json!({"entity_id":"valve.ventil"})),
        ("siren", "turn_on", json!({"entity_id":"siren.sirene","tone":"ding"})),
        ("text", "set_value", json!({"entity_id":"text.besked","value":"farvel"})),
        ("light", "turn_on", json!({"entity_id":"light.strip","hs_color":[120,100],"brightness":200})),
    ];
    for (d, s, data) in calls {
        hub.call(&mqtt, d, s, data).unwrap();
    }
    let got: Vec<(String, String)> = fake.take().into_iter().map(|p| (p.topic, p.payload)).collect();
    let expect: Vec<(&str, &str)> = vec![
        ("fan/speed/set", "3"),
        ("lock/set", "UNLOCK"),
        ("num/set", "2"),
        ("sel/set", "c"),
        ("btn/set", "restart"),
        ("upd/set", "go"),
        ("valve/set", "OPEN"),
        ("siren/set", r#"{"tone":"ding","state":"ON"}"#),
        ("text/set", "farvel"),
        ("strip/bri/set", "200"),
        ("strip/rgb/set", "0,255,0"),
        ("strip/set", "ON"),
    ];
    let got_ref: Vec<(&str, &str)> = got.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
    assert_eq!(got_ref, expect);

    // Invalid values are rejected without publishing.
    assert!(hub.call(&mqtt, "number", "set_value", json!({"entity_id":"number.niveau","value":11})).is_err());
    assert!(fake.take().is_empty());

    // Button state is the press time.
    assert!(state(&hub, "button.genstart").state.contains('T'));
}

#[tokio::test]
async fn device_based_discovery_and_debug_info() {
    let (hub, _fake, mqtt) = setup();
    let payload = json!({
        "dev": {"ids": "plug42", "name": "Stik 42", "mf": "Acme"},
        "o": {"name": "acme2mqtt"},
        "stat_t": "plug42/state",
        "cmps": {
            "power": {"p": "sensor", "val_tpl": "{{ value_json.power }}", "unit_of_meas": "W", "dev_cla": "power", "uniq_id": "plug42_power", "name": "Effekt"},
            "relay": {"p": "switch", "cmd_t": "plug42/set", "val_tpl": "{{ value_json.relay }}", "uniq_id": "plug42_relay", "name": null}
        }
    });
    msg(&mqtt, "homeassistant/device/plug42/config", &payload.to_string());
    msg(&mqtt, "plug42/state", r#"{"power": 12.5, "relay": "ON"}"#);
    assert_eq!(state(&hub, "sensor.stik_42_effekt").state, "12.5");
    assert_eq!(state(&hub, "switch.stik_42").state, "on");

    let info = mqtt.debug_info();
    assert_eq!(info["entities"].as_array().unwrap().len(), 2);
    assert_eq!(info["entities"][0]["messages"][0]["topic"], "plug42/state");

    // Removing one component via the device topic.
    let payload = json!({
        "dev": {"ids": "plug42"}, "stat_t": "plug42/state",
        "cmps": {"power": {"p": "sensor"}, "relay": {"p": "switch", "cmd_t": "plug42/set", "val_tpl": "{{ value_json.relay }}", "uniq_id": "plug42_relay", "name": null}}
    });
    msg(&mqtt, "homeassistant/device/plug42/config", &payload.to_string());
    assert!(hub.state("sensor.stik_42_effekt").is_none());
    assert_eq!(state(&hub, "switch.stik_42").state, "on");
}

#[tokio::test]
async fn expire_after_and_off_delay() {
    let (hub, _fake, mqtt) = setup();
    msg(&mqtt, "homeassistant/sensor/x/config", r#"{"name":"Udløb","uniq_id":"x1","stat_t":"x/s","exp_aft":0.05}"#);
    msg(&mqtt, "homeassistant/binary_sensor/m/config", r#"{"name":"Bevægelse","uniq_id":"m1","stat_t":"m/s","off_dly":0.05}"#);
    msg(&mqtt, "x/s", "5");
    msg(&mqtt, "m/s", "ON");
    assert_eq!(state(&hub, "sensor.udloeb").state, "5");
    assert_eq!(state(&hub, "binary_sensor.bevaegelse").state, "on");
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    mqtt.tick();
    assert_eq!(state(&hub, "sensor.udloeb").state, "unavailable");
    assert_eq!(state(&hub, "binary_sensor.bevaegelse").state, "off");
}
