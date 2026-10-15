//! The plugin over the protocol, through a fake TheHome, against a real
//! broker: the built-in one, a TLS front for it with a self-signed CA, and
//! a broker that refuses the login.

mod host;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use host::FakeHost;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
use rumqttc::tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rumqttc::tokio_rustls::{rustls, TlsAcceptor};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::{json, Value};
use thehome_mqtt::MqttIntegration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const USER: &str = "thehome";
const PASS: &str = "hemmelig";

async fn listener() -> (TcpListener, u16) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    (l, port)
}

/// A broker that answers every CONNECT with "not authorized".
async fn refusing_broker() -> u16 {
    let (l, port) = listener().await;
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 512];
                if s.read(&mut buf).await.unwrap_or(0) > 0 {
                    let _ = s.write_all(&[0x20, 0x02, 0x00, 0x05]).await;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            });
        }
    });
    port
}

struct Pki {
    ca_pem: String,
    good: (Vec<u8>, Vec<u8>),
    wrong_name: (Vec<u8>, Vec<u8>),
}

/// A CA, a server certificate for 127.0.0.1 and one for another name.
fn pki() -> Pki {
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.distinguished_name.push(DnType::CommonName, "TheHome test CA");
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca.self_signed(&ca_key).unwrap();
    let leaf = |names: Vec<String>| {
        let key = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(names).unwrap().signed_by(&key, &ca_cert, &ca_key).unwrap();
        (cert.der().to_vec(), key.serialize_der())
    };
    Pki {
        ca_pem: ca_cert.pem(),
        good: leaf(vec!["127.0.0.1".into(), "localhost".into()]),
        wrong_name: leaf(vec!["broker.example.invalid".into()]),
    }
}

/// TLS in front of a plain broker port.
async fn tls_front(cert: (Vec<u8>, Vec<u8>), upstream: u16) -> u16 {
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![CertificateDer::from(cert.0)], PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.1)))
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let (l, port) = listener().await;
    tokio::spawn(async move {
        while let Ok((s, _)) = l.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(s).await else { return };
                let Ok(mut up) = tokio::net::TcpStream::connect(("127.0.0.1", upstream)).await else { return };
                let _ = tokio::io::copy_bidirectional(&mut tls, &mut up).await;
            });
        }
    });
    port
}

/// A client that publishes for the test and records what it receives.
struct Peer {
    client: AsyncClient,
    got: Arc<Mutex<Vec<(String, String)>>>,
}

impl Peer {
    async fn connect(port: u16, subscribe: &[&str]) -> Self {
        let mut o = MqttOptions::new("test-peer", "127.0.0.1", port);
        o.set_credentials(USER, PASS);
        let (client, mut ev) = AsyncClient::new(o, 100);
        let got: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
        let g = got.clone();
        tokio::spawn(async move {
            loop {
                match ev.poll().await {
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        g.lock().unwrap().push((p.topic.clone(), String::from_utf8_lossy(&p.payload).to_string()));
                    }
                    Ok(_) => {}
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        });
        for t in subscribe {
            client.subscribe(*t, QoS::AtLeastOnce).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        Self { client, got }
    }

    async fn publish(&self, topic: &str, payload: impl Into<String>, retain: bool) {
        self.client.publish(topic, QoS::AtLeastOnce, retain, payload.into().into_bytes()).await.unwrap();
    }

    /// Wait for a message on `topic` and take it.
    async fn expect(&self, topic: &str) -> String {
        for _ in 0..200 {
            {
                let mut got = self.got.lock().unwrap();
                if let Some(i) = got.iter().position(|(t, _)| t == topic) {
                    return got.remove(i).1;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("nothing on {topic}; got {:?}", self.got.lock().unwrap());
    }
}

async fn flow(host: &FakeHost, input: Value) -> Value {
    host.call("config_flow", json!({ "flow": { "flow_id": "f", "source": "user" }, "step": "user", "input": input }))
        .await
        .unwrap()["result"]
        .clone()
}

fn base_error(r: &Value) -> String {
    assert_eq!(r["type"], "form", "{r}");
    r["errors"]["base"].as_str().unwrap_or_else(|| panic!("no base error in {r}")).to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn plugin_end_to_end() {
    let port = 18_830 + (std::process::id() % 1000) as u16;
    thehome_mqtt::broker::ensure_started(port, Some(USER.into()), Some(PASS.into())).await.unwrap();
    let host = FakeHost::start(MqttIntegration::default());
    host.call("initialize", json!({ "api_version": 1, "data_dir": "." })).await.unwrap();

    // --- setup flow: the form, then errors
    let r = host.call("config_flow", json!({ "flow": { "flow_id": "f", "source": "user" }, "step": "user" })).await.unwrap();
    let props: Vec<&String> = r["result"]["schema"]["properties"].as_object().unwrap().keys().collect();
    assert_eq!(&props[..5], ["builtin", "broker", "port", "username", "password"]);

    let (_closed, closed_port) = {
        let (l, p) = listener().await;
        drop(l);
        ((), p)
    };
    let r = flow(&host, json!({ "broker": "127.0.0.1", "port": closed_port })).await;
    assert_eq!(base_error(&r), "cannot_connect");

    let refusing = refusing_broker().await;
    let r = flow(&host, json!({ "broker": "127.0.0.1", "port": refusing, "username": "x", "password": "y" })).await;
    assert_eq!(base_error(&r), "invalid_auth");

    let r = flow(&host, json!({ "broker": "" })).await;
    assert_eq!(r["errors"]["broker"], "required");

    // --- TLS against a front with a self-signed CA
    let pki = pki();
    let tls_port = tls_front(pki.good.clone(), port).await;
    let tls = |extra: Value| {
        let mut v = json!({ "broker": "127.0.0.1", "port": tls_port, "username": USER, "password": PASS, "tls": "on" });
        v.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        v
    };
    let r = flow(&host, tls(json!({}))).await;
    assert!(base_error(&r).contains("ikke udstedt af en kendt udsteder"), "{r}");
    let r = flow(&host, tls(json!({ "certificate": pki.ca_pem }))).await;
    assert_eq!(r["type"], "create_entry", "{r}");
    assert_eq!(r["data"]["tls"], true);
    assert_eq!(r["data"]["port"], tls_port);
    let r = flow(&host, tls(json!({ "tls_insecure": true }))).await;
    assert_eq!(r["type"], "create_entry", "{r}");
    assert_eq!(r["data"]["tls_insecure"], true);
    let wrong_port = tls_front(pki.wrong_name.clone(), port).await;
    let r = flow(&host, json!({ "broker": "127.0.0.1", "port": wrong_port, "username": USER, "password": PASS, "tls": "on", "certificate": pki.ca_pem })).await;
    assert!(base_error(&r).contains("passer ikke til værtsnavnet"), "{r}");
    // TLS to a broker that does not speak it
    let r = flow(&host, json!({ "broker": "127.0.0.1", "port": port, "username": USER, "password": PASS, "tls": "on", "tls_insecure": true })).await;
    assert!(base_error(&r).contains("TLS"), "{r}");
    let r = flow(&host, json!({ "broker": "127.0.0.1", "port": tls_port, "tls": "on", "certificate": "ikke et certifikat" })).await;
    assert!(base_error(&r).contains("CA-certifikatet"), "{r}");

    // --- plain, with the TLS ports left to "auto"
    let r = flow(&host, json!({ "broker": "127.0.0.1", "port": port, "username": USER, "password": PASS })).await;
    assert_eq!(r["type"], "create_entry", "{r}");
    assert_eq!(r["title"], format!("127.0.0.1:{port}"));
    assert_eq!(r["data"]["tls"], false);
    let data = r["data"].clone();

    // --- set up
    let peer = Peer::connect(port, &["homeassistant/status", "test/#", "trv/#"]).await;
    let entry = json!({ "entry_id": "e1", "domain": "mqtt", "data": data, "options": { "subscriptions": ["garage/#"] } });
    host.call("setup_entry", json!({ "entry": entry })).await.unwrap();
    assert_eq!(peer.expect("homeassistant/status").await, "online");
    {
        let services = host.rec.lock().unwrap().services.clone();
        for s in ["publish", "subscribe", "unsubscribe", "debug_info", "zigbee2mqtt_permit_join"] {
            assert!(services.iter().any(|x| x == s), "{s} not registered: {services:?}");
        }
    }

    // --- discovery
    let dev = json!({ "ids": "testdev", "name": "Testenhed", "mf": "Acme" });
    let configs = [
        ("homeassistant/sensor/t1/config", json!({ "name": "Temperatur", "uniq_id": "t1", "stat_t": "test/temp", "unit_of_meas": "°C", "dev_cla": "temperature", "avty_t": "test/avail", "dev": dev })),
        ("homeassistant/binary_sensor/d1/config", json!({ "name": "Dør", "uniq_id": "d1", "stat_t": "test/door", "pl_on": "open", "pl_off": "closed", "dev_cla": "door", "dev": dev })),
        ("homeassistant/switch/sw1/config", json!({ "name": "Stik", "uniq_id": "sw1", "cmd_t": "test/sw/set", "stat_t": "test/sw" })),
        ("homeassistant/light/l1/config", json!({ "name": "Lampe", "uniq_id": "l1", "schema": "json", "cmd_t": "test/lamp/set", "stat_t": "test/lamp", "brightness": true, "supported_color_modes": ["color_temp"] })),
    ];
    for (t, c) in &configs {
        peer.publish(t, c.to_string(), true).await;
    }
    peer.publish("homeassistant/climate/clim1/config", support_fixture("climate_config.json"), true).await;

    let sensor = host.wait_entity("sensor.testenhed_temperatur").await;
    assert_eq!(sensor["unit_of_measurement"], "°C");
    assert_eq!(sensor["device_class"], "temperature");
    assert!(sensor["device_id"].is_string(), "the device key is replaced by TheHome's id: {sensor}");
    host.wait_entity("binary_sensor.testenhed_doer").await;
    host.wait_entity("switch.stik").await;
    host.wait_entity("light.lampe").await;
    host.wait_entity("climate.termostat").await;
    {
        let rec = host.rec.lock().unwrap();
        let (_, d) = rec.devices.iter().find(|(_, d)| d["name"] == "Testenhed").unwrap();
        assert_eq!(d["identifiers"], json!([["mqtt", "testdev"]]));
        assert_eq!(d["manufacturer"], "Acme");
    }

    // --- states and availability
    host.wait_state("sensor.testenhed_temperatur", "unavailable").await;
    peer.publish("test/avail", "online", false).await;
    peer.publish("test/temp", "21.5", false).await;
    host.wait_state("sensor.testenhed_temperatur", "21.5").await;
    peer.publish("test/door", "open", false).await;
    host.wait_state("binary_sensor.testenhed_doer", "on").await;
    peer.publish("test/sw", "ON", false).await;
    host.wait_state("switch.stik", "on").await;
    peer.publish("test/lamp", r#"{"state":"ON","brightness":128,"color_mode":"color_temp","color_temp":300}"#, false).await;
    let attrs = host.wait_state("light.lampe", "on").await;
    assert_eq!(attrs["brightness"], 128);
    peer.publish("trv/state", r#"{"mode":"heat","target":21.5,"current":19.8}"#, false).await;
    let attrs = host.wait_state("climate.termostat", "heat").await;
    assert_eq!(attrs["current_temperature"], 19.8);
    peer.publish("test/avail", "offline", false).await;
    host.wait_state("sensor.testenhed_temperatur", "unavailable").await;

    // --- commands
    let call = |entity_id: &str, service: &str, data: Value| {
        host.call("entity/call_service", json!({ "entry_id": "e1", "entity_id": entity_id, "service": service, "data": data }))
    };
    call("switch.stik", "turn_off", json!({})).await.unwrap();
    assert_eq!(peer.expect("test/sw/set").await, "OFF");
    call("light.lampe", "turn_on", json!({ "brightness": 200 })).await.unwrap();
    let body: Value = serde_json::from_str(&peer.expect("test/lamp/set").await).unwrap();
    assert_eq!(body, json!({ "state": "ON", "brightness": 200 }));
    call("climate.termostat", "set_temperature", json!({ "temperature": 22 })).await.unwrap();
    assert_eq!(peer.expect("trv/temp/set").await, "22");
    let err = call("switch.stik", "set_value", json!({ "value": 1 })).await.unwrap_err();
    assert_eq!(err["data"]["kind"], "not_supported", "{err}");
    let err = call("sensor.findes_ikke", "turn_on", json!({})).await.unwrap_err();
    assert_eq!(err["data"]["kind"], "not_found", "{err}");

    // --- the publish service, subscriptions and debug info
    let svc = |service: &str, data: Value| host.call("service/call", json!({ "entry_id": "e1", "service": service, "data": data }));
    svc("publish", json!({ "topic": "test/out", "payload": "hej", "qos": "1" })).await.unwrap();
    assert_eq!(peer.expect("test/out").await, "hej");
    svc("publish", json!({ "topic": "test/out", "payload_template": "{{ 2 * 21 }}" })).await.unwrap();
    assert_eq!(peer.expect("test/out").await, "42");
    assert_eq!(svc("publish", json!({ "topic": "test/#" })).await.unwrap_err()["data"]["kind"], "invalid_argument");

    peer.publish("garage/port", "open", false).await;
    let ev = host.wait_event("mqtt_message").await;
    assert_eq!(ev, json!({ "topic": "garage/port", "payload": "open", "qos": 0, "retain": false }));
    svc("subscribe", json!({ "topic": "kælder/+" })).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    peer.publish("kælder/vand", "tør", false).await;
    assert_eq!(host.wait_event("mqtt_message").await["topic"], "kælder/vand");
    let info = svc("debug_info", json!({})).await.unwrap();
    assert_eq!(info["connected"], true);
    assert_eq!(info["entities"].as_array().unwrap().len(), 5);

    // --- removal: an empty retained config
    peer.publish("homeassistant/switch/sw1/config", "", true).await;
    host.wait_state("switch.stik", "unavailable").await;
    let info = svc("debug_info", json!({})).await.unwrap();
    assert_eq!(info["entities"].as_array().unwrap().len(), 4);

    // --- diagnostics hide secrets
    let d = host.call("diagnostics", json!({ "entry": entry })).await.unwrap();
    assert_eq!(d["config"]["password"], "**REDACTED**");
    assert_eq!(d["state"]["connected"], true);

    // --- unload says goodbye
    host.call("unload_entry", json!({ "entry_id": "e1" })).await.unwrap();
    assert_eq!(peer.expect("homeassistant/status").await, "offline");
}

fn support_fixture(name: &str) -> String {
    std::fs::read_to_string(format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
}

#[tokio::test]
async fn options_flow_checks_topics() {
    let host = FakeHost::start(MqttIntegration::default());
    let entry = json!({ "entry_id": "e1", "domain": "mqtt", "data": {}, "options": {} });
    let call = |input: Value| {
        host.call("options_flow", json!({ "flow": { "flow_id": "o", "source": "options", "entry": entry }, "step": "init", "input": input }))
    };
    let r = call(Value::Null).await.unwrap()["result"].clone();
    assert_eq!(r["step_id"], "options");
    assert_eq!(r["schema"]["properties"]["discovery_prefix"]["default"], "homeassistant");
    let r = call(json!({ "subscriptions": ["ok/+", "dårlig/#/x"] })).await.unwrap()["result"].clone();
    assert!(r["errors"]["subscriptions"].as_str().unwrap().contains("dårlig/#/x"), "{r}");
    let r = call(json!({ "discovery": false, "discovery_prefix": "ha", "subscriptions": "a/b, c/#" })).await.unwrap()["result"].clone();
    assert_eq!(r["data"], json!({ "discovery": false, "discovery_prefix": "ha", "birth_message": true, "subscriptions": ["a/b", "c/#"] }));
}

/// A real cloud broker: HiveMQ Cloud wants TLS with SNI on 8883. Made-up
/// credentials must get through the TLS handshake and then be refused.
/// Needs the internet: `cargo test -- --ignored hivemq`.
#[tokio::test]
#[ignore]
async fn hivemq_cloud_tls_and_sni() {
    let host = FakeHost::start(MqttIntegration::default());
    let cloud = "f70096dcfeac4790aa317a39cf949c6b.s2.eu.hivemq.cloud";
    // auto: TLS and port 8883 for a host outside the local network
    let r = flow(&host, json!({ "broker": cloud, "username": "thehome-test-findes-ikke", "password": "forkert-kodeord-1" })).await;
    assert_eq!(base_error(&r), "invalid_auth", "{r}");
    let r = flow(&host, json!({ "broker": cloud, "username": "thehome-test-findes-ikke", "password": "forkert-kodeord-1", "transport": "websockets" })).await;
    assert_eq!(base_error(&r), "invalid_auth", "websocket: {r}");
}
