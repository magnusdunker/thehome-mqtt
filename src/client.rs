//! Network side: rumqttc client, TLS, reconnect loop and connection test.

use std::sync::Arc;
use std::time::Duration;

use rumqttc::tokio_rustls::rustls;
use rumqttc::{
    AsyncClient, ConnectReturnCode, ConnectionError, Event, EventLoop, LastWill, MqttOptions, Packet, QoS,
    TlsConfiguration, Transport as RTransport,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::entity::Publish;
use crate::manager::{Mqtt, Transport};

/// `tcp` (MQTT) or `websockets` (MQTT over WebSocket).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Wire {
    #[default]
    Tcp,
    WebSockets,
}

#[derive(Clone, Debug, Default)]
pub struct ConnConfig {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls: bool,
    /// PEM CA certificate(s) for TLS; the built-in web roots when None.
    pub ca: Option<String>,
    /// Accept any server certificate (self-signed local brokers).
    pub insecure: bool,
    /// PEM client certificate and key for brokers that want one.
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
    pub wire: Wire,
    pub ws_path: String,
    pub keepalive: u64,
    pub client_id: String,
    pub prefix: String,
    pub birth: bool,
}

/// TLS on or off when the user leaves it to us: on for the TLS ports and
/// for hosts that are not on the local network.
pub fn auto_tls(host: &str, port: Option<u16>) -> bool {
    match port {
        Some(8883 | 8884 | 443) => true,
        Some(_) => false,
        None => !is_local_host(host),
    }
}

/// Private and link-local addresses, `localhost`, `.local`/`.lan`/`.home`
/// names and single-label names count as local.
pub fn is_local_host(host: &str) -> bool {
    let h = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if let Ok(ip) = h.trim_start_matches('[').trim_end_matches(']').parse::<std::net::IpAddr>() {
        return match ip {
            std::net::IpAddr::V4(v4) => v4.is_private() || v4.is_loopback() || v4.is_link_local(),
            std::net::IpAddr::V6(v6) => {
                v6.is_loopback() || (v6.segments()[0] & 0xfe00) == 0xfc00 || (v6.segments()[0] & 0xffc0) == 0xfe80
            }
        };
    }
    h == "localhost"
        || !h.contains('.')
        || [".local", ".lan", ".home", ".internal", ".localdomain", ".home.arpa"]
            .iter()
            .any(|s| h.ends_with(s))
}

impl ConnConfig {
    pub fn from_entry(data: &Value, options: &Value) -> Self {
        let s = |k: &str| data.get(k).and_then(Value::as_str).map(str::trim).filter(|s| !s.is_empty()).map(str::to_string);
        let host = s("broker").unwrap_or_else(|| "127.0.0.1".into());
        let port = data.get("port").and_then(Value::as_u64).map(|p| p as u16);
        let tls = data.get("tls").and_then(Value::as_bool).unwrap_or_else(|| auto_tls(&host, port));
        let wire = match data.get("transport").and_then(Value::as_str) {
            Some("websockets") => Wire::WebSockets,
            _ => Wire::Tcp,
        };
        let default_port = match (&wire, tls) {
            (Wire::Tcp, false) => 1883,
            (Wire::Tcp, true) => 8883,
            (Wire::WebSockets, false) => 8080,
            (Wire::WebSockets, true) => 8884,
        };
        Self {
            port: port.unwrap_or(default_port),
            host,
            username: s("username"),
            password: data.get("password").and_then(Value::as_str).filter(|p| !p.is_empty()).map(str::to_string),
            tls,
            ca: s("certificate"),
            insecure: data.get("tls_insecure").and_then(Value::as_bool).unwrap_or(false),
            client_cert: s("client_cert"),
            client_key: s("client_key"),
            wire,
            ws_path: s("ws_path").unwrap_or_else(|| "/mqtt".into()),
            keepalive: data.get("keepalive").and_then(Value::as_u64).filter(|k| *k >= 5).unwrap_or(30),
            client_id: s("client_id").unwrap_or_else(random_client_id),
            prefix: options
                .get("discovery_prefix")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or("homeassistant")
                .to_string(),
            birth: options.get("birth_message").and_then(Value::as_bool).unwrap_or(true),
        }
    }

    /// Where we connect, for titles and logs.
    pub fn address(&self) -> String {
        match self.wire {
            Wire::Tcp => format!("{}:{}", self.host, self.port),
            Wire::WebSockets => self.ws_url(),
        }
    }

    fn ws_url(&self) -> String {
        let scheme = if self.tls { "wss" } else { "ws" };
        let path = if self.ws_path.starts_with('/') { self.ws_path.clone() } else { format!("/{}", self.ws_path) };
        format!("{scheme}://{}:{}{path}", self.host, self.port)
    }

    pub fn options(&self) -> Result<MqttOptions, String> {
        let broker = match self.wire {
            Wire::Tcp => self.host.clone(),
            Wire::WebSockets => self.ws_url(),
        };
        let mut o = MqttOptions::new(&self.client_id, broker, self.port);
        o.set_keep_alive(Duration::from_secs(self.keepalive));
        o.set_clean_session(true);
        // Zigbee2MQTT bridge messages can be large.
        o.set_max_packet_size(16 * 1024 * 1024, 16 * 1024 * 1024);
        if let Some(u) = &self.username {
            o.set_credentials(u, self.password.clone().unwrap_or_default());
        }
        if self.birth {
            o.set_last_will(LastWill::new(format!("{}/status", self.prefix), "offline", QoS::AtLeastOnce, false));
        }
        let transport = match (&self.wire, self.tls) {
            (Wire::Tcp, false) => RTransport::Tcp,
            (Wire::Tcp, true) => RTransport::Tls(self.tls_config()?),
            (Wire::WebSockets, false) => RTransport::Ws,
            (Wire::WebSockets, true) => RTransport::Wss(self.tls_config()?),
        };
        o.set_transport(transport);
        Ok(o)
    }

    /// rustls with the web roots (or the given CA), SNI from the host name,
    /// optional client certificate, or no verification at all.
    fn tls_config(&self) -> Result<TlsConfiguration, String> {
        let builder = rustls::ClientConfig::builder();
        let builder = if self.insecure {
            builder.dangerous().with_custom_certificate_verifier(Arc::new(NoVerify))
        } else {
            let mut roots = rustls::RootCertStore::empty();
            match &self.ca {
                Some(pem) => {
                    let certs = pem_certs(pem).map_err(|e| format!("CA-certifikatet kunne ikke læses ({e})"))?;
                    if certs.is_empty() {
                        return Err("CA-certifikatet indeholder intet certifikat".into());
                    }
                    for c in certs {
                        roots.add(c).map_err(|e| format!("CA-certifikatet kunne ikke bruges ({e})"))?;
                    }
                }
                None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
            }
            builder.with_root_certificates(roots)
        };
        let config = match (&self.client_cert, &self.client_key) {
            (Some(cert), Some(key)) => {
                let certs = pem_certs(cert).map_err(|e| format!("klientcertifikatet kunne ikke læses ({e})"))?;
                let key = pem_key(key)?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| format!("klientcertifikatet kunne ikke bruges ({e})"))?
            }
            (Some(_), None) | (None, Some(_)) => return Err("klientcertifikat og nøgle skal angives sammen".into()),
            (None, None) => builder.with_no_client_auth(),
        };
        Ok(TlsConfiguration::Rustls(Arc::new(config)))
    }
}

fn pem_certs(pem: &str) -> Result<Vec<CertificateDer<'static>>, std::io::Error> {
    rustls_pemfile::certs(&mut pem.as_bytes()).collect()
}

fn pem_key(pem: &str) -> Result<PrivateKeyDer<'static>, String> {
    rustls_pemfile::private_key(&mut pem.as_bytes())
        .map_err(|e| format!("klientnøglen kunne ikke læses ({e})"))?
        .ok_or_else(|| "klientnøglen indeholder ingen privat nøgle".to_string())
}

/// Accepts any server certificate, but still checks the handshake
/// signatures. Only for brokers with self-signed certificates.
#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &rustls::crypto::ring::default_provider().signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &rustls::crypto::ring::default_provider().signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider().signature_verification_algorithms.supported_schemes()
    }
}

pub fn random_client_id() -> String {
    use rand::Rng;
    let n: u32 = rand::thread_rng().gen();
    format!("thehome-{n:08x}")
}

fn qos(q: u8) -> QoS {
    match q {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

enum Op {
    Publish(Publish),
    Subscribe(String, u8),
    Unsubscribe(String),
}

/// Non-blocking transport that queues operations for the async client.
pub struct ClientTransport {
    tx: mpsc::UnboundedSender<Op>,
}

impl Transport for ClientTransport {
    fn publish(&self, p: Publish) {
        let _ = self.tx.send(Op::Publish(p));
    }
    fn subscribe(&self, filter: &str, q: u8) {
        let _ = self.tx.send(Op::Subscribe(filter.to_string(), q));
    }
    fn unsubscribe(&self, filter: &str) {
        let _ = self.tx.send(Op::Unsubscribe(filter.to_string()));
    }
}

/// The client: the transport, the client handle, the event loop and the
/// future that forwards queued operations to the client.
pub type Connection = (Arc<ClientTransport>, AsyncClient, EventLoop, std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>);

/// Create the client. Fails only on bad TLS settings.
pub fn connect(cfg: &ConnConfig) -> Result<Connection, String> {
    let (client, eventloop) = AsyncClient::new(cfg.options()?, 256);
    let (tx, mut rx) = mpsc::unbounded_channel::<Op>();
    let c = client.clone();
    let forward = async move {
        while let Some(op) = rx.recv().await {
            let r = match op {
                Op::Publish(p) => c.publish(p.topic, qos(p.qos), p.retain, p.payload.into_bytes()).await,
                Op::Subscribe(f, q) => c.subscribe(f, qos(q)).await,
                Op::Unsubscribe(f) => c.unsubscribe(f).await,
            };
            if let Err(e) = r {
                crate::debug!("MQTT request failed: {e}");
            }
        }
    };
    Ok((Arc::new(ClientTransport { tx }), client, eventloop, Box::pin(forward)))
}

/// Drive the event loop until cancelled, reconnecting with backoff.
pub async fn run(mqtt: Arc<Mqtt>, mut eventloop: EventLoop, cancel: CancellationToken) {
    let mut backoff = Duration::from_secs(1);
    let mut was_connected = false;
    let mut last_error = String::new();
    // Timers run on their own: dropping `poll()` in the middle of a connect
    // closes the half-made session, and the broker publishes our will.
    let (m, c) = (mqtt.clone(), cancel.clone());
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = c.cancelled() => break,
                _ = tick.tick() => m.tick(),
            }
        }
    });
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            ev = eventloop.poll() => match ev {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    crate::info!("forbundet til MQTT-brokeren");
                    backoff = Duration::from_secs(1);
                    was_connected = true;
                    last_error.clear();
                    mqtt.on_connected();
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    mqtt.handle_message(&p.topic, &p.payload, p.qos as u8, p.retain);
                }
                Ok(_) => {}
                Err(e) => {
                    let why = describe(&e);
                    if was_connected {
                        crate::warn!("forbindelsen til MQTT-brokeren blev afbrudt: {why}");
                        was_connected = false;
                    } else if why != last_error {
                        // log a new reason once, not every retry
                        crate::warn!("kunne ikke forbinde til MQTT-brokeren: {why}");
                    }
                    last_error = why;
                    mqtt.on_disconnected();
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        }
    }
    mqtt.shutdown();
}

/// Why a connection failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    InvalidAuth,
    CannotConnect,
    CertUntrusted,
    CertHostname,
    CertExpired,
    TlsFailed,
    Settings(String),
}

impl Failure {
    /// The flow error code. TheHome's web shows codes it knows in the
    /// user's language and others as they are, so the TLS reasons are
    /// written out in Danish, without `:` and `.`, which the translation
    /// lookup would read as separators.
    pub fn code(&self) -> String {
        match self {
            Failure::InvalidAuth => "invalid_auth".into(),
            Failure::CannotConnect => "cannot_connect".into(),
            Failure::CertUntrusted => {
                "Brokerens certifikat er ikke udstedt af en kendt udsteder, angiv CA-certifikatet eller slå certifikattjek fra for en lokal broker".into()
            }
            Failure::CertHostname => "Brokerens certifikat passer ikke til værtsnavnet".into(),
            Failure::CertExpired => "Brokerens certifikat er udløbet eller endnu ikke gyldigt".into(),
            Failure::TlsFailed => "TLS-forbindelsen kunne ikke oprettes, tjek at porten bruger TLS".into(),
            Failure::Settings(m) => m.replace([':', '.'], ","),
        }
    }
}

/// The rustls error behind a connection error, if any.
fn rustls_error(e: &ConnectionError) -> Option<rustls::Error> {
    let io = match e {
        ConnectionError::Io(io) => io,
        ConnectionError::Tls(rumqttc::TlsError::Io(io)) => io,
        ConnectionError::Tls(rumqttc::TlsError::TLS(t)) => return Some(t.clone()),
        _ => return None,
    };
    io.get_ref()?.downcast_ref::<rustls::Error>().cloned()
}

/// Classify a connection error.
pub fn classify(e: &ConnectionError, tls: bool) -> Failure {
    use rustls::CertificateError as C;
    if let ConnectionError::ConnectionRefused(ConnectReturnCode::BadUserNamePassword | ConnectReturnCode::NotAuthorized) = e {
        return Failure::InvalidAuth;
    }
    if let Some(r) = rustls_error(e) {
        return match r {
            rustls::Error::InvalidCertificate(C::NotValidForName) => Failure::CertHostname,
            rustls::Error::InvalidCertificate(C::Expired | C::NotValidYet) => Failure::CertExpired,
            rustls::Error::InvalidCertificate(_) => Failure::CertUntrusted,
            _ => Failure::TlsFailed,
        };
    }
    if tls {
        if let ConnectionError::Tls(_) = e {
            return Failure::TlsFailed;
        }
        // a plain MQTT broker on the port answers the TLS hello with garbage
        if let ConnectionError::Io(io) = e {
            if matches!(io.kind(), std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof) {
                return Failure::TlsFailed;
            }
        }
    }
    Failure::CannotConnect
}

/// A connection error for the log.
pub fn describe(e: &ConnectionError) -> String {
    match rustls_error(e) {
        Some(r) => format!("TLS: {r}"),
        None => e.to_string(),
    }
}

/// Try to connect once.
pub async fn test_connection(cfg: &ConnConfig) -> Result<(), Failure> {
    let mut cfg = cfg.clone();
    cfg.client_id = random_client_id();
    cfg.birth = false;
    let opts = cfg.options().map_err(Failure::Settings)?;
    let (client, mut eventloop) = AsyncClient::new(opts, 10);
    let res = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => return Ok(()),
                Ok(_) => {}
                Err(e) => {
                    crate::info!("testforbindelse til {} fejlede: {}", cfg.address(), describe(&e));
                    return Err(classify(&e, cfg.tls));
                }
            }
        }
    })
    .await
    .unwrap_or(Err(Failure::CannotConnect));
    let _ = client.try_disconnect();
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn local_hosts() {
        for h in ["192.168.1.10", "10.0.0.2", "172.16.5.1", "127.0.0.1", "localhost", "mosquitto", "broker.local", "nas.lan", "fd00::1", "[fe80::1]"] {
            assert!(is_local_host(h), "{h}");
        }
        for h in ["f70096dcfeac4790aa317a39cf949c6b.s2.eu.hivemq.cloud", "test.mosquitto.org", "8.8.8.8", "2a00:1450::1"] {
            assert!(!is_local_host(h), "{h}");
        }
    }

    #[test]
    fn tls_and_port_defaults() {
        assert!(auto_tls("xxx.s2.eu.hivemq.cloud", None));
        assert!(!auto_tls("192.168.1.10", None));
        assert!(auto_tls("192.168.1.10", Some(8883)));
        assert!(!auto_tls("xxx.s2.eu.hivemq.cloud", Some(1883)));

        let cloud = ConnConfig::from_entry(&json!({ "broker": "xxx.s2.eu.hivemq.cloud" }), &Value::Null);
        assert!(cloud.tls);
        assert_eq!(cloud.port, 8883);
        let lan = ConnConfig::from_entry(&json!({ "broker": "192.168.1.10" }), &Value::Null);
        assert!(!lan.tls);
        assert_eq!(lan.port, 1883);
        let ws = ConnConfig::from_entry(&json!({ "broker": "xxx.s2.eu.hivemq.cloud", "transport": "websockets" }), &Value::Null);
        assert_eq!(ws.port, 8884);
        assert_eq!(ws.address(), "wss://xxx.s2.eu.hivemq.cloud:8884/mqtt");
        // stored settings win
        let off = ConnConfig::from_entry(&json!({ "broker": "xxx.s2.eu.hivemq.cloud", "tls": false, "port": 1883 }), &Value::Null);
        assert!(!off.tls);
    }

    #[test]
    fn tls_settings_errors() {
        let base = json!({ "broker": "x.example.com", "tls": true });
        let mut cfg = ConnConfig::from_entry(&base, &Value::Null);
        assert!(cfg.options().is_ok());
        cfg.ca = Some("ikke pem".into());
        assert!(cfg.options().unwrap_err().contains("CA-certifikatet"));
        cfg.ca = None;
        cfg.client_cert = Some("x".into());
        assert!(cfg.options().unwrap_err().contains("sammen"));
        // codes have no ':' or '.', which the web's translation lookup splits on
        for f in [Failure::CertUntrusted, Failure::CertHostname, Failure::CertExpired, Failure::TlsFailed, Failure::Settings("a: b.c".into())] {
            assert!(!f.code().contains([':', '.']), "{}", f.code());
        }
    }
}
