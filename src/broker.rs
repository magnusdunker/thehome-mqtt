//! Optional built-in MQTT broker (rumqttd). It runs on its own thread for
//! the lifetime of the process; rumqttd has no clean shutdown.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::sync::OnceLock;
use std::time::Duration;

use parking_lot::Mutex;
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};

static STARTED: OnceLock<Mutex<Option<u16>>> = OnceLock::new();

fn config(port: u16, username: Option<&str>, password: Option<&str>) -> Config {
    let auth = username.map(|u| {
        let mut m = HashMap::new();
        m.insert(u.to_string(), password.unwrap_or_default().to_string());
        m
    });
    let server = ServerSettings {
        name: "v4".into(),
        listen: SocketAddr::from(([0, 0, 0, 0], port)),
        tls: None,
        next_connection_delay_ms: 1,
        connections: ConnectionSettings {
            connection_timeout_ms: 60_000,
            max_payload_size: 16 * 1024 * 1024,
            max_inflight_count: 500,
            auth,
            external_auth: None,
            dynamic_filters: true,
        },
    };
    let mut v4 = HashMap::new();
    v4.insert("1".to_string(), server);
    Config {
        id: 0,
        router: RouterConfig {
            max_connections: 10_010,
            max_outgoing_packet_count: 1_000,
            max_segment_size: 100 * 1024 * 1024,
            max_segment_count: 10,
            custom_segment: None,
            initialized_filters: None,
            shared_subscriptions_strategy: Default::default(),
        },
        v4: Some(v4),
        ..Default::default()
    }
}

fn listening(port: u16) -> bool {
    TcpStream::connect_timeout(&SocketAddr::from(([127, 0, 0, 1], port)), Duration::from_millis(300)).is_ok()
}

/// Start the broker once per process and wait until it accepts connections.
pub async fn ensure_started(port: u16, username: Option<String>, password: Option<String>) -> Result<(), String> {
    let lock = STARTED.get_or_init(|| Mutex::new(None));
    {
        let mut started = lock.lock();
        match *started {
            Some(p) if p == port => {}
            Some(p) => return Err(format!("den indbyggede broker kører allerede på port {p}")),
            None => {
                let cfg = config(port, username.as_deref(), password.as_deref());
                std::thread::Builder::new()
                    .name("mqtt-broker".into())
                    .spawn(move || {
                        let mut broker = Broker::new(cfg);
                        if let Err(e) = broker.start() {
                            crate::error!("den indbyggede MQTT-broker stoppede: {e}");
                        }
                    })
                    .map_err(|e| e.to_string())?;
                *started = Some(port);
                crate::info!("indbygget MQTT-broker startet på port {port}");
            }
        }
    }
    for _ in 0..30 {
        if tokio::task::spawn_blocking(move || listening(port)).await.unwrap_or(false) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!("brokeren svarer ikke på port {port}"))
}
