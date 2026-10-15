//! What the MQTT core needs from TheHome. In the built-in integration this
//! was the hub itself; here it is a trait, so the core stays synchronous:
//! [`crate::host::HostHub`] queues the calls for TheHome over the plugin
//! protocol, and tests use an in-memory fake.

use serde_json::{Map, Value};

use crate::types::{DeviceSpec, EntitySpec, Issue};

/// Calls must not block.
pub trait HubApi: Send + Sync {
    /// Register or update a device. Returns a key that stands for the
    /// device's id in [`EntitySpec::device_id`].
    fn register_device(&self, spec: DeviceSpec) -> String;
    /// Forget a device nothing uses anymore.
    fn remove_device(&self, key: &str);
    /// Add an entity whose service calls come back to the integration.
    /// Returns a handle for [`HubApi::set_state`], `None` if disabled.
    fn add_entity(&self, spec: EntitySpec) -> Option<String>;
    fn remove_entity(&self, handle: &str);
    fn set_state(&self, handle: &str, state: String, attributes: Map<String, Value>);
    fn fire_event(&self, event_type: &str, data: Value);
    fn create_issue(&self, issue: Issue);
    fn delete_issue(&self, issue_id: &str);
    /// The key of a device TheHome knows by `device_id`, if it is ours.
    fn device_key(&self, device_id: &str) -> Option<String>;
    /// TheHome's id for a device key, once TheHome has registered it.
    fn device_id(&self, key: &str) -> Option<String>;
}

/// The key for a device: its first identifier, else its first connection.
pub fn device_key(spec: &DeviceSpec) -> Option<String> {
    spec.identifiers
        .first()
        .map(|(d, i)| format!("id:{d}:{i}"))
        .or_else(|| spec.connections.first().map(|(t, v)| format!("conn:{t}:{v}")))
}
