//! The parts of TheHome's core types the MQTT code needs. They serialize to
//! the same JSON as TheHome's `EntitySpec` and `DeviceSpec`, which is what
//! the plugin protocol's `entity/add` and `device/register` take.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const STATE_UNKNOWN: &str = "unknown";
pub const STATE_UNAVAILABLE: &str = "unavailable";

/// What the integration provides when adding an entity.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct EntitySpec {
    pub domain: String,
    pub unique_id: String,
    /// With a device, this is appended to the device name. `None` with a
    /// device means "this is the device's main entity".
    pub name: Option<String>,
    pub device_id: Option<String>,
    pub icon: Option<String>,
    pub device_class: Option<String>,
    pub unit_of_measurement: Option<String>,
    pub state_class: Option<String>,
    pub entity_category: Option<String>,
    pub suggested_object_id: Option<String>,
    pub disabled_by_default: bool,
    /// Static attributes like `supported_features`, `min_mireds`.
    pub capabilities: Value,
}

/// What the integration provides when registering a device.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DeviceSpec {
    pub identifiers: Vec<(String, String)>,
    pub connections: Vec<(String, String)>,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub model_id: Option<String>,
    pub sw_version: Option<String>,
    pub hw_version: Option<String>,
    pub serial_number: Option<String>,
    pub name: String,
    /// Identifier of the parent device (e.g. the coordinator).
    pub via_device: Option<(String, String)>,
    /// Area name suggested by the integration; used only on creation.
    pub suggested_area: Option<String>,
    pub entry_type: Option<String>,
    pub configuration_url: Option<String>,
}

/// A repair issue shown to the user.
#[derive(Clone, Debug, PartialEq)]
pub struct Issue {
    pub issue_id: String,
    pub severity: String,
    pub title: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ServiceError {
    #[error("service not found: {0}")]
    NotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("device unavailable: {0}")]
    Unavailable(String),
    #[error("{0}")]
    Failed(String),
}

impl From<ServiceError> for thehome_plugin::Error {
    fn from(e: ServiceError) -> Self {
        use thehome_plugin::Error;
        match e {
            ServiceError::NotFound(m) => Error::new("not_found", m),
            ServiceError::InvalidArgument(m) => Error::invalid(m),
            ServiceError::NotSupported(m) => Error::not_supported(m),
            ServiceError::Unavailable(m) => Error::unavailable(m),
            ServiceError::Failed(m) => Error::failed(m),
        }
    }
}
