//! Jinja-like templates used by MQTT discovery (`value_template`,
//! `command_template` ...). Built on minijinja with the Home Assistant
//! filters that MQTT devices commonly use.

use std::sync::OnceLock;

use minijinja::value::{Rest, Value as JValue};
use minijinja::{Environment, Error, ErrorKind, UndefinedBehavior};
use serde_json::{Map, Value};

fn env() -> &'static Environment<'static> {
    static ENV: OnceLock<Environment<'static>> = OnceLock::new();
    ENV.get_or_init(|| {
        let mut env = Environment::new();
        env.set_undefined_behavior(UndefinedBehavior::Chainable);
        env.add_filter("float", to_float);
        env.add_filter("int", to_int);
        env.add_filter("is_defined", is_defined);
        env.add_filter("iif", iif);
        env.add_filter("to_json", to_json);
        env.add_filter("from_json", from_json);
        env.add_filter("multiply", multiply);
        env.add_filter("bool", to_bool);
        env.add_function("float", |v: JValue, d: Rest<JValue>| to_float(v, d));
        env.add_function("int", |v: JValue, d: Rest<JValue>| to_int(v, d));
        env.add_function("iif", iif);
        env.add_function("is_number", |v: JValue| num(&v).is_some());
        env
    })
}

fn num(v: &JValue) -> Option<f64> {
    if let Some(s) = v.as_str() {
        return s.trim().parse().ok();
    }
    if v.is_number() {
        return f64::try_from(v.clone()).ok();
    }
    if v.kind() == minijinja::value::ValueKind::Bool {
        return Some(if v.is_true() { 1.0 } else { 0.0 });
    }
    None
}

fn to_float(v: JValue, default: Rest<JValue>) -> Result<JValue, Error> {
    match num(&v) {
        Some(f) => Ok(JValue::from(f)),
        None => default
            .first()
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::InvalidOperation, format!("cannot convert {v} to float"))),
    }
}

fn to_int(v: JValue, default: Rest<JValue>) -> Result<JValue, Error> {
    match num(&v) {
        Some(f) => Ok(JValue::from(f.trunc() as i64)),
        None => default
            .first()
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::InvalidOperation, format!("cannot convert {v} to int"))),
    }
}

fn to_bool(v: JValue) -> bool {
    match v.as_str() {
        Some(s) => matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on" | "enable"),
        None => v.is_true(),
    }
}

fn is_defined(v: JValue) -> Result<JValue, Error> {
    if v.is_undefined() {
        Err(Error::new(ErrorKind::UndefinedError, "value is undefined"))
    } else {
        Ok(v)
    }
}

fn iif(v: JValue, rest: Rest<JValue>) -> JValue {
    let truthy = if v.is_none() || v.is_undefined() { None } else { Some(v.is_true()) };
    match truthy {
        Some(true) => rest.first().cloned().unwrap_or(JValue::from(true)),
        Some(false) => rest.get(1).cloned().unwrap_or(JValue::from(false)),
        None => rest.get(2).cloned().or_else(|| rest.get(1).cloned()).unwrap_or(JValue::from(false)),
    }
}

fn to_json(v: JValue) -> Result<String, Error> {
    serde_json::to_string(&v).map_err(|e| Error::new(ErrorKind::InvalidOperation, e.to_string()))
}

fn from_json(v: JValue) -> Result<JValue, Error> {
    let s = v.as_str().unwrap_or_default();
    let parsed: Value =
        serde_json::from_str(s).map_err(|e| Error::new(ErrorKind::InvalidOperation, e.to_string()))?;
    Ok(JValue::from_serialize(&parsed))
}

fn multiply(v: JValue, by: f64) -> Result<JValue, Error> {
    num(&v)
        .map(|f| JValue::from(f * by))
        .ok_or_else(|| Error::new(ErrorKind::InvalidOperation, "not a number"))
}

/// Render a template with the given variables.
pub fn render(template: &str, vars: &Map<String, Value>) -> Result<String, String> {
    env()
        .render_str(template, vars)
        .map(|s| s.trim().to_string())
        .map_err(|e| e.to_string())
}

/// Render a value template for an incoming payload: `value` is the payload
/// string and `value_json` the parsed JSON (if it is JSON).
pub fn render_value(template: &str, payload: &str) -> Result<String, String> {
    let mut vars = Map::new();
    vars.insert("value".into(), Value::String(payload.to_string()));
    if let Ok(j) = serde_json::from_str::<Value>(payload) {
        vars.insert("value_json".into(), j);
    }
    render(template, &vars)
}

/// Render a command template; `value` is the value being sent.
pub fn render_command(template: &str, value: Value, extra: &Map<String, Value>) -> Result<String, String> {
    let mut vars = extra.clone();
    vars.insert("value".into(), value);
    render(template, &vars)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn value_json_paths() {
        let p = r#"{"temperature": 21.5, "nested": {"a": "x"}, "state": "ON"}"#;
        assert_eq!(render_value("{{ value_json.temperature }}", p).unwrap(), "21.5");
        assert_eq!(render_value("{{ value_json['nested'].a }}", p).unwrap(), "x");
        assert_eq!(render_value("{{ value_json.missing.deeper }}", p).unwrap(), "");
        assert_eq!(render_value("{{ value | float * 2 }}", "1.5").unwrap(), "3.0");
        assert_eq!(render_value("{{ value_json.temperature | int }}", p).unwrap(), "21");
        assert_eq!(render_value("{{ value_json.x | float(0) }}", p).unwrap(), "0");
        assert_eq!(
            render_value("{% if value_json.state == 'ON' %}on{% else %}off{% endif %}", p).unwrap(),
            "on"
        );
        assert_eq!(render_value("{{ (value_json.state == 'ON') | iif('a', 'b') }}", p).unwrap(), "a");
        assert!(render_value("{{ value_json.missing | is_defined }}", p).is_err());
        assert_eq!(render_value("{{ value_json.nested | to_json }}", p).unwrap(), r#"{"a":"x"}"#);
    }

    #[test]
    fn command_template() {
        let out = render_command(r#"{"state": "{{ value }}"}"#, json!("ON"), &Map::new()).unwrap();
        assert_eq!(out, r#"{"state": "ON"}"#);
        let mut extra = Map::new();
        extra.insert("red".into(), json!(1));
        assert_eq!(render_command("{{ red }},{{ value }}", json!(2), &extra).unwrap(), "1,2");
    }
}
