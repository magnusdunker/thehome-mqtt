//! MQTT topic filter matching.

/// True if `topic` matches `filter` (with `+` and `#` wildcards).
pub fn matches(filter: &str, topic: &str) -> bool {
    if filter == topic {
        return true;
    }
    // Topics starting with `$` are not matched by leading wildcards.
    if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// Valid topic for publishing: non-empty, no wildcards.
pub fn valid_publish_topic(topic: &str) -> bool {
    !topic.is_empty() && !topic.contains(['+', '#']) && topic.len() < 65_536
}

/// Valid subscription filter.
pub fn valid_filter(filter: &str) -> bool {
    if filter.is_empty() {
        return false;
    }
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, l) in levels.iter().enumerate() {
        if l.contains('#') && (*l != "#" || i != levels.len() - 1) {
            return false;
        }
        if l.contains('+') && *l != "+" {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcards() {
        assert!(matches("a/+/c", "a/b/c"));
        assert!(!matches("a/+/c", "a/b/d"));
        assert!(matches("a/#", "a/b/c"));
        assert!(matches("a/#", "a"));
        assert!(matches("homeassistant/#", "homeassistant/sensor/x/config"));
        assert!(!matches("#", "$SYS/broker"));
        assert!(!matches("a/b", "a/b/c"));
        assert!(valid_filter("a/+/#"));
        assert!(!valid_filter("a/#/b"));
        assert!(!valid_filter("a/b+"));
    }
}
