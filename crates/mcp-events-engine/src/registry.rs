//! Event-type registry backing `events/list` and name lookup.

use std::collections::HashMap;
use std::sync::Arc;

use mcp_events_wire::EventDefinition;

struct RegistryInner {
    defs: Vec<EventDefinition>,
    index: HashMap<String, usize>,
}

/// Immutable registry of event definitions. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct Registry(Arc<RegistryInner>);

impl Registry {
    /// Definitions are kept in the given order for `list()`. On duplicate
    /// names, the later definition wins for `get()`.
    pub fn new(defs: Vec<EventDefinition>) -> Self {
        let mut index = HashMap::with_capacity(defs.len());
        for (i, d) in defs.iter().enumerate() {
            if index.insert(d.name.clone(), i).is_some() {
                tracing::warn!(name = %d.name, "duplicate event definition; later one wins for lookup");
            }
        }
        Self(Arc::new(RegistryInner { defs, index }))
    }

    pub fn list(&self) -> Vec<EventDefinition> {
        self.0.defs.clone()
    }

    pub fn get(&self, name: &str) -> Option<EventDefinition> {
        self.0
            .index
            .get(name)
            .and_then(|&i| self.0.defs.get(i))
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcp_events_wire::DeliveryMode;

    fn def(name: &str, desc: &str) -> EventDefinition {
        EventDefinition {
            name: name.to_owned(),
            description: Some(desc.to_owned()),
            delivery: vec![DeliveryMode::Poll, DeliveryMode::Push],
            input_schema: None,
            payload_schema: None,
            meta: None,
        }
    }

    #[test]
    fn list_preserves_order_and_get_finds_by_name() {
        let r = Registry::new(vec![def("a.changed", "A"), def("b.changed", "B")]);
        let listed = r.list();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].name, "a.changed");
        assert_eq!(listed[1].name, "b.changed");
        assert_eq!(r.get("b.changed").unwrap().description.as_deref(), Some("B"));
        assert!(r.get("missing").is_none());
    }

    #[test]
    fn duplicate_name_later_definition_wins_for_get() {
        let r = Registry::new(vec![def("x", "first"), def("x", "second")]);
        assert_eq!(r.get("x").unwrap().description.as_deref(), Some("second"));
        assert_eq!(r.list().len(), 2);
    }

    #[test]
    fn clones_share_state() {
        let r = Registry::new(vec![def("a", "A")]);
        let r2 = r.clone();
        assert_eq!(r2.get("a").unwrap().name, "a");
    }
}
