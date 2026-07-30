use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Task,
}

impl Capability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Task => "task",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerRegistration {
    pub handler_id: String,
    pub capability: Capability,
    pub default_queue: String,
    pub timeout_seconds: Option<u64>,
}

#[derive(Clone)]
pub struct WorkerRegistry {
    registrations: Arc<RwLock<BTreeMap<String, HandlerRegistration>>>,
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerRegistry {
    pub fn new() -> Self {
        Self {
            registrations: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn define_handler(&self, handler_id: impl Into<String>) -> HandlerRegistrationBuilder<'_> {
        let handler_id = handler_id.into();
        self.registrations
            .write()
            .expect("worker registry lock should not be poisoned")
            .insert(
                handler_id.clone(),
                HandlerRegistration {
                    handler_id: handler_id.clone(),
                    capability: Capability::Task,
                    default_queue: "default".to_owned(),
                    timeout_seconds: None,
                },
            );

        HandlerRegistrationBuilder {
            registry: self,
            handler_id,
        }
    }

    pub fn manifest(&self) -> Vec<HandlerRegistration> {
        self.registrations
            .read()
            .expect("worker registry lock should not be poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>()
    }

    pub fn registration(&self, handler_id: &str) -> Option<HandlerRegistration> {
        self.registrations
            .read()
            .expect("worker registry lock should not be poisoned")
            .get(handler_id)
            .cloned()
    }

    /// Captures all registrations under one read lock for publication-time resolution.
    pub fn manifest_snapshot(&self) -> BTreeMap<String, HandlerRegistration> {
        self.registrations
            .read()
            .expect("worker registry lock should not be poisoned")
            .clone()
    }
}

pub struct HandlerRegistrationBuilder<'a> {
    registry: &'a WorkerRegistry,
    handler_id: String,
}

impl<'a> HandlerRegistrationBuilder<'a> {
    pub fn capability(self, capability: Capability) -> Self {
        self.update(|registration| registration.capability = capability)
    }

    pub fn default_queue(self, default_queue: impl Into<String>) -> Self {
        let default_queue = default_queue.into();
        self.update(move |registration| registration.default_queue = default_queue)
    }

    pub fn timeout_seconds(self, timeout_seconds: u64) -> Self {
        self.update(|registration| registration.timeout_seconds = Some(timeout_seconds))
    }

    fn update(self, apply: impl FnOnce(&mut HandlerRegistration)) -> Self {
        if let Some(registration) = self
            .registry
            .registrations
            .write()
            .expect("worker registry lock should not be poisoned")
            .get_mut(self.handler_id.as_str())
        {
            apply(registration);
        }

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_snapshot_is_stable_after_registry_mutation() {
        let registry = WorkerRegistry::new();
        registry
            .define_handler("orders.fetch")
            .capability(Capability::Task)
            .default_queue("orders-v1")
            .timeout_seconds(30);

        let snapshot = registry.manifest_snapshot();

        registry
            .define_handler("orders.fetch")
            .capability(Capability::Task)
            .default_queue("orders-v2")
            .timeout_seconds(60);
        registry.define_handler("orders.notify");

        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            snapshot
                .get("orders.fetch")
                .expect("captured registration should remain available"),
            &HandlerRegistration {
                handler_id: "orders.fetch".to_owned(),
                capability: Capability::Task,
                default_queue: "orders-v1".to_owned(),
                timeout_seconds: Some(30),
            }
        );
        assert_eq!(
            registry
                .registration("orders.fetch")
                .expect("live registration should exist")
                .default_queue,
            "orders-v2"
        );
        assert!(!snapshot.contains_key("orders.notify"));
    }
}
