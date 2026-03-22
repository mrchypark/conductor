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
