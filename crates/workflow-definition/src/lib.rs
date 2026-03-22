mod dsl;
pub mod validate;

pub use dsl::*;
pub use validate::{validate_definition, ValidationError};

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        validate::{validate_definition, ValidationError},
        BranchMetadata, Edge, EmitEventMetadata, EventTrigger, FlowDefinition, SleepMetadata,
        StepDefinition, StepKind, StepMetadata, TaskMetadata, Trigger, WaitForEventMetadata,
    };

    #[test]
    fn definition_roundtrip_serializes() {
        let definition = FlowDefinition {
            flow_id: "order-fulfillment".to_owned(),
            version: 1,
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "order.created".to_owned(),
            })),
            steps: vec![
                StepDefinition {
                    step_id: "reserve_inventory".to_owned(),
                    kind: StepKind::Task,
                    metadata: StepMetadata {
                        task: Some(TaskMetadata {
                            handler: "inventory.reserve".to_owned(),
                        }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    step_id: "wait_payment".to_owned(),
                    kind: StepKind::WaitForEvent,
                    metadata: StepMetadata {
                        wait_for_event: Some(WaitForEventMetadata {
                            event_type: "payment.captured".to_owned(),
                        }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    step_id: "notify".to_owned(),
                    kind: StepKind::EmitEvent,
                    metadata: StepMetadata {
                        emit_event: Some(EmitEventMetadata {
                            event_type: "order.ready".to_owned(),
                        }),
                        ..StepMetadata::default()
                    },
                },
            ],
            edges: vec![
                Edge {
                    from: "reserve_inventory".to_owned(),
                    to: "wait_payment".to_owned(),
                },
                Edge {
                    from: "wait_payment".to_owned(),
                    to: "notify".to_owned(),
                },
            ],
        };

        let json = serde_json::to_value(&definition).expect("definition serializes");

        assert_eq!(
            json,
            json!({
                "flow_id": "order-fulfillment",
                "version": 1,
                "trigger": {
                    "type": "event",
                    "event_type": "order.created"
                },
                "steps": [
                    {
                        "step_id": "reserve_inventory",
                        "kind": "task",
                        "task": {
                            "handler": "inventory.reserve"
                        }
                    },
                    {
                        "step_id": "wait_payment",
                        "kind": "wait_for_event",
                        "wait_for_event": {
                            "event_type": "payment.captured"
                        }
                    },
                    {
                        "step_id": "notify",
                        "kind": "emit_event",
                        "emit_event": {
                            "event_type": "order.ready"
                        }
                    }
                ],
                "edges": [
                    {
                        "from": "reserve_inventory",
                        "to": "wait_payment"
                    },
                    {
                        "from": "wait_payment",
                        "to": "notify"
                    }
                ]
            })
        );

        let roundtrip: FlowDefinition =
            serde_json::from_value(json).expect("definition deserializes");

        assert_eq!(roundtrip, definition);
    }

    #[test]
    fn definition_rejects_unknown_edge_target() {
        let definition = FlowDefinition {
            flow_id: "missing-edge-target".to_owned(),
            version: 1,
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "order.created".to_owned(),
            })),
            steps: vec![StepDefinition {
                step_id: "start".to_owned(),
                kind: StepKind::Task,
                metadata: StepMetadata {
                    task: Some(TaskMetadata {
                        handler: "inventory.reserve".to_owned(),
                    }),
                    ..StepMetadata::default()
                },
            }],
            edges: vec![Edge {
                from: "start".to_owned(),
                to: "missing".to_owned(),
            }],
        };

        let error =
            validate_definition(&definition).expect_err("definition should fail validation");

        assert_eq!(
            error,
            ValidationError::UnknownEdgeTarget {
                edge_from: "start".to_owned(),
                edge_to: "missing".to_owned(),
            }
        );
    }

    #[test]
    fn definition_rejects_duplicate_step_ids() {
        let definition = FlowDefinition {
            flow_id: "duplicate-step".to_owned(),
            version: 1,
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "order.created".to_owned(),
            })),
            steps: vec![
                StepDefinition {
                    step_id: "start".to_owned(),
                    kind: StepKind::Task,
                    metadata: StepMetadata {
                        task: Some(TaskMetadata {
                            handler: "inventory.reserve".to_owned(),
                        }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    step_id: "start".to_owned(),
                    kind: StepKind::Sleep,
                    metadata: StepMetadata {
                        sleep: Some(SleepMetadata { seconds: 30 }),
                        ..StepMetadata::default()
                    },
                },
            ],
            edges: vec![],
        };

        let error =
            validate_definition(&definition).expect_err("definition should fail validation");

        assert_eq!(
            error,
            ValidationError::DuplicateStepId {
                step_id: "start".to_owned(),
            }
        );
    }

    #[test]
    fn definition_rejects_missing_trigger() {
        let definition = FlowDefinition {
            flow_id: "missing-trigger".to_owned(),
            version: 1,
            trigger: None,
            steps: vec![],
            edges: vec![],
        };

        let error =
            validate_definition(&definition).expect_err("definition should fail validation");

        assert_eq!(error, ValidationError::MissingTrigger);
    }

    #[test]
    fn definition_rejects_unsupported_step_kind_metadata() {
        let definition = FlowDefinition {
            flow_id: "invalid-metadata".to_owned(),
            version: 1,
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "order.created".to_owned(),
            })),
            steps: vec![StepDefinition {
                step_id: "branch".to_owned(),
                kind: StepKind::Branch,
                metadata: StepMetadata {
                    task: Some(TaskMetadata {
                        handler: "branch.handler".to_owned(),
                    }),
                    branch: Some(BranchMetadata {
                        expression: "payload.approved".to_owned(),
                    }),
                    ..StepMetadata::default()
                },
            }],
            edges: vec![],
        };

        let error =
            validate_definition(&definition).expect_err("definition should fail validation");

        assert_eq!(
            error,
            ValidationError::UnsupportedStepKindMetadata {
                step_id: "branch".to_owned(),
                kind: StepKind::Branch,
                metadata_field: "task".to_owned(),
            }
        );
    }
}
