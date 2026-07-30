mod dsl;
pub mod validate;

pub use dsl::*;
pub use validate::{validate_definition, ValidationError};

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        validate::{validate_definition, ValidationError},
        BranchMetadata, Edge, EventTrigger, FlowDefinition, SleepMetadata, StepDefinition,
        StepKind, StepMetadata, TaskMetadata, Trigger, WorkflowDefaults, MAX_TASK_TIMEOUT_SECONDS,
    };

    #[test]
    fn definition_roundtrip_serializes() {
        let definition = FlowDefinition {
            workflow_id: "order-fulfillment".to_owned(),
            workflow_version: 1,
            defaults: WorkflowDefaults::default(),
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "order.created".to_owned(),
            })),
            steps: vec![
                StepDefinition {
                    id: "reserve_inventory".to_owned(),
                    kind: StepKind::Task,
                    metadata: StepMetadata {
                        task: Some(TaskMetadata {
                            handler: "inventory.reserve".to_owned(),
                            capability: None,
                            queue: None,
                            timeout_seconds: None,
                        }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    id: "cooldown".to_owned(),
                    kind: StepKind::Sleep,
                    metadata: StepMetadata {
                        sleep: Some(SleepMetadata { seconds: 30 }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    id: "notify_customer".to_owned(),
                    kind: StepKind::Branch,
                    metadata: StepMetadata {
                        branch: Some(BranchMetadata {
                            expression: "true".to_owned(),
                        }),
                        ..StepMetadata::default()
                    },
                },
            ],
            edges: vec![
                Edge {
                    from: "reserve_inventory".to_owned(),
                    to: "cooldown".to_owned(),
                },
                Edge {
                    from: "cooldown".to_owned(),
                    to: "notify_customer".to_owned(),
                },
            ],
        };

        let json = serde_json::to_value(&definition).expect("definition serializes");

        assert_eq!(
            json,
            json!({
                "workflow_id": "order-fulfillment",
                "workflow_version": 1,
                "defaults": {},
                "trigger": {
                    "type": "event",
                    "event_type": "order.created"
                },
                "steps": [
                    {
                        "id": "reserve_inventory",
                        "kind": "task",
                        "task": {
                            "handler": "inventory.reserve"
                        }
                    },
                    {
                        "id": "cooldown",
                        "kind": "sleep",
                        "sleep": {
                            "seconds": 30
                        }
                    },
                    {
                        "id": "notify_customer",
                        "kind": "branch",
                        "branch": {
                            "expression": "true"
                        }
                    }
                ],
                "edges": [
                    {
                        "from": "reserve_inventory",
                        "to": "cooldown"
                    },
                    {
                        "from": "cooldown",
                        "to": "notify_customer"
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
            workflow_id: "missing-edge-target".to_owned(),
            workflow_version: 1,
            defaults: WorkflowDefaults::default(),
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "order.created".to_owned(),
            })),
            steps: vec![StepDefinition {
                id: "start".to_owned(),
                kind: StepKind::Task,
                metadata: StepMetadata {
                    task: Some(TaskMetadata {
                        handler: "inventory.reserve".to_owned(),
                        capability: None,
                        queue: None,
                        timeout_seconds: None,
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
    fn definition_rejects_unknown_edge_source() {
        let definition = FlowDefinition {
            workflow_id: "missing-edge-source".to_owned(),
            workflow_version: 1,
            defaults: WorkflowDefaults::default(),
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "order.created".to_owned(),
            })),
            steps: vec![StepDefinition {
                id: "end".to_owned(),
                kind: StepKind::Task,
                metadata: StepMetadata {
                    task: Some(TaskMetadata {
                        handler: "inventory.reserve".to_owned(),
                        capability: None,
                        queue: None,
                        timeout_seconds: None,
                    }),
                    ..StepMetadata::default()
                },
            }],
            edges: vec![Edge {
                from: "missing".to_owned(),
                to: "end".to_owned(),
            }],
        };

        let error =
            validate_definition(&definition).expect_err("definition should fail validation");

        assert_eq!(
            error,
            ValidationError::UnknownEdgeSource {
                edge_from: "missing".to_owned(),
                edge_to: "end".to_owned(),
            }
        );
    }

    #[test]
    fn definition_rejects_duplicate_step_ids() {
        let definition = FlowDefinition {
            workflow_id: "duplicate-step".to_owned(),
            workflow_version: 1,
            defaults: WorkflowDefaults::default(),
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "order.created".to_owned(),
            })),
            steps: vec![
                StepDefinition {
                    id: "start".to_owned(),
                    kind: StepKind::Task,
                    metadata: StepMetadata {
                        task: Some(TaskMetadata {
                            handler: "inventory.reserve".to_owned(),
                            capability: None,
                            queue: None,
                            timeout_seconds: None,
                        }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    id: "start".to_owned(),
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
            workflow_id: "missing-trigger".to_owned(),
            workflow_version: 1,
            defaults: WorkflowDefaults::default(),
            trigger: None,
            steps: vec![],
            edges: vec![],
        };

        let error =
            validate_definition(&definition).expect_err("definition should fail validation");

        assert_eq!(
            error,
            ValidationError::MissingTrigger {
                workflow_id: "missing-trigger".to_owned(),
            }
        );
    }

    #[test]
    fn definition_rejects_unsupported_step_kind_metadata() {
        let definition = FlowDefinition {
            workflow_id: "invalid-metadata".to_owned(),
            workflow_version: 1,
            defaults: WorkflowDefaults::default(),
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "order.created".to_owned(),
            })),
            steps: vec![StepDefinition {
                id: "branch".to_owned(),
                kind: StepKind::Branch,
                metadata: StepMetadata {
                    task: Some(TaskMetadata {
                        handler: "branch.handler".to_owned(),
                        capability: None,
                        queue: None,
                        timeout_seconds: None,
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

    #[test]
    fn definition_rejects_missing_required_step_metadata() {
        let cases = [
            ("task", StepKind::Task, "task"),
            ("sleep", StepKind::Sleep, "sleep"),
            ("branch", StepKind::Branch, "branch"),
        ];

        for (step_id, kind, metadata_field) in cases {
            let definition = FlowDefinition {
                workflow_id: format!("missing-{metadata_field}"),
                workflow_version: 1,
                defaults: WorkflowDefaults::default(),
                trigger: Some(Trigger::Event(EventTrigger {
                    event_type: "order.created".to_owned(),
                })),
                steps: vec![StepDefinition {
                    id: step_id.to_owned(),
                    kind,
                    metadata: StepMetadata::default(),
                }],
                edges: vec![],
            };

            let error =
                validate_definition(&definition).expect_err("definition should fail validation");

            assert_eq!(
                error,
                ValidationError::MissingRequiredStepMetadata {
                    step_id: step_id.to_owned(),
                    kind,
                    metadata_field: metadata_field.to_owned(),
                }
            );
        }
    }

    #[test]
    fn definition_rejects_wait_for_event_step_kind() {
        let error = serde_json::from_value::<FlowDefinition>(json!({
            "workflow_id": "unsupported-wait",
            "workflow_version": 1,
            "defaults": {},
            "trigger": {
                "type": "event",
                "event_type": "order.created"
            },
            "steps": [
                {
                    "id": "wait_payment",
                    "kind": "wait_for_event",
                    "wait_for_event": {
                        "event_type": "payment.captured"
                    }
                }
            ],
            "edges": []
        }))
        .expect_err("unsupported wait_for_event step kind should fail to deserialize");

        assert!(error.to_string().contains("wait_for_event"));
    }

    #[test]
    fn definition_rejects_emit_event_step_kind() {
        let error = serde_json::from_value::<FlowDefinition>(json!({
            "workflow_id": "unsupported-emit",
            "workflow_version": 1,
            "defaults": {},
            "trigger": {
                "type": "event",
                "event_type": "order.created"
            },
            "steps": [
                {
                    "id": "emit_ready",
                    "kind": "emit_event",
                    "emit_event": {
                        "event_type": "order.ready"
                    }
                }
            ],
            "edges": []
        }))
        .expect_err("unsupported emit_event step kind should fail to deserialize");

        assert!(error.to_string().contains("emit_event"));
    }

    #[test]
    fn definition_accepts_maximum_task_timeout() {
        let definition = definition_with_task_timeout(MAX_TASK_TIMEOUT_SECONDS);

        validate_definition(&definition).expect("maximum task timeout should be supported");
    }

    #[test]
    fn definition_rejects_task_timeout_above_maximum() {
        let timeout_seconds = MAX_TASK_TIMEOUT_SECONDS + 1;
        let definition = definition_with_task_timeout(timeout_seconds);

        let error =
            validate_definition(&definition).expect_err("oversized task timeout should fail");

        assert_eq!(
            error,
            ValidationError::TaskTimeoutOutOfRange {
                step_id: "run_task".to_owned(),
                timeout_seconds,
                max_timeout_seconds: MAX_TASK_TIMEOUT_SECONDS,
            }
        );
    }

    fn definition_with_task_timeout(timeout_seconds: u64) -> FlowDefinition {
        FlowDefinition {
            workflow_id: "task-timeout-boundary".to_owned(),
            workflow_version: 1,
            defaults: WorkflowDefaults::default(),
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "task.requested".to_owned(),
            })),
            steps: vec![StepDefinition {
                id: "run_task".to_owned(),
                kind: StepKind::Task,
                metadata: StepMetadata {
                    task: Some(TaskMetadata {
                        handler: "task.run".to_owned(),
                        capability: None,
                        queue: None,
                        timeout_seconds: Some(timeout_seconds),
                    }),
                    ..StepMetadata::default()
                },
            }],
            edges: Vec::new(),
        }
    }
}
