use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::tasks::RUN_STATUS_RUNNING;
use super::{tasks::next_runtime_id, StartedRun, TaskRuntime, TaskRuntimeError};
use crate::time::now_unix_seconds;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: Option<String>,
    pub name: String,
    pub idempotency_key: Option<String>,
    pub data: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventIngestResult {
    pub accepted: bool,
    pub event_id: String,
    pub deduplicated: bool,
    pub started_runs: Vec<StartedRun>,
}

impl TaskRuntime {
    pub async fn ingest_event(
        &self,
        event_type: &str,
        payload: Value,
    ) -> Result<Vec<StartedRun>, TaskRuntimeError> {
        let results = self
            .ingest_events(vec![EventEnvelope {
                id: None,
                name: event_type.to_owned(),
                idempotency_key: None,
                data: payload,
            }])
            .await?;

        Ok(results
            .into_iter()
            .flat_map(|result| result.started_runs)
            .collect())
    }

    pub async fn ingest_events(
        &self,
        events: Vec<EventEnvelope>,
    ) -> Result<Vec<EventIngestResult>, TaskRuntimeError> {
        let mut results = Vec::with_capacity(events.len());

        for event in events {
            let now = now_unix_seconds();
            let event_id = event.id.clone().unwrap_or_else(|| next_runtime_id("event"));
            let dedupe_key = event
                .idempotency_key
                .clone()
                .or_else(|| event.id.clone())
                .filter(|value| !value.is_empty());

            let (stored, inserted) = self
                .persistence
                .insert_or_load_event_received(
                    &event_id,
                    &event.name,
                    &event.data,
                    dedupe_key.as_deref(),
                    now,
                )
                .await?;

            self.persistence
                .ensure_event_received_transition(&stored.event_id, &stored.event_type, now)
                .await?;

            if stored.processing_state == "applied" {
                results.push(EventIngestResult {
                    accepted: true,
                    event_id: stored.event_id,
                    deduplicated: true,
                    started_runs: Vec::new(),
                });
                continue;
            }

            let started_runs = self
                .apply_event(&stored.event_id, &stored.event_type, &stored.payload, now)
                .await?;
            self.persistence
                .mark_event_applied(&stored.event_id, now)
                .await?;

            results.push(EventIngestResult {
                accepted: true,
                event_id: stored.event_id,
                deduplicated: !inserted,
                started_runs: if inserted { started_runs } else { Vec::new() },
            });
        }

        Ok(results)
    }

    pub(crate) async fn apply_event(
        &self,
        event_id: &str,
        event_type: &str,
        payload: &Value,
        now: i64,
    ) -> Result<Vec<StartedRun>, TaskRuntimeError> {
        let published_definitions = self
            .persistence
            .list_latest_eligible_published_definitions()
            .await?;
        let mut started_runs = Vec::new();

        for definition in published_definitions
            .into_iter()
            .filter(|stored| stored.executable().trigger_event_type == event_type)
        {
            let executable = definition.executable();
            let run_id = deterministic_run_id(
                event_id,
                &executable.workflow_id,
                executable.workflow_version,
            );
            let run = crate::definition_store::WorkflowRunRecord {
                run_id: run_id.clone(),
                workflow_id: executable.workflow_id.clone(),
                workflow_version: executable.workflow_version,
                status: RUN_STATUS_RUNNING.to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: now,
            };
            let inserted = self.persistence.insert_run_if_absent(&run).await?;
            if !inserted {
                let existing = self.persistence.get_run(&run_id).await?.ok_or_else(|| {
                    TaskRuntimeError::RunNotFound {
                        run_id: run_id.clone(),
                    }
                })?;
                if existing.status == RUN_STATUS_RUNNING {
                    self.schedule_root_steps(&run_id, &definition, payload.clone(), now)
                        .await?;
                    self.reconcile_run_status(&run_id, now).await?;
                }
                started_runs.push(StartedRun {
                    run_id,
                    workflow_id: executable.workflow_id.clone(),
                    workflow_version: executable.workflow_version,
                });
                continue;
            }
            self.record_transition(
                &run_id,
                "run_started",
                Some(json!({
                    "event_id": event_id,
                    "workflow_id": executable.workflow_id,
                })),
                now,
            )
            .await?;
            self.schedule_root_steps(&run_id, &definition, payload.clone(), now)
                .await?;
            self.reconcile_run_status(&run_id, now).await?;

            started_runs.push(StartedRun {
                run_id,
                workflow_id: executable.workflow_id.clone(),
                workflow_version: executable.workflow_version,
            });
        }

        Ok(started_runs)
    }
}

fn deterministic_run_id(event_id: &str, workflow_id: &str, workflow_version: u32) -> String {
    let mut hasher = Sha256::new();
    hasher.update(event_id.as_bytes());
    hasher.update(b":");
    hasher.update(workflow_id.as_bytes());
    hasher.update(b":");
    hasher.update(workflow_version.to_string().as_bytes());
    let digest = hasher.finalize();
    format!("run-{digest:x}")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use tempfile::TempDir;
    use workflow_definition::{
        Edge, EventTrigger, FlowDefinition, StepDefinition, StepKind, StepMetadata, TaskMetadata,
        Trigger, WorkflowDefaults,
    };
    use workflow_protocol::{
        CompleteTask, PollTask, PollTaskResponse, TaskMutationContext, TaskOutput,
    };
    use workflow_worker::{Capability, WorkerRegistry};

    use crate::{
        definition_store::{DefinitionStore, StoredWorkflowDefinition, WorkflowRunRecord},
        persistence::{HiqlitePersistence, PersistenceError},
        runtime::{
            events::deterministic_run_id,
            tasks::{RUN_STATUS_RUNNING, STEP_STATUS_QUEUED},
            TaskRuntime, TaskRuntimeError,
        },
        stored_definition::{PublicationExecutionMetadata, ResolvedTaskRoute},
    };

    #[tokio::test]
    async fn received_event_replay_applies_durable_record_and_reports_deduplication() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let registry = WorkerRegistry::new();
        registry
            .define_handler("orders.fetch")
            .capability(Capability::Task)
            .default_queue("default");
        let runtime = TaskRuntime::new(persistence.clone(), registry);
        let now = 1_700_000_000;
        insert_snapshotted_definition(
            &persistence,
            &StoredWorkflowDefinition {
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                definition: single_task_definition(),
                content_hash: "orders-v1".to_owned(),
                created_at: now,
                published_at: Some(now),
            },
            now,
        )
        .await;
        persistence
            .insert_or_load_event_received(
                "event-before-crash",
                "order.ready",
                &json!({"order_id": "ord-replay"}),
                Some("order-replay-key"),
                now,
            )
            .await
            .expect("receipt should persist before simulated crash");
        assert_eq!(
            persistence
                .list_transitions("event-inbox")
                .await
                .expect("receipt transition should commit with the inbox row")
                .into_iter()
                .filter(|transition| transition.transition_type == "event_received")
                .count(),
            1
        );

        let results = runtime
            .ingest_events(vec![super::EventEnvelope {
                id: Some("different-retry-request-id".to_owned()),
                name: "order.ready".to_owned(),
                idempotency_key: Some("order-replay-key".to_owned()),
                data: json!({"order_id": "ord-replay"}),
            }])
            .await
            .expect("matching received replay should finish applying");

        assert_eq!(results.len(), 1);
        assert!(results[0].deduplicated);
        assert_eq!(results[0].event_id, "event-before-crash");
        assert!(results[0].started_runs.is_empty());
        let run_id = deterministic_run_id("event-before-crash", "orders", 1);
        let steps = persistence
            .list_step_instances(&run_id)
            .await
            .expect("replayed root should load");
        assert_eq!(steps.len(), 1);
        assert_eq!(
            persistence
                .list_transitions("event-inbox")
                .await
                .expect("receipt transition should be repaired")
                .into_iter()
                .filter(|transition| transition.transition_type == "event_received")
                .count(),
            1
        );
        let (stored, inserted) = persistence
            .insert_or_load_event_received(
                "another-retry-request-id",
                "order.ready",
                &json!({"order_id": "ord-replay"}),
                Some("order-replay-key"),
                now + 1,
            )
            .await
            .expect("applied replay should remain stable");
        assert!(!inserted);
        assert_eq!(stored.processing_state, "applied");
        assert_eq!(stored.event_id, "event-before-crash");
    }

    #[tokio::test]
    async fn conflicting_event_replay_is_named_and_never_overwrites_durable_evidence() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let runtime = TaskRuntime::new(persistence.clone(), WorkerRegistry::new());
        let now = 1_700_000_000;
        persistence
            .insert_or_load_event_received(
                "event-original",
                "order.ready",
                &json!({"order_id": "ord-original"}),
                Some("order-conflict-key"),
                now,
            )
            .await
            .expect("original receipt should persist");

        let error = runtime
            .ingest_events(vec![super::EventEnvelope {
                id: Some("event-conflict".to_owned()),
                name: "order.ready".to_owned(),
                idempotency_key: Some("order-conflict-key".to_owned()),
                data: json!({"order_id": "ord-conflicting"}),
            }])
            .await
            .expect_err("same dedupe key with conflicting payload must fail");
        assert!(matches!(
            error,
            TaskRuntimeError::Persistence(PersistenceError::EventInboxConflict {
                ref event_id,
                ref existing_event_id,
                ..
            }) if event_id == "event-conflict" && existing_event_id == "event-original"
        ));
        let type_error = runtime
            .ingest_events(vec![super::EventEnvelope {
                id: Some("event-type-conflict".to_owned()),
                name: "order.cancelled".to_owned(),
                idempotency_key: Some("order-conflict-key".to_owned()),
                data: json!({"order_id": "ord-original"}),
            }])
            .await
            .expect_err("same dedupe key with conflicting event type must fail");
        assert!(matches!(
            type_error,
            TaskRuntimeError::Persistence(PersistenceError::EventInboxConflict {
                ref event_id,
                ref existing_event_id,
                ..
            }) if event_id == "event-type-conflict" && existing_event_id == "event-original"
        ));

        let (stored, inserted) = persistence
            .insert_or_load_event_received(
                "event-original",
                "order.ready",
                &json!({"order_id": "ord-original"}),
                Some("order-conflict-key"),
                now + 1,
            )
            .await
            .expect("original evidence should remain unchanged");
        assert!(!inserted);
        assert_eq!(stored.payload, json!({"order_id": "ord-original"}));
        assert_eq!(stored.processing_state, "received");
    }

    #[tokio::test]
    async fn concurrent_exact_event_replay_converges_without_duplicate_runs_or_steps() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let registry = WorkerRegistry::new();
        registry
            .define_handler("orders.fetch")
            .capability(Capability::Task)
            .default_queue("default");
        let runtime = TaskRuntime::new(persistence.clone(), registry);
        let now = 1_700_000_000;
        insert_snapshotted_definition(
            &persistence,
            &StoredWorkflowDefinition {
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                definition: single_task_definition(),
                content_hash: "orders-v1".to_owned(),
                created_at: now,
                published_at: Some(now),
            },
            now,
        )
        .await;
        let envelope = super::EventEnvelope {
            id: Some("event-concurrent".to_owned()),
            name: "order.ready".to_owned(),
            idempotency_key: Some("order-concurrent-key".to_owned()),
            data: json!({"order_id": "ord-concurrent"}),
        };

        let left_runtime = runtime.clone();
        let right_runtime = runtime.clone();
        let (left, right) = tokio::join!(
            left_runtime.ingest_events(vec![envelope.clone()]),
            right_runtime.ingest_events(vec![envelope]),
        );
        let left = left.expect("left replay should converge");
        let right = right.expect("right replay should converge");
        let mut deduplicated = vec![left[0].deduplicated, right[0].deduplicated];
        deduplicated.sort_unstable();
        assert_eq!(deduplicated, vec![false, true]);

        let run_id = deterministic_run_id("event-concurrent", "orders", 1);
        assert_eq!(
            persistence
                .list_step_instances(&run_id)
                .await
                .expect("converged steps should load")
                .len(),
            1
        );
        assert_eq!(
            persistence
                .list_transitions("event-inbox")
                .await
                .expect("event transitions should load")
                .into_iter()
                .filter(|transition| transition.transition_type == "event_received")
                .count(),
            1
        );

        let replay = runtime
            .ingest_events(vec![super::EventEnvelope {
                id: Some("event-concurrent".to_owned()),
                name: "order.ready".to_owned(),
                idempotency_key: Some("order-concurrent-key".to_owned()),
                data: json!({"order_id": "ord-concurrent"}),
            }])
            .await
            .expect("applied replay should stay stable");
        assert!(replay[0].deduplicated);
        assert!(replay[0].started_runs.is_empty());
    }

    #[tokio::test]
    async fn apply_event_repairs_existing_run_missing_root_steps_once() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let registry = WorkerRegistry::new();
        registry
            .define_handler("orders.fetch")
            .capability(Capability::Task)
            .default_queue("default");
        let runtime = TaskRuntime::new(persistence.clone(), registry);
        let now = 1_700_000_000;
        let definition = single_task_definition();

        insert_snapshotted_definition(
            &persistence,
            &StoredWorkflowDefinition {
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                definition,
                content_hash: "orders-v1".to_owned(),
                created_at: now,
                published_at: Some(now),
            },
            now,
        )
        .await;
        let mut pre_snapshot = single_task_definition();
        pre_snapshot.workflow_version = 2;
        persistence
            .insert_published_definition(&StoredWorkflowDefinition {
                workflow_id: "orders".to_owned(),
                workflow_version: 2,
                definition: pre_snapshot,
                content_hash: "orders-v2".to_owned(),
                created_at: now + 1,
                published_at: Some(now + 1),
            })
            .await
            .expect("newer pre-snapshot definition should insert");
        let event_id = "evt-order-ready-1";
        let run_id = deterministic_run_id(event_id, "orders", 1);
        persistence
            .insert_run(&WorkflowRunRecord {
                run_id: run_id.clone(),
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                status: RUN_STATUS_RUNNING.to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: now,
            })
            .await
            .expect("partial run should insert");

        runtime
            .apply_event(
                event_id,
                "order.ready",
                &json!({ "order_id": "ord-123" }),
                now,
            )
            .await
            .expect("event apply should repair missing roots");
        runtime
            .apply_event(
                event_id,
                "order.ready",
                &json!({ "order_id": "ord-123" }),
                now,
            )
            .await
            .expect("event reapply should stay idempotent");

        let steps = persistence
            .list_step_instances(&run_id)
            .await
            .expect("steps should load");
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].step_id, "fetch_order");
        assert_eq!(steps[0].status, STEP_STATUS_QUEUED);

        let poll = runtime
            .poll_task(PollTask {
                worker_id: "worker-1".to_owned(),
                worker_name: "worker-1".to_owned(),
                queues: vec!["default".to_owned()],
                capabilities: vec![Capability::Task.as_str().to_owned()],
                max_inflight: 1,
            })
            .await
            .expect("poll should succeed");
        assert!(matches!(
            poll,
            workflow_protocol::PollTaskResponse::Assigned(_)
        ));
        assert!(matches!(
            poll_queue(&runtime, "default", "worker-2").await,
            PollTaskResponse::NoTaskAvailable
        ));
    }

    #[tokio::test]
    async fn apply_event_repairs_existing_root_step_missing_task_once() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let registry = WorkerRegistry::new();
        registry
            .define_handler("orders.fetch")
            .capability(Capability::Task)
            .default_queue("default");
        let runtime = TaskRuntime::new(persistence.clone(), registry);
        let now = 1_700_000_000;

        insert_snapshotted_definition(
            &persistence,
            &StoredWorkflowDefinition {
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                definition: single_task_definition(),
                content_hash: "orders-v1".to_owned(),
                created_at: now,
                published_at: Some(now),
            },
            now,
        )
        .await;

        let event_id = "evt-order-ready-2";
        let run_id = deterministic_run_id(event_id, "orders", 1);
        persistence
            .insert_run(&WorkflowRunRecord {
                run_id: run_id.clone(),
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                status: RUN_STATUS_RUNNING.to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: now,
            })
            .await
            .expect("partial run should insert");
        persistence
            .insert_step_instance(&crate::runtime::StepInstanceRecord {
                step_instance_id: "step-fetch".to_owned(),
                run_id: run_id.clone(),
                step_id: "fetch_order".to_owned(),
                status: STEP_STATUS_QUEUED.to_owned(),
                attempt_count: 0,
                input: Some(json!({ "order_id": "ord-123" })),
                output: None,
                error: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("partial step should insert");

        runtime
            .apply_event(
                event_id,
                "order.ready",
                &json!({ "order_id": "ord-123" }),
                now,
            )
            .await
            .expect("event apply should repair missing task");
        runtime
            .apply_event(
                event_id,
                "order.ready",
                &json!({ "order_id": "ord-123" }),
                now,
            )
            .await
            .expect("event reapply should stay idempotent");

        let steps = persistence
            .list_step_instances(&run_id)
            .await
            .expect("steps should load");
        assert_eq!(steps.len(), 1);

        let poll = runtime
            .poll_task(PollTask {
                worker_id: "worker-1".to_owned(),
                worker_name: "worker-1".to_owned(),
                queues: vec!["default".to_owned()],
                capabilities: vec![Capability::Task.as_str().to_owned()],
                max_inflight: 1,
            })
            .await
            .expect("poll should succeed");
        assert!(matches!(
            poll,
            workflow_protocol::PollTaskResponse::Assigned(_)
        ));
    }

    #[tokio::test]
    async fn published_routes_pin_downstream_scheduling_while_new_version_uses_new_defaults() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let registry = WorkerRegistry::new();
        registry
            .define_handler("routes.root")
            .capability(Capability::Task)
            .default_queue("root-v1");
        registry
            .define_handler("routes.child")
            .capability(Capability::Task)
            .default_queue("child-v1");
        let definitions = DefinitionStore::with_registry(persistence.clone(), registry.clone());
        let runtime = TaskRuntime::new(persistence, registry.clone());

        let draft = definitions
            .replace_draft(route_snapshot_definition())
            .await
            .expect("draft should replace");
        let validation = definitions
            .validate_draft("route-snapshot")
            .await
            .expect("draft should validate");
        assert!(validation.ok);
        assert_eq!(validation.content_hash, draft.content_hash);
        definitions
            .publish_draft("route-snapshot", &validation.content_hash)
            .await
            .expect("v1 should publish");

        let v1_runs = runtime
            .ingest_event("route.ready", json!({"version": 1}))
            .await
            .expect("v1 event should start");
        assert_eq!(v1_runs.len(), 1);
        assert_eq!(v1_runs[0].workflow_version, 1);
        let v1_root = assigned_task(
            &runtime,
            "root-v1",
            "worker-v1-root",
            "v1 root should retain its published route",
        )
        .await;

        registry
            .define_handler("routes.root")
            .capability(Capability::Task)
            .default_queue("root-v2");
        registry
            .define_handler("routes.child")
            .capability(Capability::Task)
            .default_queue("child-v2");

        runtime
            .complete_task(CompleteTask {
                lease: TaskMutationContext {
                    task_id: v1_root.task_id,
                    attempt_id: v1_root.attempt_id,
                    worker_id: "worker-v1-root".to_owned(),
                    fencing_token: v1_root.fencing_token,
                },
                result: TaskOutput {
                    payload: json!({"root": "complete"}),
                },
            })
            .await
            .expect("v1 root should complete");
        assert!(matches!(
            poll_queue(&runtime, "child-v2", "wrong-v2-worker").await,
            PollTaskResponse::NoTaskAvailable
        ));
        let v1_child = assigned_task(
            &runtime,
            "child-v1",
            "worker-v1-child",
            "v1 child must use the v1 published route",
        )
        .await;
        assert_eq!(v1_child.handler_id, "routes.child");

        definitions
            .publish_draft("route-snapshot", &validation.content_hash)
            .await
            .expect("v2 should publish with changed registry defaults");
        let v2_runs = runtime
            .ingest_event("route.ready", json!({"version": 2}))
            .await
            .expect("v2 event should start");
        assert_eq!(v2_runs.len(), 1);
        assert_eq!(v2_runs[0].workflow_version, 2);
        let v2_root = assigned_task(
            &runtime,
            "root-v2",
            "worker-v2-root",
            "v2 root should use the new published route",
        )
        .await;
        assert_eq!(v2_root.handler_id, "routes.root");
    }

    fn single_task_definition() -> FlowDefinition {
        FlowDefinition {
            workflow_id: "orders".to_owned(),
            workflow_version: 1,
            defaults: WorkflowDefaults::default(),
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "order.ready".to_owned(),
            })),
            steps: vec![StepDefinition {
                id: "fetch_order".to_owned(),
                kind: StepKind::Task,
                metadata: StepMetadata {
                    task: Some(TaskMetadata {
                        handler: "orders.fetch".to_owned(),
                        capability: None,
                        queue: None,
                        timeout_seconds: None,
                    }),
                    ..StepMetadata::default()
                },
            }],
            edges: Vec::new(),
        }
    }

    fn route_snapshot_definition() -> FlowDefinition {
        FlowDefinition {
            workflow_id: "route-snapshot".to_owned(),
            workflow_version: 0,
            defaults: WorkflowDefaults::default(),
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "route.ready".to_owned(),
            })),
            steps: vec![
                StepDefinition {
                    id: "root".to_owned(),
                    kind: StepKind::Task,
                    metadata: StepMetadata {
                        task: Some(TaskMetadata {
                            handler: "routes.root".to_owned(),
                            capability: None,
                            queue: None,
                            timeout_seconds: None,
                        }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    id: "child".to_owned(),
                    kind: StepKind::Task,
                    metadata: StepMetadata {
                        task: Some(TaskMetadata {
                            handler: "routes.child".to_owned(),
                            capability: None,
                            queue: None,
                            timeout_seconds: None,
                        }),
                        ..StepMetadata::default()
                    },
                },
            ],
            edges: vec![Edge {
                from: "root".to_owned(),
                to: "child".to_owned(),
            }],
        }
    }

    async fn poll_queue(runtime: &TaskRuntime, queue: &str, worker_id: &str) -> PollTaskResponse {
        runtime
            .poll_task(PollTask {
                worker_id: worker_id.to_owned(),
                worker_name: worker_id.to_owned(),
                queues: vec![queue.to_owned()],
                capabilities: vec![Capability::Task.as_str().to_owned()],
                max_inflight: 1,
            })
            .await
            .expect("poll should succeed")
    }

    async fn assigned_task(
        runtime: &TaskRuntime,
        queue: &str,
        worker_id: &str,
        message: &str,
    ) -> workflow_protocol::AssignedTask {
        match poll_queue(runtime, queue, worker_id).await {
            PollTaskResponse::Assigned(task) => *task,
            PollTaskResponse::NoTaskAvailable => panic!("{message}"),
        }
    }

    async fn insert_snapshotted_definition(
        persistence: &HiqlitePersistence,
        record: &StoredWorkflowDefinition,
        _now: i64,
    ) {
        let metadata = PublicationExecutionMetadata::v1(BTreeMap::from([(
            "fetch_order".to_owned(),
            ResolvedTaskRoute::new("orders.fetch", "task", "default", None)
                .expect("route should be valid"),
        )]))
        .expect("metadata should be valid");
        let metadata_json = serde_json::to_string(&metadata).expect("metadata should serialize");
        persistence
            .insert_snapshotted_published_definition(record, &metadata_json)
            .await
            .expect("snapshotted definition should insert");
    }
}
