mod messages;
mod types;

pub use messages::*;
pub use types::*;

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        AssignedTask, CompleteTask, EmitEvent, FailTask, FailurePayload, GetRunResponse,
        HeartbeatLease, PollTask, PollTaskResponse, TaskMutationContext, TaskOutput,
    };

    #[test]
    fn complete_task_contains_attempt_and_fencing_fields() {
        let message = CompleteTask {
            lease: TaskMutationContext {
                task_id: "task-123".into(),
                attempt_id: "attempt-9".into(),
                worker_id: "worker-a".into(),
                fencing_token: 42,
            },
            result: TaskOutput {
                payload: json!({
                    "ok": true
                }),
            },
        };

        let json = serde_json::to_value(&message).expect("complete task serializes");

        assert_eq!(
            json,
            json!({
                "task_id": "task-123",
                "attempt_id": "attempt-9",
                "worker_id": "worker-a",
                "fencing_token": 42,
                "result": {
                    "payload": {
                        "ok": true
                    }
                }
            })
        );
    }

    #[test]
    fn poll_task_serializes_worker_boundary() {
        let poll = PollTask {
            worker_id: "worker-a".into(),
            worker_name: "payments-worker".into(),
            queues: vec!["payments".into(), "priority".into()],
            capabilities: vec!["emit_event".into(), "sleep".into()],
            max_inflight: 32,
        };

        let json = serde_json::to_value(&poll).expect("poll task serializes");

        assert_eq!(
            json,
            json!({
                "worker_id": "worker-a",
                "worker_name": "payments-worker",
                "queues": ["payments", "priority"],
                "capabilities": ["emit_event", "sleep"],
                "max_inflight": 32
            })
        );
    }

    #[test]
    fn poll_task_response_serializes_assigned_or_empty() {
        let assigned = serde_json::to_value(PollTaskResponse::Assigned(AssignedTask {
            task_id: "task-123".into(),
            run_id: "run-456".into(),
            attempt_id: "attempt-9".into(),
            fencing_token: 42,
            workflow_id: "order-fulfillment".into(),
            workflow_version: 1,
            step_id: "reserve_inventory".into(),
            step_kind: "task".into(),
            input: json!({
                "order_id": "ord-1"
            }),
        }))
        .expect("assigned poll response serializes");

        let empty =
            serde_json::to_value(PollTaskResponse::NoTaskAvailable).expect("empty poll serializes");

        assert_eq!(
            assigned,
            json!({
                "status": "assigned",
                "task_id": "task-123",
                "run_id": "run-456",
                "attempt_id": "attempt-9",
                "fencing_token": 42,
                "workflow_id": "order-fulfillment",
                "workflow_version": 1,
                "step_id": "reserve_inventory",
                "step_kind": "task",
                "input": {
                    "order_id": "ord-1"
                }
            })
        );
        assert_eq!(
            empty,
            json!({
                "status": "no_task_available"
            })
        );
    }

    #[test]
    fn get_run_response_serializes_basic_run_inspection() {
        let response = GetRunResponse {
            run_id: "run-456".into(),
            workflow_id: "order-fulfillment".into(),
            workflow_version: 1,
            status: "running".into(),
        };

        let json = serde_json::to_value(&response).expect("get run response serializes");

        assert_eq!(
            json,
            json!({
                "run_id": "run-456",
                "workflow_id": "order-fulfillment",
                "workflow_version": 1,
                "status": "running"
            })
        );
    }

    #[test]
    fn fail_task_contains_structured_failure_payload() {
        let fail = FailTask {
            lease: TaskMutationContext {
                task_id: "task-123".into(),
                attempt_id: "attempt-9".into(),
                worker_id: "worker-a".into(),
                fencing_token: 42,
            },
            failure: FailurePayload {
                classification: "application".into(),
                code: "PAYMENT_DECLINED".into(),
                message: "processor rejected the card".into(),
                retryable: false,
                details: json!({
                    "provider": "stripe"
                }),
            },
        };

        let json = serde_json::to_value(&fail).expect("fail task serializes");

        assert_eq!(
            json,
            json!({
                "task_id": "task-123",
                "attempt_id": "attempt-9",
                "worker_id": "worker-a",
                "fencing_token": 42,
                "failure": {
                    "classification": "application",
                    "code": "PAYMENT_DECLINED",
                    "message": "processor rejected the card",
                    "retryable": false,
                    "details": {
                        "provider": "stripe"
                    }
                }
            })
        );
    }

    #[test]
    fn task_mutations_share_lease_fields() {
        let heartbeat = serde_json::to_value(HeartbeatLease {
            lease: TaskMutationContext {
                task_id: "task-123".into(),
                attempt_id: "attempt-9".into(),
                worker_id: "worker-a".into(),
                fencing_token: 42,
            },
        })
        .expect("heartbeat serializes");

        let fail = serde_json::to_value(FailTask {
            lease: TaskMutationContext {
                task_id: "task-123".into(),
                attempt_id: "attempt-9".into(),
                worker_id: "worker-a".into(),
                fencing_token: 42,
            },
            failure: FailurePayload {
                classification: "application".into(),
                code: "PAYMENT_DECLINED".into(),
                message: "task failed".into(),
                retryable: false,
                details: json!({}),
            },
        })
        .expect("fail task serializes");

        let emit = serde_json::to_value(EmitEvent {
            lease: TaskMutationContext {
                task_id: "task-123".into(),
                attempt_id: "attempt-9".into(),
                worker_id: "worker-a".into(),
                fencing_token: 42,
            },
            event_type: "payment.captured".into(),
            payload: json!({
                "order_id": "order-1"
            }),
        })
        .expect("emit event serializes");

        for value in [heartbeat, fail, emit] {
            assert_eq!(value["task_id"], "task-123");
            assert_eq!(value["attempt_id"], "attempt-9");
            assert_eq!(value["worker_id"], "worker-a");
            assert_eq!(value["fencing_token"], 42);
        }
    }
}
