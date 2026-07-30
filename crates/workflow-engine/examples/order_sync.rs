use serde_json::json;
use tempfile::TempDir;
use workflow_engine::WorkflowEngineApp;
use workflow_sdk::WorkflowBuilder;
use workflow_worker::{Capability, InProcessWorker};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = TempDir::new()?;
    let app = WorkflowEngineApp::for_test(tempdir.path()).await?;
    let registry = app.worker_registry();

    registry
        .define_handler("orders.fetch")
        .capability(Capability::Task)
        .default_queue("default");
    registry
        .define_handler("orders.notify")
        .capability(Capability::Task)
        .default_queue("default");

    let definition = WorkflowBuilder::new("order-sync")
        .event_trigger("order.created")
        .task_step("fetch_order", "orders.fetch")
        .sleep_step("cooldown", 1)
        .task_step("notify_customer", "orders.notify")
        .edge("fetch_order", "cooldown")
        .edge("cooldown", "notify_customer")
        .compile();

    app.replace_draft(definition).await?;
    let validation = app.validate_draft("order-sync").await?;
    if !validation.ok {
        return Err("draft validation failed".into());
    }
    app.publish_draft("order-sync", &validation.content_hash)
        .await?;

    let started = app
        .ingest_event("order.created", json!({ "order_id": "ord-123" }))
        .await?;
    let run_id = started[0].run_id.clone();

    InProcessWorker::new(
        app.clone(),
        "fetch-worker".to_owned(),
        "fetch-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("orders.fetch", |input| {
        Ok(json!({
            "order_id": input["order_id"].clone(),
            "status": "fetched",
        }))
    })
    .run_once()
    .await?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    app.recovery_scan_at(now + 2).await?;

    InProcessWorker::new(
        app.clone(),
        "notify-worker".to_owned(),
        "notify-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("orders.notify", |input| {
        Ok(json!({
            "order_id": input["order_id"].clone(),
            "notification": "sent",
        }))
    })
    .run_once()
    .await?;

    let run = app
        .get_run(&run_id)
        .await?
        .expect("run should exist after execution");
    let steps = app.list_step_instances(&run_id).await?;
    let transitions = app.list_transitions(&run_id).await?;

    assert_eq!(run.status, "completed");
    assert_eq!(steps.len(), 3);
    assert_eq!(
        steps[0].output,
        Some(json!({
            "order_id": "ord-123",
            "status": "fetched",
        }))
    );
    assert_eq!(
        steps[1].output,
        Some(json!({
            "order_id": "ord-123",
            "status": "fetched",
        }))
    );
    assert_eq!(
        steps[2].output,
        Some(json!({
            "order_id": "ord-123",
            "notification": "sent",
        }))
    );
    assert!(transitions
        .iter()
        .any(|record| record.transition_type == "timer_fired"));
    assert!(transitions
        .iter()
        .any(|record| record.transition_type == "run_completed"));

    println!("run status: {}", run.status);
    println!("completed steps: {}", steps.len());
    println!("transitions: {}", transitions.len());

    Ok(())
}
