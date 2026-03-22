use workflow_definition::{
    BranchMetadata, EmitEventMetadata, EventTrigger, FlowDefinition, SleepMetadata, StepDefinition,
    StepKind, StepMetadata, TaskMetadata, Trigger, WaitForEventMetadata, WorkflowDefaults,
};

use crate::builder::{BuilderStep, WorkflowBuilder};

pub fn compile_workflow(builder: WorkflowBuilder) -> FlowDefinition {
    FlowDefinition {
        workflow_id: builder.workflow_id,
        workflow_version: builder.workflow_version,
        defaults: WorkflowDefaults::default(),
        trigger: builder
            .trigger
            .map(|event_type| Trigger::Event(EventTrigger { event_type })),
        steps: builder.steps.into_iter().map(compile_step).collect(),
        edges: builder.edges,
    }
}

fn compile_step(step: BuilderStep) -> StepDefinition {
    match step {
        BuilderStep::Task { id, handler } => StepDefinition {
            id,
            kind: StepKind::Task,
            metadata: StepMetadata {
                task: Some(TaskMetadata { handler }),
                ..StepMetadata::default()
            },
        },
        BuilderStep::Sleep { id, seconds } => StepDefinition {
            id,
            kind: StepKind::Sleep,
            metadata: StepMetadata {
                sleep: Some(SleepMetadata { seconds }),
                ..StepMetadata::default()
            },
        },
        BuilderStep::WaitForEvent { id, event_type } => StepDefinition {
            id,
            kind: StepKind::WaitForEvent,
            metadata: StepMetadata {
                wait_for_event: Some(WaitForEventMetadata { event_type }),
                ..StepMetadata::default()
            },
        },
        BuilderStep::Branch { id, expression } => StepDefinition {
            id,
            kind: StepKind::Branch,
            metadata: StepMetadata {
                branch: Some(BranchMetadata { expression }),
                ..StepMetadata::default()
            },
        },
        BuilderStep::EmitEvent { id, event_type } => StepDefinition {
            id,
            kind: StepKind::EmitEvent,
            metadata: StepMetadata {
                emit_event: Some(EmitEventMetadata { event_type }),
                ..StepMetadata::default()
            },
        },
    }
}
