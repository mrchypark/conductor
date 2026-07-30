use workflow_definition::{
    BranchMetadata, EventTrigger, FlowDefinition, SleepMetadata, StepDefinition, StepKind,
    StepMetadata, TaskMetadata, Trigger, WorkflowDefaults,
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
        BuilderStep::Task {
            id,
            handler,
            capability,
            queue,
            timeout_seconds,
        } => StepDefinition {
            id,
            kind: StepKind::Task,
            metadata: StepMetadata {
                task: Some(TaskMetadata {
                    handler,
                    capability,
                    queue,
                    timeout_seconds,
                }),
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
        BuilderStep::Branch { id, expression } => StepDefinition {
            id,
            kind: StepKind::Branch,
            metadata: StepMetadata {
                branch: Some(BranchMetadata { expression }),
                ..StepMetadata::default()
            },
        },
    }
}
