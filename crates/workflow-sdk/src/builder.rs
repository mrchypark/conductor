use workflow_definition::Edge;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowBuilder {
    pub(crate) workflow_id: String,
    pub(crate) workflow_version: u32,
    pub(crate) trigger: Option<String>,
    pub(crate) steps: Vec<BuilderStep>,
    pub(crate) edges: Vec<Edge>,
}

impl WorkflowBuilder {
    pub fn new(workflow_id: impl Into<String>) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            workflow_version: 0,
            trigger: None,
            steps: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn workflow_version(mut self, workflow_version: u32) -> Self {
        self.workflow_version = workflow_version;
        self
    }

    pub fn event_trigger(mut self, event_type: impl Into<String>) -> Self {
        self.trigger = Some(event_type.into());
        self
    }

    pub fn task_step(mut self, step_id: impl Into<String>, handler: impl Into<String>) -> Self {
        self.steps.push(BuilderStep::Task {
            id: step_id.into(),
            handler: handler.into(),
        });
        self
    }

    pub fn sleep_step(mut self, step_id: impl Into<String>, seconds: u64) -> Self {
        self.steps.push(BuilderStep::Sleep {
            id: step_id.into(),
            seconds,
        });
        self
    }

    pub fn wait_for_event_step(
        mut self,
        step_id: impl Into<String>,
        event_type: impl Into<String>,
    ) -> Self {
        self.steps.push(BuilderStep::WaitForEvent {
            id: step_id.into(),
            event_type: event_type.into(),
        });
        self
    }

    pub fn branch_step(
        mut self,
        step_id: impl Into<String>,
        expression: impl Into<String>,
    ) -> Self {
        self.steps.push(BuilderStep::Branch {
            id: step_id.into(),
            expression: expression.into(),
        });
        self
    }

    pub fn emit_event_step(
        mut self,
        step_id: impl Into<String>,
        event_type: impl Into<String>,
    ) -> Self {
        self.steps.push(BuilderStep::EmitEvent {
            id: step_id.into(),
            event_type: event_type.into(),
        });
        self
    }

    pub fn edge(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.edges.push(Edge {
            from: from.into(),
            to: to.into(),
        });
        self
    }

    pub fn compile(self) -> workflow_definition::FlowDefinition {
        crate::compile::compile_workflow(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BuilderStep {
    Task { id: String, handler: String },
    Sleep { id: String, seconds: u64 },
    WaitForEvent { id: String, event_type: String },
    Branch { id: String, expression: String },
    EmitEvent { id: String, event_type: String },
}
