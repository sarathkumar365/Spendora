use std::sync::Arc;

use agent::{
    audit::AuditSink, coordinator::RunCoordinator, llm::LlmProvider, tools::ToolRegistry,
};
use storage_sqlite::SqlitePool;

#[derive(Clone)]
pub struct AppState {
    pub db: SqlitePool,
    /// Built once at startup. `None` if the LLM provider env isn't configured —
    /// the agent endpoints will return a clear error in that case.
    pub agent_provider: Option<Arc<dyn LlmProvider>>,
    pub agent_registry: Arc<ToolRegistry>,
    /// Audit sink for the agent. `Some(DbAuditSink)` in production; `None` for tests that
    /// don't care about persistence (the agent_chat handler defaults to NoopSink in that case).
    pub agent_audit: Option<Arc<dyn AuditSink>>,
    /// Coordinator for paused runs awaiting a category-confirmation continuation.
    pub agent_run_coordinator: Arc<RunCoordinator>,
}

impl AppState {
    /// Test helper: state with no agent provider, an empty registry, no audit sink,
    /// and a fresh coordinator.
    #[cfg(test)]
    pub fn new_for_tests(db: SqlitePool) -> Self {
        Self {
            db,
            agent_provider: None,
            agent_registry: Arc::new(ToolRegistry::new()),
            agent_audit: None,
            agent_run_coordinator: RunCoordinator::new(),
        }
    }
}
