use std::sync::Arc;

use agent::{llm::LlmProvider, tools::ToolRegistry};
use storage_sqlite::SqlitePool;

#[derive(Clone)]
pub struct AppState {
    pub db: SqlitePool,
    /// Built once at startup. `None` if the LLM provider env isn't configured —
    /// the agent endpoints will return a clear error in that case.
    pub agent_provider: Option<Arc<dyn LlmProvider>>,
    pub agent_registry: Arc<ToolRegistry>,
}

impl AppState {
    /// Test helper: build state with no agent provider and an empty registry. Tests for
    /// non-agent endpoints don't need either.
    #[cfg(test)]
    pub fn new_for_tests(db: SqlitePool) -> Self {
        Self {
            db,
            agent_provider: None,
            agent_registry: Arc::new(ToolRegistry::new()),
        }
    }
}
