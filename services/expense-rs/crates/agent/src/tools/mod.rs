use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use storage_sqlite::SqlitePool;

use crate::llm::{LlmProvider, ToolDefinition, ToolFunctionSchema};

/// Dependencies handed to every tool's `invoke` call.
///
/// Held by value but the inner fields are cheap to clone (a pool ref + an Arc), so passing
/// `AgentDeps` to a tool is essentially free. Tools that need direct LLM access (e.g. for
/// merchant classification) call `deps.llm.complete(...)`; tools that only read SQL ignore it.
#[derive(Clone)]
pub struct AgentDeps<'a> {
    pub db: &'a SqlitePool,
    pub llm: Arc<dyn LlmProvider>,
}

impl<'a> AgentDeps<'a> {
    pub fn new(db: &'a SqlitePool, llm: Arc<dyn LlmProvider>) -> Self {
        Self { db, llm }
    }
}

pub mod accounts_tool;
pub mod aggregate_tool;
pub mod common;
pub mod compare_tool;
pub mod detail_tool;
pub mod echo_tool;
pub mod query_tool;
pub mod recurring_tool;
pub mod resolve_category_tool;

#[cfg(test)]
mod tests;

pub use accounts_tool::ListAccountsAndCardsTool;
pub use aggregate_tool::AggregateTransactionsTool;
pub use compare_tool::ComparePeriodsTool;
pub use detail_tool::TransactionDetailTool;
pub use echo_tool::EchoTool;
pub use query_tool::QueryTransactionsTool;
pub use recurring_tool::FindRecurringTool;
pub use resolve_category_tool::ResolveCategoryIntentTool;

#[derive(Debug, Clone, Serialize)]
pub struct ToolOutput {
    /// Human-readable summary fed to the LLM and shown in the UI chip.
    pub summary: String,
    /// Structured payload returned to the LLM as the tool message content.
    pub data: Value,
    /// Transaction IDs returned by this tool — used to whitelist citation chips.
    #[serde(default)]
    pub transaction_ids: Vec<String>,
}

impl ToolOutput {
    pub fn text(summary: impl Into<String>) -> Self {
        let summary = summary.into();
        Self {
            data: json!({ "message": summary }),
            summary,
            transaction_ids: Vec::new(),
        }
    }
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn parameters_schema(&self) -> Value;
    async fn invoke(&self, deps: AgentDeps<'_>, args: Value) -> Result<ToolOutput>;

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            r#type: "function",
            function: ToolFunctionSchema {
                name: self.name().to_string(),
                description: self.description().to_string(),
                parameters: self.parameters_schema(),
            },
        }
    }
}

#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: HashMap<&'static str, Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T: Tool + 'static>(&mut self, tool: T) {
        self.tools.insert(tool.name(), Arc::new(tool));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.get(name).cloned()
    }

    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools.values().map(|t| t.definition()).collect()
    }

    pub fn names(&self) -> Vec<&'static str> {
        self.tools.keys().copied().collect()
    }
}

/// Default registry for the agent: real data tools only. The `EchoTool` is kept in the
/// crate for debugging and tests but is NOT registered here — gpt-4o-mini was calling it
/// gratuitously when present.
pub fn build_default_registry() -> ToolRegistry {
    let mut reg = ToolRegistry::new();
    reg.register(ListAccountsAndCardsTool);
    reg.register(QueryTransactionsTool);
    reg.register(AggregateTransactionsTool);
    reg.register(ComparePeriodsTool);
    reg.register(FindRecurringTool);
    reg.register(TransactionDetailTool);
    reg.register(ResolveCategoryIntentTool);
    reg
}
