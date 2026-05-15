use anyhow::Result;
use async_trait::async_trait;
use serde_json::{json, Value};
use storage_sqlite::SqlitePool;

use super::{Tool, ToolOutput};

pub struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn name(&self) -> &'static str {
        "echo"
    }

    fn description(&self) -> &'static str {
        "Echo back the provided text. Debugging-only — do not call this unless the user explicitly asks you to echo."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "The text to echo back."
                }
            },
            "required": ["text"]
        })
    }

    async fn invoke(&self, _db: &SqlitePool, args: Value) -> Result<ToolOutput> {
        let text = args
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(ToolOutput {
            summary: format!("echoed: {text}"),
            data: json!({ "echo": text }),
            transaction_ids: Vec::new(),
        })
    }
}
