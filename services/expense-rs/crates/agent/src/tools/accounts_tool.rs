use anyhow::Result;
use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::Row;

use super::{AgentDeps, Tool, ToolOutput};

pub struct ListAccountsAndCardsTool;

#[async_trait]
impl Tool for ListAccountsAndCardsTool {
    fn name(&self) -> &'static str {
        "list_accounts_and_cards"
    }

    fn description(&self) -> &'static str {
        "List the user's accounts and cards. Use this when the user references an account or \
         card by name/last4 and you need to disambiguate, or when they ask 'what accounts do I \
         have?'. Always call this before querying transactions if the user mentions a specific \
         account by name."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    }

    async fn invoke(&self, deps: AgentDeps<'_>, _args: Value) -> Result<ToolOutput> {
        let db = deps.db;
        let rows = sqlx::query(
            "SELECT id, name, currency_code, account_type, account_number_ending, customer_name \
             FROM accounts \
             ORDER BY name ASC",
        )
        .fetch_all(db)
        .await?;

        let accounts: Vec<Value> = rows
            .iter()
            .map(|row| {
                json!({
                    "id": row.get::<String, _>("id"),
                    "name": row.get::<String, _>("name"),
                    "currency": row.get::<String, _>("currency_code"),
                    "account_type": row.get::<Option<String>, _>("account_type"),
                    "last4": row.get::<Option<String>, _>("account_number_ending"),
                    "customer_name": row.get::<Option<String>, _>("customer_name"),
                })
            })
            .collect();

        let summary = format!("{} account(s)", accounts.len());

        Ok(ToolOutput {
            summary,
            data: json!({ "accounts": accounts }),
            transaction_ids: Vec::new(),
        })
    }
}
