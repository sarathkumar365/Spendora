use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use storage_sqlite::{
    append_category_history, find_category_by_slug_or_name, load_assignments_for_category,
    upsert_merchant_category_assignment,
};

use super::{AgentDeps, Tool, ToolOutput};

pub struct ConfirmCategoryAssignmentsTool;

#[derive(Debug, Deserialize)]
struct Args {
    category: String,
    assignments: Vec<AssignmentInput>,
}

#[derive(Debug, Deserialize)]
struct AssignmentInput {
    merchant_signature_id: String,
    included: bool,
}

#[async_trait]
impl Tool for ConfirmCategoryAssignmentsTool {
    fn name(&self) -> &'static str {
        "confirm_category_assignments"
    }

    fn description(&self) -> &'static str {
        "Persist the user's confirm/exclude choices from a category confirmation card. Each \
         assignment is recorded as either user_confirmed (included=true) or user_overridden \
         (included=false). user_overridden choices are never re-suggested by the LLM. Call this \
         AFTER resolve_category_intent and BEFORE the final aggregate_transactions call. Returns \
         the updated confirmed merchants for the category so you can pass their normalized_keys \
         straight into the next tool."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Category name or slug, e.g. 'groceries'."
                },
                "assignments": {
                    "type": "array",
                    "description": "Per-merchant user decisions.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "merchant_signature_id": { "type": "string" },
                            "included": { "type": "boolean" }
                        },
                        "required": ["merchant_signature_id", "included"]
                    }
                }
            },
            "required": ["category", "assignments"],
            "additionalProperties": false
        })
    }

    async fn invoke(&self, deps: AgentDeps<'_>, args: Value) -> Result<ToolOutput> {
        let args: Args = serde_json::from_value(args.clone())
            .map_err(|e| anyhow!("invalid arguments: {e}"))?;
        let db = deps.db;

        let category = find_category_by_slug_or_name(db, &args.category)
            .await?
            .ok_or_else(|| anyhow!("unknown category '{}'", args.category))?;

        if args.assignments.is_empty() {
            return Err(anyhow!("assignments must not be empty"));
        }

        let now = chrono::Utc::now().to_rfc3339();
        let mut confirmed_count = 0;
        let mut excluded_count = 0;

        for a in &args.assignments {
            // Verify the merchant exists to avoid orphan rows.
            let exists: Option<(i64,)> = sqlx::query_as(
                "SELECT 1 FROM merchant_signatures WHERE id = ?1",
            )
            .bind(&a.merchant_signature_id)
            .fetch_optional(db)
            .await?;
            if exists.is_none() {
                return Err(anyhow!(
                    "unknown merchant_signature_id '{}'",
                    a.merchant_signature_id
                ));
            }

            let source = if a.included { "user_confirmed" } else { "user_overridden" };
            upsert_merchant_category_assignment(
                db,
                &a.merchant_signature_id,
                &category.id,
                source,
                a.included,
                None,
                Some(&now),
            )
            .await?;
            append_category_history(
                db,
                &a.merchant_signature_id,
                &category.id,
                source,
                Some(if a.included { "included" } else { "excluded" }),
            )
            .await?;

            if a.included {
                confirmed_count += 1;
            } else {
                excluded_count += 1;
            }
        }

        // Load the now-current state for the category so the agent can use it directly.
        let current = load_assignments_for_category(db, &category.id).await?;
        let confirmed: Vec<Value> = current
            .iter()
            .filter(|a| a.source == "user_confirmed" && a.included)
            .map(|a| {
                json!({
                    "merchant_signature_id": a.merchant_signature_id,
                    "label": a.display_label,
                    "normalized_key": a.normalized_key,
                })
            })
            .collect();
        let excluded: Vec<Value> = current
            .iter()
            .filter(|a| a.source == "user_overridden")
            .map(|a| {
                json!({
                    "merchant_signature_id": a.merchant_signature_id,
                    "label": a.display_label,
                    "normalized_key": a.normalized_key,
                })
            })
            .collect();

        let summary = format!(
            "saved {confirmed_count} included, {excluded_count} excluded for {}",
            category.name
        );

        Ok(ToolOutput {
            summary,
            data: json!({
                "category": { "id": category.id, "name": category.name, "slug": category.slug },
                "confirmed": confirmed,
                "excluded": excluded,
            }),
            transaction_ids: Vec::new(),
        })
    }
}
