use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use storage_sqlite::normalize_merchant_key;

use super::{AgentDeps, Tool, ToolOutput};

pub struct TransactionDetailTool;

#[derive(Debug, Deserialize)]
struct DetailArgs {
    transaction_id: String,
    #[serde(default)]
    similar_lookback_months: Option<i64>,
}

#[async_trait]
impl Tool for TransactionDetailTool {
    fn name(&self) -> &'static str {
        "transaction_detail"
    }

    fn description(&self) -> &'static str {
        "Drill into a single transaction by ID. Returns the transaction itself, the account, and \
         a history of similar charges from the same merchant: count over the lookback window, \
         total paid, last 10 similar transactions. Use when the user asks 'what is this \
         charge?', 'explain this transaction', or wants context on a specific txn ID."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "transaction_id": { "type": "string", "description": "Transaction UUID." },
                "similar_lookback_months": {
                    "type": "integer",
                    "description": "Months back to scan for similar charges. Default 12, max 36."
                }
            },
            "required": ["transaction_id"],
            "additionalProperties": false
        })
    }

    async fn invoke(&self, deps: AgentDeps<'_>, args: Value) -> Result<ToolOutput> {
        let db = deps.db;
        let args: DetailArgs = serde_json::from_value(args.clone())
            .map_err(|e| anyhow!("invalid arguments: {e}"))?;

        let lookback = args.similar_lookback_months.unwrap_or(12).clamp(1, 36);

        let row = sqlx::query(
            "SELECT t.id, t.account_id, t.amount_cents, t.currency_code, t.description, \
             t.booked_at, t.direction, t.source, t.statement_id, \
             a.name AS account_name, a.account_number_ending AS account_last4 \
             FROM transactions t LEFT JOIN accounts a ON a.id = t.account_id \
             WHERE t.id = ?1",
        )
        .bind(&args.transaction_id)
        .fetch_optional(db)
        .await?;

        let Some(row) = row else {
            return Err(anyhow!(
                "transaction not found: {}",
                args.transaction_id
            ));
        };

        let description: String = row.get("description");
        let booked: String = row.get("booked_at");

        let primary = json!({
            "id": row.get::<String, _>("id"),
            "account_id": row.get::<String, _>("account_id"),
            "account_name": row.get::<Option<String>, _>("account_name"),
            "account_last4": row.get::<Option<String>, _>("account_last4"),
            "amount_cents": row.get::<i64, _>("amount_cents"),
            "currency": row.get::<String, _>("currency_code"),
            "description": description.clone(),
            "booked_at": booked.clone(),
            "direction": row.get::<String, _>("direction"),
            "source": row.get::<String, _>("source"),
            "statement_id": row.get::<Option<String>, _>("statement_id"),
        });

        // Similar charges: same normalized merchant key over lookback window
        let merchant_norm = normalize_merchant_key(&description);
        let date_from = chrono::NaiveDate::parse_from_str(&booked[..10.min(booked.len())], "%Y-%m-%d")
            .ok()
            .and_then(|d| d.checked_sub_months(chrono::Months::new(lookback as u32)))
            .map(|d| d.to_string())
            .unwrap_or_else(|| "1900-01-01".to_string());

        // Use a LIKE on the most distinctive substring of the merchant
        let like_token = merchant_norm
            .split_whitespace()
            .max_by_key(|w| w.len())
            .unwrap_or("")
            .to_string();

        let (similar_rows, similar_count, similar_total) = if like_token.len() < 3 {
            (Vec::new(), 0_i64, 0_i64)
        } else {
            let pat = format!("%{}%", like_token);
            let rows = sqlx::query(
                "SELECT id, account_id, amount_cents, currency_code, description, booked_at, direction \
                 FROM transactions \
                 WHERE LOWER(description) LIKE ?1 AND id <> ?2 AND booked_at >= ?3 \
                 ORDER BY booked_at DESC LIMIT 10",
            )
            .bind(&pat)
            .bind(&args.transaction_id)
            .bind(&date_from)
            .fetch_all(db)
            .await?;

            let totals: (i64, i64) = sqlx::query(
                "SELECT COUNT(*) AS cnt, COALESCE(SUM(ABS(amount_cents)), 0) AS total \
                 FROM transactions WHERE LOWER(description) LIKE ?1 AND id <> ?2 AND booked_at >= ?3",
            )
            .bind(&pat)
            .bind(&args.transaction_id)
            .bind(&date_from)
            .fetch_one(db)
            .await
            .map(|r| (
                r.try_get::<i64, _>("cnt").unwrap_or(0),
                r.try_get::<i64, _>("total").unwrap_or(0),
            ))
            .unwrap_or((0, 0));

            (rows, totals.0, totals.1)
        };

        let mut all_ids: Vec<String> = vec![args.transaction_id.clone()];
        let similar: Vec<Value> = similar_rows
            .iter()
            .map(|r| {
                let id: String = r.get("id");
                all_ids.push(id.clone());
                json!({
                    "id": id,
                    "account_id": r.get::<String, _>("account_id"),
                    "amount_cents": r.get::<i64, _>("amount_cents"),
                    "currency": r.get::<String, _>("currency_code"),
                    "description": r.get::<String, _>("description"),
                    "booked_at": r.get::<String, _>("booked_at"),
                    "direction": r.get::<String, _>("direction"),
                })
            })
            .collect();

        let summary = if similar_count > 0 {
            format!(
                "1 txn + {similar_count} similar in last {lookback}mo · total ${:.2}",
                similar_total as f64 / 100.0
            )
        } else {
            "1 txn · no similar charges found".to_string()
        };

        let mut combined: Vec<Value> = Vec::with_capacity(1 + similar.len());
        combined.push(primary.clone());
        combined.extend(similar.iter().cloned());

        Ok(ToolOutput {
            summary,
            data: json!({
                "transaction": primary,
                "merchant_normalized": merchant_norm,
                "similar_count_total": similar_count,
                "similar_total_cents": similar_total,
                "lookback_months": lookback,
                "similar_sample": similar,
                "transactions": combined,
            }),
            transaction_ids: all_ids,
        })
    }
}

