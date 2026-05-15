use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{QueryBuilder, Row, Sqlite};
// SqlitePool now reached via AgentDeps::db

use super::common::{
    description_matches_keys, push_merchant_substrings_or, resolve_merchant_ids_to_key_set,
    validate_date_opt, validate_direction,
};
use super::{AgentDeps, Tool, ToolOutput};

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 500;

pub struct QueryTransactionsTool;

#[derive(Debug, Default, Deserialize)]
struct QueryArgs {
    #[serde(default)]
    date_from: Option<String>,
    #[serde(default)]
    date_to: Option<String>,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    merchant_substring: Option<String>,
    /// OR-combined list of merchant substrings. Legacy LIKE-match.
    /// Mutually exclusive with `merchant_substring` and `merchant_signature_ids`.
    #[serde(default)]
    merchant_substrings: Vec<String>,
    /// **Preferred for category questions.** UUIDs from resolve_category_intent. Resolves
    /// to canonical normalized keys and matches exactly. Mutually exclusive with the
    /// substring filters above.
    #[serde(default)]
    merchant_signature_ids: Vec<String>,
    #[serde(default)]
    amount_min_cents: Option<i64>,
    #[serde(default)]
    amount_max_cents: Option<i64>,
    #[serde(default)]
    direction: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    order: Option<String>,
}

#[async_trait]
impl Tool for QueryTransactionsTool {
    fn name(&self) -> &'static str {
        "query_transactions"
    }

    fn description(&self) -> &'static str {
        "Return a filtered list of transactions. Use for 'show me X transactions' or when the \
         user wants individual rows. Returns up to 500 transactions per call. For totals or \
         counts, prefer aggregate_transactions. Amounts are in CENTS (1 dollar = 100). Dates \
         are ISO YYYY-MM-DD. The `direction` field is 'debit' (money out / spending) or \
         'credit' (money in / income/refunds). The booked_at field is the transaction date."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "date_from": {
                    "type": "string",
                    "description": "Inclusive start date (YYYY-MM-DD). Omit to leave open-ended."
                },
                "date_to": {
                    "type": "string",
                    "description": "Inclusive end date (YYYY-MM-DD). Omit to leave open-ended."
                },
                "account_id": {
                    "type": "string",
                    "description": "Restrict to a specific account UUID. Obtain via list_accounts_and_cards."
                },
                "merchant_substring": {
                    "type": "string",
                    "description": "Case-insensitive substring match against the transaction description / merchant. E.g. 'amazon' or 'uber'."
                },
                "merchant_substrings": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Legacy OR'd list of substrings (LIKE-matched). Prefer `merchant_signature_ids` for category questions."
                },
                "merchant_signature_ids": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "PREFERRED for category questions. UUIDs from resolve_category_intent / confirm_category_assignments. Server resolves them to canonical normalized keys and matches exactly."
                },
                "amount_min_cents": {
                    "type": "integer",
                    "description": "Inclusive minimum absolute amount in cents. E.g. 10000 = $100."
                },
                "amount_max_cents": {
                    "type": "integer",
                    "description": "Inclusive maximum absolute amount in cents."
                },
                "direction": {
                    "type": "string",
                    "enum": ["debit", "credit"],
                    "description": "Filter by money direction: debit = outflow/spending, credit = inflow/income/refunds."
                },
                "limit": {
                    "type": "integer",
                    "description": "Max rows to return. Default 100, cap 500."
                },
                "order": {
                    "type": "string",
                    "enum": ["recent_first", "oldest_first", "largest_first", "smallest_first"],
                    "description": "Sort order. Default recent_first."
                }
            },
            "additionalProperties": false
        })
    }

    async fn invoke(&self, deps: AgentDeps<'_>, args: Value) -> Result<ToolOutput> {
        let db = deps.db;
        let args: QueryArgs = serde_json::from_value(args.clone())
            .map_err(|e| anyhow!("invalid arguments: {e}"))?;

        validate_date_opt(args.date_from.as_deref(), "date_from")?;
        validate_date_opt(args.date_to.as_deref(), "date_to")?;
        validate_direction(args.direction.as_deref())?;
        let filters_in_use = [
            args.merchant_substring.is_some(),
            !args.merchant_substrings.is_empty(),
            !args.merchant_signature_ids.is_empty(),
        ]
        .iter()
        .filter(|x| **x)
        .count();
        if filters_in_use > 1 {
            return Err(anyhow!(
                "pass only one of merchant_substring / merchant_substrings / merchant_signature_ids"
            ));
        }

        // Resolve the new merchant_signature_ids filter, if any, into the allowed key set
        // we'll post-filter rows against in Rust.
        let allowed_keys = resolve_merchant_ids_to_key_set(db, &args.merchant_signature_ids)
            .await?;

        let limit = args
            .limit
            .unwrap_or(DEFAULT_LIMIT)
            .clamp(1, MAX_LIMIT);
        let order_sql = match args.order.as_deref().unwrap_or("recent_first") {
            "recent_first" => "t.booked_at DESC",
            "oldest_first" => "t.booked_at ASC",
            "largest_first" => "ABS(t.amount_cents) DESC",
            "smallest_first" => "ABS(t.amount_cents) ASC",
            other => return Err(anyhow!("unknown order '{other}'")),
        };

        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
            "SELECT t.id, t.account_id, t.amount_cents, t.currency_code, t.description, \
             t.booked_at, t.direction, a.name AS account_name \
             FROM transactions t \
             LEFT JOIN accounts a ON a.id = t.account_id \
             WHERE 1=1",
        );

        if let Some(v) = args.date_from {
            qb.push(" AND t.booked_at >= ").push_bind(v);
        }
        if let Some(v) = args.date_to {
            qb.push(" AND t.booked_at <= ").push_bind(v);
        }
        if let Some(v) = args.account_id {
            qb.push(" AND t.account_id = ").push_bind(v);
        }
        if let Some(v) = args.merchant_substring {
            let pat = format!("%{}%", v.to_lowercase());
            qb.push(" AND LOWER(t.description) LIKE ").push_bind(pat);
        }
        push_merchant_substrings_or(&mut qb, "t.description", &args.merchant_substrings);
        if let Some(v) = args.amount_min_cents {
            qb.push(" AND ABS(t.amount_cents) >= ").push_bind(v);
        }
        if let Some(v) = args.amount_max_cents {
            qb.push(" AND ABS(t.amount_cents) <= ").push_bind(v);
        }
        if let Some(v) = args.direction {
            qb.push(" AND t.direction = ").push_bind(v);
        }

        // When the new ID filter is active, drop the SQL LIMIT and apply it in Rust *after*
        // the merchant-key post-filter; otherwise we'd potentially truncate rows that would
        // have matched. Bound raw rows with a safety cap.
        const POST_FILTER_RAW_CAP: i64 = 5000;
        if allowed_keys.is_some() {
            qb.push(format!(" ORDER BY {order_sql} LIMIT "));
            qb.push_bind(POST_FILTER_RAW_CAP);
        } else {
            qb.push(format!(" ORDER BY {order_sql} LIMIT "));
            qb.push_bind(limit);
        }

        let rows = qb.build().fetch_all(db).await?;
        let window_row_count = rows.len() as i64;

        let mut txn_ids: Vec<String> = Vec::with_capacity(rows.len());
        let mut total_outflow_cents: i64 = 0;
        let mut total_inflow_cents: i64 = 0;

        let mut items: Vec<Value> = Vec::with_capacity(rows.len());
        for row in &rows {
            let description: String = row.get("description");
            if let Some(keys) = &allowed_keys {
                if !description_matches_keys(&description, keys) {
                    continue;
                }
            }
            let id: String = row.get("id");
            let amount: i64 = row.get("amount_cents");
            let direction: String = row.get("direction");
            if direction == "debit" {
                total_outflow_cents += amount.abs();
            } else if direction == "credit" {
                total_inflow_cents += amount.abs();
            }
            txn_ids.push(id.clone());
            items.push(json!({
                "id": id,
                "account_id": row.get::<String, _>("account_id"),
                "account_name": row.get::<Option<String>, _>("account_name"),
                "amount_cents": amount,
                "currency": row.get::<String, _>("currency_code"),
                "description": description,
                "booked_at": row.get::<String, _>("booked_at"),
                "direction": direction,
            }));
            if items.len() as i64 >= limit {
                break;
            }
        }
        let matched_row_count = items.len() as i64;

        let window_has_any_data = window_row_count > 0;
        let summary = if items.is_empty() && !window_has_any_data {
            "no transactions in window".to_string()
        } else if items.is_empty() {
            format!(
                "{} txn(s) in window, but none matched the merchant filter",
                window_row_count
            )
        } else {
            format!(
                "{} txns · outflow ${:.2} · inflow ${:.2}",
                items.len(),
                total_outflow_cents as f64 / 100.0,
                total_inflow_cents as f64 / 100.0,
            )
        };

        Ok(ToolOutput {
            summary,
            data: json!({
                "count": items.len(),
                "limit_hit": items.len() as i64 == limit,
                "total_outflow_cents": total_outflow_cents,
                "total_inflow_cents": total_inflow_cents,
                "window_has_any_data": window_has_any_data,
                "window_row_count": window_row_count,
                "matched_row_count": matched_row_count,
                "transactions": items,
            }),
            transaction_ids: txn_ids,
        })
    }
}

