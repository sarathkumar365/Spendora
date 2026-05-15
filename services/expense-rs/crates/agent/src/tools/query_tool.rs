use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{QueryBuilder, Row, Sqlite};
// SqlitePool now reached via AgentDeps::db

use super::common::{validate_date_opt, validate_direction};
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
        if let Some(v) = args.amount_min_cents {
            qb.push(" AND ABS(t.amount_cents) >= ").push_bind(v);
        }
        if let Some(v) = args.amount_max_cents {
            qb.push(" AND ABS(t.amount_cents) <= ").push_bind(v);
        }
        if let Some(v) = args.direction {
            qb.push(" AND t.direction = ").push_bind(v);
        }

        qb.push(format!(" ORDER BY {order_sql} LIMIT "));
        qb.push_bind(limit);

        let rows = qb.build().fetch_all(db).await?;

        let mut txn_ids: Vec<String> = Vec::with_capacity(rows.len());
        let mut total_outflow_cents: i64 = 0;
        let mut total_inflow_cents: i64 = 0;

        let items: Vec<Value> = rows
            .iter()
            .map(|row| {
                let id: String = row.get("id");
                let amount: i64 = row.get("amount_cents");
                let direction: String = row.get("direction");
                if direction == "debit" {
                    total_outflow_cents += amount.abs();
                } else if direction == "credit" {
                    total_inflow_cents += amount.abs();
                }
                txn_ids.push(id.clone());
                json!({
                    "id": id,
                    "account_id": row.get::<String, _>("account_id"),
                    "account_name": row.get::<Option<String>, _>("account_name"),
                    "amount_cents": amount,
                    "currency": row.get::<String, _>("currency_code"),
                    "description": row.get::<String, _>("description"),
                    "booked_at": row.get::<String, _>("booked_at"),
                    "direction": direction,
                })
            })
            .collect();

        let summary = if items.is_empty() {
            "0 transactions".to_string()
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
                "transactions": items,
            }),
            transaction_ids: txn_ids,
        })
    }
}

