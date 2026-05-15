use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{QueryBuilder, Row, Sqlite};
// SqlitePool now reached via AgentDeps::db

use super::common::{validate_date_opt, validate_direction};
use super::{AgentDeps, Tool, ToolOutput};

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 500;

pub struct AggregateTransactionsTool;

#[derive(Debug, Default, Deserialize)]
struct AggArgs {
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
    group_by: String,
    metric: String,
    #[serde(default)]
    limit: Option<i64>,
}

#[async_trait]
impl Tool for AggregateTransactionsTool {
    fn name(&self) -> &'static str {
        "aggregate_transactions"
    }

    fn description(&self) -> &'static str {
        "Aggregate transactions by a dimension. Use this for 'how much', 'total', 'average', \
         'count' style questions, or anything that asks for spending broken down by category, \
         merchant, account, or time. Returns rows of {group, value} plus a grand total. Amounts \
         are in CENTS. For sum/avg/min/max, the metric is over ABS(amount_cents) (always \
         non-negative). Apply a direction filter ('debit' for spending, 'credit' for inflow) to \
         scope the metric to a money direction."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "date_from": { "type": "string", "description": "Inclusive YYYY-MM-DD." },
                "date_to": { "type": "string", "description": "Inclusive YYYY-MM-DD." },
                "account_id": { "type": "string" },
                "merchant_substring": { "type": "string" },
                "amount_min_cents": { "type": "integer" },
                "amount_max_cents": { "type": "integer" },
                "direction": {
                    "type": "string",
                    "enum": ["debit", "credit"],
                    "description": "Almost always set this. 'debit' = spending, 'credit' = inflow."
                },
                "group_by": {
                    "type": "string",
                    "enum": ["merchant", "account", "direction", "day", "week", "month", "year"],
                    "description": "Dimension to group by. 'merchant' uses the transaction description verbatim."
                },
                "metric": {
                    "type": "string",
                    "enum": ["sum", "count", "avg", "min", "max"],
                    "description": "Aggregation function. sum/avg/min/max are over ABS(amount_cents)."
                },
                "limit": {
                    "type": "integer",
                    "description": "Max groups to return (default 50, cap 500). Groups are ranked by metric DESC."
                }
            },
            "required": ["group_by", "metric"],
            "additionalProperties": false
        })
    }

    async fn invoke(&self, deps: AgentDeps<'_>, args: Value) -> Result<ToolOutput> {
        let db = deps.db;
        let args: AggArgs = serde_json::from_value(args.clone())
            .map_err(|e| anyhow!("invalid arguments: {e}"))?;

        validate_date_opt(args.date_from.as_deref(), "date_from")?;
        validate_date_opt(args.date_to.as_deref(), "date_to")?;
        validate_direction(args.direction.as_deref())?;

        let (group_sql, group_alias) = match args.group_by.as_str() {
            "merchant" => ("LOWER(TRIM(t.description))", "merchant"),
            "account" => ("COALESCE(a.name, t.account_id)", "account"),
            "direction" => ("t.direction", "direction"),
            "day" => ("substr(t.booked_at, 1, 10)", "day"),
            "week" => ("strftime('%Y-W%W', t.booked_at)", "week"),
            "month" => ("substr(t.booked_at, 1, 7)", "month"),
            "year" => ("substr(t.booked_at, 1, 4)", "year"),
            other => return Err(anyhow!("unsupported group_by '{other}'")),
        };

        let metric_sql = match args.metric.as_str() {
            "sum" => "SUM(ABS(t.amount_cents))",
            "count" => "COUNT(*)",
            "avg" => "AVG(ABS(t.amount_cents))",
            "min" => "MIN(ABS(t.amount_cents))",
            "max" => "MAX(ABS(t.amount_cents))",
            other => return Err(anyhow!("unsupported metric '{other}'")),
        };

        let limit = args.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);

        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "SELECT {group_sql} AS group_key, {metric_sql} AS metric_value, COUNT(*) AS row_count \
             FROM transactions t \
             LEFT JOIN accounts a ON a.id = t.account_id \
             WHERE 1=1"
        ));

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

        qb.push(" GROUP BY group_key ORDER BY metric_value DESC LIMIT ");
        qb.push_bind(limit);

        let rows = qb.build().fetch_all(db).await?;

        let mut grand_total: f64 = 0.0;
        let groups: Vec<Value> = rows
            .iter()
            .map(|row| {
                let key: Option<String> = row.try_get::<Option<String>, _>("group_key").ok().flatten();
                let value: f64 = row
                    .try_get::<f64, _>("metric_value")
                    .ok()
                    .or_else(|| row.try_get::<i64, _>("metric_value").ok().map(|v| v as f64))
                    .unwrap_or(0.0);
                let count: i64 = row.try_get::<i64, _>("row_count").unwrap_or(0);
                grand_total += value;
                json!({
                    group_alias: key.unwrap_or_default(),
                    "value": value,
                    "row_count": count,
                })
            })
            .collect();

        let summary = if groups.is_empty() {
            format!("no data for {} by {}", args.metric, args.group_by)
        } else if args.metric == "sum" {
            format!(
                "{} group(s) · total ${:.2}",
                groups.len(),
                grand_total / 100.0
            )
        } else if args.metric == "count" {
            format!(
                "{} group(s) · {} rows",
                groups.len(),
                grand_total as i64
            )
        } else {
            format!("{} group(s)", groups.len())
        };

        Ok(ToolOutput {
            summary,
            data: json!({
                "group_by": args.group_by,
                "metric": args.metric,
                "metric_unit": match args.metric.as_str() {
                    "count" => "rows",
                    _ => "cents",
                },
                "groups": groups,
                "grand_total": grand_total,
            }),
            transaction_ids: Vec::new(),
        })
    }
}

