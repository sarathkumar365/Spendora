use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{QueryBuilder, Row, Sqlite};
// SqlitePool now reached via AgentDeps::db

use chrono::Datelike;
use std::collections::HashMap;
use storage_sqlite::SqlitePool;

use super::common::{
    description_matches_keys, push_merchant_substrings_or, resolve_merchant_ids_to_key_set,
    validate_date_opt, validate_direction,
};
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
    /// OR-combined list of merchant substrings (legacy; LIKE-match on description).
    /// Mutually exclusive with `merchant_substring` and `merchant_signature_ids`.
    #[serde(default)]
    merchant_substrings: Vec<String>,
    /// **Preferred for category questions.** OR-combined list of merchant_signature_id values
    /// (UUIDs from `resolve_category_intent` / `confirm_category_assignments`). Each ID is
    /// resolved to its canonical normalized_key and matched against normalize(description)
    /// — exact, no false positives from substring overlap. Mutually exclusive with the
    /// substring filters above.
    #[serde(default)]
    merchant_signature_ids: Vec<String>,
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
                "merchant_substrings": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Legacy OR'd list of substrings (LIKE-matched against description). Prefer `merchant_signature_ids` for category questions. Mutually exclusive with `merchant_substring` and `merchant_signature_ids`."
                },
                "merchant_signature_ids": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "PREFERRED for category questions. UUIDs returned by resolve_category_intent / confirm_category_assignments. Server resolves them to canonical normalized keys and matches exactly — no false positives. Mutually exclusive with `merchant_substring` and `merchant_substrings`."
                },
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

        // New path: when the agent passes merchant_signature_ids, resolve them to canonical
        // normalized keys and filter rows in Rust by exact normalized match. This is the
        // category-question path — replaces the LIKE substring matching that was prone to
        // false positives (and to the agent passing UUIDs as substrings).
        if !args.merchant_signature_ids.is_empty() {
            return aggregate_by_signature_ids(db, args).await;
        }

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

/// Aggregate via the new merchant_signature_ids path: fetch raw rows in the window with
/// all non-merchant filters applied via SQL, then in Rust filter by exact normalized-key
/// match and aggregate by the requested group_by/metric.
///
/// Returns `window_has_any_data` so the agent can distinguish "empty window" from
/// "merchant filter matched nothing" — addresses Fix #4 from the audit.
async fn aggregate_by_signature_ids(db: &SqlitePool, args: AggArgs) -> Result<ToolOutput> {
    // Validate group_by/metric early so error messages match the SQL path.
    match args.group_by.as_str() {
        "merchant" | "account" | "direction" | "day" | "week" | "month" | "year" => {}
        other => return Err(anyhow!("unsupported group_by '{other}'")),
    };
    match args.metric.as_str() {
        "sum" | "count" | "avg" | "min" | "max" => {}
        other => return Err(anyhow!("unsupported metric '{other}'")),
    };
    let limit = args.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);

    let allowed_keys = resolve_merchant_ids_to_key_set(db, &args.merchant_signature_ids)
        .await?
        .unwrap_or_default();

    // Fetch raw rows with the non-merchant filters applied via SQL.
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
        "SELECT t.id, t.account_id, t.amount_cents, t.description, t.booked_at, \
         t.direction, COALESCE(a.name, t.account_id) AS account_name \
         FROM transactions t \
         LEFT JOIN accounts a ON a.id = t.account_id \
         WHERE 1=1",
    );
    if let Some(v) = &args.date_from {
        qb.push(" AND t.booked_at >= ").push_bind(v.clone());
    }
    if let Some(v) = &args.date_to {
        qb.push(" AND t.booked_at <= ").push_bind(v.clone());
    }
    if let Some(v) = &args.account_id {
        qb.push(" AND t.account_id = ").push_bind(v.clone());
    }
    if let Some(v) = args.amount_min_cents {
        qb.push(" AND ABS(t.amount_cents) >= ").push_bind(v);
    }
    if let Some(v) = args.amount_max_cents {
        qb.push(" AND ABS(t.amount_cents) <= ").push_bind(v);
    }
    if let Some(v) = &args.direction {
        qb.push(" AND t.direction = ").push_bind(v.clone());
    }

    let rows = qb.build().fetch_all(db).await?;
    let window_has_any_data = !rows.is_empty();

    // Filter by exact normalized-key match and bucket by group_by.
    #[derive(Default)]
    struct Bucket {
        sum: f64,
        count: i64,
        min: Option<f64>,
        max: Option<f64>,
    }
    let mut buckets: HashMap<String, Bucket> = HashMap::new();
    let mut matched_rows: i64 = 0;
    for r in &rows {
        let description: String = r.get("description");
        if !description_matches_keys(&description, &allowed_keys) {
            continue;
        }
        matched_rows += 1;
        let amount: i64 = r.get("amount_cents");
        let abs_amount = amount.abs() as f64;
        let booked: String = r.get("booked_at");
        let direction: String = r.get("direction");
        let account_name: String = r.get("account_name");

        let key = match args.group_by.as_str() {
            "merchant" => description.to_lowercase().trim().to_string(),
            "account" => account_name,
            "direction" => direction,
            "day" => booked.chars().take(10).collect(),
            "week" => {
                // SQLite's strftime('%Y-W%W', ...) emits ISO-ish "YYYY-Www". Mirror that.
                match chrono::NaiveDate::parse_from_str(&booked[..10.min(booked.len())], "%Y-%m-%d") {
                    Ok(d) => format!("{}-W{:02}", d.format("%Y"), d.iso_week().week()),
                    Err(_) => booked.chars().take(7).collect(),
                }
            }
            "month" => booked.chars().take(7).collect(),
            "year" => booked.chars().take(4).collect(),
            _ => unreachable!(),
        };

        let entry = buckets.entry(key).or_default();
        entry.count += 1;
        entry.sum += abs_amount;
        entry.min = Some(entry.min.map_or(abs_amount, |m| m.min(abs_amount)));
        entry.max = Some(entry.max.map_or(abs_amount, |m| m.max(abs_amount)));
    }

    let group_alias = match args.group_by.as_str() {
        "merchant" => "merchant",
        "account" => "account",
        "direction" => "direction",
        "day" => "day",
        "week" => "week",
        "month" => "month",
        "year" => "year",
        _ => unreachable!(),
    };

    let mut sortable: Vec<(String, f64, i64)> = buckets
        .into_iter()
        .map(|(k, b)| {
            let metric_value = match args.metric.as_str() {
                "sum" => b.sum,
                "count" => b.count as f64,
                "avg" => {
                    if b.count > 0 {
                        b.sum / b.count as f64
                    } else {
                        0.0
                    }
                }
                "min" => b.min.unwrap_or(0.0),
                "max" => b.max.unwrap_or(0.0),
                _ => 0.0,
            };
            (k, metric_value, b.count)
        })
        .collect();
    sortable.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    sortable.truncate(limit as usize);

    let mut grand_total: f64 = 0.0;
    let groups: Vec<Value> = sortable
        .into_iter()
        .map(|(k, value, count)| {
            grand_total += value;
            json!({
                group_alias: k,
                "value": value,
                "row_count": count,
            })
        })
        .collect();

    let summary = if groups.is_empty() && !window_has_any_data {
        format!(
            "no transactions in window ({}..{})",
            args.date_from.as_deref().unwrap_or("…"),
            args.date_to.as_deref().unwrap_or("…")
        )
    } else if groups.is_empty() {
        format!(
            "{} txn(s) in window, but none matched the selected merchants",
            rows.len()
        )
    } else if args.metric == "sum" {
        format!(
            "{} group(s) · total ${:.2} · {} matched txns",
            groups.len(),
            grand_total / 100.0,
            matched_rows
        )
    } else if args.metric == "count" {
        format!(
            "{} group(s) · {} rows · {} matched txns",
            groups.len(),
            grand_total as i64,
            matched_rows
        )
    } else {
        format!("{} group(s) · {} matched txns", groups.len(), matched_rows)
    };

    Ok(ToolOutput {
        summary,
        data: json!({
            "group_by": args.group_by,
            "metric": args.metric,
            "metric_unit": if args.metric == "count" { "rows" } else { "cents" },
            "groups": groups,
            "grand_total": grand_total,
            "window_has_any_data": window_has_any_data,
            "window_row_count": rows.len(),
            "matched_row_count": matched_rows,
            "filter_used": "merchant_signature_ids",
        }),
        transaction_ids: Vec::new(),
    })
}

