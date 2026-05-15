use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{QueryBuilder, Row, Sqlite};
use std::collections::{HashMap, HashSet};
use storage_sqlite::SqlitePool;

use super::common::{
    description_matches_keys, resolve_merchant_ids_to_key_set, validate_date, validate_direction,
};
use super::{AgentDeps, Tool, ToolOutput};

const MAX_LIMIT: i64 = 200;

pub struct ComparePeriodsTool;

#[derive(Debug, Deserialize)]
struct CompareArgs {
    window_a: Window,
    window_b: Window,
    #[serde(default)]
    group_by: Option<String>,
    metric: String,
    #[serde(default)]
    direction: Option<String>,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    merchant_substring: Option<String>,
    /// Preferred for category questions — UUIDs from resolve_category_intent. Mutually
    /// exclusive with `merchant_substring`.
    #[serde(default)]
    merchant_signature_ids: Vec<String>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    label_a: Option<String>,
    #[serde(default)]
    label_b: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Window {
    date_from: String,
    date_to: String,
}

#[async_trait]
impl Tool for ComparePeriodsTool {
    fn name(&self) -> &'static str {
        "compare_periods"
    }

    fn description(&self) -> &'static str {
        "Compare two date windows side by side. Use for questions like 'April vs March', \
         'this month vs last', 'this year vs last year'. Pick the same date math on both sides. \
         Returns: totals for window A, totals for window B, absolute and percent differences, \
         and optionally a per-group breakdown when group_by is set. All amounts in cents."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "window_a": {
                    "type": "object",
                    "description": "First window (the older period in most comparisons).",
                    "properties": {
                        "date_from": { "type": "string" },
                        "date_to": { "type": "string" }
                    },
                    "required": ["date_from", "date_to"]
                },
                "window_b": {
                    "type": "object",
                    "description": "Second window (the newer period — diffs are computed as B − A).",
                    "properties": {
                        "date_from": { "type": "string" },
                        "date_to": { "type": "string" }
                    },
                    "required": ["date_from", "date_to"]
                },
                "metric": {
                    "type": "string",
                    "enum": ["sum", "count", "avg"],
                    "description": "Aggregation function. sum/avg are over ABS(amount_cents)."
                },
                "group_by": {
                    "type": "string",
                    "enum": ["merchant", "account", "direction"],
                    "description": "Optional. If set, also returns per-group diffs."
                },
                "direction": {
                    "type": "string",
                    "enum": ["debit", "credit"],
                    "description": "Almost always set this. 'debit' = spending."
                },
                "account_id": { "type": "string" },
                "merchant_substring": { "type": "string" },
                "merchant_signature_ids": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "PREFERRED for category questions. UUIDs from resolve_category_intent. Mutually exclusive with merchant_substring."
                },
                "limit": {
                    "type": "integer",
                    "description": "Cap on per-group rows when group_by is set (default 50, max 200)."
                },
                "label_a": { "type": "string", "description": "Optional human label for window A, e.g. 'March'." },
                "label_b": { "type": "string", "description": "Optional human label for window B, e.g. 'April'." }
            },
            "required": ["window_a", "window_b", "metric"],
            "additionalProperties": false
        })
    }

    async fn invoke(&self, deps: AgentDeps<'_>, args: Value) -> Result<ToolOutput> {
        let db = deps.db;
        let args: CompareArgs = serde_json::from_value(args.clone())
            .map_err(|e| anyhow!("invalid arguments: {e}"))?;

        validate_date(&args.window_a.date_from, "window_a.date_from")?;
        validate_date(&args.window_a.date_to, "window_a.date_to")?;
        validate_date(&args.window_b.date_from, "window_b.date_from")?;
        validate_date(&args.window_b.date_to, "window_b.date_to")?;
        validate_direction(args.direction.as_deref())?;
        if args.merchant_substring.is_some() && !args.merchant_signature_ids.is_empty() {
            return Err(anyhow!(
                "pass only one of merchant_substring or merchant_signature_ids"
            ));
        }
        let allowed_keys = resolve_merchant_ids_to_key_set(db, &args.merchant_signature_ids)
            .await?;

        let metric_sql = match args.metric.as_str() {
            "sum" => "SUM(ABS(t.amount_cents))",
            "count" => "COUNT(*)",
            "avg" => "AVG(ABS(t.amount_cents))",
            other => return Err(anyhow!("unsupported metric '{other}'")),
        };

        let group_sql_opt = match args.group_by.as_deref() {
            None => None,
            Some("merchant") => Some(("LOWER(TRIM(t.description))", "merchant")),
            Some("account") => Some(("COALESCE(a.name, t.account_id)", "account")),
            Some("direction") => Some(("t.direction", "direction")),
            Some(other) => return Err(anyhow!("unsupported group_by '{other}'")),
        };

        let limit = args.limit.unwrap_or(50).clamp(1, MAX_LIMIT);

        let label_a = args.label_a.clone().unwrap_or_else(|| {
            format!("{}…{}", args.window_a.date_from, args.window_a.date_to)
        });
        let label_b = args.label_b.clone().unwrap_or_else(|| {
            format!("{}…{}", args.window_b.date_from, args.window_b.date_to)
        });

        let filters_a = CompareFilters {
            win: &args.window_a,
            direction: args.direction.as_deref(),
            account_id: args.account_id.as_deref(),
            merchant_substring: args.merchant_substring.as_deref(),
        };
        let filters_b = CompareFilters {
            win: &args.window_b,
            direction: args.direction.as_deref(),
            account_id: args.account_id.as_deref(),
            merchant_substring: args.merchant_substring.as_deref(),
        };

        // Grand totals for both windows
        let (total_a, total_b) = if let Some(keys) = &allowed_keys {
            (
                total_by_ids(db, &args.metric, &filters_a, keys).await?,
                total_by_ids(db, &args.metric, &filters_b, keys).await?,
            )
        } else {
            (
                run_total(db, metric_sql, &filters_a).await?,
                run_total(db, metric_sql, &filters_b).await?,
            )
        };
        let (abs_diff, pct_diff) = diff(total_a, total_b);

        let mut data = json!({
            "metric": args.metric,
            "metric_unit": if args.metric == "count" { "rows" } else { "cents" },
            "window_a": { "label": label_a, "date_from": args.window_a.date_from, "date_to": args.window_a.date_to, "value": total_a },
            "window_b": { "label": label_b, "date_from": args.window_b.date_from, "date_to": args.window_b.date_to, "value": total_b },
            "absolute_diff": abs_diff,
            "percent_diff": pct_diff,
        });

        let mut summary_extra = String::new();

        if let Some((group_sql, alias)) = group_sql_opt {
            let (map_a, map_b) = if let Some(keys) = &allowed_keys {
                (
                    grouped_by_ids(db, &args.metric, alias, &filters_a, keys, limit).await?,
                    grouped_by_ids(db, &args.metric, alias, &filters_b, keys, limit).await?,
                )
            } else {
                (
                    run_grouped(db, metric_sql, group_sql, &filters_a, limit).await?,
                    run_grouped(db, metric_sql, group_sql, &filters_b, limit).await?,
                )
            };

            let mut all_keys: Vec<String> = map_a
                .keys()
                .chain(map_b.keys())
                .cloned()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            // Sort by absolute B value desc as the default order
            all_keys.sort_by(|x, y| {
                let bx = map_b.get(x).copied().unwrap_or(0.0).abs();
                let by = map_b.get(y).copied().unwrap_or(0.0).abs();
                by.partial_cmp(&bx).unwrap_or(std::cmp::Ordering::Equal)
            });
            all_keys.truncate(limit as usize);

            let groups: Vec<Value> = all_keys
                .into_iter()
                .map(|key| {
                    let a = map_a.get(&key).copied().unwrap_or(0.0);
                    let b = map_b.get(&key).copied().unwrap_or(0.0);
                    let (d, pct) = diff(a, b);
                    json!({
                        alias: key,
                        "a": a,
                        "b": b,
                        "absolute_diff": d,
                        "percent_diff": pct,
                    })
                })
                .collect();

            data["groups"] = Value::Array(groups);
            summary_extra = format!(" · grouped by {alias}");
        }

        let summary = if args.metric == "count" {
            format!(
                "{label_a}: {a} rows · {label_b}: {b} rows · Δ {d}{extra}",
                a = total_a as i64,
                b = total_b as i64,
                d = abs_diff as i64,
                extra = summary_extra,
            )
        } else {
            format!(
                "{label_a}: ${a:.2} · {label_b}: ${b:.2} · Δ ${d:+.2} ({pct:+.1}%){extra}",
                a = total_a / 100.0,
                b = total_b / 100.0,
                d = abs_diff / 100.0,
                pct = pct_diff,
                extra = summary_extra,
            )
        };

        Ok(ToolOutput {
            summary,
            data,
            transaction_ids: Vec::new(),
        })
    }
}

struct CompareFilters<'a> {
    win: &'a Window,
    direction: Option<&'a str>,
    account_id: Option<&'a str>,
    merchant_substring: Option<&'a str>,
}

fn apply_filters<'a>(qb: &mut QueryBuilder<'a, Sqlite>, f: &'a CompareFilters<'a>) {
    qb.push(" AND t.booked_at >= ").push_bind(f.win.date_from.clone());
    qb.push(" AND t.booked_at <= ").push_bind(f.win.date_to.clone());
    if let Some(v) = f.direction {
        qb.push(" AND t.direction = ").push_bind(v.to_string());
    }
    if let Some(v) = f.account_id {
        qb.push(" AND t.account_id = ").push_bind(v.to_string());
    }
    if let Some(v) = f.merchant_substring {
        let pat = format!("%{}%", v.to_lowercase());
        qb.push(" AND LOWER(t.description) LIKE ").push_bind(pat);
    }
}

async fn run_total(db: &SqlitePool, metric_sql: &str, filters: &CompareFilters<'_>) -> Result<f64> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
        "SELECT {metric_sql} AS value FROM transactions t \
         LEFT JOIN accounts a ON a.id = t.account_id \
         WHERE 1=1"
    ));
    apply_filters(&mut qb, filters);
    let row = qb.build().fetch_one(db).await?;
    Ok(row
        .try_get::<f64, _>("value")
        .ok()
        .or_else(|| row.try_get::<i64, _>("value").ok().map(|v| v as f64))
        .unwrap_or(0.0))
}

async fn run_grouped(
    db: &SqlitePool,
    metric_sql: &str,
    group_sql: &str,
    filters: &CompareFilters<'_>,
    limit: i64,
) -> Result<HashMap<String, f64>> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
        "SELECT {group_sql} AS group_key, {metric_sql} AS value \
         FROM transactions t \
         LEFT JOIN accounts a ON a.id = t.account_id \
         WHERE 1=1"
    ));
    apply_filters(&mut qb, filters);
    qb.push(" GROUP BY group_key ORDER BY value DESC LIMIT ").push_bind(limit);
    let rows = qb.build().fetch_all(db).await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let key: Option<String> = r.try_get::<Option<String>, _>("group_key").ok().flatten();
            let value: f64 = r
                .try_get::<f64, _>("value")
                .ok()
                .or_else(|| r.try_get::<i64, _>("value").ok().map(|v| v as f64))
                .unwrap_or(0.0);
            key.map(|k| (k, value))
        })
        .collect())
}

/// Total for one window using the merchant_signature_ids path: fetch raw rows, post-filter
/// by exact normalized-key match, aggregate in Rust.
async fn total_by_ids(
    db: &SqlitePool,
    metric: &str,
    filters: &CompareFilters<'_>,
    allowed_keys: &HashSet<String>,
) -> Result<f64> {
    let rows = fetch_raw_rows(db, filters).await?;
    let mut sum: f64 = 0.0;
    let mut count: i64 = 0;
    let mut min: Option<f64> = None;
    let mut max: Option<f64> = None;
    for r in &rows {
        let description: String = r.get("description");
        if !description_matches_keys(&description, allowed_keys) {
            continue;
        }
        let amount: i64 = r.get("amount_cents");
        let v = amount.abs() as f64;
        sum += v;
        count += 1;
        min = Some(min.map_or(v, |m| m.min(v)));
        max = Some(max.map_or(v, |m| m.max(v)));
    }
    Ok(match metric {
        "sum" => sum,
        "count" => count as f64,
        "avg" => {
            if count > 0 {
                sum / count as f64
            } else {
                0.0
            }
        }
        "min" => min.unwrap_or(0.0),
        "max" => max.unwrap_or(0.0),
        _ => 0.0,
    })
}

async fn grouped_by_ids(
    db: &SqlitePool,
    metric: &str,
    alias: &str,
    filters: &CompareFilters<'_>,
    allowed_keys: &HashSet<String>,
    limit: i64,
) -> Result<HashMap<String, f64>> {
    let rows = fetch_raw_rows(db, filters).await?;
    #[derive(Default)]
    struct Bucket {
        sum: f64,
        count: i64,
        min: Option<f64>,
        max: Option<f64>,
    }
    let mut buckets: HashMap<String, Bucket> = HashMap::new();
    for r in &rows {
        let description: String = r.get("description");
        if !description_matches_keys(&description, allowed_keys) {
            continue;
        }
        let amount: i64 = r.get("amount_cents");
        let v = amount.abs() as f64;
        let direction: String = r.get("direction");
        let account_name: String = r.get("account_name");
        let key = match alias {
            "merchant" => description.to_lowercase().trim().to_string(),
            "account" => account_name,
            "direction" => direction,
            _ => description.to_lowercase().trim().to_string(),
        };
        let entry = buckets.entry(key).or_default();
        entry.count += 1;
        entry.sum += v;
        entry.min = Some(entry.min.map_or(v, |m| m.min(v)));
        entry.max = Some(entry.max.map_or(v, |m| m.max(v)));
    }
    let mut out: Vec<(String, f64)> = buckets
        .into_iter()
        .map(|(k, b)| {
            let metric_value = match metric {
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
            (k, metric_value)
        })
        .collect();
    out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    out.truncate(limit as usize);
    Ok(out.into_iter().collect())
}

async fn fetch_raw_rows(
    db: &SqlitePool,
    f: &CompareFilters<'_>,
) -> Result<Vec<sqlx::sqlite::SqliteRow>> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
        "SELECT t.amount_cents, t.description, t.direction, \
         COALESCE(a.name, t.account_id) AS account_name \
         FROM transactions t \
         LEFT JOIN accounts a ON a.id = t.account_id \
         WHERE 1=1",
    );
    qb.push(" AND t.booked_at >= ").push_bind(f.win.date_from.clone());
    qb.push(" AND t.booked_at <= ").push_bind(f.win.date_to.clone());
    if let Some(v) = f.direction {
        qb.push(" AND t.direction = ").push_bind(v.to_string());
    }
    if let Some(v) = f.account_id {
        qb.push(" AND t.account_id = ").push_bind(v.to_string());
    }
    // Note: merchant_substring intentionally ignored here — when merchant_signature_ids
    // is used we apply the key-set filter in Rust afterwards.
    Ok(qb.build().fetch_all(db).await?)
}

fn diff(a: f64, b: f64) -> (f64, f64) {
    let abs = b - a;
    let pct = if a.abs() < f64::EPSILON {
        if b.abs() < f64::EPSILON {
            0.0
        } else {
            100.0
        }
    } else {
        (b - a) / a * 100.0
    };
    (abs, pct)
}

