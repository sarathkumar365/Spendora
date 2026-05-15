use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use std::collections::HashMap;
use storage_sqlite::{normalize_merchant_key, SqlitePool};

use super::{Tool, ToolOutput};

pub struct FindRecurringTool;

#[derive(Debug, Deserialize)]
struct RecurringArgs {
    #[serde(default)]
    lookback_months: Option<i64>,
    #[serde(default)]
    min_occurrences: Option<i64>,
    #[serde(default)]
    merchant_substring: Option<String>,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    date_to: Option<String>,
}

#[async_trait]
impl Tool for FindRecurringTool {
    fn name(&self) -> &'static str {
        "find_recurring"
    }

    fn description(&self) -> &'static str {
        "Detect recurring charges (subscriptions, monthly bills, paychecks). Groups debit \
         transactions by normalized merchant over a lookback window and identifies ones with a \
         regular cadence (weekly, biweekly, monthly, quarterly, or yearly). Returns merchant, \
         cadence label, average amount, last charge, estimated next charge, and sample \
         transaction IDs. Use for 'what subscriptions do I have?', 'what recurring charges?', \
         'find my monthly bills'."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "lookback_months": {
                    "type": "integer",
                    "description": "How many months back from today (or date_to) to scan. Default 6, max 24."
                },
                "min_occurrences": {
                    "type": "integer",
                    "description": "Minimum charges with regular cadence to flag as recurring. Default 3."
                },
                "merchant_substring": {
                    "type": "string",
                    "description": "Optional filter to scope to one merchant pattern."
                },
                "account_id": { "type": "string" },
                "date_to": {
                    "type": "string",
                    "description": "Anchor end date YYYY-MM-DD. Defaults to today."
                }
            },
            "additionalProperties": false
        })
    }

    async fn invoke(&self, db: &SqlitePool, args: Value) -> Result<ToolOutput> {
        let args: RecurringArgs = serde_json::from_value(args.clone())
            .map_err(|e| anyhow!("invalid arguments: {e}"))?;

        let lookback = args.lookback_months.unwrap_or(6).clamp(1, 24);
        let min_occ = args.min_occurrences.unwrap_or(3).max(2);

        let anchor = match args.date_to.as_deref() {
            Some(d) => NaiveDate::parse_from_str(d, "%Y-%m-%d")
                .map_err(|_| anyhow!("date_to must be YYYY-MM-DD"))?,
            None => chrono::Utc::now().date_naive(),
        };
        let date_from = anchor
            .checked_sub_months(chrono::Months::new(lookback as u32))
            .ok_or_else(|| anyhow!("date arithmetic overflow"))?;

        let mut query = String::from(
            "SELECT id, account_id, amount_cents, currency_code, description, booked_at \
             FROM transactions \
             WHERE direction = 'debit' AND booked_at >= ?1 AND booked_at <= ?2",
        );
        let mut bind_extra: Vec<String> = Vec::new();
        if let Some(account) = &args.account_id {
            query.push_str(" AND account_id = ?3");
            bind_extra.push(account.clone());
        }
        if let Some(needle) = &args.merchant_substring {
            let idx = 3 + bind_extra.len();
            query.push_str(&format!(" AND LOWER(description) LIKE ?{idx}"));
            bind_extra.push(format!("%{}%", needle.to_lowercase()));
        }
        query.push_str(" ORDER BY booked_at ASC");

        let mut q = sqlx::query(&query)
            .bind(date_from.to_string())
            .bind(anchor.to_string());
        for v in &bind_extra {
            q = q.bind(v);
        }

        let rows = q.fetch_all(db).await?;

        #[derive(Default)]
        struct Bucket {
            ids: Vec<String>,
            account_ids: Vec<String>,
            amounts: Vec<i64>,
            currencies: Vec<String>,
            dates: Vec<NaiveDate>,
            original_label: String,
        }

        let mut buckets: HashMap<String, Bucket> = HashMap::new();
        for row in rows {
            let descr: String = row.get("description");
            let key = normalize_merchant_key(&descr);
            if key.is_empty() {
                continue;
            }
            let booked: String = row.get("booked_at");
            let Ok(date) = NaiveDate::parse_from_str(&booked[..10.min(booked.len())], "%Y-%m-%d")
            else {
                continue;
            };
            let entry = buckets.entry(key.clone()).or_default();
            if entry.original_label.is_empty() {
                entry.original_label = descr.clone();
            }
            entry.ids.push(row.get::<String, _>("id"));
            entry.account_ids.push(row.get::<String, _>("account_id"));
            entry.amounts.push(row.get::<i64, _>("amount_cents"));
            entry.currencies.push(row.get::<String, _>("currency_code"));
            entry.dates.push(date);
        }

        let mut recurring: Vec<Value> = Vec::new();
        let mut sample_txn_ids: Vec<String> = Vec::new();

        for (_, b) in buckets {
            if (b.dates.len() as i64) < min_occ {
                continue;
            }
            let intervals = compute_intervals(&b.dates);
            if intervals.is_empty() {
                continue;
            }
            let median = median_days(&intervals);
            let Some(cadence) = classify_cadence(median, &intervals) else {
                continue;
            };

            let avg_cents: i64 = if b.amounts.is_empty() {
                0
            } else {
                let sum: i64 = b.amounts.iter().map(|a| a.abs()).sum();
                sum / (b.amounts.len() as i64)
            };
            // Safe: bucket has >= min_occ (≥ 2) entries by this point.
            let Some(&last_charge) = b.dates.last() else { continue };
            let Some(&first_charge) = b.dates.first() else { continue };
            let next_estimated = last_charge
                .checked_add_days(chrono::Days::new(median as u64))
                .map(|d| d.to_string());

            let mut samples = b.ids.clone();
            samples.truncate(5);
            for s in &samples {
                if !sample_txn_ids.contains(s) {
                    sample_txn_ids.push(s.clone());
                }
            }

            recurring.push(json!({
                "merchant": b.original_label,
                "occurrences": b.dates.len(),
                "cadence": cadence,
                "median_interval_days": median,
                "avg_amount_cents": avg_cents,
                "currency": b.currencies.first().cloned().unwrap_or_else(|| "CAD".into()),
                "first_charge": first_charge.to_string(),
                "last_charge": last_charge.to_string(),
                "estimated_next": next_estimated,
                "sample_txn_ids": samples,
                "total_paid_cents": b.amounts.iter().map(|a| a.abs()).sum::<i64>(),
            }));
        }

        // Sort by total spend desc
        recurring.sort_by(|a, b| {
            let ta = a.get("total_paid_cents").and_then(|v| v.as_i64()).unwrap_or(0);
            let tb = b.get("total_paid_cents").and_then(|v| v.as_i64()).unwrap_or(0);
            tb.cmp(&ta)
        });

        let summary = if recurring.is_empty() {
            format!(
                "no recurring charges found ({lookback}mo lookback, ≥{min_occ} occurrences)"
            )
        } else {
            format!("{} recurring charge(s) detected", recurring.len())
        };

        Ok(ToolOutput {
            summary,
            data: json!({
                "lookback_months": lookback,
                "min_occurrences": min_occ,
                "date_from": date_from.to_string(),
                "date_to": anchor.to_string(),
                "recurring": recurring,
            }),
            transaction_ids: sample_txn_ids,
        })
    }
}

fn compute_intervals(dates: &[NaiveDate]) -> Vec<i64> {
    let mut sorted: Vec<NaiveDate> = dates.to_vec();
    sorted.sort();
    sorted
        .windows(2)
        .map(|w| (w[1] - w[0]).num_days())
        .filter(|d| *d > 0)
        .collect()
}

fn median_days(intervals: &[i64]) -> i64 {
    let mut v = intervals.to_vec();
    v.sort();
    let n = v.len();
    if n == 0 {
        0
    } else if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2
    }
}

/// Classify cadence from median interval; require the bulk of intervals to fall in the bucket.
fn classify_cadence(median: i64, intervals: &[i64]) -> Option<&'static str> {
    let label = match median {
        5..=9 => "weekly",
        12..=17 => "biweekly",
        25..=35 => "monthly",
        55..=70 => "bi-monthly",
        85..=100 => "quarterly",
        165..=200 => "semi-annual",
        330..=400 => "annual",
        _ => return None,
    };
    let (lo, hi) = bucket(median);
    let inside = intervals.iter().filter(|d| **d >= lo && **d <= hi).count();
    let ratio = inside as f64 / intervals.len() as f64;
    if ratio >= 0.6 {
        Some(label)
    } else {
        None
    }
}

fn bucket(median: i64) -> (i64, i64) {
    match median {
        5..=9 => (4, 10),
        12..=17 => (11, 18),
        25..=35 => (23, 37),
        55..=70 => (50, 75),
        85..=100 => (80, 105),
        165..=200 => (160, 210),
        330..=400 => (320, 410),
        _ => (median - 5, median + 5),
    }
}
