use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use storage_sqlite::{
    find_category_by_slug_or_name, list_merchants_in_window,
    load_assignments_for_category, upsert_merchant_category_assignment, MerchantInWindow,
};

use crate::llm::{ChatCompletionRequest, ChatMessage, ToolDefinition};

use super::{AgentDeps, Tool, ToolOutput};

/// Threshold under which the LLM's confidence is discarded (treated as "not in this category").
/// Minimum confidence to flag a merchant as `included=true` in the suggested list.
/// Raised from 0.4 → 0.5 in Phase 7c after the audit showed Dollarama / pharmacy slipping in.
const MIN_LLM_CONFIDENCE: f64 = 0.5;
/// Cap on the number of merchants we feed to the LLM in one classification call.
const MAX_MERCHANTS_PER_CALL: usize = 100;
/// Default lookback when caller doesn't pass dates.
const DEFAULT_LOOKBACK_MONTHS: i64 = 12;
/// Cap on merchant signature scan to keep prompts small.
const MERCHANT_WINDOW_LIMIT: i64 = 200;

pub struct ResolveCategoryIntentTool;

#[derive(Debug, Deserialize)]
struct Args {
    /// Category name OR slug (e.g. "groceries" or "Groceries").
    category: String,
    #[serde(default)]
    date_from: Option<String>,
    #[serde(default)]
    date_to: Option<String>,
    #[serde(default)]
    account_id: Option<String>,
}

#[async_trait]
impl Tool for ResolveCategoryIntentTool {
    fn name(&self) -> &'static str {
        "resolve_category_intent"
    }

    fn description(&self) -> &'static str {
        "Identify which of the user's merchants belong to a category (groceries, dining, transit, \
         utilities, entertainment, shopping, subscriptions, healthcare, travel, income, transfers, \
         fees, other). Call this FIRST for any category question — it returns: \n\
         - `confirmed`: merchants the user already approved for this category. Use these directly.\n\
         - `suggested`: merchants the classifier thinks belong but the user hasn't confirmed. \
           If non-empty, your final message MUST be exactly `CATEGORY_CONFIRMATION_NEEDED: <slug>` \
           so the UI can show a confirmation card. Do NOT compute the final answer yet.\n\
         - `excluded`: merchants the user explicitly said do NOT belong. Never include them.\n\
         If `suggested` is empty, proceed straight to `aggregate_transactions` with \
         `merchant_substrings` set to the confirmed merchants' normalized keys."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Category name or slug (e.g. 'groceries')."
                },
                "date_from": {
                    "type": "string",
                    "description": "Inclusive YYYY-MM-DD. Defaults to 12 months before date_to."
                },
                "date_to": {
                    "type": "string",
                    "description": "Inclusive YYYY-MM-DD. Defaults to today."
                },
                "account_id": {
                    "type": "string",
                    "description": "Optional: scope to a single account."
                }
            },
            "required": ["category"],
            "additionalProperties": false
        })
    }

    async fn invoke(&self, deps: AgentDeps<'_>, args: Value) -> Result<ToolOutput> {
        let args: Args = serde_json::from_value(args.clone())
            .map_err(|e| anyhow!("invalid arguments: {e}"))?;
        let db = deps.db;

        let category = find_category_by_slug_or_name(db, &args.category)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "unknown category '{}' — supported: groceries, dining, transit, utilities, \
                     entertainment, shopping, subscriptions, healthcare, travel, income, \
                     transfers, fees, other",
                    args.category
                )
            })?;

        let (date_from, date_to) = resolve_window(args.date_from.as_deref(), args.date_to.as_deref())?;

        // Lazily populates merchant_signatures for any unseen merchants.
        let merchants_in_window = list_merchants_in_window(
            db,
            &date_from,
            &date_to,
            args.account_id.as_deref(),
            Some("debit"),
            MERCHANT_WINDOW_LIMIT,
        )
        .await?;

        let existing = load_assignments_for_category(db, &category.id).await?;
        let mut included_ids: HashSet<String> = HashSet::new();
        let mut excluded_ids: HashSet<String> = HashSet::new();
        let mut suggested_ids: HashSet<String> = HashSet::new();
        let mut known_classified_ids: HashSet<String> = HashSet::new();
        for a in &existing {
            known_classified_ids.insert(a.merchant_signature_id.clone());
            match a.source.as_str() {
                "user_confirmed" if a.included => {
                    included_ids.insert(a.merchant_signature_id.clone());
                }
                "user_overridden" => {
                    excluded_ids.insert(a.merchant_signature_id.clone());
                }
                "llm_suggested" if a.included => {
                    suggested_ids.insert(a.merchant_signature_id.clone());
                }
                _ => {
                    // llm_suggested with included=false: classifier said this merchant does NOT
                    // belong. Don't show, don't re-ask. Tracked via known_classified_ids.
                }
            }
        }

        let mut confirmed_out: Vec<Value> = Vec::new();
        let mut suggested_out: Vec<Value> = Vec::new();
        let mut excluded_out: Vec<Value> = Vec::new();
        let mut to_classify: Vec<&MerchantInWindow> = Vec::new();

        for m in &merchants_in_window {
            if included_ids.contains(&m.merchant_signature_id) {
                confirmed_out.push(merchant_payload(m, None));
            } else if excluded_ids.contains(&m.merchant_signature_id) {
                excluded_out.push(merchant_payload(m, None));
            } else if suggested_ids.contains(&m.merchant_signature_id) {
                // Already-pending suggestion. Carry forward without re-asking the LLM.
                let existing_conf = existing
                    .iter()
                    .find(|a| a.merchant_signature_id == m.merchant_signature_id)
                    .and_then(|a| a.confidence);
                suggested_out.push(merchant_payload(m, existing_conf));
            } else if known_classified_ids.contains(&m.merchant_signature_id) {
                // Previously classified as below-threshold for this category. Don't re-ask.
            } else {
                to_classify.push(m);
            }
        }

        let mut llm_called = false;
        if !to_classify.is_empty() {
            llm_called = true;
            let batch_size = to_classify.len().min(MAX_MERCHANTS_PER_CALL);
            let batch = &to_classify[..batch_size];
            let scores =
                classify_with_llm(deps.llm.as_ref(), &category.name, batch).await?;

            for m in batch {
                let score = scores
                    .get(&m.normalized_key)
                    .copied()
                    .unwrap_or(0.0);
                let included = score >= MIN_LLM_CONFIDENCE;
                // Persist EVERY classified merchant so we don't re-ask. Below-threshold = excluded
                // from this category (included=false), but we leave the source as llm_suggested.
                upsert_merchant_category_assignment(
                    db,
                    &m.merchant_signature_id,
                    &category.id,
                    "llm_suggested",
                    included,
                    Some(score),
                    None,
                )
                .await?;
                if included {
                    suggested_out.push(merchant_payload(m, Some(score)));
                }
            }
        }

        let requires_confirmation = !suggested_out.is_empty();
        let unique_confirmed = confirmed_out.len();
        let unique_suggested = suggested_out.len();

        let summary = if requires_confirmation {
            format!(
                "{} confirmed · {} new suggestion(s) need user OK",
                unique_confirmed, unique_suggested
            )
        } else if unique_confirmed > 0 {
            format!("{} confirmed merchants — proceed", unique_confirmed)
        } else {
            "no merchants match this category in the window".to_string()
        };

        let data = json!({
            "category": {
                "id": category.id,
                "name": category.name,
                "slug": category.slug,
            },
            "date_from": date_from,
            "date_to": date_to,
            "window_merchant_count": merchants_in_window.len(),
            "requires_user_confirmation": requires_confirmation,
            "llm_called": llm_called,
            "confirmed": confirmed_out,
            "suggested": suggested_out,
            "excluded": excluded_out,
        });

        Ok(ToolOutput {
            summary,
            data,
            transaction_ids: Vec::new(),
        })
    }
}

fn merchant_payload(m: &MerchantInWindow, confidence: Option<f64>) -> Value {
    json!({
        "merchant_signature_id": m.merchant_signature_id,
        "label": m.display_label,
        "normalized_key": m.normalized_key,
        "txn_count": m.txn_count,
        "total_cents": m.total_cents,
        "sample_descriptions": m.sample_descriptions,
        "confidence": confidence,
    })
}

fn resolve_window(date_from: Option<&str>, date_to: Option<&str>) -> Result<(String, String)> {
    let to = match date_to {
        Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| anyhow!("date_to must be YYYY-MM-DD"))?,
        None => chrono::Utc::now().date_naive(),
    };
    let from = match date_from {
        Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| anyhow!("date_from must be YYYY-MM-DD"))?,
        None => to
            .checked_sub_months(chrono::Months::new(DEFAULT_LOOKBACK_MONTHS as u32))
            .ok_or_else(|| anyhow!("date arithmetic overflow"))?,
    };
    if from > to {
        return Err(anyhow!("date_from is after date_to"));
    }
    Ok((from.to_string(), to.to_string()))
}

/// Per-category inclusion/exclusion rules baked into the classifier prompt.
/// Audited bugs (e.g. Dollarama tagged as groceries) come from missing exclusion lists;
/// this is where we encode the user-meaning of each category beyond just the label.
fn category_specific_guidance(category_name: &str) -> &'static str {
    let key = category_name.to_lowercase();
    if key.contains("groceries") {
        "INCLUDE: dedicated supermarkets and grocery stores (e.g. Loblaws, Metro, Sobeys, \
         Walmart Supercentre's grocery half, No Frills, FreshCo, Food Basics, Farm Boy, \
         Whole Foods, ethnic grocery stores).\n\
         EXCLUDE: general-merchandise stores (Dollarama, Dollar Tree), pharmacies (Shoppers \
         Drug Mart, CVS), restaurants and prepared-meal services (Tim Hortons, Uber Eats, \
         DoorDash, meal-kit boxes), gas stations even when they sell snacks, warehouse clubs \
         when the user shops there for non-food (Costco is borderline — score 0.5 unless \
         clearly grocery-dominant)."
    } else if key.contains("dining") || key.contains("restaurant") {
        "INCLUDE: restaurants, cafes, fast food, food trucks, food delivery (Uber Eats, \
         DoorDash, SkipTheDishes), bars and pubs.\n\
         EXCLUDE: grocery stores (even if they sell prepared food), meal kits delivered as \
         groceries, coffee subscriptions, vending machines."
    } else if key.contains("transit") || key.contains("fuel") {
        "INCLUDE: rideshare (Uber, Lyft), taxis, public transit (Presto, TTC, GO Transit), \
         airline tickets when used for transport, parking, tolls, gas stations.\n\
         EXCLUDE: vehicle purchase or financing, car insurance, car repair (use Other)."
    } else if key.contains("utilities") || key.contains("bills") {
        "INCLUDE: electricity, gas, water, internet, mobile/landline phone, cable/streaming \
         bundles billed by a utility, garbage/recycling.\n\
         EXCLUDE: rent (use Housing/Other), tenant insurance, standalone streaming services \
         like Netflix (those are Subscriptions/Entertainment)."
    } else if key.contains("entertainment") {
        "INCLUDE: movie tickets, concerts, sports tickets, museums, theme parks, video games \
         (one-time purchase).\n\
         EXCLUDE: streaming subscriptions (Netflix, Spotify — those are Subscriptions), bars \
         (Dining), books unless clearly leisure."
    } else if key.contains("subscriptions") {
        "INCLUDE: recurring monthly/annual digital services — Netflix, Spotify, Apple Music, \
         YouTube Premium, software SaaS, news/magazine subscriptions, gym memberships if \
         auto-billed.\n\
         EXCLUDE: utility bills (those are Utilities), one-time digital purchases."
    } else if key.contains("shopping") {
        "INCLUDE: general retail — Amazon, eBay, clothing stores, electronics retailers, \
         home goods, Costco when general-merchandise dominant, Dollarama and dollar stores.\n\
         EXCLUDE: groceries, dining, gas, services. When a merchant is ambiguous between \
         groceries and shopping, lean shopping for general-purpose stores like Walmart."
    } else if key.contains("healthcare") {
        "INCLUDE: pharmacies (Shoppers Drug Mart, Rexall, CVS), doctor's offices, dental, \
         optometrist, physiotherapy, prescription refills, medical supplies, lab fees.\n\
         EXCLUDE: gym memberships (Subscriptions), beauty products from general retail."
    } else if key.contains("travel") {
        "INCLUDE: airlines, hotels, Airbnb, car rentals, travel insurance, travel agencies, \
         cruise lines, foreign exchange when clearly tied to a trip.\n\
         EXCLUDE: local transit (Transit), commuting fuel."
    } else if key.contains("income") {
        "INCLUDE: payroll deposits, bonuses, freelance/contract payments received, dividends, \
         interest paid TO the user.\n\
         EXCLUDE: transfers between own accounts (those are Transfers)."
    } else if key.contains("transfers") {
        "INCLUDE: transfers between the user's own accounts, e-transfers sent and received \
         between own accounts, credit card payments from chequing.\n\
         EXCLUDE: payments to a vendor (those are Shopping/Dining/etc), income from external \
         parties."
    } else if key.contains("fees") {
        "INCLUDE: bank fees (NSF, overdraft, monthly), credit card annual fees, interest \
         charges, ATM withdrawal fees, foreign transaction fees, late payment fees.\n\
         EXCLUDE: actual purchases on a credit card."
    } else {
        "Use general world knowledge of merchant categories. When in doubt, score low (<0.5)."
    }
}

/// Ask the LLM to classify each merchant for the given category. Returns
/// `normalized_key -> confidence (0..1)`. Robust to JSON formatting glitches.
async fn classify_with_llm(
    llm: &dyn crate::llm::LlmProvider,
    category_name: &str,
    merchants: &[&MerchantInWindow],
) -> Result<HashMap<String, f64>> {
    let lines: Vec<String> = merchants
        .iter()
        .map(|m| format!("- {} (key: {})", m.display_label, m.normalized_key))
        .collect();

    let guidance = category_specific_guidance(category_name);
    let system = format!(
        "You are a strict merchant classifier. Decide which merchant strings BELONG to the \
         category \"{category_name}\" — and which do NOT. Be CONSERVATIVE: when a merchant \
         could belong to multiple categories, score it low (<0.5) unless it is overwhelmingly \
         dominated by this one. False positives are worse than misses — the user will be \
         re-prompted for anything borderline.\n\n\
         {guidance}\n\n\
         Output rules:\n\
         - Respond with ONLY valid JSON — no prose, no markdown fences.\n\
         - Map each merchant's `key` (the lowercase key, not the display label) to a \
           confidence between 0.0 and 1.0.\n\
         - 0.0 = definitely not in this category. 1.0 = definitely in.\n\
         - Use <0.5 for ambiguous / multi-purpose / unrelated merchants.\n\
         - Use >=0.85 ONLY when you are nearly certain.\n\
         - Include EVERY merchant in your response."
    );
    let user = format!(
        "Category: {category_name}\n\nMerchants:\n{}\n\nReturn JSON like {{\"key1\": 0.95, \"key2\": 0.10, ...}}.",
        lines.join("\n")
    );

    let req = ChatCompletionRequest {
        messages: vec![
            ChatMessage::System { content: system },
            ChatMessage::User { content: user },
        ],
        tools: Vec::<ToolDefinition>::new(),
        temperature: 0.0,
    };

    let response = llm.complete(req).await?;
    let content = match response.message {
        ChatMessage::Assistant { content: Some(c), .. } => c,
        _ => return Err(anyhow!("classifier LLM returned no content")),
    };

    parse_classifier_json(&content)
        .ok_or_else(|| anyhow!("classifier returned unparseable JSON: {}", trim_for_log(&content, 256)))
}

/// Extract a JSON object from LLM output, tolerating ```json ... ``` fences and surrounding prose.
fn parse_classifier_json(content: &str) -> Option<HashMap<String, f64>> {
    let start = content.find('{')?;
    let end = content.rfind('}')?;
    if end <= start {
        return None;
    }
    let candidate = &content[start..=end];
    let parsed: HashMap<String, serde_json::Value> = serde_json::from_str(candidate).ok()?;
    let mut out = HashMap::with_capacity(parsed.len());
    for (k, v) in parsed {
        let conf = v.as_f64().or_else(|| v.as_i64().map(|n| n as f64))?;
        out.insert(k.to_lowercase(), conf.clamp(0.0, 1.0));
    }
    Some(out)
}

fn trim_for_log(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_handles_clean_json() {
        let out = parse_classifier_json(r#"{"loblaws": 0.95, "metro": 0.92}"#).unwrap();
        assert_eq!(out.get("loblaws"), Some(&0.95));
        assert_eq!(out.get("metro"), Some(&0.92));
    }

    #[test]
    fn parser_extracts_json_from_markdown_fence() {
        let raw = "Here is the result:\n```json\n{\"a\": 0.8, \"b\": 0.1}\n```";
        let out = parse_classifier_json(raw).unwrap();
        assert_eq!(out.get("a"), Some(&0.8));
    }

    #[test]
    fn parser_returns_none_for_garbage() {
        assert!(parse_classifier_json("not json at all").is_none());
    }

    #[test]
    fn parser_clamps_out_of_range_values() {
        let out = parse_classifier_json(r#"{"a": 1.5, "b": -0.2}"#).unwrap();
        assert_eq!(out.get("a"), Some(&1.0));
        assert_eq!(out.get("b"), Some(&0.0));
    }

    #[test]
    fn resolve_window_defaults_to_12mo_lookback() {
        let (from, to) = resolve_window(None, Some("2026-05-15")).unwrap();
        assert_eq!(to, "2026-05-15");
        assert_eq!(from, "2025-05-15");
    }

    #[test]
    fn resolve_window_rejects_inverted_range() {
        let err = resolve_window(Some("2026-05-20"), Some("2026-05-10")).unwrap_err();
        assert!(err.to_string().contains("after"));
    }
}
