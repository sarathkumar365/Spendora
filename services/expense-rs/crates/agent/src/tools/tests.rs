use super::*;
use crate::llm::{
    ChatCompletionRequest, ChatCompletionResponse, ChatMessage, LlmProvider, LlmProviderKind,
};
use anyhow::anyhow;
use async_trait::async_trait;
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use storage_sqlite::{connect, run_migrations, SqlitePool};

/// Stub LLM provider used by tool tests that don't exercise the LLM path.
/// Calling `complete` from a test that uses this stub is a bug — it panics.
struct PanicLlm;

#[async_trait]
impl LlmProvider for PanicLlm {
    async fn complete(
        &self,
        _req: ChatCompletionRequest,
    ) -> anyhow::Result<ChatCompletionResponse> {
        Err(anyhow!(
            "PanicLlm.complete called — this tool test should not invoke the LLM"
        ))
    }
    fn model_label(&self) -> String {
        "panic:test".to_string()
    }
    fn kind(&self) -> LlmProviderKind {
        LlmProviderKind::OpenAi
    }
}

fn test_deps(db: &SqlitePool) -> AgentDeps<'_> {
    AgentDeps::new(db, Arc::new(PanicLlm))
}

/// LLM stub for category classification tests. Returns a JSON object mapping each merchant
/// `key:` in the prompt to a pre-seeded confidence. Tracks call count for assertions.
struct StubClassifier {
    scores: HashMap<String, f64>,
    call_count: Mutex<usize>,
}

impl StubClassifier {
    fn new(scores: &[(&str, f64)]) -> Self {
        Self {
            scores: scores.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            call_count: Mutex::new(0),
        }
    }
}

#[async_trait]
impl LlmProvider for StubClassifier {
    async fn complete(&self, req: ChatCompletionRequest) -> anyhow::Result<ChatCompletionResponse> {
        *self.call_count.lock().unwrap() += 1;
        // Pull merchant keys out of the user message; if absent, score nothing.
        let user_text = req
            .messages
            .iter()
            .filter_map(|m| match m {
                ChatMessage::User { content } => Some(content.as_str()),
                _ => None,
            })
            .last()
            .unwrap_or("")
            .to_string();
        let mut out = serde_json::Map::new();
        for line in user_text.lines() {
            if let Some(idx) = line.find("(key: ") {
                let rest = &line[idx + 6..];
                if let Some(end) = rest.find(')') {
                    let key = rest[..end].to_string();
                    let score = self.scores.get(&key).copied().unwrap_or(0.0);
                    out.insert(key, json!(score));
                }
            }
        }
        let content = serde_json::to_string(&out).unwrap();
        Ok(ChatCompletionResponse {
            message: ChatMessage::Assistant {
                content: Some(content),
                tool_calls: Vec::new(),
            },
            finish_reason: "stop".to_string(),
            usage: None,
        })
    }
    fn model_label(&self) -> String {
        "stub:classifier".to_string()
    }
    fn kind(&self) -> LlmProviderKind {
        LlmProviderKind::OpenAi
    }
}

fn classifier_deps<'a>(
    db: &'a SqlitePool,
    classifier: Arc<StubClassifier>,
) -> AgentDeps<'a> {
    AgentDeps::new(db, classifier)
}

async fn setup_db_with_fixture() -> SqlitePool {
    let dir = std::env::current_dir().expect("cwd").join(".tmp");
    std::fs::create_dir_all(&dir).expect("create tmp");
    let db_path: PathBuf = dir.join(format!(
        "agent-tools-test-{}.db",
        expense_core::new_idempotency_key()
    ));
    if db_path.exists() {
        std::fs::remove_file(&db_path).ok();
    }
    let pool = connect(&db_path).await.expect("connect");
    run_migrations(&pool).await.expect("migrate");

    // Connection row first (FK target for accounts)
    sqlx::query(
        "INSERT INTO connections (id, provider, status) VALUES ('conn-1', 'manual', 'active')",
    )
    .execute(&pool)
    .await
    .expect("seed connection");

    // Seed two accounts
    sqlx::query(
        "INSERT INTO accounts (id, connection_id, name, currency_code, account_type, account_number_ending, customer_name) VALUES \
         ('acct-a', 'conn-1', 'Visa ending 1234', 'CAD', 'credit_card', '1234', 'TEST USER'), \
         ('acct-b', 'conn-1', 'Chequing 5678', 'CAD', 'chequing', '5678', 'TEST USER')",
    )
    .execute(&pool)
    .await
    .expect("seed accounts");

    // Seed transactions: 4 debits, 2 credits, mixed merchants/dates
    let txns = [
        ("t1", "acct-a", "2026-04-05", 5000, "debit", "Amazon"),
        ("t2", "acct-a", "2026-04-10", 12000, "debit", "Amazon"),
        ("t3", "acct-a", "2026-04-15", 800, "debit", "Tim Hortons"),
        ("t4", "acct-b", "2026-04-20", 25000, "debit", "Rent Payment"),
        ("t5", "acct-b", "2026-04-25", 300000, "credit", "Payroll Inc"),
        ("t6", "acct-a", "2026-03-30", 4500, "credit", "Refund Amazon"),
    ];
    for (id, account, date, cents, direction, descr) in txns {
        sqlx::query(
            "INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, direction, direction_source) \
             VALUES (?1, ?2, ?1, ?3, 'CAD', ?4, ?5, 'manual', ?6, 'seed')",
        )
        .bind(id)
        .bind(account)
        .bind(cents as i64)
        .bind(descr)
        .bind(date)
        .bind(direction)
        .execute(&pool)
        .await
        .expect("seed txn");
    }

    pool
}

#[tokio::test]
async fn list_accounts_returns_seeded_accounts() {
    let pool = setup_db_with_fixture().await;
    let out = ListAccountsAndCardsTool
        .invoke(test_deps(&pool), json!({}))
        .await
        .expect("ok");
    let accounts = out.data.get("accounts").unwrap().as_array().unwrap();
    assert_eq!(accounts.len(), 2);
    let names: Vec<&str> = accounts
        .iter()
        .map(|a| a.get("name").unwrap().as_str().unwrap())
        .collect();
    assert!(names.contains(&"Visa ending 1234"));
    assert!(names.contains(&"Chequing 5678"));
}

#[tokio::test]
async fn query_filters_by_merchant_substring() {
    let pool = setup_db_with_fixture().await;
    let out = QueryTransactionsTool
        .invoke(
            test_deps(&pool),
            json!({
                "merchant_substring": "amazon",
                "direction": "debit"
            }),
        )
        .await
        .expect("ok");
    assert_eq!(out.data.get("count").unwrap().as_i64().unwrap(), 2);
    assert_eq!(out.transaction_ids.len(), 2);
    assert!(out.transaction_ids.iter().any(|id| id == "t1"));
    assert!(out.transaction_ids.iter().any(|id| id == "t2"));
}

#[tokio::test]
async fn query_respects_date_range_and_direction() {
    let pool = setup_db_with_fixture().await;
    let out = QueryTransactionsTool
        .invoke(
            test_deps(&pool),
            json!({
                "date_from": "2026-04-01",
                "date_to": "2026-04-30",
                "direction": "debit"
            }),
        )
        .await
        .expect("ok");
    assert_eq!(out.data.get("count").unwrap().as_i64().unwrap(), 4);
    let total_out = out
        .data
        .get("total_outflow_cents")
        .unwrap()
        .as_i64()
        .unwrap();
    assert_eq!(total_out, 5000 + 12000 + 800 + 25000);
}

#[tokio::test]
async fn aggregate_sum_by_merchant_debit() {
    let pool = setup_db_with_fixture().await;
    let out = AggregateTransactionsTool
        .invoke(
            test_deps(&pool),
            json!({
                "group_by": "merchant",
                "metric": "sum",
                "direction": "debit",
                "date_from": "2026-04-01",
                "date_to": "2026-04-30"
            }),
        )
        .await
        .expect("ok");
    let groups = out.data.get("groups").unwrap().as_array().unwrap();
    // Top group should be rent payment ($250)
    let top = &groups[0];
    assert!(top
        .get("merchant")
        .unwrap()
        .as_str()
        .unwrap()
        .contains("rent"));
    assert_eq!(top.get("value").unwrap().as_f64().unwrap(), 25000.0);
}

#[tokio::test]
async fn aggregate_sum_by_account_debit() {
    let pool = setup_db_with_fixture().await;
    let out = AggregateTransactionsTool
        .invoke(
            test_deps(&pool),
            json!({
                "group_by": "account",
                "metric": "sum",
                "direction": "debit"
            }),
        )
        .await
        .expect("ok");
    let groups = out.data.get("groups").unwrap().as_array().unwrap();
    assert_eq!(groups.len(), 2);
}

#[tokio::test]
async fn aggregate_rejects_unknown_group_by() {
    let pool = setup_db_with_fixture().await;
    let err = AggregateTransactionsTool
        .invoke(
            test_deps(&pool),
            json!({ "group_by": "shoe_size", "metric": "sum" }),
        )
        .await
        .expect_err("should fail");
    assert!(err.to_string().contains("shoe_size"));
}

#[tokio::test]
async fn query_rejects_bad_date() {
    let pool = setup_db_with_fixture().await;
    let err = QueryTransactionsTool
        .invoke(test_deps(&pool), json!({ "date_from": "yesterday" }))
        .await
        .expect_err("should fail");
    assert!(err.to_string().contains("YYYY-MM-DD"));
}

#[tokio::test]
async fn compare_periods_sum_debit() {
    let pool = setup_db_with_fixture().await;
    // March vs April debits: March = $45 refund credit (not debit), April debits = $50+$120+$8+$250 = $428
    let out = ComparePeriodsTool
        .invoke(
            test_deps(&pool),
            json!({
                "window_a": { "date_from": "2026-03-01", "date_to": "2026-03-31" },
                "window_b": { "date_from": "2026-04-01", "date_to": "2026-04-30" },
                "metric": "sum",
                "direction": "debit",
                "label_a": "March",
                "label_b": "April"
            }),
        )
        .await
        .expect("ok");
    let a = out.data.get("window_a").unwrap().get("value").unwrap().as_f64().unwrap();
    let b = out.data.get("window_b").unwrap().get("value").unwrap().as_f64().unwrap();
    assert_eq!(a, 0.0); // March has no debits in fixture
    assert_eq!(b, (5000 + 12000 + 800 + 25000) as f64);
    let abs = out.data.get("absolute_diff").unwrap().as_f64().unwrap();
    assert_eq!(abs, b - a);
}

#[tokio::test]
async fn compare_periods_groups_by_merchant() {
    let pool = setup_db_with_fixture().await;
    // Force same window so groups should have non-zero on both sides where merchants repeat
    let out = ComparePeriodsTool
        .invoke(
            test_deps(&pool),
            json!({
                "window_a": { "date_from": "2026-04-01", "date_to": "2026-04-15" },
                "window_b": { "date_from": "2026-04-16", "date_to": "2026-04-30" },
                "metric": "sum",
                "direction": "debit",
                "group_by": "merchant"
            }),
        )
        .await
        .expect("ok");
    let groups = out.data.get("groups").unwrap().as_array().unwrap();
    // Should have rent payment in window B
    let rent = groups
        .iter()
        .find(|g| g.get("merchant").and_then(|v| v.as_str()).map(|s| s.contains("rent")).unwrap_or(false));
    assert!(rent.is_some());
}

#[tokio::test]
async fn transaction_detail_finds_similar_amazon() {
    let pool = setup_db_with_fixture().await;
    let out = TransactionDetailTool
        .invoke(test_deps(&pool), json!({ "transaction_id": "t1" }))
        .await
        .expect("ok");
    let primary = out.data.get("transaction").unwrap();
    assert_eq!(primary.get("id").unwrap().as_str().unwrap(), "t1");
    // Similar should include t2 (Amazon) at minimum
    let similar_count = out
        .data
        .get("similar_count_total")
        .unwrap()
        .as_i64()
        .unwrap();
    assert!(similar_count >= 1);
    assert!(out.transaction_ids.contains(&"t1".to_string()));
}

#[tokio::test]
async fn transaction_detail_returns_error_for_missing_id() {
    let pool = setup_db_with_fixture().await;
    let err = TransactionDetailTool
        .invoke(test_deps(&pool), json!({ "transaction_id": "nope" }))
        .await
        .expect_err("should fail");
    assert!(err.to_string().contains("not found"));
}

#[tokio::test]
async fn find_recurring_detects_monthly_subscription() {
    let pool = setup_db_with_fixture().await;
    // Seed a clean monthly subscription pattern
    let dates = ["2025-11-15", "2025-12-15", "2026-01-15", "2026-02-15", "2026-03-15"];
    for (i, d) in dates.iter().enumerate() {
        sqlx::query(
            "INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, direction, direction_source) \
             VALUES (?1, 'acct-a', ?1, 1599, 'CAD', 'Netflix.com', ?2, 'manual', 'debit', 'seed')",
        )
        .bind(format!("rec-{i}"))
        .bind(*d)
        .execute(&pool)
        .await
        .expect("seed monthly");
    }

    let out = FindRecurringTool
        .invoke(test_deps(&pool), json!({ "lookback_months": 6, "date_to": "2026-03-31" }))
        .await
        .expect("ok");
    let recurring = out.data.get("recurring").unwrap().as_array().unwrap();
    let netflix = recurring
        .iter()
        .find(|r| {
            r.get("merchant")
                .and_then(|v| v.as_str())
                .map(|s| s.to_lowercase().contains("netflix"))
                .unwrap_or(false)
        })
        .expect("netflix should be detected");
    assert_eq!(netflix.get("cadence").unwrap().as_str(), Some("monthly"));
    assert!(netflix.get("occurrences").unwrap().as_i64().unwrap() >= 5);
}

#[tokio::test]
async fn find_recurring_skips_irregular_charges() {
    let pool = setup_db_with_fixture().await;
    // Random irregular gaps shouldn't be flagged
    let dates = ["2026-01-05", "2026-01-22", "2026-03-01"];
    for (i, d) in dates.iter().enumerate() {
        sqlx::query(
            "INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, direction, direction_source) \
             VALUES (?1, 'acct-a', ?1, 4500, 'CAD', 'RANDO PURCHASE', ?2, 'manual', 'debit', 'seed')",
        )
        .bind(format!("irr-{i}"))
        .bind(*d)
        .execute(&pool)
        .await
        .expect("seed irregular");
    }
    let out = FindRecurringTool
        .invoke(test_deps(&pool), json!({ "lookback_months": 6, "date_to": "2026-03-31", "merchant_substring": "rando" }))
        .await
        .expect("ok");
    let recurring = out.data.get("recurring").unwrap().as_array().unwrap();
    assert!(recurring.is_empty(), "irregular should not be flagged");
}

#[tokio::test]
async fn aggregate_count_by_direction() {
    let pool = setup_db_with_fixture().await;
    let out = AggregateTransactionsTool
        .invoke(
            test_deps(&pool),
            json!({ "group_by": "direction", "metric": "count" }),
        )
        .await
        .expect("ok");
    let groups = out.data.get("groups").unwrap().as_array().unwrap();
    let debits = groups
        .iter()
        .find(|g| g.get("direction").unwrap().as_str() == Some("debit"))
        .unwrap();
    assert_eq!(debits.get("value").unwrap().as_f64().unwrap(), 4.0);
    let credits = groups
        .iter()
        .find(|g| g.get("direction").unwrap().as_str() == Some("credit"))
        .unwrap();
    assert_eq!(credits.get("value").unwrap().as_f64().unwrap(), 2.0);
}

// --------- resolve_category_intent tests (Phase 5c) ---------

async fn seed_grocery_fixture() -> SqlitePool {
    let pool = setup_db_with_fixture().await;
    // Seed merchants spanning various categories for April 2026.
    let txns = [
        ("g1", "acct-a", "2026-04-05", 5000, "debit", "LOBLAWS GREAT FOOD #1234"),
        ("g2", "acct-a", "2026-04-12", 3200, "debit", "LOBLAWS GREAT FOOD #1234"),
        ("g3", "acct-a", "2026-04-15", 2400, "debit", "METRO #455"),
        ("g4", "acct-a", "2026-04-18", 14200, "debit", "COSTCO WHOLESALE"),
        ("g5", "acct-a", "2026-04-22", 1599, "debit", "NETFLIX.COM"),
    ];
    for (id, account, date, cents, direction, descr) in txns {
        sqlx::query(
            "INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, direction, direction_source) \
             VALUES (?1, ?2, ?1, ?3, 'CAD', ?4, ?5, 'manual', ?6, 'seed')",
        )
        .bind(id)
        .bind(account)
        .bind(cents as i64)
        .bind(descr)
        .bind(date)
        .bind(direction)
        .execute(&pool)
        .await
        .expect("seed grocery txn");
    }
    pool
}

#[tokio::test]
async fn resolve_category_intent_first_run_returns_suggestions_and_persists() {
    let pool = seed_grocery_fixture().await;
    let classifier = Arc::new(StubClassifier::new(&[
        ("loblaws great food", 0.95),
        ("metro", 0.93),
        ("costco wholesale", 0.55),
        ("netflix", 0.02),
    ]));

    let out = ResolveCategoryIntentTool
        .invoke(
            classifier_deps(&pool, classifier.clone()),
            json!({
                "category": "groceries",
                "date_from": "2026-04-01",
                "date_to": "2026-04-30"
            }),
        )
        .await
        .expect("ok");

    assert_eq!(*classifier.call_count.lock().unwrap(), 1);
    let confirmed = out.data.get("confirmed").unwrap().as_array().unwrap();
    let suggested = out.data.get("suggested").unwrap().as_array().unwrap();
    assert!(confirmed.is_empty(), "nothing confirmed yet on first run");
    assert_eq!(
        suggested.len(),
        3,
        "three above 0.4 threshold (loblaws, metro, costco)"
    );
    assert!(out
        .data
        .get("requires_user_confirmation")
        .unwrap()
        .as_bool()
        .unwrap());
}

#[tokio::test]
async fn resolve_category_intent_does_not_re_ask_llm_for_known_merchants() {
    let pool = seed_grocery_fixture().await;
    let classifier = Arc::new(StubClassifier::new(&[
        ("loblaws great food", 0.95),
        ("metro", 0.93),
        ("costco wholesale", 0.55),
        ("netflix", 0.02),
    ]));

    // First run: classifier fires.
    let _ = ResolveCategoryIntentTool
        .invoke(
            classifier_deps(&pool, classifier.clone()),
            json!({
                "category": "groceries",
                "date_from": "2026-04-01",
                "date_to": "2026-04-30"
            }),
        )
        .await
        .expect("ok");
    assert_eq!(*classifier.call_count.lock().unwrap(), 1);

    // Second run on same window: every merchant already has an llm_suggested row, so the LLM
    // should NOT be called again.
    let _ = ResolveCategoryIntentTool
        .invoke(
            classifier_deps(&pool, classifier.clone()),
            json!({
                "category": "groceries",
                "date_from": "2026-04-01",
                "date_to": "2026-04-30"
            }),
        )
        .await
        .expect("ok");
    assert_eq!(
        *classifier.call_count.lock().unwrap(),
        1,
        "no new LLM call when all merchants already classified"
    );
}

#[tokio::test]
async fn resolve_category_intent_rejects_unknown_category() {
    let pool = seed_grocery_fixture().await;
    let classifier = Arc::new(StubClassifier::new(&[]));
    let err = ResolveCategoryIntentTool
        .invoke(
            classifier_deps(&pool, classifier),
            json!({ "category": "moonshine" }),
        )
        .await
        .expect_err("should fail");
    assert!(err.to_string().contains("unknown category"));
}

#[tokio::test]
async fn resolve_category_intent_skips_llm_when_window_empty() {
    let pool = setup_db_with_fixture().await;
    let classifier = Arc::new(StubClassifier::new(&[]));
    let out = ResolveCategoryIntentTool
        .invoke(
            classifier_deps(&pool, classifier.clone()),
            json!({
                "category": "groceries",
                "date_from": "2099-01-01",
                "date_to": "2099-01-31"
            }),
        )
        .await
        .expect("ok");
    assert_eq!(*classifier.call_count.lock().unwrap(), 0);
    let suggested = out.data.get("suggested").unwrap().as_array().unwrap();
    assert!(suggested.is_empty());
    assert!(!out
        .data
        .get("requires_user_confirmation")
        .unwrap()
        .as_bool()
        .unwrap());
}

// --------- confirm_category_assignments + merchant_substrings tests (Phase 5d) ---------

#[tokio::test]
async fn confirm_category_assignments_persists_user_choices() {
    let pool = seed_grocery_fixture().await;
    let classifier = Arc::new(StubClassifier::new(&[
        ("loblaws great food", 0.95),
        ("metro", 0.93),
        ("costco wholesale", 0.55),
        ("netflix", 0.02),
    ]));

    // Run the classifier first so merchant_signatures exist + suggestions are persisted.
    let resolve_out = ResolveCategoryIntentTool
        .invoke(
            classifier_deps(&pool, classifier.clone()),
            json!({ "category": "groceries", "date_from": "2026-04-01", "date_to": "2026-04-30" }),
        )
        .await
        .expect("resolve ok");

    // Pull two suggested merchant IDs from the resolver output for the confirm payload.
    let suggested = resolve_out.data.get("suggested").unwrap().as_array().unwrap();
    let loblaws_id = suggested
        .iter()
        .find(|m| m.get("normalized_key").unwrap().as_str().unwrap().contains("loblaws"))
        .unwrap()
        .get("merchant_signature_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let costco_id = suggested
        .iter()
        .find(|m| m.get("normalized_key").unwrap().as_str().unwrap().contains("costco"))
        .unwrap()
        .get("merchant_signature_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();

    let confirm_out = ConfirmCategoryAssignmentsTool
        .invoke(
            test_deps(&pool),
            json!({
                "category": "groceries",
                "assignments": [
                    { "merchant_signature_id": loblaws_id, "included": true },
                    { "merchant_signature_id": costco_id,  "included": false },
                ]
            }),
        )
        .await
        .expect("confirm ok");

    let confirmed = confirm_out.data.get("confirmed").unwrap().as_array().unwrap();
    let excluded = confirm_out.data.get("excluded").unwrap().as_array().unwrap();
    assert_eq!(confirmed.len(), 1);
    assert_eq!(excluded.len(), 1);

    // History row count should be 2 (one per assignment).
    let history_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM category_resolution_history")
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(history_count, 2);
}

#[tokio::test]
async fn confirm_rejects_unknown_merchant_signature() {
    let pool = seed_grocery_fixture().await;
    let err = ConfirmCategoryAssignmentsTool
        .invoke(
            test_deps(&pool),
            json!({
                "category": "groceries",
                "assignments": [
                    { "merchant_signature_id": "bogus-id", "included": true }
                ]
            }),
        )
        .await
        .expect_err("should fail");
    assert!(err.to_string().contains("unknown merchant_signature_id"));
}

#[tokio::test]
async fn aggregate_supports_merchant_substrings_array() {
    let pool = seed_grocery_fixture().await;
    // Sum debits matching either 'loblaws' OR 'metro' in April.
    let out = AggregateTransactionsTool
        .invoke(
            test_deps(&pool),
            json!({
                "group_by": "merchant",
                "metric": "sum",
                "direction": "debit",
                "date_from": "2026-04-01",
                "date_to": "2026-04-30",
                "merchant_substrings": ["loblaws", "metro"]
            }),
        )
        .await
        .expect("ok");

    let groups = out.data.get("groups").unwrap().as_array().unwrap();
    // loblaws (5000+3200) + metro (2400) — but grouped by merchant they remain separate.
    let total: f64 = groups.iter().map(|g| g.get("value").unwrap().as_f64().unwrap()).sum();
    assert_eq!(total, (5000 + 3200 + 2400) as f64);
}

#[tokio::test]
async fn query_supports_merchant_substrings_array() {
    let pool = seed_grocery_fixture().await;
    let out = QueryTransactionsTool
        .invoke(
            test_deps(&pool),
            json!({
                "merchant_substrings": ["loblaws", "costco"],
                "direction": "debit",
                "date_from": "2026-04-01",
                "date_to": "2026-04-30"
            }),
        )
        .await
        .expect("ok");
    let count = out.data.get("count").unwrap().as_i64().unwrap();
    // 2 loblaws + 1 costco
    assert_eq!(count, 3);
}

#[tokio::test]
async fn query_rejects_both_merchant_filter_forms_together() {
    let pool = seed_grocery_fixture().await;
    let err = QueryTransactionsTool
        .invoke(
            test_deps(&pool),
            json!({
                "merchant_substring": "loblaws",
                "merchant_substrings": ["metro"]
            }),
        )
        .await
        .expect_err("should fail");
    assert!(err.to_string().contains("not both"));
}
