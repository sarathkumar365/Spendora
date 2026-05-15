//! Read-only API endpoints over the `agent_events` audit table.
//!
//! - GET /api/v1/audit/conversations             → per-conversation rollup (cost + tokens)
//! - GET /api/v1/audit/runs                      → recent completed runs
//! - GET /api/v1/audit/runs/:run_id/events       → full event sequence for one run (replay)
//! - GET /api/v1/audit/summary?days=N            → 7d/30d/all-time totals

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use storage_sqlite::{
    agent_cost_since, list_agent_events_for_run, list_conversation_summaries, list_recent_runs,
    AgentCostSummary, AgentEventRecord, ConversationSummary, RunSummary,
};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    #[serde(default)]
    pub limit: Option<i64>,
}

fn clamped_limit(p: &PaginationParams, default: i64, max: i64) -> i64 {
    p.limit.unwrap_or(default).clamp(1, max)
}

pub async fn list_audit_conversations(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Vec<ConversationSummary>>, (StatusCode, String)> {
    let limit = clamped_limit(&params, 50, 500);
    let conversations = list_conversation_summaries(&state.db, limit)
        .await
        .map_err(internal_error)?;
    Ok(Json(conversations))
}

pub async fn list_audit_runs(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Vec<RunSummary>>, (StatusCode, String)> {
    let limit = clamped_limit(&params, 100, 500);
    let runs = list_recent_runs(&state.db, limit)
        .await
        .map_err(internal_error)?;
    Ok(Json(runs))
}

pub async fn get_audit_run_events(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> Result<Json<Vec<AgentEventRecord>>, (StatusCode, String)> {
    let events = list_agent_events_for_run(&state.db, &run_id)
        .await
        .map_err(internal_error)?;
    if events.is_empty() {
        return Err((StatusCode::NOT_FOUND, format!("run not found: {run_id}")));
    }
    Ok(Json(events))
}

#[derive(Debug, Deserialize)]
pub struct SummaryParams {
    /// Look back N days (capped at 365). Omit to get all-time totals.
    #[serde(default)]
    pub days: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct AuditSummaryResponse {
    pub window_days: Option<i64>,
    pub since_iso: String,
    pub total_cost_micros: i64,
    pub total_cost_dollars: f64,
    pub total_prompt_tokens: i64,
    pub total_completion_tokens: i64,
    pub llm_call_count: i64,
}

pub async fn get_audit_summary(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SummaryParams>,
) -> Result<Json<AuditSummaryResponse>, (StatusCode, String)> {
    let (window_days, since_iso) = match params.days {
        Some(d) => {
            let clamped = d.clamp(1, 365);
            (Some(clamped), days_ago_iso(clamped))
        }
        None => (None, "1970-01-01T00:00:00Z".to_string()),
    };
    let AgentCostSummary {
        total_cost_micros,
        total_prompt_tokens,
        total_completion_tokens,
        llm_call_count,
    } = agent_cost_since(&state.db, &since_iso)
        .await
        .map_err(internal_error)?;

    Ok(Json(AuditSummaryResponse {
        window_days,
        since_iso,
        total_cost_micros,
        total_cost_dollars: total_cost_micros as f64 / 1_000_000.0,
        total_prompt_tokens,
        total_completion_tokens,
        llm_call_count,
    }))
}

fn days_ago_iso(days: i64) -> String {
    let cutoff = chrono::Utc::now() - chrono::Duration::days(days);
    cutoff.to_rfc3339()
}

fn internal_error(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
