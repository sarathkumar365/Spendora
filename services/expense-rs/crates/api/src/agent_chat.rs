use std::{sync::Arc, time::Duration};

use agent::{
    context::{AccountSummary, AgentContext, DataRange},
    llm::ChatMessage as AgentChatMessage,
    runtime::{build_initial_messages, AgentEvent, AgentRunner},
};
use async_stream::stream;
use axum::{
    extract::State,
    http::StatusCode,
    response::{sse::Event, Sse},
    Json,
};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::Row;
use std::convert::Infallible;
use storage_sqlite::SqlitePool;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    pub message: String,
    #[serde(default)]
    pub history: Vec<HistoryMessage>,
}

#[derive(Debug, Deserialize)]
pub struct HistoryMessage {
    pub role: HistoryRole,
    pub content: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HistoryRole {
    User,
    Assistant,
}

#[derive(Debug, Serialize)]
pub struct ContextResponse {
    pub today: String,
    pub timezone: String,
    pub currency_default: String,
    pub provider: String,
    pub model: String,
    pub registered_tools: Vec<String>,
    pub accounts: Vec<AccountSummary>,
    pub data_range: DataRange,
}

async fn load_accounts(db: &SqlitePool) -> anyhow::Result<Vec<AccountSummary>> {
    let rows = sqlx::query(
        "SELECT id, name, currency_code, account_type, account_number_ending, customer_name \
         FROM accounts ORDER BY name ASC",
    )
    .fetch_all(db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| AccountSummary {
            id: r.get("id"),
            name: r.get("name"),
            currency: r.get("currency_code"),
            account_type: r.get("account_type"),
            last4: r.get("account_number_ending"),
            customer_name: r.get("customer_name"),
        })
        .collect())
}

async fn load_data_range(db: &SqlitePool) -> anyhow::Result<DataRange> {
    let row = sqlx::query(
        "SELECT MIN(booked_at) AS min_d, MAX(booked_at) AS max_d, COUNT(*) AS cnt FROM transactions",
    )
    .fetch_one(db)
    .await?;
    Ok(DataRange {
        earliest_booked_at: row.try_get::<Option<String>, _>("min_d").unwrap_or(None),
        latest_booked_at: row.try_get::<Option<String>, _>("max_d").unwrap_or(None),
        transaction_count: row.try_get::<i64, _>("cnt").unwrap_or(0),
    })
}

pub async fn get_agent_context_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ContextResponse>, (StatusCode, String)> {
    let provider = state.agent_provider.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "agent llm provider not configured: set OPENAI_API_KEY (or AGENT_LLM_PROVIDER=local)"
                .to_string(),
        )
    })?;
    let mut tools: Vec<String> = state
        .agent_registry
        .names()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    tools.sort();

    let accounts = load_accounts(&state.db)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    let data_range = load_data_range(&state.db)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    let ctx = AgentContext::new(
        provider.kind().as_str().to_string(),
        provider.model_label(),
        tools.clone(),
        accounts.clone(),
        data_range.clone(),
    );

    Ok(Json(ContextResponse {
        today: ctx.today,
        timezone: ctx.timezone,
        currency_default: ctx.currency_default,
        provider: ctx.provider,
        model: ctx.model,
        registered_tools: tools,
        accounts,
        data_range,
    }))
}

pub async fn post_agent_chat_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ChatRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, String)> {
    if body.message.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "message must not be empty".into()));
    }

    let provider = state.agent_provider.clone().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "agent llm provider not configured: set OPENAI_API_KEY (or AGENT_LLM_PROVIDER=local)"
                .to_string(),
        )
    })?;
    let registry = state.agent_registry.clone();
    let mut tool_names: Vec<String> = registry
        .names()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    tool_names.sort();

    let accounts = load_accounts(&state.db)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    let data_range = load_data_range(&state.db)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    let ctx = AgentContext::new(
        provider.kind().as_str().to_string(),
        provider.model_label(),
        tool_names,
        accounts,
        data_range,
    );
    let system_prompt = ctx.system_prompt();

    let history: Vec<AgentChatMessage> = body
        .history
        .into_iter()
        .map(|m| match m.role {
            HistoryRole::User => AgentChatMessage::User { content: m.content },
            HistoryRole::Assistant => AgentChatMessage::Assistant {
                content: Some(m.content),
                tool_calls: Vec::new(),
            },
        })
        .collect();

    let initial = build_initial_messages(system_prompt, history, body.message)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let (tx, mut rx) = mpsc::channel::<AgentEvent>(64);
    let runner = AgentRunner::new(provider, registry);
    let db = state.db.clone();

    tokio::spawn(async move {
        info!("agent chat run starting");
        runner.run(&db, initial, tx).await;
        info!("agent chat run finished");
    });

    let event_stream = stream! {
        while let Some(event) = rx.recv().await {
            let kind = event_kind(&event);
            let payload = match serde_json::to_string(&event) {
                Ok(s) => s,
                Err(err) => {
                    error!(error = %err, "failed to serialize agent event");
                    json!({ "kind": "error", "message": "internal serialization error" }).to_string()
                }
            };
            yield Ok::<Event, Infallible>(Event::default().event(kind).data(payload));
        }
    };

    Ok(Sse::new(event_stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    ))
}

fn event_kind(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::Started { .. } => "started",
        AgentEvent::ToolCallStart { .. } => "tool_call_start",
        AgentEvent::ToolCallResult { .. } => "tool_call_result",
        AgentEvent::AssistantMessage { .. } => "assistant_message",
        AgentEvent::Followups { .. } => "followups",
        AgentEvent::CategoryConfirmationNeeded { .. } => "category_confirmation_needed",
        AgentEvent::Truncated { .. } => "truncated",
        AgentEvent::Error { .. } => "error",
        AgentEvent::Done { .. } => "done",
    }
}
