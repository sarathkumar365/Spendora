//! Audit sink: persists every agent event to SQLite via a background writer task so the
//! hot path (SSE stream) never blocks on DB I/O.
//!
//! Sinks are pluggable via the `AuditSink` trait. Production wiring uses `DbAuditSink`.
//! Tests get `NoopSink` (the default for `AgentRunner`).

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use storage_sqlite::{insert_agent_event, AgentEventRow, SqlitePool};
use tokio::sync::mpsc;
use tracing::warn;

/// Strongly-typed audit event shape — keeps the call site honest about which fields belong
/// on which kind of event. Builders below assemble these from runtime data.
#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub conversation_id: String,
    pub run_id: String,
    pub sequence: i64,
    pub event_kind: String,
    pub duration_ms: Option<i64>,
    pub payload: Value,
    pub status: Option<String>,
    pub model: Option<String>,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub cost_micros: Option<i64>,
    pub user_message_excerpt: Option<String>,
    pub tool_name: Option<String>,
    pub ok: Option<bool>,
    pub error_message: Option<String>,
}

impl AuditEvent {
    pub fn new(
        conversation_id: impl Into<String>,
        run_id: impl Into<String>,
        sequence: i64,
        event_kind: impl Into<String>,
    ) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            run_id: run_id.into(),
            sequence,
            event_kind: event_kind.into(),
            duration_ms: None,
            payload: Value::Null,
            status: None,
            model: None,
            prompt_tokens: None,
            completion_tokens: None,
            cost_micros: None,
            user_message_excerpt: None,
            tool_name: None,
            ok: None,
            error_message: None,
        }
    }

    pub fn with_payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }

    pub fn into_row(self) -> AgentEventRow {
        let payload_json = if self.payload.is_null() {
            "{}".to_string()
        } else {
            self.payload.to_string()
        };
        AgentEventRow {
            id: None,
            conversation_id: self.conversation_id,
            run_id: self.run_id,
            sequence: self.sequence,
            event_kind: self.event_kind,
            occurred_at: None,
            duration_ms: self.duration_ms,
            payload_json,
            status: self.status,
            model: self.model,
            prompt_tokens: self.prompt_tokens,
            completion_tokens: self.completion_tokens,
            cost_micros: self.cost_micros,
            user_message_excerpt: self.user_message_excerpt,
            tool_name: self.tool_name,
            ok: self.ok,
            error_message: self.error_message,
        }
    }
}

/// Behavior contract for an audit sink. Methods MUST NOT block the caller — implementations
/// either drop the event silently if the background writer can't keep up, or buffer.
#[async_trait]
pub trait AuditSink: Send + Sync {
    /// Fire-and-forget record of a single event. Returns immediately.
    async fn record(&self, event: AuditEvent);

    /// Best-effort flush + close. Called once at the end of a run so the `run_ended` event
    /// is durable before the SSE stream closes.
    async fn flush(&self) {}
}

/// Default sink for tests and any context where audit isn't wired. Swallows every event.
pub struct NoopSink;

#[async_trait]
impl AuditSink for NoopSink {
    async fn record(&self, _event: AuditEvent) {}
}

/// Production sink — events go into a tokio mpsc channel; a background task flushes them
/// to SQLite. Channel is bounded so a stuck writer eventually applies backpressure
/// (`record` then awaits a slot — still non-blocking on the LLM call, just on subsequent
/// audit writes).
pub struct DbAuditSink {
    tx: mpsc::Sender<AgentEventRow>,
    flush_signal: Arc<tokio::sync::Notify>,
}

impl DbAuditSink {
    pub fn spawn(pool: SqlitePool) -> Arc<Self> {
        let (tx, mut rx) = mpsc::channel::<AgentEventRow>(256);
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = notify.clone();
        let pool_clone = pool.clone();
        tokio::spawn(async move {
            while let Some(row) = rx.recv().await {
                if let Err(err) = insert_agent_event(&pool_clone, row).await {
                    warn!(error = %err, "agent audit insert failed; dropping event");
                }
                notify_clone.notify_waiters();
            }
        });
        Arc::new(Self {
            tx,
            flush_signal: notify,
        })
    }
}

#[async_trait]
impl AuditSink for DbAuditSink {
    async fn record(&self, event: AuditEvent) {
        let row = event.into_row();
        // Try-send first to keep the hot path non-blocking when the buffer has room.
        match self.tx.try_send(row) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(row)) => {
                // Buffer saturated — pause the caller briefly until the writer catches up.
                if let Err(err) = self.tx.send(row).await {
                    warn!(error = %err, "agent audit channel closed; dropping event");
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!("agent audit channel closed; dropping event");
            }
        }
    }

    async fn flush(&self) {
        // Wait for one writer cycle so the most recent event is durable.
        let notified = self.flush_signal.notified();
        tokio::pin!(notified);
        let _ = tokio::time::timeout(std::time::Duration::from_millis(500), notified).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_sqlite::{connect, list_agent_events_for_run, run_migrations};

    async fn fresh_pool() -> SqlitePool {
        let path = std::env::current_dir()
            .expect("cwd")
            .join(".tmp")
            .join(format!("audit-test-{}.db", expense_core::new_idempotency_key()));
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let pool = connect(&path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        pool
    }

    #[tokio::test]
    async fn noop_sink_does_not_persist_anything() {
        let pool = fresh_pool().await;
        let sink = NoopSink;
        sink.record(AuditEvent::new("c1", "r1", 0, "run_started"))
            .await;
        let events = list_agent_events_for_run(&pool, "r1").await.expect("list");
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn db_sink_persists_events_in_order() {
        let pool = fresh_pool().await;
        let sink = DbAuditSink::spawn(pool.clone());

        for seq in 0..5 {
            sink.record(
                AuditEvent::new("conv-1", "run-1", seq, "tool_call").with_payload(
                    serde_json::json!({ "seq": seq }),
                ),
            )
            .await;
        }
        sink.flush().await;
        // Tiny extra grace for the background writer.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let events = list_agent_events_for_run(&pool, "run-1")
            .await
            .expect("list");
        assert_eq!(events.len(), 5);
        for (i, e) in events.iter().enumerate() {
            assert_eq!(e.sequence, i as i64);
            assert_eq!(e.event_kind, "tool_call");
        }
    }

    #[tokio::test]
    async fn db_sink_records_promoted_columns() {
        let pool = fresh_pool().await;
        let sink = DbAuditSink::spawn(pool.clone());

        let mut event = AuditEvent::new("conv-2", "run-2", 0, "llm_call");
        event.model = Some("openai:gpt-4o-mini".to_string());
        event.prompt_tokens = Some(123);
        event.completion_tokens = Some(45);
        event.cost_micros = Some(890);
        event.duration_ms = Some(1500);
        sink.record(event).await;
        sink.flush().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let events = list_agent_events_for_run(&pool, "run-2")
            .await
            .expect("list");
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.model.as_deref(), Some("openai:gpt-4o-mini"));
        assert_eq!(e.prompt_tokens, Some(123));
        assert_eq!(e.completion_tokens, Some(45));
        assert_eq!(e.cost_micros, Some(890));
        assert_eq!(e.duration_ms, Some(1500));
    }
}
