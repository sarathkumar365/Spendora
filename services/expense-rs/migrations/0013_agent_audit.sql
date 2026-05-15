-- Phase 6a: agent audit trail (single events table).
-- One row per discrete event the agent emits. Conversations and runs are
-- defined by `event_kind` filters + `conversation_id` / `run_id` grouping.

CREATE TABLE IF NOT EXISTS agent_events (
  id TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  event_kind TEXT NOT NULL,
  occurred_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  duration_ms INTEGER,
  payload_json TEXT NOT NULL,

  -- Promoted-to-column fields for fast filtering and rollups (NULL when N/A):
  status TEXT,                   -- run_started + run_ended
  model TEXT,                    -- run_started + llm_call
  prompt_tokens INTEGER,         -- llm_call (and totals on run_ended)
  completion_tokens INTEGER,     -- llm_call (and totals on run_ended)
  cost_micros INTEGER,           -- llm_call (and totals on run_ended) -- USD micros
  user_message_excerpt TEXT,     -- run_started only
  tool_name TEXT,                -- tool_call only
  ok INTEGER,                    -- tool_call (0/1)
  error_message TEXT             -- error or failed run_ended
);

CREATE INDEX IF NOT EXISTS idx_agent_events_conv_time
  ON agent_events(conversation_id, occurred_at);

CREATE INDEX IF NOT EXISTS idx_agent_events_run_seq
  ON agent_events(run_id, sequence);

CREATE INDEX IF NOT EXISTS idx_agent_events_kind_time
  ON agent_events(event_kind, occurred_at);
