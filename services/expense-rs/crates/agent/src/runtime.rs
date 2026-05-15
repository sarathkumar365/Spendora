use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage_sqlite::SqlitePool;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::llm::{ChatCompletionRequest, ChatMessage, LlmProvider, ToolCall};
use crate::tools::{AgentDeps, ToolRegistry};

pub const DEFAULT_MAX_ITERATIONS: usize = 6;

/// Hard cap on the JSON size we feed back to the LLM as a tool result. SQLite tools can
/// occasionally return very large payloads (e.g. 500-row query_transactions); the UI still
/// gets the full data via the SSE event, but the LLM-bound copy is summarised to keep prompts
/// fast and cheap.
const MAX_TOOL_PAYLOAD_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentEvent {
    /// Emitted once at the very start with run metadata.
    Started {
        model: String,
        provider: String,
    },
    /// Emitted before a tool runs.
    ToolCallStart {
        id: String,
        name: String,
        arguments: Value,
    },
    /// Emitted after a tool runs (success or failure). `data` contains the full tool payload
    /// so the UI can cache transactions for the citation drawer.
    ToolCallResult {
        id: String,
        name: String,
        ok: bool,
        summary: String,
        transaction_ids: Vec<String>,
        data: Value,
        error: Option<String>,
    },
    /// Final assistant text answer (with the FOLLOWUPS line already stripped).
    AssistantMessage {
        content: String,
    },
    /// Suggested follow-up prompts extracted from the assistant message.
    Followups {
        suggestions: Vec<String>,
    },
    /// Emitted instead of `AssistantMessage` when the agent ends a run with the sentinel
    /// `CATEGORY_CONFIRMATION_NEEDED: <slug>`. Carries the latest `resolve_category_intent`
    /// payload so the UI can render an inline confirmation card.
    CategoryConfirmationNeeded {
        category_slug: String,
        payload: Value,
    },
    /// Iteration cap hit without a final answer.
    Truncated {
        reason: String,
    },
    /// Fatal error during the run.
    Error {
        message: String,
    },
    /// Always emitted last.
    Done {
        iterations: usize,
        cited_transaction_ids: Vec<String>,
    },
}

pub struct AgentRunner {
    pub provider: std::sync::Arc<dyn LlmProvider>,
    pub registry: std::sync::Arc<ToolRegistry>,
    pub max_iterations: usize,
    pub temperature: f32,
}

impl AgentRunner {
    pub fn new(
        provider: std::sync::Arc<dyn LlmProvider>,
        registry: std::sync::Arc<ToolRegistry>,
    ) -> Self {
        Self {
            provider,
            registry,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            temperature: 0.2,
        }
    }

    /// Run the multi-turn loop. Each AgentEvent is pushed into `events`.
    /// `messages` should already include any system + prior conversation turns
    /// plus the latest user message.
    pub async fn run(
        &self,
        db: &SqlitePool,
        mut messages: Vec<ChatMessage>,
        events: mpsc::Sender<AgentEvent>,
    ) {
        let _ = events
            .send(AgentEvent::Started {
                model: self.provider.model_label(),
                provider: self.provider.kind().as_str().to_string(),
            })
            .await;

        let mut iterations = 0usize;
        let mut cited: Vec<String> = Vec::new();
        // The latest payload from a `resolve_category_intent` tool call in this run, used to
        // hydrate the CategoryConfirmationNeeded event if the agent ends with the sentinel.
        let mut latest_category_resolution: Option<Value> = None;
        let tools = self.registry.definitions();

        loop {
            if events.is_closed() {
                info!("agent run cancelled (client disconnected)");
                return;
            }
            iterations += 1;
            if iterations > self.max_iterations {
                let _ = events
                    .send(AgentEvent::Truncated {
                        reason: format!(
                            "iteration cap of {} reached without a final answer",
                            self.max_iterations
                        ),
                    })
                    .await;
                break;
            }

            let req = ChatCompletionRequest {
                messages: messages.clone(),
                tools: tools.clone(),
                temperature: self.temperature,
            };

            let response = tokio::select! {
                biased;
                _ = events.closed() => {
                    info!("agent run cancelled mid-LLM call (client disconnected)");
                    return;
                }
                result = self.provider.complete(req) => match result {
                    Ok(r) => r,
                    Err(err) => {
                        error!(error = %err, "llm provider call failed");
                        let _ = events
                            .send(AgentEvent::Error {
                                message: format!("llm provider error: {err}"),
                            })
                            .await;
                        break;
                    }
                }
            };

            let (assistant_content, assistant_tool_calls) = match &response.message {
                ChatMessage::Assistant {
                    content,
                    tool_calls,
                } => (content.clone(), tool_calls.clone()),
                _ => {
                    let _ = events
                        .send(AgentEvent::Error {
                            message: "llm returned non-assistant message".to_string(),
                        })
                        .await;
                    break;
                }
            };

            // Persist assistant turn in conversation history for the next iteration.
            messages.push(response.message.clone());

            if assistant_tool_calls.is_empty() {
                let raw = assistant_content.unwrap_or_default();
                let (cleaned, followups) = extract_followups(&raw);

                if let Some(slug) = extract_confirmation_slug(&cleaned) {
                    let payload = latest_category_resolution.clone().unwrap_or_else(|| {
                        serde_json::json!({
                            "category": { "slug": slug.clone() },
                            "warning": "agent emitted the confirmation sentinel without first calling resolve_category_intent",
                            "confirmed": [],
                            "suggested": [],
                            "excluded": [],
                        })
                    });
                    let _ = events
                        .send(AgentEvent::CategoryConfirmationNeeded {
                            category_slug: slug,
                            payload,
                        })
                        .await;
                    break;
                }

                let _ = events
                    .send(AgentEvent::AssistantMessage { content: cleaned })
                    .await;
                if !followups.is_empty() {
                    let _ = events
                        .send(AgentEvent::Followups {
                            suggestions: followups,
                        })
                        .await;
                }
                break;
            }

            // Execute each tool call sequentially, append a Tool message per call.
            for call in assistant_tool_calls {
                let parsed_args: Value = serde_json::from_str(&call.function.arguments)
                    .unwrap_or_else(|_| Value::String(call.function.arguments.clone()));

                let _ = events
                    .send(AgentEvent::ToolCallStart {
                        id: call.id.clone(),
                        name: call.function.name.clone(),
                        arguments: parsed_args.clone(),
                    })
                    .await;

                let tool_name = call.function.name.clone();
                let (tool_msg, tool_data) = self
                    .execute_tool(db, &call, parsed_args, &events, &mut cited)
                    .await;
                if tool_name == "resolve_category_intent" {
                    if let Some(data) = tool_data {
                        latest_category_resolution = Some(data);
                    }
                }
                messages.push(tool_msg);
            }
        }

        let _ = events
            .send(AgentEvent::Done {
                iterations,
                cited_transaction_ids: cited,
            })
            .await;
    }

    async fn execute_tool(
        &self,
        db: &SqlitePool,
        call: &ToolCall,
        args: Value,
        events: &mpsc::Sender<AgentEvent>,
        cited: &mut Vec<String>,
    ) -> (ChatMessage, Option<Value>) {
        let tool = match self.registry.get(&call.function.name) {
            Some(t) => t,
            None => {
                let err = format!("unknown tool: {}", call.function.name);
                warn!(tool = %call.function.name, "agent tried to call an unknown tool");
                let _ = events
                    .send(AgentEvent::ToolCallResult {
                        id: call.id.clone(),
                        name: call.function.name.clone(),
                        ok: false,
                        summary: err.clone(),
                        transaction_ids: Vec::new(),
                        data: Value::Null,
                        error: Some(err.clone()),
                    })
                    .await;
                return (
                    ChatMessage::Tool {
                        tool_call_id: call.id.clone(),
                        content: serde_json::json!({ "error": err }).to_string(),
                    },
                    None,
                );
            }
        };

        let deps = AgentDeps::new(db, self.provider.clone());
        match tool.invoke(deps, args).await {
            Ok(out) => {
                info!(
                    tool = %call.function.name,
                    txn_ids = out.transaction_ids.len(),
                    "tool invoked"
                );
                for id in &out.transaction_ids {
                    if !cited.contains(id) {
                        cited.push(id.clone());
                    }
                }
                let data = out.data.clone();
                let _ = events
                    .send(AgentEvent::ToolCallResult {
                        id: call.id.clone(),
                        name: call.function.name.clone(),
                        ok: true,
                        summary: out.summary.clone(),
                        transaction_ids: out.transaction_ids.clone(),
                        data: data.clone(),
                        error: None,
                    })
                    .await;
                let llm_content = truncate_tool_payload(&out.data, MAX_TOOL_PAYLOAD_BYTES);
                (
                    ChatMessage::Tool {
                        tool_call_id: call.id.clone(),
                        content: llm_content,
                    },
                    Some(data),
                )
            }
            Err(err) => {
                let msg = format!("{err:#}");
                warn!(tool = %call.function.name, error = %msg, "tool invocation failed");
                let _ = events
                    .send(AgentEvent::ToolCallResult {
                        id: call.id.clone(),
                        name: call.function.name.clone(),
                        ok: false,
                        summary: format!("tool failed: {msg}"),
                        transaction_ids: Vec::new(),
                        data: Value::Null,
                        error: Some(msg.clone()),
                    })
                    .await;
                (
                    ChatMessage::Tool {
                        tool_call_id: call.id.clone(),
                        content: serde_json::json!({ "error": msg }).to_string(),
                    },
                    None,
                )
            }
        }
    }
}

/// Extract a `FOLLOWUPS: [...]` line from anywhere in the assistant message. Robust to:
/// - the line not being last (gpt-4o-mini sometimes adds trailing whitespace/newlines)
/// - leading markdown markers (e.g. `**FOLLOWUPS:**`, `> FOLLOWUPS:`)
/// - extra whitespace inside the JSON brackets
///
/// Returns the cleaned content (with the line stripped) and the parsed list.
/// If the marker is absent or the JSON is malformed, returns the input unchanged + empty vec.
fn extract_followups(content: &str) -> (String, Vec<String>) {
    let mut found: Option<(usize, usize, Vec<String>)> = None;

    for (line_start, line) in line_offsets(content) {
        // Strip markdown bold/quote noise from the front, keep the original line span for removal.
        let stripped = line
            .trim_start_matches('>')
            .trim_start_matches([' ', '\t'])
            .trim_start_matches("**")
            .trim_start();
        let Some(after_label) = stripped
            .strip_prefix("FOLLOWUPS:")
            .or_else(|| stripped.strip_prefix("FOLLOWUPS :"))
            .or_else(|| stripped.strip_prefix("**FOLLOWUPS:**"))
        else {
            continue;
        };
        // Find the [...] block on this line (and possibly continuing).
        let rest = after_label.trim_start();
        let Some(start) = rest.find('[') else { continue };
        let Some(end) = rest.rfind(']') else { continue };
        if end <= start {
            continue;
        }
        let json_block = &rest[start..=end];
        let Ok(parsed) = serde_json::from_str::<Vec<String>>(json_block) else {
            continue;
        };
        let line_end = line_start + line.len();
        found = Some((line_start, line_end, parsed));
        break;
    }

    let Some((start, end, parsed)) = found else {
        return (content.to_string(), Vec::new());
    };

    // Splice the line out (plus a trailing newline if present).
    let mut cleaned = String::with_capacity(content.len());
    cleaned.push_str(&content[..start]);
    let after = &content[end..];
    cleaned.push_str(after.strip_prefix('\n').unwrap_or(after));
    (cleaned.trim_end().to_string(), parsed)
}

/// Bound the size of a tool payload before it goes back to the LLM. If the JSON serialises to
/// more than `max_bytes`, we trim large arrays — preserving the top-level summary fields
/// (count, totals) — and append a truncation note so the model knows it didn't see everything.
fn truncate_tool_payload(data: &Value, max_bytes: usize) -> String {
    let full = data.to_string();
    if full.len() <= max_bytes {
        return full;
    }

    // For objects, replace any large array with a head sample + a note.
    if let Value::Object(map) = data {
        let mut trimmed = serde_json::Map::with_capacity(map.len());
        for (k, v) in map {
            match v {
                Value::Array(arr) if arr.len() > 20 => {
                    let head: Vec<Value> = arr.iter().take(20).cloned().collect();
                    trimmed.insert(k.clone(), Value::Array(head));
                    trimmed.insert(
                        format!("_{k}_truncated_note"),
                        Value::String(format!(
                            "truncated to first 20 of {} entries to fit context",
                            arr.len()
                        )),
                    );
                }
                _ => {
                    trimmed.insert(k.clone(), v.clone());
                }
            }
        }
        let s = Value::Object(trimmed).to_string();
        if s.len() <= max_bytes {
            return s;
        }
        // Still too big: fall through to byte-level truncation.
    }

    let mut end = max_bytes;
    while end > 0 && !full.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n…[truncated; original payload was {} bytes]",
        &full[..end],
        full.len()
    )
}

/// Detect the `CATEGORY_CONFIRMATION_NEEDED: <slug>` sentinel in the assistant message.
/// Returns the slug (e.g. "groceries") if present anywhere in the content; otherwise None.
/// Robust to surrounding text, markdown bold, and trailing punctuation.
fn extract_confirmation_slug(content: &str) -> Option<String> {
    for (_, line) in line_offsets(content) {
        let stripped = line
            .trim_start_matches('>')
            .trim_start_matches([' ', '\t'])
            .trim_start_matches("**")
            .trim_start();
        let Some(after_label) = stripped
            .strip_prefix("CATEGORY_CONFIRMATION_NEEDED:")
            .or_else(|| stripped.strip_prefix("CATEGORY_CONFIRMATION_NEEDED :"))
        else {
            continue;
        };
        let slug_part = after_label.trim().trim_end_matches("**").trim();
        let Some(token) = slug_part.split_whitespace().next() else {
            continue;
        };
        let slug = token.trim_end_matches(['.', ',', ';', ':']);
        if slug.is_empty() {
            continue;
        }
        return Some(slug.to_string());
    }
    None
}

/// Iterate over (offset, line-without-newline) pairs of `s`.
fn line_offsets(s: &str) -> impl Iterator<Item = (usize, &str)> {
    let mut pos = 0;
    std::iter::from_fn(move || {
        if pos >= s.len() {
            return None;
        }
        let remainder = &s[pos..];
        let line_end = remainder.find('\n').map(|i| pos + i).unwrap_or(s.len());
        let line = &s[pos..line_end];
        let start = pos;
        pos = line_end + 1; // skip the newline
        Some((start, line))
    })
}

/// Convenience: convert an external conversation (system prompt + user turns) into the
/// initial `messages` vector for `AgentRunner::run`.
pub fn build_initial_messages(
    system_prompt: String,
    history: Vec<ChatMessage>,
    user_message: String,
) -> Result<Vec<ChatMessage>> {
    if user_message.trim().is_empty() {
        return Err(anyhow!("user message must not be empty"));
    }
    let mut messages = Vec::with_capacity(history.len() + 2);
    messages.push(ChatMessage::System {
        content: system_prompt,
    });
    messages.extend(history);
    messages.push(ChatMessage::User {
        content: user_message,
    });
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_followups_picks_up_trailing_json_line() {
        let input =
            "Here is the answer.\nMore detail.\nFOLLOWUPS: [\"Compare to last month?\", \"Show top 5\"]";
        let (cleaned, follows) = extract_followups(input);
        assert_eq!(cleaned, "Here is the answer.\nMore detail.");
        assert_eq!(follows, vec!["Compare to last month?", "Show top 5"]);
    }

    #[test]
    fn extract_followups_no_marker_returns_input_unchanged() {
        let input = "Just an answer with no marker.";
        let (cleaned, follows) = extract_followups(input);
        assert_eq!(cleaned, input);
        assert!(follows.is_empty());
    }

    #[test]
    fn extract_followups_malformed_json_yields_no_followups_and_keeps_line() {
        let input = "An answer.\nFOLLOWUPS: not json";
        let (cleaned, follows) = extract_followups(input);
        assert_eq!(cleaned, input);
        assert!(follows.is_empty());
    }

    #[test]
    fn extract_followups_handles_bold_markdown_wrapper() {
        let input = "Answer text.\n**FOLLOWUPS:** [\"a\", \"b\"]";
        let (cleaned, follows) = extract_followups(input);
        assert_eq!(cleaned, "Answer text.");
        assert_eq!(follows, vec!["a", "b"]);
    }

    #[test]
    fn extract_followups_strips_line_when_not_last() {
        let input = "First line.\nFOLLOWUPS: [\"q\"]\nTrailing text the model leaked.";
        let (cleaned, follows) = extract_followups(input);
        assert_eq!(cleaned, "First line.\nTrailing text the model leaked.");
        assert_eq!(follows, vec!["q"]);
    }

    #[test]
    fn extract_followups_handles_quote_prefix() {
        let input = "Answer.\n> FOLLOWUPS: [\"one\", \"two\"]";
        let (cleaned, follows) = extract_followups(input);
        assert_eq!(cleaned, "Answer.");
        assert_eq!(follows, vec!["one", "two"]);
    }

    #[test]
    fn extract_confirmation_slug_picks_up_basic_sentinel() {
        let s = extract_confirmation_slug("CATEGORY_CONFIRMATION_NEEDED: groceries");
        assert_eq!(s.as_deref(), Some("groceries"));
    }

    #[test]
    fn extract_confirmation_slug_strips_trailing_punctuation() {
        let s = extract_confirmation_slug("CATEGORY_CONFIRMATION_NEEDED: groceries.");
        assert_eq!(s.as_deref(), Some("groceries"));
    }

    #[test]
    fn extract_confirmation_slug_tolerates_markdown_bold() {
        let s = extract_confirmation_slug("**CATEGORY_CONFIRMATION_NEEDED: dining**");
        assert_eq!(s.as_deref(), Some("dining"));
    }

    #[test]
    fn extract_confirmation_slug_finds_line_mid_message() {
        let s = extract_confirmation_slug(
            "Let me check.\nCATEGORY_CONFIRMATION_NEEDED: transit\nThat's the plan."
        );
        assert_eq!(s.as_deref(), Some("transit"));
    }

    #[test]
    fn extract_confirmation_slug_returns_none_when_absent() {
        assert_eq!(extract_confirmation_slug("just an answer"), None);
    }

    #[test]
    fn truncate_tool_payload_passes_through_small_payloads() {
        let data = serde_json::json!({ "count": 3, "transactions": [1, 2, 3] });
        let out = truncate_tool_payload(&data, 1024);
        assert!(out.contains("\"count\":3"));
        assert!(out.contains("[1,2,3]"));
        assert!(!out.contains("_truncated_note"));
    }

    #[test]
    fn truncate_tool_payload_trims_large_arrays_and_keeps_metadata() {
        let big: Vec<i64> = (0..200).collect();
        let data = serde_json::json!({
            "count": 200,
            "total_outflow_cents": 1234567,
            "transactions": big,
        });
        // Force trimming by using a tight budget.
        let out = truncate_tool_payload(&data, 256);
        assert!(out.contains("\"count\":200"));
        assert!(out.contains("\"total_outflow_cents\":1234567"));
        assert!(out.contains("_transactions_truncated_note"));
    }
}
