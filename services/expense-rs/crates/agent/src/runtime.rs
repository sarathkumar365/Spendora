use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage_sqlite::SqlitePool;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::llm::{ChatCompletionRequest, ChatMessage, LlmProvider, ToolCall};
use crate::tools::{AgentDeps, ToolRegistry};

pub const DEFAULT_MAX_ITERATIONS: usize = 6;

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

                let tool_msg = self
                    .execute_tool(db, &call, parsed_args, &events, &mut cited)
                    .await;
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
    ) -> ChatMessage {
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
                return ChatMessage::Tool {
                    tool_call_id: call.id.clone(),
                    content: serde_json::json!({ "error": err }).to_string(),
                };
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
                        data,
                        error: None,
                    })
                    .await;
                ChatMessage::Tool {
                    tool_call_id: call.id.clone(),
                    content: out.data.to_string(),
                }
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
                ChatMessage::Tool {
                    tool_call_id: call.id.clone(),
                    content: serde_json::json!({ "error": msg }).to_string(),
                }
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
}
