use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmProviderKind {
    OpenAi,
    Local,
}

impl LlmProviderKind {
    pub fn from_env() -> Self {
        match env::var("AGENT_LLM_PROVIDER")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "local" => Self::Local,
            _ => Self::OpenAi,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Local => "local",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "lowercase")]
pub enum ChatMessage {
    System {
        content: String,
    },
    User {
        content: String,
    },
    Assistant {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<ToolCall>,
    },
    Tool {
        tool_call_id: String,
        content: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type", default = "default_tool_type")]
    pub r#type: String,
    pub function: ToolCallFunction,
}

fn default_tool_type() -> String {
    "function".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallFunction {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolDefinition {
    #[serde(rename = "type")]
    pub r#type: &'static str,
    pub function: ToolFunctionSchema,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolFunctionSchema {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Debug, Clone)]
pub struct ChatCompletionRequest {
    pub messages: Vec<ChatMessage>,
    pub tools: Vec<ToolDefinition>,
    pub temperature: f32,
}

#[derive(Debug, Clone)]
pub struct ChatCompletionResponse {
    pub message: ChatMessage,
    pub finish_reason: String,
}

#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn complete(&self, req: ChatCompletionRequest) -> Result<ChatCompletionResponse>;
    fn model_label(&self) -> String;
    fn kind(&self) -> LlmProviderKind;
}

pub fn build_provider_from_env() -> Result<std::sync::Arc<dyn LlmProvider>> {
    match LlmProviderKind::from_env() {
        LlmProviderKind::OpenAi => Ok(std::sync::Arc::new(OpenAiProvider::from_env()?)),
        LlmProviderKind::Local => Ok(std::sync::Arc::new(LocalOpenAiCompatibleProvider::from_env()?)),
    }
}

pub struct OpenAiProvider {
    client: reqwest::Client,
    api_key: String,
    model: String,
    base_url: String,
}

impl OpenAiProvider {
    pub fn from_env() -> Result<Self> {
        let api_key = env::var("OPENAI_API_KEY")
            .map_err(|_| anyhow!("OPENAI_API_KEY is required for the OpenAI agent provider"))?;
        let model = env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string());
        let base_url =
            env::var("OPENAI_BASE_URL").unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(90))
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            client,
            api_key,
            model,
            base_url,
        })
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    async fn complete(&self, req: ChatCompletionRequest) -> Result<ChatCompletionResponse> {
        chat_completion(
            &self.client,
            &self.base_url,
            Some(&self.api_key),
            &self.model,
            req,
        )
        .await
    }

    fn model_label(&self) -> String {
        format!("openai:{}", self.model)
    }

    fn kind(&self) -> LlmProviderKind {
        LlmProviderKind::OpenAi
    }
}

pub struct LocalOpenAiCompatibleProvider {
    client: reqwest::Client,
    api_key: Option<String>,
    model: String,
    base_url: String,
}

impl LocalOpenAiCompatibleProvider {
    pub fn from_env() -> Result<Self> {
        let base_url = env::var("LOCAL_LLM_BASE_URL").map_err(|_| {
            anyhow!("LOCAL_LLM_BASE_URL is required when AGENT_LLM_PROVIDER=local")
        })?;
        let model = env::var("LOCAL_LLM_MODEL").map_err(|_| {
            anyhow!("LOCAL_LLM_MODEL is required when AGENT_LLM_PROVIDER=local")
        })?;
        let api_key = env::var("LOCAL_LLM_API_KEY").ok().filter(|v| !v.is_empty());
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(180))
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            client,
            api_key,
            model,
            base_url,
        })
    }
}

#[async_trait]
impl LlmProvider for LocalOpenAiCompatibleProvider {
    async fn complete(&self, req: ChatCompletionRequest) -> Result<ChatCompletionResponse> {
        chat_completion(
            &self.client,
            &self.base_url,
            self.api_key.as_deref(),
            &self.model,
            req,
        )
        .await
    }

    fn model_label(&self) -> String {
        format!("local:{}", self.model)
    }

    fn kind(&self) -> LlmProviderKind {
        LlmProviderKind::Local
    }
}

async fn chat_completion(
    client: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
    model: &str,
    req: ChatCompletionRequest,
) -> Result<ChatCompletionResponse> {
    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));

    let mut body = serde_json::json!({
        "model": model,
        "messages": req.messages,
        "temperature": req.temperature,
    });
    if !req.tools.is_empty() {
        body["tools"] = serde_json::to_value(&req.tools)?;
        body["tool_choice"] = Value::String("auto".to_string());
    }

    let mut request = client.post(&url).json(&body);
    if let Some(key) = api_key {
        request = request.bearer_auth(key);
    }

    let response = request
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .context("read chat completion body")?;
    if !status.is_success() {
        return Err(anyhow!(
            "llm provider returned {status}: {}",
            truncate_for_log(&text, 1024)
        ));
    }

    let json: Value = serde_json::from_str(&text)
        .with_context(|| format!("parse chat completion json: {}", truncate_for_log(&text, 512)))?;

    let choice = json
        .get("choices")
        .and_then(|c| c.get(0))
        .ok_or_else(|| anyhow!("chat completion missing choices[0]"))?;
    let finish_reason = choice
        .get("finish_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("stop")
        .to_string();
    let message_value = choice
        .get("message")
        .ok_or_else(|| anyhow!("chat completion missing choices[0].message"))?;

    let content = message_value
        .get("content")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let tool_calls: Vec<ToolCall> = message_value
        .get("tool_calls")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|tc| serde_json::from_value::<ToolCall>(tc.clone()).ok())
                .collect()
        })
        .unwrap_or_default();

    Ok(ChatCompletionResponse {
        message: ChatMessage::Assistant {
            content,
            tool_calls,
        },
        finish_reason,
    })
}

fn truncate_for_log(s: &str, max: usize) -> String {
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
mod truncate_tests {
    use super::truncate_for_log;

    #[test]
    fn short_string_returned_unchanged() {
        assert_eq!(truncate_for_log("hello", 1024), "hello");
    }

    #[test]
    fn long_ascii_truncates_at_max() {
        let s = "a".repeat(2000);
        let out = truncate_for_log(&s, 1024);
        assert_eq!(out.chars().count(), 1024 + 1); // 1024 'a' + '…'
    }

    #[test]
    fn does_not_panic_on_multibyte_boundary() {
        // 1023 'a' + 'é' (2 bytes). Naive &s[..1024] would slice mid-é and panic.
        let mut s = "a".repeat(1023);
        s.push('é');
        s.push_str(&"b".repeat(1000));
        let out = truncate_for_log(&s, 1024);
        assert!(out.ends_with('…'));
        assert!(out.starts_with("aaaa"));
    }
}
