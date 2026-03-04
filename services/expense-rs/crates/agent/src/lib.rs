use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PdfExtractionProvider {
    OpenRouter,
    Mistral,
    HuggingFace,
    LlamaParse,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentCapability {
    pub id: &'static str,
    pub enabled: bool,
    pub configured_provider: Option<PdfExtractionProvider>,
    pub configured_via: Option<&'static str>,
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentStub {
    pub mode: &'static str,
    pub capabilities: Vec<AgentCapability>,
}

pub fn status() -> AgentStub {
    let (provider, configured_via) = configured_provider();
    let enabled = provider.is_some();

    AgentStub {
        mode: "rules-first",
        capabilities: vec![AgentCapability {
            id: "ai_pdf_extraction",
            enabled,
            configured_provider: provider,
            configured_via,
            status: if enabled {
                "configured"
            } else {
                "not_configured"
            },
        }],
    }
}

fn configured_provider() -> (Option<PdfExtractionProvider>, Option<&'static str>) {
    if is_set("OPENROUTER_API_KEY") {
        return (
            Some(PdfExtractionProvider::OpenRouter),
            Some("OPENROUTER_API_KEY"),
        );
    }
    if is_set("MISTRAL_API_KEY") {
        return (
            Some(PdfExtractionProvider::Mistral),
            Some("MISTRAL_API_KEY"),
        );
    }
    if is_set("HF_TOKEN") {
        return (Some(PdfExtractionProvider::HuggingFace), Some("HF_TOKEN"));
    }
    if is_set("LLAMAPARSE_API_KEY") || is_set("LLAMA_CLOUD_API_KEY") {
        return (
            Some(PdfExtractionProvider::LlamaParse),
            Some(if is_set("LLAMAPARSE_API_KEY") {
                "LLAMAPARSE_API_KEY"
            } else {
                "LLAMA_CLOUD_API_KEY"
            }),
        );
    }

    (None, None)
}

fn is_set(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unset_all() {
        for key in [
            "OPENROUTER_API_KEY",
            "MISTRAL_API_KEY",
            "HF_TOKEN",
            "LLAMAPARSE_API_KEY",
            "LLAMA_CLOUD_API_KEY",
        ] {
            unsafe { std::env::remove_var(key) };
        }
    }

    #[test]
    fn status_defaults_to_rules_first_mode() {
        let _guard = env_lock().lock().expect("env lock");
        unset_all();
        let value = status();
        assert_eq!(value.mode, "rules-first");
    }

    #[test]
    fn status_serializes_with_mode_and_capability() {
        let _guard = env_lock().lock().expect("env lock");
        unset_all();
        let json = serde_json::to_string(&status()).expect("serialization should work");
        assert!(json.contains("\"mode\":\"rules-first\""));
        assert!(json.contains("\"id\":\"ai_pdf_extraction\""));
    }

    #[test]
    fn status_marks_ai_pdf_capability_not_configured_without_env() {
        let _guard = env_lock().lock().expect("env lock");
        unset_all();
        let cap = status()
            .capabilities
            .into_iter()
            .find(|c| c.id == "ai_pdf_extraction")
            .expect("capability should exist");
        assert!(!cap.enabled);
        assert_eq!(cap.status, "not_configured");
        assert!(cap.configured_provider.is_none());
    }

    #[test]
    fn status_prefers_openrouter_when_configured() {
        let _guard = env_lock().lock().expect("env lock");
        unset_all();
        unsafe { std::env::set_var("OPENROUTER_API_KEY", "x") };

        let cap = status()
            .capabilities
            .into_iter()
            .find(|c| c.id == "ai_pdf_extraction")
            .expect("capability should exist");

        assert!(cap.enabled);
        assert_eq!(cap.status, "configured");
        assert_eq!(
            cap.configured_provider,
            Some(PdfExtractionProvider::OpenRouter)
        );
        assert_eq!(cap.configured_via, Some("OPENROUTER_API_KEY"));

        unset_all();
    }

    #[test]
    fn status_uses_hf_when_only_hf_is_configured() {
        let _guard = env_lock().lock().expect("env lock");
        unset_all();
        unsafe { std::env::set_var("HF_TOKEN", "x") };

        let cap = status()
            .capabilities
            .into_iter()
            .find(|c| c.id == "ai_pdf_extraction")
            .expect("capability should exist");

        assert_eq!(
            cap.configured_provider,
            Some(PdfExtractionProvider::HuggingFace)
        );
        assert_eq!(cap.configured_via, Some("HF_TOKEN"));

        unset_all();
    }
}
