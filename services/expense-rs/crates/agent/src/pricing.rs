//! Estimated USD cost per LLM call based on a static price table.
//!
//! Prices are expressed in **micro-dollars per million tokens** (so a price of `150_000`
//! means $0.15 per 1M input tokens). Stored in i64 to avoid float drift; the audit DB stores
//! `cost_micros` (micro-dollars) so the conversion to/from dollars only happens at display.
//!
//! For models not in the table, calling `cost_micros_for` returns `None` (the audit row gets
//! a NULL cost, and the UI surfaces "(pricing unavailable)").
//!
//! Env override: `AGENT_PRICING_OVERRIDE="openai:gpt-5=300000,1200000;some:model=0,0"` —
//! semicolon-separated entries, each `model_label=in_per_1m,out_per_1m` in micro-dollars.

use crate::llm::TokenUsage;

#[derive(Debug, Clone, Copy)]
pub struct ModelPricing {
    /// Micro-dollars per 1,000,000 input tokens.
    pub input_per_million_micros: i64,
    /// Micro-dollars per 1,000,000 output tokens.
    pub output_per_million_micros: i64,
}

impl ModelPricing {
    pub fn cost_micros(&self, usage: &TokenUsage) -> i64 {
        // micro-dollars = tokens * (micros_per_million / 1_000_000)
        // To avoid early underflow, compute (tokens * per_million) then divide.
        let prompt =
            (usage.prompt_tokens.max(0) * self.input_per_million_micros) / 1_000_000;
        let completion =
            (usage.completion_tokens.max(0) * self.output_per_million_micros) / 1_000_000;
        prompt + completion
    }
}

/// Built-in price table. As of 2026-05.
/// Sources: OpenAI public pricing. Numbers are micro-dollars per 1M tokens.
/// Match keys against `LlmProvider::model_label()`, e.g. "openai:gpt-4o-mini".
const BUILTIN_PRICING: &[(&str, ModelPricing)] = &[
    (
        "openai:gpt-4o-mini",
        ModelPricing {
            input_per_million_micros: 150_000,
            output_per_million_micros: 600_000,
        },
    ),
    (
        "openai:gpt-4o",
        ModelPricing {
            input_per_million_micros: 2_500_000,
            output_per_million_micros: 10_000_000,
        },
    ),
    (
        "openai:gpt-4-turbo",
        ModelPricing {
            input_per_million_micros: 10_000_000,
            output_per_million_micros: 30_000_000,
        },
    ),
    // OpenAI-compatible model strings the user might hit via OPENAI_BASE_URL=OpenRouter:
    (
        "openai:openai/gpt-4o-mini",
        ModelPricing {
            input_per_million_micros: 150_000,
            output_per_million_micros: 600_000,
        },
    ),
    (
        "openai:anthropic/claude-haiku-4-5",
        ModelPricing {
            input_per_million_micros: 1_000_000,
            output_per_million_micros: 5_000_000,
        },
    ),
];

pub fn pricing_for(model_label: &str) -> Option<ModelPricing> {
    if let Some(p) = lookup_env_override(model_label) {
        return Some(p);
    }
    BUILTIN_PRICING
        .iter()
        .find(|(k, _)| *k == model_label)
        .map(|(_, p)| *p)
}

pub fn cost_micros_for(model_label: &str, usage: &TokenUsage) -> Option<i64> {
    pricing_for(model_label).map(|p| p.cost_micros(usage))
}

fn lookup_env_override(model_label: &str) -> Option<ModelPricing> {
    let raw = std::env::var("AGENT_PRICING_OVERRIDE").ok()?;
    for entry in raw.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((model, prices)) = entry.split_once('=') else {
            continue;
        };
        if model.trim() != model_label {
            continue;
        }
        let Some((in_s, out_s)) = prices.split_once(',') else {
            continue;
        };
        let (Ok(input), Ok(output)) = (in_s.trim().parse::<i64>(), out_s.trim().parse::<i64>())
        else {
            continue;
        };
        return Some(ModelPricing {
            input_per_million_micros: input,
            output_per_million_micros: output,
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serialises tests that mutate the AGENT_PRICING_OVERRIDE env var so they don't race
    /// against tests reading the built-in price table.
    fn env_lock() -> &'static Mutex<()> {
        static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn usage(p: i64, c: i64) -> TokenUsage {
        TokenUsage {
            prompt_tokens: p,
            completion_tokens: c,
        }
    }

    #[test]
    fn cost_for_known_model_matches_published_rates() {
        let _guard = env_lock().lock().expect("env lock");
        unsafe {
            std::env::remove_var("AGENT_PRICING_OVERRIDE");
        }
        // gpt-4o-mini: $0.15/M input, $0.60/M output → 100 input + 50 output =
        //   100*150_000/1M + 50*600_000/1M = 15 + 30 = 45 micros.
        let cost = cost_micros_for("openai:gpt-4o-mini", &usage(100, 50)).expect("ok");
        assert_eq!(cost, 45);
    }

    #[test]
    fn cost_for_unknown_model_returns_none() {
        let _guard = env_lock().lock().expect("env lock");
        unsafe { std::env::remove_var("AGENT_PRICING_OVERRIDE"); }
        assert!(cost_micros_for("openai:unknown-model-xyz", &usage(100, 50)).is_none());
    }

    #[test]
    fn cost_handles_zero_tokens() {
        let _guard = env_lock().lock().expect("env lock");
        unsafe { std::env::remove_var("AGENT_PRICING_OVERRIDE"); }
        let cost = cost_micros_for("openai:gpt-4o-mini", &usage(0, 0)).expect("ok");
        assert_eq!(cost, 0);
    }

    #[test]
    fn cost_handles_large_token_counts() {
        let _guard = env_lock().lock().expect("env lock");
        unsafe { std::env::remove_var("AGENT_PRICING_OVERRIDE"); }
        // 1M tokens at $0.15/M = $0.15 = 150_000 micros.
        let cost = cost_micros_for("openai:gpt-4o-mini", &usage(1_000_000, 0)).expect("ok");
        assert_eq!(cost, 150_000);
    }

    #[test]
    fn env_override_takes_precedence() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "AGENT_PRICING_OVERRIDE";
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, "openai:gpt-4o-mini=999,888;openai:custom-x=10,20");
        }

        let p = pricing_for("openai:gpt-4o-mini").expect("ok");
        assert_eq!(p.input_per_million_micros, 999);
        assert_eq!(p.output_per_million_micros, 888);

        let p2 = pricing_for("openai:custom-x").expect("ok");
        assert_eq!(p2.input_per_million_micros, 10);
        assert_eq!(p2.output_per_million_micros, 20);

        // Restore.
        unsafe {
            match prev {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }
}
