use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentStub {
    pub mode: &'static str,
}

pub fn status() -> AgentStub {
    AgentStub {
        mode: "rules-first",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_defaults_to_rules_first_mode() {
        let value = status();
        assert_eq!(value.mode, "rules-first");
    }

    #[test]
    fn status_serializes_with_mode_field() {
        let json = serde_json::to_string(&status()).expect("serialization should work");
        assert!(json.contains("\"mode\":\"rules-first\""));
    }
}
