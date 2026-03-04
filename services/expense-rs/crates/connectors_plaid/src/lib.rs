use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaidStub {
    pub status: &'static str,
}

pub fn connector_status() -> PlaidStub {
    PlaidStub {
        status: "not-configured",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_status_defaults_to_not_configured() {
        let status = connector_status();
        assert_eq!(status.status, "not-configured");
    }

    #[test]
    fn connector_status_serializes_with_status_field() {
        let json = serde_json::to_string(&connector_status()).expect("serialization should work");
        assert!(json.contains("\"status\":\"not-configured\""));
    }
}
