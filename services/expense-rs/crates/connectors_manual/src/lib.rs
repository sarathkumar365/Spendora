use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManualImportStub {
    pub status: &'static str,
}

pub fn connector_status() -> ManualImportStub {
    ManualImportStub {
        status: "ready-for-csv",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_status_defaults_to_ready_for_csv() {
        let status = connector_status();
        assert_eq!(status.status, "ready-for-csv");
    }

    #[test]
    fn connector_status_serializes_with_status_field() {
        let json = serde_json::to_string(&connector_status()).expect("serialization should work");
        assert!(json.contains("\"status\":\"ready-for-csv\""));
    }
}
