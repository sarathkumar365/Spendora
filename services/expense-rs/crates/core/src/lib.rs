use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{env, path::PathBuf};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub service: &'static str,
    pub status: &'static str,
    pub now_utc: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("resource not found")]
    NotFound,
    #[error("validation failed: {0}")]
    Validation(String),
}

pub fn new_health_status(service: &'static str) -> HealthStatus {
    HealthStatus {
        service,
        status: "ok",
        now_utc: Utc::now(),
    }
}

pub fn new_idempotency_key() -> String {
    Uuid::new_v4().to_string()
}

pub fn default_app_data_dir() -> PathBuf {
    #[cfg(target_os = "windows")]
    {
        if let Ok(app_data) = env::var("APPDATA") {
            return PathBuf::from(app_data).join("ExpenseTrackerDesktop");
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Ok(home) = env::var("HOME") {
            return PathBuf::from(home)
                .join("Library")
                .join("Application Support")
                .join("ExpenseTrackerDesktop");
        }
    }

    if let Ok(home) = env::var("HOME") {
        return PathBuf::from(home)
            .join(".local")
            .join("share")
            .join("ExpenseTrackerDesktop");
    }

    PathBuf::from(".").join("data")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn health_status_is_ok_and_namespaced() {
        let status = new_health_status("expense-api");
        assert_eq!(status.service, "expense-api");
        assert_eq!(status.status, "ok");
    }

    #[test]
    fn idempotency_key_generation_is_unique() {
        let mut keys = HashSet::new();
        for _ in 0..100 {
            let inserted = keys.insert(new_idempotency_key());
            assert!(inserted, "duplicate idempotency key generated");
        }
    }

    #[test]
    fn app_data_dir_uses_expected_suffix() {
        let path = default_app_data_dir();
        let rendered = path.display().to_string();
        assert!(
            rendered.contains("ExpenseTrackerDesktop") || rendered.ends_with("./data"),
            "unexpected app data path: {rendered}"
        );
    }
}
