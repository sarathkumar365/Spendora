//! Helpers shared across the data tools.

use anyhow::{anyhow, Result};
use sqlx::{QueryBuilder, Sqlite};

pub use storage_sqlite::normalize_merchant_key as normalize_merchant;

/// Append an OR'd `LIKE` filter for each substring. Caller must wrap in an `AND (...)` block.
/// Returns true if any clauses were appended.
pub fn push_merchant_substrings_or<'a>(
    qb: &mut QueryBuilder<'a, Sqlite>,
    descr_col: &str,
    substrings: &'a [String],
) -> bool {
    let non_empty: Vec<&'a str> = substrings
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if non_empty.is_empty() {
        return false;
    }
    qb.push(" AND (");
    for (i, s) in non_empty.iter().enumerate() {
        if i > 0 {
            qb.push(" OR ");
        }
        qb.push(format!("LOWER({descr_col}) LIKE "));
        qb.push_bind(format!("%{}%", s.to_lowercase()));
    }
    qb.push(")");
    true
}

/// Validate an optional ISO date string in the form `YYYY-MM-DD`.
/// Returns `Ok(())` if the value is `None`.
pub fn validate_date_opt(value: Option<&str>, field: &str) -> Result<()> {
    let Some(v) = value else { return Ok(()) };
    validate_date(v, field)
}

/// Validate a required ISO date string in the form `YYYY-MM-DD`.
pub fn validate_date(value: &str, field: &str) -> Result<()> {
    if chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_err() {
        return Err(anyhow!("{field} must be ISO YYYY-MM-DD, got '{value}'"));
    }
    Ok(())
}

/// Validate a direction string is one of "debit" or "credit".
pub fn validate_direction(value: Option<&str>) -> Result<()> {
    if let Some(v) = value {
        if v != "debit" && v != "credit" {
            return Err(anyhow!("direction must be 'debit' or 'credit', got '{v}'"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_validation_accepts_iso() {
        validate_date("2026-04-30", "x").unwrap();
        validate_date_opt(Some("2026-04-30"), "x").unwrap();
        validate_date_opt(None, "x").unwrap();
    }

    #[test]
    fn date_validation_rejects_garbage() {
        assert!(validate_date("yesterday", "x").is_err());
        assert!(validate_date("2026/04/30", "x").is_err());
        assert!(validate_date_opt(Some("not-a-date"), "x").is_err());
    }

    #[test]
    fn direction_validation() {
        validate_direction(Some("debit")).unwrap();
        validate_direction(Some("credit")).unwrap();
        validate_direction(None).unwrap();
        assert!(validate_direction(Some("withdrawal")).is_err());
    }
}
