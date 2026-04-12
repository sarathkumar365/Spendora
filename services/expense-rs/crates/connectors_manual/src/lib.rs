use expense_core::{compute_row_hash, normalize_description, parse_amount_cents};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedRow {
    pub row_index: i64,
    pub booked_at: String,
    pub amount_cents: i64,
    pub description: String,
    pub confidence: f64,
    pub parse_error: Option<String>,
    pub normalized_txn_hash: String,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedImport {
    pub rows: Vec<ParsedRow>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

pub fn parse_pdf(bytes: &[u8], account_id: &str) -> ParsedImport {
    parse_text_like_statement(bytes, account_id, "pdf")
}

pub fn parse_csv(bytes: &[u8], account_id: &str) -> ParsedImport {
    parse_text_like_statement(bytes, account_id, "csv")
}

fn parse_text_like_statement(bytes: &[u8], account_id: &str, parser_label: &str) -> ParsedImport {
    let mut rows = Vec::new();
    let mut warnings = Vec::new();
    let mut errors = Vec::new();

    let content = match String::from_utf8(bytes.to_vec()) {
        Ok(value) => value,
        Err(_) => {
            errors.push(format!(
                "{parser_label} payload is not UTF-8 text; parser expects text extraction output"
            ));
            return ParsedImport {
                rows,
                warnings,
                errors,
            };
        }
    };

    for (line_idx, line) in content.lines().enumerate() {
        let clean = line.trim();
        if clean.is_empty() {
            continue;
        }

        let parts: Vec<&str> = if clean.contains('|') {
            clean.split('|').map(str::trim).collect()
        } else {
            clean.split(',').map(str::trim).collect()
        };

        if parts.len() < 3 {
            warnings.push(format!(
                "line {} skipped: expected date,description,amount",
                line_idx + 1
            ));
            continue;
        }

        let booked_at = parts[0].to_string();
        let description = normalize_description(parts[1]);
        let amount_cents = match parse_amount_cents(parts[2]) {
            Ok(v) => v,
            Err(err) => {
                rows.push(ParsedRow {
                    row_index: (line_idx + 1) as i64,
                    booked_at,
                    amount_cents: 0,
                    description,
                    confidence: 0.0,
                    parse_error: Some(err.to_string()),
                    normalized_txn_hash: String::new(),
                    metadata: None,
                });
                continue;
            }
        };

        let confidence = if is_valid_iso_date(&booked_at) {
            0.92
        } else {
            0.68
        };
        let parse_error = if confidence < 0.75 {
            Some("date format not ISO (YYYY-MM-DD), review required".to_string())
        } else {
            None
        };

        let normalized_txn_hash =
            compute_row_hash(account_id, &booked_at, amount_cents, &description);

        rows.push(ParsedRow {
            row_index: (line_idx + 1) as i64,
            booked_at,
            amount_cents,
            description,
            confidence,
            parse_error,
            normalized_txn_hash,
            metadata: None,
        });
    }

    if rows.is_empty() && errors.is_empty() {
        warnings.push("no transaction rows parsed".to_string());
    }

    ParsedImport {
        rows,
        warnings,
        errors,
    }
}

fn is_valid_iso_date(value: &str) -> bool {
    let chunks: Vec<&str> = value.split('-').collect();
    if chunks.len() != 3 {
        return false;
    }
    chunks[0].len() == 4
        && chunks[1].len() == 2
        && chunks[2].len() == 2
        && chunks
            .iter()
            .all(|chunk| chunk.chars().all(|ch| ch.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pdf_parses_pipe_delimited_rows() {
        let payload = b"2026-03-01|Coffee Shop|12.40\n2026-03-02|Grocery|-45.10";
        let result = parse_pdf(payload, "acct-1");
        assert_eq!(result.rows.len(), 2);
        assert!(result.errors.is_empty());
        assert!(result.rows.iter().all(|r| r.parse_error.is_none()));
    }

    #[test]
    fn parse_csv_marks_invalid_dates_for_review() {
        let payload = b"03/01/2026,Coffee,12.40";
        let result = parse_csv(payload, "acct-1");
        assert_eq!(result.rows.len(), 1);
        assert!(result.rows[0].parse_error.is_some());
        assert!(result.rows[0].confidence < 0.75);
    }

    #[test]
    fn parser_is_deterministic_for_same_input() {
        let payload = b"2026-03-01,Coffee,12.40";
        let a = parse_pdf(payload, "acct-1");
        let b = parse_pdf(payload, "acct-1");
        assert_eq!(a.rows[0].normalized_txn_hash, b.rows[0].normalized_txn_hash);
    }

    #[test]
    fn parse_pdf_reports_utf8_error_for_binary_payload() {
        let payload = [0xff, 0xfe, 0xfd, 0x00];
        let result = parse_pdf(&payload, "acct-1");
        assert!(result.rows.is_empty());
        assert_eq!(result.errors.len(), 1);
        assert!(result.errors[0].contains("not UTF-8"));
    }

    #[test]
    fn parse_pdf_skips_bad_lines_but_keeps_good_rows() {
        let payload = b"2026-03-01|Coffee|12.40\nbad_line\n2026-03-02|Transit|4.15";
        let result = parse_pdf(payload, "acct-1");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.warnings.len(), 1);
        assert!(result.warnings[0].contains("line 2 skipped"));
    }
}
