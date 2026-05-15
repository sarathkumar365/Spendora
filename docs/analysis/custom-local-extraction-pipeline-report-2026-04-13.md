# Custom In-App Statement Extraction Pipeline Report

Date: 2026-04-13  
Author: Senior engineering assessment for Spendora

## 1) Executive Summary

You can build a fully local, in-app PDF statement extraction pipeline in this repo, but it is a non-trivial product effort, not a small connector swap.

Main conclusion:
- A reliable local-first pipeline is feasible.
- It will take roughly `6-10 weeks` for a production-grade v1 (single engineer), plus ongoing model/rules maintenance.
- The highest risk is not OCR itself, but statement layout variance across issuers + preserving accuracy over time.
- Your current codebase already has good normalization/review/commit infrastructure to reuse. The missing piece is the actual `local_ocr` extraction engine and a quality benchmark harness.

## 2) Current Spendora Reality (Code + DB Evidence)

What exists today:
- `managed` extraction is implemented (LlamaExtract jobs + provider diagnostics + retries/fallback).
- `local_ocr` mode is wired in API/settings/worker routing.
- Statement-v2 schema and downstream DB fields already exist.

Critical gap:
- `local_ocr` is a stub that always returns `LOCAL_OCR_NOT_IMPLEMENTED`.
  - `services/expense-rs/crates/connectors_ai/src/lib.rs` (`local_ocr_stub`)
  - `services/expense-rs/crates/worker/src/main.rs` (fails import on local_ocr path)

Other important observations:
- `connectors_manual::parse_pdf()` still expects UTF-8 text payload, not binary PDF parsing.
  - `services/expense-rs/crates/connectors_manual/src/lib.rs`
- In current runtime DB (`services/expense-rs/.runtime/expense.db`), imports are all managed:
  - `imports.status`: committed=3
  - `imports.extraction_mode`: managed=3
  - `imports.effective_provider`: llamaextract_jobs=3
- `transactions`: 233 rows, all with `type`/`direction` set (`debit` 193, `credit` 40), sign consistency checks currently pass in sampled runtime DB.
- Statement balance subtotal fields (`opening_balance_cents`, `closing_balance_cents`, `total_debits_cents`, `total_credits_cents`) are null in current statements, which limits reconciliation confidence from persisted data.

## 3) What “Correct Current Results” Should Mean

Before building the engine, define correctness explicitly:

Required measurable targets (v1):
- Row recall: `>= 99.0%` (transactions found vs ground truth).
- Row precision: `>= 99.5%` (extracted rows that are real transactions).
- Amount exact match: `>= 99.8%`.
- Date exact match: `>= 99.5%`.
- Direction/sign consistency: `>= 99.9%`.
- Statement-level reconciliation pass (where summary exists): `>= 98%`.

And enforce by regression:
- Every code change to extraction runs against a fixed gold dataset of statement PDFs + expected JSON.

## 4) Recommended Local Architecture (In-App)

Implement as a deterministic multi-stage pipeline inside worker, keeping your current import/review/commit contract unchanged.

### Stage A: PDF Triage (per page)
- Detect if page has embedded extractable text.
- If yes: prefer native text extraction path.
- If no / low text density: render page image + OCR path.

### Stage B: Data Acquisition
- Text PDF path:
  - Use a local PDF extractor to pull words/blocks with coordinates.
- Scanned path:
  - Render page image at controlled DPI.
  - OCR to words/lines with bounding boxes.

### Stage C: Layout + Table Reconstruction
- Identify statement zones: header, account summary, transaction table, footer.
- Build row candidates from aligned text cells and positional heuristics.
- Parse canonical fields: `transaction_date`, `details`, `amount`, `type`.

### Stage D: Normalization + Quality Gates
- Reuse existing v2 normalization and metadata strategy.
- Keep or extend current direction/sign conflict checks.
- Add deterministic reconciliation checks (opening + movement ~= closing, and debit/credit subtotal checks).
- Mark low-confidence rows as review-required, never silently coerce.

### Stage E: Persistence + Auditability
- Persist:
  - full payload snapshot
  - parse flags per row
  - extraction diagnostics by stage
  - quality metrics and reconciliation result
- Keep your current `import_rows` + commit flow, with additional diagnostics fields rather than changing app behavior abruptly.

## 5) Implementation Plan (Practical, Repo-Aligned)

### Phase 0 (3-5 days): Benchmark Harness First
- Add fixtures directory for PDF + expected normalized output.
- Add evaluation runner producing precision/recall/field-accuracy/reconciliation metrics.
- Block merges if regression threshold fails.

### Phase 1 (1.5-2 weeks): Local Extraction Core
- Replace `local_ocr_stub` with real pipeline entrypoint.
- Build PDF triage (text-first vs OCR fallback).
- Output to current `ExtractionResult` + row metadata.

### Phase 2 (2-3 weeks): Transaction Table Extraction Reliability
- Implement row/column alignment and multi-line description handling.
- Add issuer-specific adapters for top 3-5 statement templates.
- Add hard parsing for dates, signed amounts, and debit/credit cues.

### Phase 3 (1-2 weeks): Reconciliation + Review UX Contract
- Persist quality/reconciliation metrics from extractor to diagnostics.
- Expose metrics via import status API for debugging and trust.
- Add clear review reasons for ambiguous rows.

### Phase 4 (1-2 weeks): Hardening + Packaging
- Performance optimization (parallel page OCR, caching).
- Cross-platform packaging concerns (macOS/Windows/Linux binaries/dependencies).
- Soak test with unseen statement formats.

## 6) Technology Choices (Local, No External APIs)

Best-practical option for this Rust app:
- Core app stays Rust (existing worker path).
- Use robust local OCR + PDF stack, invoked from Rust:
  - text extraction for born-digital PDFs
  - OCR engine for scanned PDFs
  - optional table-structure model for difficult layouts

Engineering caution:
- Pure Rust-only ecosystem for high-accuracy document OCR/layout is still thinner than Python/C++ tooling.
- Most teams use a hybrid local stack (Rust orchestrator + native libs / local model runtime), not purely handcrafted regex parsing.

## 7) Risk Register

Top risks:
- Layout variance by bank/template breaks row alignment.
- OCR quality drops on low-DPI scans, rotated pages, or faint prints.
- False positives from non-transaction lines (fees summary, balances, notes).
- Packaging native OCR/runtime dependencies across desktop OS targets.
- No benchmark set means “seems good” can mask regressions.

Mitigations:
- Gold dataset + regression CI is mandatory before shipping.
- Issuer-template plugins (deterministic first, ML fallback second).
- Strict parse flags and review-required route for ambiguity.
- Keep managed extraction as temporary fallback until local accuracy is proven.

## 8) Recommendation

Recommended strategy:
1. Build local pipeline incrementally behind `local_ocr` (new real implementation).
2. Keep current managed pipeline as safety fallback during validation period.
3. Promote local to default only after benchmark pass criteria are met for consecutive runs on representative PDFs.

This gives you independence from external systems without risking data quality regressions in production.

## 9) External Research Notes (Primary Sources)

These sources support the local-stack design constraints:

- Tesseract documentation and quality guidance:
  - https://tesseract-ocr.github.io/
  - https://tesseract-ocr.github.io/tessdoc/ImproveQuality.html
- OCRmyPDF docs (local OCR pipeline behavior and dependencies):
  - https://ocrmypdf.readthedocs.io/en/v16.0.1post1/introduction.html
- PaddleOCR table structure docs (for complex statement table layouts):
  - https://www.paddleocr.ai/v3.0.1/en/version3.x/module_usage/table_structure_recognition.html
- Camelot FAQ limitation (text PDFs only, not scanned):
  - https://camelot-py.readthedocs.io/en/master/user/faq.html
- PDFium licensing / ecosystem pointers:
  - https://github.com/PDFium/PDFium
  - https://docs.rs/crate/pdfium-render/latest
- PyMuPDF text extraction modes (layout granularity context):
  - https://pymupdf.readthedocs.io/en/latest/app1.html
