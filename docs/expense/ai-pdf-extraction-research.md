# AI PDF Extraction Research and Agent Capability Plan (March 4, 2026)

This document records current research on using external AI services for PDF extraction (instead of implementing full parsing/OCR in-house), and defines how to expose this as an agent capability in this repo.

## 1) Current Repo Reality Check

Based on current code:
- `services/expense-rs/crates/agent` is a stub and does not call any AI provider.
- `connectors_manual::parse_pdf` expects UTF-8 text payload, not raw binary PDFs.
- There is currently no provider client for OpenRouter, Hugging Face, Mistral, or LlamaParse.

Conclusion: AI-provider integration is not set up yet; it needs explicit capability + provider wiring.

## 2) What Teams Use in 2025-2026 for PDF Extraction

Pattern used in production:
1. Send PDF (URL or base64) to managed parser/OCR API.
2. Request strict JSON output schema (transactions or line-items).
3. Validate/normalize output in app layer.
4. Route low-confidence rows to human review.

Reason: managed document APIs are faster to ship and more robust for mixed-quality PDFs than in-house OCR+layout pipelines.

## 3) Provider Shortlist for This Repo (Tauri + Rust, local app)

### A) OpenRouter (recommended first integration path)
Why:
- Supports PDF input in chat completions.
- Supports `file-parser` plugin with selectable engines (`pdf-text`, `mistral-ocr`, `native`).
- Supports structured outputs via JSON schema.

Implication for us:
- Single provider endpoint can cover both text-PDF and scanned-PDF modes.
- We can ask for strict schema and map directly into `ImportRow`.

### B) Mistral OCR API (direct)
Why:
- Dedicated OCR/document AI endpoint and OCR models.
- Returns structured content suitable for parser pipelines.

Implication:
- Good direct fallback if we want to bypass OpenRouter abstraction.

### C) LlamaParse (LlamaCloud Parse)
Why:
- Purpose-built document parsing service (layout-aware, many file formats).
- Strong for noisy enterprise PDFs where simple OCR is insufficient.

Implication:
- Good second integration if bank statements have many layout variants.

### D) Hugging Face Inference Providers
Why:
- Unified API over multiple providers and model families.

Caveat:
- Great for model orchestration, but statement-grade extraction quality still depends heavily on selected model + prompt design + post-validation.

## 4) Primary Sources (latest checked)

- OpenRouter PDF Inputs:
  - https://openrouter.ai/docs/guides/overview/multimodal/pdfs
- OpenRouter Plugins (file-parser):
  - https://openrouter.ai/docs/guides/features/plugins/overview
- OpenRouter Structured Outputs:
  - https://openrouter.ai/docs/guides/features/structured-outputs
- OpenRouter API reference overview:
  - https://openrouter.ai/docs/api/reference/overview

- Mistral OCR capability docs:
  - https://docs.mistral.ai/capabilities/document_ai/basic_ocr
- Mistral OCR model page (OCR 3):
  - https://docs.mistral.ai/models/ocr-3-25-12

- Hugging Face Inference Providers:
  - https://huggingface.co/docs/inference-providers/index
- HF provider details (hf-inference):
  - https://huggingface.co/docs/inference-providers/providers/hf-inference

- LlamaCloud Parse overview:
  - https://docs.cloud.llamaindex.ai/
- LlamaIndex platform overview:
  - https://www.llamaindex.ai/

- Reference managed bank-statement alternatives:
  - Google Document AI pretrained overview:
    https://cloud.google.com/document-ai/docs/pretrained-overview
  - Google processors list (bank statement processor):
    https://cloud.google.com/document-ai/docs/processors-list
  - Google Document AI pricing:
    https://cloud.google.com/document-ai/pricing
  - Azure Document Intelligence overview (includes bank statement model):
    https://learn.microsoft.com/en-us/azure/ai-services/document-intelligence/overview?view=doc-intel-3.1.0
  - AWS Textract pricing and feature scope:
    https://aws.amazon.com/textract/pricing/

## 5) Decision Matrix for This Repository

Scoring scale: 1 (weak) to 5 (strong)

Criteria weights for this repo:
- Integration speed: 30%
- Extraction quality on real bank PDFs: 30%
- Cost predictability: 20%
- Vendor lock-in risk: 10%
- Operational simplicity: 10%

| Option | Speed | Quality | Cost predictability | Lock-in risk | Ops simplicity | Weighted result |
|---|---:|---:|---:|---:|---:|---:|
| OpenRouter + file-parser + schema output | 5 | 4 | 4 | 3 | 4 | 4.3 |
| Direct Mistral OCR integration | 4 | 4 | 4 | 3 | 4 | 4.0 |
| LlamaParse direct integration | 3 | 4.5 | 3 | 3 | 3.5 | 3.7 |
| HF Inference Providers + selected models | 3.5 | 3.5 | 3.5 | 3.5 | 3.5 | 3.5 |
| Full in-house parser/OCR | 1.5 | 2.5 (initially) | 5 | 5 | 1.5 | 2.6 |

Recommendation for next implementation:
1. Integrate OpenRouter first.
2. Use PDF engine selection:
   - `pdf-text` for text-rich PDFs.
   - `mistral-ocr` for scanned/image-heavy PDFs.
3. Enforce JSON schema output for transaction rows.
4. Keep provider abstraction so direct Mistral/HF/LlamaParse can be swapped in.

## 6) Proposed Agent Capability Contract

Capability id: `ai_pdf_extraction`

Inputs:
- raw PDF bytes (base64)
- import metadata: source account, statement period (optional)
- desired output schema (transactions)

Outputs:
- `rows[]` with normalized fields (`booked_at`, `description`, `amount_cents`)
- extraction diagnostics (`provider`, `model`, `confidence`, `warnings`, `errors`)
- optional `raw_blocks` for audit/replay

Safety/quality gates:
1. schema validation must pass (hard fail otherwise).
2. rows failing date/amount validation go to `review_required`.
3. dedupe hash generated before commit.
4. preserve original provider response artifact for debugging.

## 7) Config Shape (env-driven)

Suggested env variables:
- `OPENROUTER_API_KEY`
- `OPENROUTER_MODEL` (default configurable)
- `HF_TOKEN`
- `HF_MODEL`
- `MISTRAL_API_KEY`
- `MISTRAL_MODEL`
- `LLAMAPARSE_API_KEY`
- `LLAMAPARSE_MODE`

Selection policy:
- explicit provider override > first configured provider by precedence.
- precedence default: OpenRouter -> Mistral -> HuggingFace -> LlamaParse.

## 8) Immediate Next Step (implementation scope)

Do not implement parser internals locally.
Implement provider-backed extraction adapter in `connectors_manual` (or dedicated AI connector crate) with:
1. request builder (PDF + prompt + JSON schema)
2. response validator
3. mapping into existing `ParsedImport`
4. retry + timeout + structured error codes

This keeps existing ledger/import flow intact while outsourcing extraction complexity.

## 9) Concrete Choice for This Repo (Decision)

Chosen stack for first implementation:
1. Provider gateway: OpenRouter
2. PDF parser plugin: `file-parser`
3. Engine policy:
   - default `mistral-ocr` (best for scanned/image-heavy bank statements)
   - optional cost-optimized mode `pdf-text` for clearly text-based statements
4. Output contract: strict JSON Schema (`response_format.type = json_schema`, `strict = true`)
5. Reliability: enable Response Healing plugin for malformed JSON mitigation

Why this choice:
- Fastest integration path from Rust (single Chat Completions API).
- Covers both text PDFs and scanned PDFs.
- Keeps provider/model flexibility while preserving one stable app contract.
- Avoids building OCR/layout parsing internals in-house.

Initial provider/model guidance:
- Start with a structured-output-capable chat model through OpenRouter and keep model configurable by env var.
- Keep extraction engine explicit in request (`mistral-ocr` or `pdf-text`) rather than relying on defaults.

Configuration defaults for first release:
- `OPENROUTER_API_KEY` (required)
- `OPENROUTER_MODEL` (required, configurable)
- `OPENROUTER_PDF_ENGINE` default: `mistral-ocr`
- `OPENROUTER_ENABLE_RESPONSE_HEALING` default: `true`

## 10) Free-Only Reality Check (March 4, 2026)

If we optimize for **$0 spend**, these are the practical options:

### Option A: OpenRouter free plan (cloud, limited)
- OpenRouter free plan has explicit request limits:
  - 50 requests/day
  - 20 requests/min
- OpenRouter PDF docs show:
  - `pdf-text` engine is free
  - `mistral-ocr` engine is paid ($2 / 1,000 pages)
- Implication:
  - Zero-cost path works only for text-based PDFs and low request volume.
  - Scanned/image-heavy statements will need paid OCR engine (or local OCR stack).

### Option B: Hugging Face Inference Providers (cloud, very small credit)
- HF docs show monthly free credit for free users is `$0.10` (subject to change).
- Implication:
  - Fine for tiny experiments only, not sustained ingestion.

### Option C: LlamaParse free plan (cloud, generous starter quota)
- LlamaCloud resources page lists:
  - Free tier credits: 10,000 credits/month
  - Parse mode pricing examples:
    - Parse without AI: 1 credit/page
    - Parse with AI extraction: starts around 3 credits/page (can be higher by mode)
- Implication:
  - Best free managed quota among researched options for PDF extraction.
  - Still cloud-dependent and quota-limited.

### Option D: Fully local open-source stack (no API bill)
- Tesseract: open source OCR under Apache 2.0.
- PaddleOCR: Apache 2.0.
- OCRmyPDF: MPL-2.0 (commercially usable; modifications to OCRmyPDF itself must be published).
- Implication:
  - True zero-API-cost operation.
  - Higher implementation/maintenance complexity compared to managed APIs.

## 11) Free-First Recommendation

For strict no-cost operation with managed APIs:
1. Primary: LlamaParse free tier (best free managed quota currently documented).
2. Secondary: OpenRouter free route with `pdf-text` only for text-based PDFs.
3. Guardrails:
   - `ALLOW_PAID_PDF_OCR=false` by default
   - reject OpenRouter `mistral-ocr` engine unless spending is explicitly enabled
4. Routing policy:
   - try LlamaParse first
   - if quota exceeded, attempt OpenRouter `pdf-text`
   - if both fail, queue import as `review_required` with actionable message

## 12) Sources for Free-Limit Claims

- OpenRouter pricing/rate limits:
  - https://openrouter.ai/pricing
- OpenRouter PDF engine pricing:
  - https://openrouter.ai/docs/guides/overview/multimodal/pdfs
- OpenRouter free router page:
  - https://openrouter.ai/openrouter/free
- Hugging Face inference pricing:
  - https://huggingface.co/docs/inference-providers/main/en/pricing
- LlamaIndex/LlamaParse free plan (marketing page):
  - https://www.llamaindex.ai/
- LlamaCloud resources and credit/page pricing examples:
  - https://developers.llamaindex.ai/python/cloud/llamacloud/pricing_and_cost_analysis/
- LlamaParse parsing modes and AI/ocr mode options:
  - https://docs.cloud.llamaindex.ai/llamaparse/features/parsing_modes
- Tesseract license:
  - https://tesseract-ocr.github.io/tessdoc/
- PaddleOCR license:
  - https://github.com/PaddlePaddle/PaddleOCR
- OCRmyPDF licensing:
  - https://github.com/ocrmypdf/OCRmyPDF

## 13) Implementation Mapping in This Repo (Step 2.1)

Implemented:
1. Managed extraction orchestrator crate: `crates/connectors_ai`
2. Provider priority: `llamaparse` first, `openrouter_pdf_text` fallback
3. Retry cap: maximum 3 attempts per provider
4. Global extraction settings + per-import overrides
5. Local OCR mode exposed as stub (`LOCAL_OCR_NOT_IMPLEMENTED`)
6. Full raw provider response logging to runtime log files

Data model additions:
- `imports.extraction_mode`
- `imports.managed_provider_preference` (legacy column; not used by current API flow)
- `imports.effective_provider`
- `imports.provider_attempts_json`
- `imports.extraction_diagnostics_json`
- `imports.provider_attempt_count`
- `app_settings` table with `extraction_settings`

API additions:
- `GET /api/v1/settings/extraction`
- `PUT /api/v1/settings/extraction`
- `POST /api/v1/imports` optional extraction override fields
- `GET /api/v1/imports/:id/status` returns extraction diagnostics fields

Operational defaults:
- managed extraction default mode
- fallback enabled by default
- max retries clamped to 3
- full response logging enabled unless explicitly disabled
