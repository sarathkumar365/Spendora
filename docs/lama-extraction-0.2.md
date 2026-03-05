# Llama Extraction 0.2

## Scope
This document captures the full LlamaExtract Jobs flow analysis and related endpoint details gathered from the official Llama API references provided.

It preserves all details from the analysis, including:
- end-to-end flow
- available endpoints
- payload and response shapes
- status models
- integration implications for this codebase
- migration direction
- source URLs

## Input URLs Analyzed
- Private run URL (access-limited in this environment):  
  `https://cloud.llamaindex.ai/project/fac76828-a804-4b04-aef8-fbe74da38468/extract?run_id=1c8a1094-499b-4b38-8d9f-c571d34108eb`
- LlamaExtract REST getting-started page:  
  `https://developers.llamaindex.ai/python/cloud/llamaextract/getting_started/api/`
- TypeScript `extraction.run` docs page:  
  `https://developers.api.llamaindex.ai/api/typescript/resources/extraction/methods/run`

## Access Constraint Observed
- The private project run URL could not be opened directly from this environment (auth/cache blocked).
- Public developer docs were accessible and used as source of truth for endpoint and schema analysis.

## LlamaExtract Jobs Flow (Recommended)
1. Upload file.
2. Create (or reuse) extraction agent.
3. Create extraction job.
4. Poll job status.
5. Fetch job result.

### Step 1: Upload File
Endpoint:
- `POST /api/v1/beta/files`

Result:
- returns file object with `id` used as `file_id` in jobs.

Notes:
- REST quickstart page examples also show `POST /api/v1/files` (see endpoint drift section).

### Step 2: Create or Reuse Extraction Agent
Endpoint:
- `POST /api/v1/extraction/extraction-agents`

Core body elements:
- `config` (`ExtractConfig`)
- `data_schema`

Result:
- extraction agent object containing `id` and associated config/schema.

### Step 3: Start a Job
Primary endpoint:
- `POST /api/v1/extraction/jobs`

Request body:
- `extraction_agent_id` (uuid, required)
- `file_id` (uuid, required)
- `config_override` (optional `ExtractConfig`)

Optional query:
- `from_ui` (optional bool)

Response:
- `ExtractJob` object:
  - `id`
  - `extraction_agent`
  - `status`
  - `error` (optional)
  - `file` (deprecated object)
  - `file_id` (optional)

Alternative one-call endpoint:
- `POST /api/v1/extraction/jobs/file`
- `multipart/form-data` with:
  - `extraction_agent_id`
  - `file=@...`
- optional `from_ui`
- returns same `ExtractJob` shape.

### Step 4: Poll Job
Endpoint:
- `GET /api/v1/extraction/jobs/{job_id}`

Also available:
- `GET /api/v1/extraction/jobs` (list jobs; docs page indicates filtering by `extraction_agent_id`).

### Step 5: Retrieve Result
Endpoint:
- `GET /api/v1/extraction/jobs/{job_id}/result`

Optional query:
- `organization_id`
- `project_id`

Response shape:
- `data` (structured output)
- `extraction_agent_id`
- `extraction_metadata`
- `run_id`

## Extraction Endpoint Inventory (Read/Observed)
### Extraction (Top-level)
- `POST /api/v1/extraction/run` (stateless extraction)

### Extraction > Jobs
- `GET /api/v1/extraction/jobs`
- `POST /api/v1/extraction/jobs`
- `GET /api/v1/extraction/jobs/{job_id}`
- `POST /api/v1/extraction/jobs/file`
- `GET /api/v1/extraction/jobs/{job_id}/result`

### Extraction > Runs
- `GET /api/v1/extraction/runs`
- `GET /api/v1/extraction/runs/{run_id}`
- `DELETE /api/v1/extraction/runs/{run_id}`
- `GET /api/v1/extraction/runs/by-job/{job_id}`

### Extraction > Extraction Agents
- `POST /api/v1/extraction/extraction-agents`
- `GET /api/v1/extraction/extraction-agents`
- `GET /api/v1/extraction/extraction-agents/{extraction_agent_id}`
- `PUT /api/v1/extraction/extraction-agents/{extraction_agent_id}`
- `DELETE /api/v1/extraction/extraction-agents/{extraction_agent_id}`

### Extraction Agent Schema Utilities
- `POST /api/v1/extraction/extraction-agents/schema/validation`
- `POST /api/v1/extraction/extraction-agents/schema/generate` (listed in index)

### Files (for jobs/stateless workflows)
- `POST /api/v1/beta/files`

## Stateless Extraction (`/extraction/run`) Details
Endpoint:
- `POST /api/v1/extraction/run`

Core body:
- `config` (`ExtractConfig`)
- `data_schema` (required)
- one of:
  - `file_id`
  - `text`
  - `file` object (`data` base64 + `mime_type`)
- optional:
  - `webhook_configurations` (events, headers, output format, URL)

Optional query:
- `organization_id`
- `project_id`

Response:
- `ExtractJob` object with:
  - `id`
  - `status`
  - `error`
  - `extraction_agent`
  - `file`
  - `file_id`

## Status Models
### ExtractJob Status Enum
- `PENDING`
- `SUCCESS`
- `ERROR`
- `PARTIAL_SUCCESS`
- `CANCELLED`

### ExtractRun Status Enum
- `CREATED`
- `PENDING`
- `SUCCESS`
- `ERROR`

Important operational note:
- `ExtractJob` docs do not expose a `RUNNING` status in this model.
- Polling logic should treat only documented job states as canonical.

## ExtractConfig Fields Identified
The following knobs were identified in docs/examples and are relevant for quality/perf control:
- `chunk_mode`: `PAGE | SECTION`
- `cite_sources`
- `citation_bbox` (deprecated)
- `confidence_scores`
- `extract_model`
- `extraction_mode`: `FAST | BALANCED | PREMIUM | MULTIMODAL`
- `extraction_target`: `PER_DOC | PER_PAGE | PER_TABLE_ROW`
- `high_resolution_mode`
- `invalidate_cache`
- `multimodal_fast_mode` (deprecated)
- `num_pages_context`
- `page_range`
- `parse_model`
- `priority`: `low | medium | high | critical`
- `system_prompt`
- `use_reasoning`

## Runs Linkage and Result Semantics
From job result:
- `run_id` is returned and can be used with run endpoints.

Recommended persistence linkage in app integrations:
- persist `job_id`
- persist `run_id`
- persist terminal status and error class
- persist extraction metadata and schema version identifiers

## Endpoint Drift / Versioning Note
Observed discrepancy between references:
- API reference shows file upload under `POST /api/v1/beta/files`.
- getting-started examples still show `POST /api/v1/files`.

Action:
- verify which file endpoint is enabled for the target org/project before finalizing implementation.

## Related Parsing API (Observed as Adjacent Capability)
This is adjacent to LlamaExtract and relevant for migration decisions:
- `POST /api/v2/parse`
- `GET /api/v2/parse/{job_id}`
- `GET /api/v2/parse`

Parse status model (v2 docs):
- `PENDING | RUNNING | COMPLETED | FAILED | CANCELLED`

Important distinction:
- Parse v2 and LlamaExtract Jobs expose different object and status models.
- LlamaExtract Jobs is schema-driven extraction, while Parse is document parsing output.

## Integration Implications for This Repository
### What to Use as Source of Truth
- Prefer `GET /api/v1/extraction/jobs/{job_id}/result` `data` for structured extraction.
- Avoid relying on markdown-table OCR fallback as primary extraction when Jobs result is available.

### Polling Logic Guidance
- Continue polling only while status is non-terminal (`PENDING`).
- Treat `SUCCESS` and `PARTIAL_SUCCESS` as result-bearing terminal states.
- Treat `ERROR` and `CANCELLED` as failure terminal states.

### Reliability and Observability
- capture and store attempt metadata (submit, poll count, terminal reason)
- keep `job_id`/`run_id` for auditability and troubleshooting
- classify retryable failures (network/timeouts/5xx/rate-limit) separately from schema/data errors

### Quality Controls
- validate schema before production execution (`schema/validation`)
- version and pin extraction agent + config where possible
- treat provider-side structured result as canonical to reduce heuristic parsing errors

## Why This Is Better Than Legacy Parse-Only + Heuristics
- direct schema-constrained output (`data`) reduces brittle table parsing logic
- better confidence/citation support
- cleaner async lifecycle through jobs and runs
- more deterministic ingestion path for downstream DB mapping

## Sources
- API index:  
  `https://developers.api.llamaindex.ai/api`
- Extraction overview:  
  `https://developers.api.llamaindex.ai/api/resources/extraction`
- Extraction run (stateless):  
  `https://developers.api.llamaindex.ai/api/resources/extraction/methods/run`
- Jobs list:  
  `https://developers.api.llamaindex.ai/api/resources/extraction/subresources/jobs/methods/list`
- Jobs create:  
  `https://developers.api.llamaindex.ai/api/resources/extraction/subresources/jobs/methods/create`
- Jobs get:  
  `https://developers.api.llamaindex.ai/api/resources/extraction/subresources/jobs/methods/get`
- Jobs file (multipart):  
  `https://developers.api.llamaindex.ai/api/resources/extraction/subresources/jobs/methods/file`
- Jobs result:  
  `https://developers.api.llamaindex.ai/api/resources/extraction/subresources/jobs/methods/get_result`
- TypeScript extraction run docs:  
  `https://developers.api.llamaindex.ai/api/typescript/resources/extraction/methods/run`
- TypeScript runs list docs:  
  `https://developers.api.llamaindex.ai/api/typescript/resources/extraction/subresources/runs/methods/list`
- Extraction agent schema validation:  
  `https://developers.api.llamaindex.ai/api/resources/extraction/subresources/extraction_agents/subresources/schema/methods/validate_schema`
- Files create (`beta`):  
  `https://developers.api.llamaindex.ai/api/resources/files/methods/create`
- LlamaExtract REST getting started:  
  `https://developers.llamaindex.ai/python/cloud/llamaextract/getting_started/api/`
- Private run URL provided by user (not directly accessible in this environment):  
  `https://cloud.llamaindex.ai/project/fac76828-a804-4b04-aef8-fbe74da38468/extract?run_id=1c8a1094-499b-4b38-8d9f-c571d34108eb`
