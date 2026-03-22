# Prompt: Build a Databricks DOJ Data Migration Solution

## Overview
Build a complete Databricks-based data migration solution for a Department of Justice (DOJ) environment. The solution demonstrates AI-assisted schema mapping, medallion architecture pipelines, entity deduplication, and automated loading into a migration harness targeting Azure SQL / Dataverse (Justice Nexus). Everything must be deployable on **Azure Government (Azure Gov)** using Azure-native Databricks and Azure SQL patterns.

---

## Context & Source Systems

Three disparate source systems with different table/column naming conventions:

| System | Type | Example Tables | Notes |
|---|---|---|---|
| **System A – LegacyCase** | SQL Server | `tbl_Defendant`, `tbl_Case`, `tbl_Event` | Legacy case management |
| **System B – OpenJustice** | Public web portal (CA DOJ) | `arrests`, `arrest_dispositions`, `crimes_clearances` | California DOJ OpenJustice open data portal (`openjustice.doj.ca.gov/data`) — publicly downloadable CSVs with abbreviated field names (e.g., `F_DRUGOFF`, `F_SEXOFF`, `F_TOTAL`, `AGE_GROUP`) that intentionally stress-test the LLM semantic mapping phase |
| **System C – AdHocExports** | Excel / CSV | `Client`, `Incident`, `Lookup_*` | Ad-hoc exports to ADLS |

**Unity Catalog** must be used for all datasets — single place for permissions, auditing, and lineage (critical for cross-agency work and legal traceability).

---

## Step 1 – AI-Assisted Schema Mapping

### 1.1 Ingestion (Bronze Layer)
- Use Databricks ingestion pipelines (Auto Loader) to land all 3 sources into **bronze Delta tables**
- Support connectors: SQL Server (JDBC), HTTP/web download (OpenJustice public CSVs via URL), file-based (ADLS)
- Auto Loader must track schema changes automatically
- Bronze tables are **never modified** after landing

### 1.2 Destination Staging Schema
The target is an Azure SQL staging schema that mirrors the migration harness for Dataverse. Key staging tables:
```
Stg_Case
Stg_Contact
Stg_Participant
Stg_Code_Jurisdiction
Stg_Code_CaseType
Stg_Code_EventType
```

### 1.3 AI-Proposed Mapping (Core Feature)
Run an AI/LLM analysis over the three source schemas + destination staging schema to produce a **JSON mapping artifact**:

```json
{
  "mappings": [
    {
      "system": "LegacyCase",
      "source_table": "tbl_Defendant",
      "source_column": "DefName",
      "maps_to": "Stg_Contact.RaceCode",
      "confidence": 0.92,
      "rationale": "Name field used in case party context"
    },
    {
      "system": "OpenJustice",
      "source_table": "arrests",
      "source_column": "RACE",
      "maps_to": "Stg_Contact.RaceCode",
      "confidence": 0.88,
      "rationale": "Race field from CA DOJ OpenJustice arrests dataset — maps to contact race code"
    },
    {
      "system": "LegacyCase",
      "source_table": "tbl_Case",
      "source_column": "JurisCd",
      "maps_to": "Stg_Code_Jurisdiction.JurisdictionCode",
      "confidence": 0.95,
      "rationale": "Abbreviated jurisdiction code — semantic match on name pattern"
    }
  ]
}
```

Use a **two-phase approach** where structured profiling feeds the LLM as evidence — the LLM does not run in parallel with heuristics, it reasons *on top of* them:

**Phase 1 — Metadata Profiling (code/SQL, deterministic)**

Run a profiling pass over every source column and output structured metadata. This is auditable, repeatable, and produces no AI judgment — just facts:
- Data type and length distribution
- Null rate, uniqueness ratio, value cardinality
- Sample values and regex pattern detection (dates, codes, IDs, names, phone numbers)
- Foreign key candidates (columns whose values are subsets of another table's PK)
- Usage frequency (rows with non-null values over time, if timestamps are available)

Store profiling output as a structured JSON or Delta table, e.g.:
```json
{
  "system": "LegacyCase",
  "table": "tbl_Defendant",
  "column": "JurisCd",
  "dtype": "VARCHAR(10)",
  "null_rate": 0.02,
  "uniqueness": 0.04,
  "top_values": ["LASC", "SFSC", "OC"],
  "pattern": "2-4 uppercase alpha chars — likely a code field",
  "fk_candidate": "CodeTable.Code"
}
```

**Phase 2 — LLM Reasoning (interpretive, semantic)**

Pass the full profiling metadata *plus* column/table names and sample values to the LLM. The LLM's job is to:
- Interpret abbreviations and naming conventions (e.g., `JurisCd` → `JurisdictionCode`, `DefName` → `FullName` in a defendant context)
- Weigh the heuristic evidence to produce a confidence score
- Generate a human-readable rationale that cites both semantic and structural signals
- Flag ambiguous cases where profiling signals conflict with naming signals

The LLM output is grounded in the Phase 1 evidence, making it auditable for DOJ review — SMEs can see *both* the numerical evidence and the LLM's reasoning for every proposed mapping.

> **Why this split matters**: heuristic computation is deterministic and defensible in a legal context; the LLM adds interpretive judgment that would otherwise require a human analyst. Together they replace weeks of manual spreadsheet analysis with a reviewable, evidence-backed proposal.

### 1.4 SME Review UI
Build a simple **Databricks notebook or widget UI** where DOJ Subject Matter Experts (SMEs) can:
- Review each proposed mapping
- Approve / Reject / Edit mappings
- Add notes/rationale
- Export the final approved mapping JSON

**Key message**: *"This replaces thousands of hours of manual spreadsheet mapping with an AI-first proposal that your team validates."*

---

## Step 2 – Normalization & Deduplication

### 2.1 Medallion Architecture
Implement Bronze → Silver → Gold pipeline:

| Layer | Purpose |
|---|---|
| **Bronze** | Raw ingested data, never modified, full history |
| **Silver** | Cleaned, typed, quality rules applied; code tables separated from facts |
| **Gold** | Conformed, deduplicated entities aligned to staging schema |

Pattern must be reusable across all ~30 systems.

### 2.2 Code Table Unification (Natural Language / Genie)
Use Databricks Genie (natural language queries over silver layer) to:
- Identify all tables that appear to be code/lookup tables across all 3 systems
- Find overlapping code value sets (e.g., jurisdiction codes, case type codes)
- Detect columns/tables with 0% or very low usage over recent years

Produce a **normalized code table output**, e.g.:
```
Stg_Code_Jurisdiction:
  LegacyCase.JurisCd = "LASC"  →  JurisdictionCode = "LA_SUPER_CT"
  OpenJustice.COUNTY = "Los Angeles"   →  JurisdictionCode = "LA_SUPER_CT"
  AdHocExports.Court = "Los Angeles Superior" → JurisdictionCode = "LA_SUPER_CT"
```

### 2.3 Entity Resolution – Contacts
Focus on **Contact/Participant deduplication** (largest entity bucket).

Build a dedupe workflow:
- Normalize names (uppercase, strip punctuation, handle nicknames)
- Match on: normalized name + DOB + SSN/ID (where present) + fuzzy address/phone
- Generate ML-based similarity score for candidate pairs
- Show candidate groups for SME review:

```
Candidate Group #4821:
  - "Chris Jones"       | DOB: 1982-04-10 | System A | ID: 10042
  - "Christopher Jones" | DOB: 1982-04-10 | System B | ID: 88231
  - "C. Jones"          | DOB: 1982-04-10 | System C | ID: JNS-99
  Similarity Score: 0.97 | Recommendation: MERGE → Stg_Contact ID: NEW-4821
```

**Key message**: *"SMEs do higher-value review, not raw searching — AI does the candidate generation and scoring."*

---

## Step 3 – Populate the Migration Harness

### 3.1 Staging Schema Load
- Map **Gold Delta tables** → **Azure SQL staging schema** using the approved JSON mapping artifact from Step 1
- Use JDBC / SQL Warehouse connector
- Maintain 1:1 or many:1 relationships as defined in the mapping

### 3.2 Automated & Parameterized Load Job
Build a Databricks job with parameters:
- `load_mode`: `full` or `incremental`
- `source_system`: `all` or specific system name
- `mapping_version`: reference to approved mapping artifact version

Job steps:
1. Read Gold Delta tables
2. Apply approved field mappings + transformations
3. Apply code table substitutions
4. Insert/upsert into Azure SQL staging tables
5. Log row counts, errors, and lineage to Unity Catalog

**Key message**: *"Once mappings and rules are approved, moving data into the migration harness is just pressing 'Run' — not four months of ad-hoc scripts."*

---

## Governance & Azure Gov Requirements

- **Unity Catalog**: All datasets registered with column-level lineage, access controls per agency/role, full audit log
- **Azure Government**: All components must use Azure Gov-compatible services:
  - Azure Databricks (Azure Gov)
  - Azure Data Lake Storage Gen2
  - Azure SQL (Azure Gov)
  - No preview features that lack Azure Gov parity
- **Data security**: PII fields (SSN, DOB, name) must be tagged in Unity Catalog and masked for non-privileged users
- **Lineage tracing**: Every Gold record must trace back to its Bronze source for legal defensibility

---

## Step 4 – Excel Upload & Ingestion Status App

### 4.1 Overview
Build a **web application** (React + FastAPI or Databricks Apps) that allows DOJ staff to upload Excel files directly, monitor ingestion progress in real time, and triage any reconciliation issues that require human intervention — without needing access to Databricks notebooks.

### 4.2 File Upload Interface
- Drag-and-drop or click-to-browse file uploader accepting `.xlsx` and `.csv` files
- Allow multi-file upload (one per source system, or batch)
- On upload, user selects which **source system** the file belongs to: `LegacyCase`, `OpenJustice`, or `AdHocExports`
- File validation before submission:
  - Check file extension and MIME type
  - Preview first 5 rows and detected column headers
  - Warn if expected columns are missing based on known schema for that system
- Each uploaded file is assigned a unique **Job ID** and timestamped

### 4.3 Ingestion Status Dashboard
Display a live status board showing all uploaded files and their pipeline progress:

| Job ID | File Name | System | Uploaded | Stage | Status | Rows | Issues |
|---|---|---|---|---|---|---|---|
| JOB-001 | defendants_q1.xlsx | LegacyCase | 2 min ago | Bronze → Silver | ✅ Complete | 4,821 | 0 |
| JOB-002 | arrests_2022.csv | OpenJustice | 5 min ago | Silver → Gold | ⚠️ Review Needed | 3,102 | 7 |
| JOB-003 | clients_2024.xlsx | AdHocExports | 12 min ago | Ingesting | 🔄 Running | 1,890 | — |

Pipeline stages to display with progress indicators:
1. **Uploaded** — File received and validated
2. **Bronze** — Landed in Delta bronze table
3. **Silver** — Cleaned, typed, quality rules applied
4. **Mapping** — AI schema mapping applied
5. **Gold** — Conformed and deduplicated
6. **Staged** — Loaded into Azure SQL staging
7. **Complete** / **Failed** / **Needs Review**

### 4.4 Reconciliation & Intervention Queue
When the pipeline flags issues that require human intervention, surface them in a dedicated **Review Queue** panel:

**Types of issues to flag:**

1. **Low-confidence mappings** (confidence < 0.75)
   - Show: source column, proposed target, confidence score, rationale
   - Actions: Approve / Reject / Reassign to different target column

2. **Duplicate contact candidates** requiring merge decision
   - Show: candidate group with names, DOB, source system, IDs, similarity score
   - Actions: Merge All / Keep Separate / Merge Selected / Flag for Supervisor

3. **Unresolved code values** (source code has no mapping in normalized code table)
   - Show: source value, source system, field name, suggested normalized value
   - Actions: Accept suggestion / Map to existing code / Create new code value

4. **Schema drift** (uploaded file has new or renamed columns vs. known schema)
   - Show: new/missing columns detected vs. expected schema
   - Actions: Map new column / Mark as ignore / Update schema definition

**Review Queue UI requirements:**
- Badge count on tab showing number of items needing attention
- Filter by: issue type, source system, job ID, assigned reviewer
- Each item shows full context (file, table, column, sample data values)
- Bulk approve for high-confidence items
- Audit trail: every decision logged with reviewer name, timestamp, and note
- Email/notification trigger when queue items are assigned or resolved

### 4.5 Summary & Reconciliation Report
After all pipeline stages complete, generate a **per-job reconciliation report**:
- Total rows ingested vs. rows in source file (reconciliation count)
- Rows successfully mapped to staging schema
- Rows rejected / held pending review
- Mapping coverage % by destination table
- Duplicate contacts identified and resolved
- Download report as PDF or Excel

### 4.6 Technical Requirements for the App
- **Frontend**: React with clear status indicators (progress bars, color-coded badges)
- **Backend**: FastAPI or Databricks Apps endpoint that:
  - Accepts file upload and writes to ADLS landing zone
  - Triggers Auto Loader / Databricks job via REST API
  - Polls job status and streams updates to frontend
  - Exposes reconciliation issues from a Delta table written by the pipeline
- **Auth**: Azure AD / Entra ID SSO (Azure Gov compatible)
- **Deployment**: Containerized (Docker) for deployment on Azure Gov App Service or as a Databricks App
- **Polling interval**: Status updates every 10 seconds while job is running
- **Accessibility**: WCAG 2.1 AA compliant (government requirement)

---

## Step 5 – Interactive Pipeline Flow Visualization App

### 5.1 Overview
Build a **standalone interactive pipeline flow app** (React) that gives DOJ staff and supervisors a visual, real-time map of the entire data migration pipeline. Users can see all active jobs flowing through the medallion stages simultaneously, click any stage node to drill into its status, and immediately spot where bottlenecks or interventions are occurring — without needing to interpret logs or tables.

### 5.2 Pipeline Flow Canvas
Render the full end-to-end pipeline as an **interactive node graph** with the following stage nodes connected by animated flow lines:

```
[UPLOAD] ──► [BRONZE] ──► [SILVER] ──► [MAPPING] ──► [GOLD] ──► [STAGING] ──► [COMPLETE]
```

- Each node displays:
  - Stage name and icon
  - Count of jobs currently at that stage
  - Aggregate status color: green (all clear), amber (running), red (review needed), grey (idle)
- Animated flow lines between nodes show data moving through the pipeline:
  - Particle/dot animations along the connector lines representing active job traffic
  - Line color reflects the status of jobs in transit between stages
  - Line thickness scales with the number of concurrent jobs
- Each source system (`LegacyCase`, `OpenJustice`, `AdHocExports`) is represented by a distinct color track so users can visually follow a specific system's jobs through the pipeline

### 5.3 Stage Node Drill-Down
Clicking any stage node opens a **detail panel** (side drawer or modal) showing:

- **Stage summary**: jobs in this stage, average time spent, SLA status
- **Per-job status list**: Job ID, file name, source system, rows processed, time in stage, status badge
- **Stage-specific metrics**:
  - *Bronze*: rows landed, schema detected, Auto Loader lag
  - *Silver*: rows cleaned, rows rejected, quality rule failures
  - *Mapping*: columns mapped, avg confidence score, low-confidence count
  - *Gold*: duplicates resolved, entities merged, rows conformed
  - *Staging*: rows inserted, upserts, JDBC errors
- **Issues at this stage**: list of flagged items requiring review, each with a direct link to the reconciliation queue
- **Stage logs**: last 20 log lines with timestamp, level (INFO / WARN / ERROR), and message
- **Action buttons**:
  - Retry failed job at this stage
  - Skip stage (with approval override)
  - View full job lineage

### 5.4 Job-Level Trace View
Clicking a specific Job ID from any drill-down panel opens a **full job trace timeline**:

- Horizontal timeline bar showing time spent in each stage
- Current active stage highlighted with a pulsing indicator
- Each stage segment is color-coded by outcome (complete / running / blocked / failed)
- Tooltip on hover shows: start time, end time, duration, row count, issue count
- Sidebar shows all reconciliation issues for this job, grouped by stage

### 5.5 Global Status Header
Persistent header bar across the top of the app showing:

- Total active jobs
- Total rows in flight across all stages
- Global alert count (items needing review, across all jobs and stages)
- Last pipeline refresh timestamp
- Quick-filter buttons: All Jobs / Active Only / Needs Review / Failed
- System filter toggles: `LegacyCase` / `OpenJustice` / `AdHocExports`

### 5.6 Visual Design Requirements
- **Dark theme** with high-contrast status colors appropriate for a government operations center
- Stage nodes rendered as distinct visual blocks (not generic circles) with clear iconography
- Animated flow connectors using CSS or SVG path animations — not static arrows
- Color coding must be accessible (WCAG AA contrast ratios) and not rely solely on color (use icons + labels alongside color)
- Responsive layout that works at 1280px wide minimum (operator workstation standard)
- Auto-refresh every 10 seconds; manual refresh button always visible
- No external map or charting library dependencies — use SVG and CSS for the flow canvas

### 5.7 Technical Requirements
- **Framework**: React (single `.jsx` file for standalone delivery, or split components for full app integration)
- **State**: All pipeline state managed in React state; polling via `setInterval` against the same FastAPI backend used by App 1 (Step 4)
- **API endpoints consumed**:
  - `GET /api/jobs` — all jobs with current stage and status
  - `GET /api/stages/summary` — aggregate counts and status per stage
  - `GET /api/jobs/{job_id}/trace` — full stage timeline for a single job
  - `GET /api/stages/{stage_id}/logs` — recent log lines for a stage
- **Embeddable**: the flow canvas component must be exportable as a standalone component that can be embedded inside the App 1 dashboard (Step 4) as a tab

---

## Deliverables to Generate

Please produce the following:

**Databricks Pipelines:**
1. **`/notebooks/01_ingest_bronze.py`** — Auto Loader ingestion for all 3 sources into bronze Delta tables; includes HTTP download step for OpenJustice public CSVs from `data-openjustice.doj.ca.gov`
2. **`/notebooks/02a_metadata_profiling.py`** — Phase 1: deterministic profiling pass over all source columns (data type, null rate, uniqueness, pattern detection, FK candidates); outputs structured profiling Delta table
3. **`/notebooks/02b_llm_schema_mapping.py`** — Phase 2: passes profiling metadata + column names + sample values to LLM; LLM reasons over evidence to produce JSON mapping artifact with confidence scores and cited rationale
3. **`/notebooks/03_sme_review_widget.py`** — Interactive widget for SME mapping approval/rejection
4. **`/notebooks/04_bronze_to_silver.py`** — Cleaning, typing, code table separation pipelines
5. **`/notebooks/05_entity_resolution.py`** — Contact deduplication workflow with scoring and SME review
6. **`/notebooks/06_silver_to_gold.py`** — Conformed gold layer aligned to staging schema
7. **`/notebooks/07_load_staging.py`** — Parameterized job to load Gold → Azure SQL staging

**Upload & Status App:**
8. **`/app/frontend/src/App.jsx`** — Main React app shell with routing
9. **`/app/frontend/src/components/FileUploader.jsx`** — Drag-and-drop Excel/CSV uploader with system selector and column preview
10. **`/app/frontend/src/components/IngestionStatusBoard.jsx`** — Live pipeline status dashboard with stage progress indicators
11. **`/app/frontend/src/components/ReconciliationQueue.jsx`** — Review queue for low-confidence mappings, duplicate contacts, unmapped codes, and schema drift
12. **`/app/frontend/src/components/ReconciliationReport.jsx`** — Per-job summary report with row counts and mapping coverage
13. **`/app/backend/main.py`** — FastAPI backend: file upload endpoint, Databricks job trigger, status polling, reconciliation issue API
14. **`/app/backend/databricks_client.py`** — Databricks REST API wrapper (job runs, ADLS file write, Delta table reads)
15. **`/app/backend/models.py`** — Pydantic models for job status, reconciliation issues, mapping decisions
16. **`/app/Dockerfile`** — Container definition for Azure Gov App Service deployment
17. **`/app/docker-compose.yml`** — Local dev compose with frontend + backend

**Pipeline Flow Visualization App:**
18. **`/app/frontend/src/components/PipelineFlowCanvas.jsx`** — Interactive SVG node graph with animated flow connectors and per-system color tracks
19. **`/app/frontend/src/components/StageDetailPanel.jsx`** — Drill-down side drawer for any stage node: metrics, job list, issues, logs, and action buttons
20. **`/app/frontend/src/components/JobTraceView.jsx`** — Full horizontal timeline trace for a single job across all stages
21. **`/app/frontend/src/components/GlobalStatusHeader.jsx`** — Persistent header with active job counts, alert badge, and filter/system toggles
22. **`/app/backend/routes/pipeline.py`** — FastAPI routes: `/api/stages/summary`, `/api/jobs/{job_id}/trace`, `/api/stages/{stage_id}/logs`

**Supporting Files:**
18. **`/sql/staging_schema.sql`** — Azure SQL DDL for the migration harness staging tables
19. **`/sql/reconciliation_issues_table.sql`** — Delta table DDL for pipeline-flagged issues consumed by the app
20. **`/config/mapping_schema.json`** — JSON schema definition for the mapping artifact
21. **`/config/sample_mappings.json`** — Sample approved mappings for all 3 source systems
22. **`README.md`** — Setup instructions, architecture diagram (ASCII), and how to run end-to-end

---

## Tech Stack

| Component | Technology |
|---|---|
| Compute | Azure Databricks (Azure Gov) |
| Storage | Azure Data Lake Storage Gen2 |
| Table format | Delta Lake |
| Catalog | Unity Catalog |
| Staging target | Azure SQL Database |
| Ingestion | Auto Loader, JDBC connectors |
| AI/LLM | Databricks AI / external LLM API call |
| NL Queries | Databricks Genie |
| Language | Python (PySpark), SQL |
| App Frontend | React, Tailwind CSS |
| App Backend | FastAPI (Python) |
| App Auth | Azure AD / Entra ID SSO |
| App Deployment | Docker → Azure Gov App Service or Databricks Apps |

---

## Success Criteria

- [ ] All 3 sources land in bronze with zero manual transformation
- [ ] AI mapping covers ≥ 80% of destination columns with confidence ≥ 0.75
- [ ] SME can approve/reject/edit mappings in under 2 minutes per table
- [ ] Deduplication identifies candidate contact pairs across systems
- [ ] Full Bronze → Gold → Staging pipeline runs end-to-end with one job trigger
- [ ] All datasets visible in Unity Catalog with lineage
- [ ] Solution is rerunnable / idempotent for iterative development
- [ ] All patterns deployable on Azure Government
- [ ] App accepts Excel/CSV uploads and triggers Databricks pipeline automatically
- [ ] Ingestion status dashboard reflects live pipeline stage within 10 seconds
- [ ] Reconciliation queue surfaces all low-confidence mappings, duplicate candidates, unmapped codes, and schema drift issues
- [ ] Every SME review decision is logged with reviewer, timestamp, and note
- [ ] Per-job reconciliation report shows row-level counts and mapping coverage %
- [ ] App is deployable on Azure Gov App Service with Azure AD SSO
- [ ] App meets WCAG 2.1 AA accessibility standards
- [ ] Pipeline flow canvas renders all 7 stage nodes with animated connectors and live job counts
- [ ] Each source system's jobs are visually distinguishable by color track through the flow
- [ ] Clicking any stage node opens a drill-down panel with per-job metrics, issues, and logs
- [ ] Job trace view shows a full horizontal timeline with time-in-stage for each job
- [ ] Global status header shows live alert count and supports filtering by system and status
- [ ] Flow canvas auto-refreshes every 10 seconds and is embeddable as a tab in App 1
- [ ] All status colors meet WCAG AA contrast ratios and include icon + label alongside color
