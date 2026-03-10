# DOJ Data Migration Platform

A production-grade data migration solution for the Department of Justice, built on Databricks. Migrates criminal justice case data from three legacy source systems into a unified Azure SQL staging database via a medallion architecture with AI-assisted schema mapping and an interactive operator dashboard.


## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  SOURCE SYSTEMS                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                             │
│  │  LegacyCase  │  │ OpenJustice  │  │ AdHocExports │                             │
│  │  (SQL Server │  │  (HTTP CSV   │  │  (ADLS XLSX  │                             │
│  │   JDBC)      │  │   download)  │  │   files)     │                             │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                             │
└─────────┼─────────────────┼─────────────────┼───────────────────────────────────── ┘
          │                 │                 │
          ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  AZURE DATABRICKS (Government Cloud)                                                │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  BRONZE LAYER  (01_ingest_bronze.py)                                        │   │
│  │  Auto Loader + JDBC reads → raw Delta tables with _ingest_timestamp         │   │
│  │  PII column tagging via Unity Catalog tags                                  │   │
│  └──────────────────────────────────┬──────────────────────────────────────────┘   │
│                                     │                                               │
│  ┌──────────────────────────────────▼──────────────────────────────────────────┐   │
│  │  METADATA PROFILING  (02a_metadata_profiling.py)                            │   │
│  │  Null rates, cardinality, regex pattern detection, FK candidate scoring     │   │
│  │  → bronze.column_profiles Delta table                                        │   │
│  └──────────────────────────────────┬──────────────────────────────────────────┘   │
│                                     │                                               │
│  ┌──────────────────────────────────▼──────────────────────────────────────────┐   │
│  │  LLM SCHEMA MAPPING  (02b_llm_schema_mapping.py)                            │   │
│  │  Meta-Llama-3-3-70B-Instruct via Databricks serving endpoint               │   │
│  │  Confidence scores + rationale → bronze.schema_mappings                    │   │
│  └──────────────────────────────────┬──────────────────────────────────────────┘   │
│                                     │                                               │
│  ┌──────────────────────────────────▼──────────────────────────────────────────┐   │
│  │  SME REVIEW  (03_sme_review_widget.py)                                      │   │
│  │  ipywidgets UI: Approve / Reject / Edit mappings with audit trail           │   │
│  │  Exports approved_vYYYYMMDDTHHMMSSZ.json to ADLS                           │   │
│  └──────────────────────────────────┬──────────────────────────────────────────┘   │
│                                     │                                               │
│  ┌──────────────────────────────────▼──────────────────────────────────────────┐   │
│  │  BRONZE → SILVER  (04_bronze_to_silver.py)                                  │   │
│  │  DQ assertions, date standardisation, PII normalisation                     │   │
│  │  Rejected rows → *_quarantine tables; metrics → pipeline_metrics            │   │
│  └──────────────────────────────────┬──────────────────────────────────────────┘   │
│                                     │                                               │
│  ┌──────────────────────────────────▼──────────────────────────────────────────┐   │
│  │  ENTITY RESOLUTION  (05_entity_resolution.py)                               │   │
│  │  Jaro-Winkler + Jaccard blocking; connected-components dedup                │   │
│  │  → gold.contact_master with golden_id                                       │   │
│  └──────────────────────────────────┬──────────────────────────────────────────┘   │
│                                     │                                               │
│  ┌──────────────────────────────────▼──────────────────────────────────────────┐   │
│  │  SILVER → GOLD → STAGING  (06_silver_to_gold.py)                            │   │
│  │  Applies approved mappings; writes Stg_Case, Stg_Contact, Stg_Participant   │   │
│  │  JDBC → Azure SQL Government (usgovcloudapi.net)                            │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────┐
│  AZURE SQL (Government)     │
│  dbo.Stg_Case               │
│  dbo.Stg_Contact (SSN_Hash) │
│  dbo.Stg_Participant        │
│  dbo.Stg_Code_*             │
└─────────────────────────────┘

  Operator Dashboard (React + FastAPI)
  ├── PipelineFlowCanvas  — animated SVG node graph
  ├── IngestionStatusBoard — per-job stage progress
  ├── FileUploader          — drag-and-drop + validation
  ├── ReconciliationQueue   — mapping review UI
  ├── ReconciliationReport  — PDF/CSV export
  ├── JobTraceView          — proportional timeline
  └── StageDetailPanel      — drill-down drawer
```

---

## Data Sources

Three source systems feed the pipeline. All land in `oregon_doj_demo_catalog` under the medallion layers.

### 1. LegacyCase (primary criminal justice system)

Raw data from the legacy SQL Server case management database.

| Table | Description |
|-------|-------------|
| `bronze.legacycase_tbl_defendant` | One row per defendant per case. `DefendantID` (`DEF-000001` format) is the primary key. Contains demographics (DOB, race, gender), charge code and description, court/judge assignment, arraignment and disposition dates, sentence, and public defender flag. |
| `bronze.legacycase_tbl_case` | Case-level records keyed by `CaseID`. Contains filing date, case type, court, judge, and status. Joined to defendants via `CASE_ID`. |
| `bronze.legacycase_tbl_event` | Court events and hearings linked to cases by `CASE_ID`. Contains event type, date, and description. |

### 2. AdHocExports (diversion & program data)

Periodic manual XLSX exports from the DOJ's alternative-sentencing and diversion program database.

| Table | Description |
|-------|-------------|
| `bronze.adhoc_client` | Client enrollment records. `DEFENDANT_REF` matches `DefendantID` from LegacyCase. Contains program name, status, county, enrollment/exit dates, age at enrollment, and risk level. |

### 3. OpenJustice (statewide aggregate statistics)

Oregon DOJ's public arrest statistics. Contains no individual identifiers — used for contextual benchmarking only.

| Table | Description |
|-------|-------------|
| `bronze.openjustice_arrests` | Aggregate arrest counts by `YEAR` and `CHARGE_CATEGORY`. Used in Case Intelligence to show statewide trends for a defendant's offense type. |

### Silver Quality Tables

Produced by the pipeline's data quality and deduplication stages.

| Table | Description |
|-------|-------------|
| `silver.duplicate_contacts` | Detected duplicate defendant groups. Each row has a `duplicate_group_id`, `severity` (CRITICAL / HIGH / MEDIUM), `total_records`, `all_defendant_ids` (array<string>), and date range. |
| `silver.low_confidence_mappings` | Field mappings where automated confidence fell below 0.85. Surfaced in the Reconciliation Queue for SME review. |
| `silver.review_decisions` | Audit trail of SME approve/reject/escalate decisions. Keyed by `issue_key` (e.g. `duplicate::group_id`). |

---

## Deployed App — Feature Guide

The Databricks App at https://doj-migration-monitor-7474651760554125.aws.databricksapps.com provides the following views:

| View | Description |
|------|-------------|
| **Pipeline Dashboard** | Stage cards (Upload → Bronze → Silver → Mapping → Gold → Staging) with job counts, row throughput, and average duration. Animated medallion flow diagram. |
| **Ingestion Status Board** | Real-time job table: current stage, status badge, rows processed, and issue count. Pipeline trigger button kicks off a new Databricks job run. |
| **File Upload** | Drag-and-drop `.xlsx` / `.csv` upload. File is staged to DBFS (`/FileStore/doj-uploads`) and the orchestration job (`99898107790094`) is triggered automatically. |
| **Reconciliation Queue** | SME review interface for low-confidence mappings and unmapped codes. Reviewers approve, reject, or override proposed values; decisions persist to `silver.review_decisions`. |
| **Reconciliation Report** | Summary of all review decisions — approved counts, rejection rates, and escalations by source system and issue type. |
| **Data Quality Board** | Duplicate contact groups from `silver.duplicate_contacts` ranked by severity. Reviewers dismiss resolved groups; `ARRAY_CONTAINS` is used to match defendant IDs in the array column. |
| **Stage Detail / Job Trace** | Drill-down timeline for a single job showing per-stage start/end times, row throughput, and structured log entries. |
| **Case Intelligence** | 360° defendant profile search (see below). |

### Case Intelligence

Unified search across all three data sources. Supports two identifier spaces:

- **Bronze defendants** (`DEF-xxxxxx`) — full profile including demographics, cases, court events, program enrollments, data quality flags, and statewide charge-category arrest context.
- **Duplicate groups** — people found only in `silver.duplicate_contacts` (identified during deduplication, not yet resolved to a canonical bronze ID) are shown with an orange **Duplicate Group** badge and their group metadata.

Search queries both sources with a UNION and resolves profiles automatically based on the ID format of the selected result.

---

## App Configuration (`app/app.yaml`)

| Variable | Value | Purpose |
|----------|-------|---------|
| `DATABRICKS_HOST` | `https://fevm-oregon-doj-demo.cloud.databricks.com` | Workspace URL |
| `CATALOG` | `oregon_doj_demo_catalog` | Unity Catalog for all queries |
| `WAREHOUSE_ID` | `a9e6255f0e48dafa` | Serverless SQL warehouse |
| `DATABRICKS_JOB_ID` | `99898107790094` | Migration orchestration job |
| `ADLS_UPLOAD_PATH` | `/FileStore/doj-uploads` | DBFS staging path |
| `DEMO_MODE` | `false` | Set `true` to use mock data |

### Service Principal Permissions

After deploying, grant the app SP access to Unity Catalog:

```sql
GRANT USE CATALOG ON CATALOG oregon_doj_demo_catalog TO `<sp_client_id>`;
GRANT USE SCHEMA ON SCHEMA oregon_doj_demo_catalog.bronze TO `<sp_client_id>`;
GRANT SELECT ON SCHEMA oregon_doj_demo_catalog.bronze TO `<sp_client_id>`;
GRANT USE SCHEMA ON SCHEMA oregon_doj_demo_catalog.silver TO `<sp_client_id>`;
GRANT SELECT ON SCHEMA oregon_doj_demo_catalog.silver TO `<sp_client_id>`;
```

### Deploying Updates

```bash
# Build frontend
cd app/frontend && npm run build

# Upload backend + frontend to workspace
databricks workspace import \
  "/Users/<email>/doj-migration-monitor-src/backend/main.py" \
  --file app/backend/main.py --format=RAW --overwrite \
  --profile=fe-vm-oregon-doj-demo

databricks workspace import-dir app/frontend/dist \
  "/Users/<email>/doj-migration-monitor-src/frontend/dist" \
  --overwrite --profile=fe-vm-oregon-doj-demo

# Deploy
databricks apps deploy doj-migration-monitor \
  --source-code-path "/Workspace/Users/<email>/doj-migration-monitor-src" \
  --profile=fe-vm-oregon-doj-demo
```

---

## Repository Structure

```
doj/
├── notebooks/
│   ├── 01_ingest_bronze.py         # Bronze ingestion (JDBC, HTTP, Auto Loader)
│   ├── 02a_metadata_profiling.py   # Column profiling → column_profiles table
│   ├── 02b_llm_schema_mapping.py   # LLM-assisted schema mapping
│   ├── 03_sme_review_widget.py     # ipywidgets SME review interface
│   ├── 04_bronze_to_silver.py      # Bronze→Silver DQ + normalisation
│   ├── 05_entity_resolution.py     # Deduplication + golden record creation
│   └── 06_silver_to_gold.py        # Silver→Gold + JDBC→Azure SQL staging
│
├── app/
│   ├── Dockerfile                  # Multi-stage: Node 20 build + Python 3.11 runtime
│   ├── docker-compose.yml          # Backend + frontend with hot-reload
│   ├── backend/
│   │   ├── main.py                 # FastAPI app, lifespan, background poller
│   │   ├── models.py               # Pydantic v2 models
│   │   ├── databricks_client.py    # Async Databricks REST client
│   │   ├── requirements.txt        # Pinned production deps
│   │   └── routes/
│   │       └── pipeline.py         # /api/stages, /api/jobs, /api/reconciliation
│   └── frontend/
│       ├── package.json            # Vite + React 18 + Tailwind
│       ├── vite.config.js          # /api proxy, vendor chunking
│       ├── tailwind.config.js      # doj-* dark theme palette + 8 animations
│       └── src/
│           ├── App.jsx             # Router, FilterContext, sidebar nav
│           ├── main.jsx
│           ├── index.css
│           └── components/
│               ├── GlobalStatusHeader.jsx    # Live polling, status filters
│               ├── FileUploader.jsx          # Drag-drop + column validation
│               ├── IngestionStatusBoard.jsx  # Per-job stage progress bars
│               ├── ReconciliationQueue.jsx   # 4-tab mapping review
│               ├── ReconciliationReport.jsx  # PDF/CSV export report
│               ├── PipelineFlowCanvas.jsx    # Animated SVG pipeline graph
│               ├── StageDetailPanel.jsx      # Slide-in drill-down drawer
│               ├── JobTraceView.jsx          # Proportional timeline trace
│               ├── DataQualityBoard.jsx      # Duplicate contacts & quality flags
│               └── CaseIntelligence.jsx      # 360° defendant profile search
│
├── sql/
│   ├── staging_schema.sql              # Azure SQL DDL (Stg_Case, Stg_Contact, ...)
│   └── reconciliation_issues_table.sql # Unity Catalog Delta table for issues
│
└── config/
    ├── mapping_schema.json     # JSON Schema draft-07 for mapping artifact
    └── sample_mappings.json    # 20 pre-approved sample mappings (3 systems)
```

## Prerequisites

| Component | Requirement |
|-----------|-------------|
| Azure Databricks | Runtime 14.3 LTS+ (Unity Catalog enabled) |
| Azure Databricks | Serverless compute or DBR cluster with `requests`, `openpyxl`, `ipywidgets` |
| Azure ADLS Gen2 | Storage account at `dojstorage.dfs.core.usgovcloudapi.net` |
| Azure SQL | Government Cloud instance with `dbo` schema access |
| Databricks Secrets | Scope `doj-scope` with keys below |
| Model Serving | Endpoint `databricks-meta-llama-3-3-70b-instruct` (or `databricks-claude-sonnet-4-6`) |
| Python | 3.11+ (backend) |
| Node.js | 18+ (frontend build) |

### Required Databricks Secrets (`doj-scope`)

```
legacycase-jdbc-url       — JDBC URL for SQL Server (encrypted)
legacycase-jdbc-user      — Service account username
legacycase-jdbc-password  — Service account password
azure-sql-server          — Azure SQL hostname
azure-sql-database        — Database name
azure-sql-user            — SQL auth username
azure-sql-password        — SQL auth password
llm-endpoint-token        — Databricks token for model serving
```

Create the secrets scope:
```bash
databricks secrets create-scope doj-scope
databricks secrets put-secret doj-scope legacycase-jdbc-url
# ... repeat for each key
```

## Running the Notebooks

### Step 1: Configure Unity Catalog

```sql
CREATE CATALOG IF NOT EXISTS doj_catalog;
CREATE SCHEMA IF NOT EXISTS doj_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS doj_catalog.silver;
CREATE SCHEMA IF NOT EXISTS doj_catalog.gold;
```

### Step 2: Run Notebooks in Order

```
01_ingest_bronze.py       → Reads source systems, writes bronze Delta tables
02a_metadata_profiling.py → Profiles all bronze columns
02b_llm_schema_mapping.py → Generates LLM mapping suggestions
03_sme_review_widget.py   → (Interactive) SME approves/rejects/edits mappings
04_bronze_to_silver.py    → Cleans and standardises data
05_entity_resolution.py   → Deduplicates contacts → gold.contact_master
06_silver_to_gold.py      → Writes gold tables + loads Azure SQL staging
```

Each notebook accepts widgets / `dbutils.widgets`:

| Notebook | Widget | Default |
|----------|--------|---------|
| `01` | `source_system` | `all` (or `LegacyCase`, `OpenJustice`, `AdHocExports`) |
| `01` | `load_mode` | `incremental` (or `full`) |
| `06` | `load_mode` | `incremental` |
| `06` | `source_system` | `all` |
| `06` | `mapping_version` | *(latest approved)* |

### Orchestration (Databricks Workflow)

Create a multi-task workflow in the Databricks UI or via CLI:

```json
{
  "name": "DOJ Migration Pipeline",
  "tasks": [
    { "task_key": "ingest",    "notebook_task": { "notebook_path": "/doj/notebooks/01_ingest_bronze" } },
    { "task_key": "profile",   "depends_on": [{"task_key": "ingest"}],   "notebook_task": { "notebook_path": "/doj/notebooks/02a_metadata_profiling" } },
    { "task_key": "llm_map",   "depends_on": [{"task_key": "profile"}],  "notebook_task": { "notebook_path": "/doj/notebooks/02b_llm_schema_mapping" } },
    { "task_key": "bronze_sil","depends_on": [{"task_key": "llm_map"}],  "notebook_task": { "notebook_path": "/doj/notebooks/04_bronze_to_silver" } },
    { "task_key": "entity_res","depends_on": [{"task_key": "bronze_sil"}],"notebook_task": { "notebook_path": "/doj/notebooks/05_entity_resolution" } },
    { "task_key": "sil_gold",  "depends_on": [{"task_key": "entity_res"}],"notebook_task": { "notebook_path": "/doj/notebooks/06_silver_to_gold" } }
  ]
}
```

> Note: `03_sme_review_widget.py` is interactive and runs between `02b` and `04` — it cannot be automated in a workflow.

## Running the Operator Dashboard

### Local Development

```bash
# 1. Configure environment
cp app/backend/.env.example app/backend/.env
# Edit .env: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_JOB_IDS (JSON)

# 2. Start backend
cd app/backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000

# 3. Start frontend (separate terminal)
cd app/frontend
npm install
npm run dev
# → http://localhost:5173
```

### Docker Compose

```bash
cd app
docker-compose up --build
# Backend:  http://localhost:8000
# Frontend: http://localhost:5173
```

### Azure App Service (Government)

```bash
# Build production image
docker build -t doj-migration-app ./app

# Tag and push to Azure Container Registry
az acr login --name <your-acr>
docker tag doj-migration-app <your-acr>.azurecr.us/doj-migration:latest
docker push <your-acr>.azurecr.us/doj-migration:latest

# Deploy to App Service
az webapp create \
  --resource-group doj-rg \
  --plan doj-plan \
  --name doj-migration-app \
  --deployment-container-image-name <your-acr>.azurecr.us/doj-migration:latest
```

### Required Environment Variables (Backend)

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Workspace URL (e.g. `https://adb-xxx.azuredatabricks.net`) |
| `DATABRICKS_TOKEN` | Personal access token or service principal token |
| `DATABRICKS_JOB_IDS` | JSON map: `{"ingest": 101, "profile": 102, ...}` |
| `DATABRICKS_WAREHOUSE_ID` | SQL warehouse ID for read queries |
| `CATALOG` | Unity Catalog name (default: `doj_catalog`) |
| `DEMO_MODE` | Set `true` to use mock data (no Databricks connection needed) |

## Azure SQL Staging Schema

Run `sql/staging_schema.sql` against your Azure SQL Government instance:

```bash
sqlcmd -S <server>.database.usgovcloudapi.net \
       -d <database> \
       -U <user> -P <password> \
       -i sql/staging_schema.sql
```

Key tables created:
- `dbo.Stg_Case` — case records with `SSN_Hash` (SHA-256), `GoldenId`
- `dbo.Stg_Contact` — deduplicated contacts
- `dbo.Stg_Participant` — case-contact role bridge
- `dbo.Stg_Code_Jurisdiction`, `Stg_Code_CaseType`, `Stg_Code_EventType` — reference tables
- `dbo.MigrationAuditLog` — row-level load audit

## Mapping Artifact Format

The approved mapping JSON (produced by `03_sme_review_widget.py`) follows `config/mapping_schema.json`. Example entry:

```json
{
  "source_system": "LegacyCase",
  "source_table": "tbl_Defendant",
  "source_column": "DefFirstName",
  "target_table": "Stg_Contact",
  "target_column": "FirstName",
  "transform": "UPPER(TRIM({{source}}))",
  "confidence": 0.96,
  "rationale": "Column name and sample values confirm personal first name",
  "status": "APPROVED",
  "reviewer": "jane.smith@doj.gov"
}
```

See `config/sample_mappings.json` for 20 pre-approved examples across all three source systems.

## Data Quality

Each notebook enforces DQ rules and quarantines failing rows:

| Rule | Table | Quarantine |
|------|-------|-----------|
| case_id NOT NULL | tbl_Case | `silver_quarantine.case_record` |
| defendant_id NOT NULL | tbl_Defendant | `silver_quarantine.defendant` |
| admit_date ≤ today | encounters | `silver_quarantine.encounters` |
| age 0–120 | all person tables | per-table quarantine |
| SSN format XXX-XX-XXXX | contacts | `silver_quarantine.contact` |

View rejection rates:
```sql
SELECT source_table, rows_read, rows_written, rows_rejected, rejection_rate
FROM doj_catalog.silver.pipeline_metrics
ORDER BY pipeline_run DESC;
```

## Security Notes

- All PII columns tagged with `pii_type` in Unity Catalog (see `01_ingest_bronze.py`)
- SSN stored as `SSN_Hash` (SHA-256) in Azure SQL staging
- Secrets accessed only via `dbutils.secrets.get()` — never in plaintext
- JDBC connections use `encrypt=true;trustServerCertificate=false`
- Azure SQL connections use TLS 1.2 minimum (Government Cloud default)

## Demo Mode

Set `DEMO_MODE=true` in the backend environment to run the dashboard without a live Databricks connection. All API endpoints return realistic mock data including:
- 5 active jobs across 3 systems at different pipeline stages
- 7 reconciliation issues (mapping conflicts, duplicates, schema drift)
- Animated pipeline flow with particle effects

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `Secret not found in scope doj-scope` | Secrets not created | Run `databricks secrets put-secret ...` |
| `Table not found: doj_catalog.bronze.*` | Catalog/schema not created | Run the `CREATE CATALOG/SCHEMA` SQL above |
| LLM endpoint 429 errors | Rate limiting | Notebook has exponential backoff; wait and retry |
| Entity resolution takes >2 hours | Large dataset | Increase cluster size or reduce blocking threshold |
| Azure SQL JDBC write fails | Network/firewall | Add Databricks NAT IP to Azure SQL firewall |
| Frontend shows "No data" | Backend not running | Check `DATABRICKS_HOST` and token; try `DEMO_MODE=true` |
