-- =============================================================================
-- DOJ Migration Pipeline — Unity Catalog Delta Table DDL
-- Table: doj_catalog.gold.reconciliation_issues
-- =============================================================================
-- Purpose   : Persists every reconciliation issue raised during the mapping
--             and quality-check stages so that SMEs can review and resolve
--             them through the self-service UI.
-- Catalog   : doj_catalog  (Unity Catalog)
-- Schema    : gold          (curated / governed layer)
-- Storage   : Managed Delta table; partitioned for efficient filtering by
--             the most common query predicates: issue_type and status.
-- =============================================================================

-- Unity Catalog requires the catalog to exist before USE can run.
-- In a production pipeline this would be preceded by:
--   CREATE CATALOG IF NOT EXISTS doj_catalog;
--   CREATE SCHEMA IF NOT EXISTS doj_catalog.gold;

USE CATALOG doj_catalog;
USE SCHEMA gold;

-- =============================================================================
-- Drop and recreate (idempotent for CI / initial deploy)
-- Comment out the DROP in production to preserve historical issues.
-- =============================================================================
-- DROP TABLE IF EXISTS doj_catalog.gold.reconciliation_issues;

CREATE TABLE IF NOT EXISTS doj_catalog.gold.reconciliation_issues (

    -- -------------------------------------------------------------------------
    -- Primary identity
    -- -------------------------------------------------------------------------
    issue_id            STRING          NOT NULL
        COMMENT 'UUID v4 — unique identifier for this reconciliation issue.',

    job_id              STRING          NOT NULL
        COMMENT 'Parent migration job identifier (UUID v4).',

    -- -------------------------------------------------------------------------
    -- Classification
    -- -------------------------------------------------------------------------
    issue_type          STRING          NOT NULL
        COMMENT 'Issue category: LOW_CONFIDENCE_MAPPING | DUPLICATE_CONTACT | UNMAPPED_CODE | SCHEMA_DRIFT.',

    source_system       STRING          NOT NULL
        COMMENT 'Originating source system: LEGACY_CASE | OPEN_JUSTICE | AD_HOC_EXPORTS.',

    source_table        STRING          NOT NULL
        COMMENT 'Table name in the source system, e.g. tbl_Defendant.',

    source_column       STRING          NOT NULL
        COMMENT 'Column name in the source system, e.g. RACE_CODE.',

    -- -------------------------------------------------------------------------
    -- Data values
    -- -------------------------------------------------------------------------
    current_value       STRING
        COMMENT 'Raw value as it exists in the source extract.',

    proposed_value      STRING
        COMMENT 'AI / heuristic-proposed target value after transformation.',

    -- -------------------------------------------------------------------------
    -- Confidence and rationale
    -- -------------------------------------------------------------------------
    confidence          DOUBLE          NOT NULL
        COMMENT 'Mapping confidence score in range [0.0, 1.0].',

    rationale           STRING          NOT NULL
        COMMENT 'Human-readable explanation of why the issue was raised.',

    -- -------------------------------------------------------------------------
    -- Lifecycle / review
    -- -------------------------------------------------------------------------
    status              STRING          NOT NULL    DEFAULT 'PENDING'
        COMMENT 'Issue lifecycle: PENDING | APPROVED | REJECTED | ESCALATED.',

    reviewer            STRING
        COMMENT 'Username or UPN of the SME who reviewed the issue.',

    review_timestamp    TIMESTAMP
        COMMENT 'UTC timestamp when the review decision was submitted.',

    reviewer_note       STRING
        COMMENT 'Optional free-text justification from the reviewer.',

    -- -------------------------------------------------------------------------
    -- Pipeline stage context
    -- -------------------------------------------------------------------------
    pipeline_stage      STRING
        COMMENT 'Pipeline stage at which the issue was detected: MAPPING | SILVER | GOLD.',

    record_key          STRING
        COMMENT 'Natural key of the affected record in the source system (for traceability).',

    -- -------------------------------------------------------------------------
    -- Audit / metadata
    -- -------------------------------------------------------------------------
    created_at          TIMESTAMP       NOT NULL    DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'UTC timestamp when the issue was first persisted.',

    updated_at          TIMESTAMP       NOT NULL    DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'UTC timestamp of the most recent update (set by MERGE).',

    mapping_version     STRING
        COMMENT 'Semantic version of the mapping artifact in use when the issue was raised.',

    databricks_run_id   STRING
        COMMENT 'Databricks job run_id that produced this issue (for lineage).'

)
USING DELTA
PARTITIONED BY (issue_type, status)
LOCATION 'abfss://gold@dojdatalake.dfs.core.windows.net/reconciliation_issues'
TBLPROPERTIES (
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'quality.owner'                     = 'doj-migration-team',
    'quality.sensitivity'               = 'SENSITIVE',
    'quality.data_domain'               = 'criminal-justice',
    'pipelines.autoOptimize.zOrderCols' = 'job_id,source_table'
)
COMMENT 'Gold-layer table of all reconciliation issues raised during DOJ data migration. Partitioned by issue_type and status for efficient SME review queries.';

-- =============================================================================
-- Delta table constraints (Unity Catalog CHECK constraints)
-- =============================================================================

ALTER TABLE doj_catalog.gold.reconciliation_issues
    ADD CONSTRAINT chk_confidence_range
    CHECK (confidence >= 0.0 AND confidence <= 1.0);

ALTER TABLE doj_catalog.gold.reconciliation_issues
    ADD CONSTRAINT chk_issue_type_values
    CHECK (issue_type IN (
        'LOW_CONFIDENCE_MAPPING',
        'DUPLICATE_CONTACT',
        'UNMAPPED_CODE',
        'SCHEMA_DRIFT'
    ));

ALTER TABLE doj_catalog.gold.reconciliation_issues
    ADD CONSTRAINT chk_status_values
    CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'ESCALATED'));

ALTER TABLE doj_catalog.gold.reconciliation_issues
    ADD CONSTRAINT chk_source_system_values
    CHECK (source_system IN (
        'LEGACY_CASE',
        'OPEN_JUSTICE',
        'AD_HOC_EXPORTS'
    ));

-- =============================================================================
-- Liquid clustering index for high-cardinality query patterns
-- Uncomment to replace partition-based pruning with adaptive clustering
-- (requires Delta 3.0 / DBR 13.3+)
-- =============================================================================
-- ALTER TABLE doj_catalog.gold.reconciliation_issues
--     CLUSTER BY (job_id, issue_type, status);

-- =============================================================================
-- Row-level security policy (Unity Catalog row filter)
-- Restrict reviewers to only see issues from their assigned source system.
-- Uncomment and adapt after creating the filter function.
-- =============================================================================
-- CREATE FUNCTION IF NOT EXISTS doj_catalog.gold.fn_row_filter_by_system(
--     source_system STRING
-- )
-- RETURN is_member('doj-admins')
--     OR (is_member('legacy-case-reviewers') AND source_system = 'LEGACY_CASE')
--     OR (is_member('open-justice-reviewers') AND source_system = 'OPEN_JUSTICE')
--     OR (is_member('adhoc-reviewers')        AND source_system = 'AD_HOC_EXPORTS');
--
-- ALTER TABLE doj_catalog.gold.reconciliation_issues
--     SET ROW FILTER doj_catalog.gold.fn_row_filter_by_system ON (source_system);

-- =============================================================================
-- Companion view: pending issues only (used by the UI default query)
-- =============================================================================
CREATE OR REPLACE VIEW doj_catalog.gold.v_pending_reconciliation_issues
    COMMENT 'Filtered view of reconciliation issues with status = PENDING.'
AS
SELECT
    issue_id,
    job_id,
    issue_type,
    source_system,
    source_table,
    source_column,
    current_value,
    proposed_value,
    confidence,
    rationale,
    pipeline_stage,
    record_key,
    created_at,
    mapping_version
FROM doj_catalog.gold.reconciliation_issues
WHERE status = 'PENDING'
ORDER BY confidence ASC, created_at ASC;
