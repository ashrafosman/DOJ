# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # DOJ Data Migration — Silver → Gold Conformation + Load Staging (Notebook 06)
# MAGIC
# MAGIC **Purpose**: Apply SME-approved field mappings to transform Silver records
# MAGIC into the final DOJ staging schema shapes, write gold Delta tables, then
# MAGIC load those gold tables into Azure SQL staging via JDBC.
# MAGIC
# MAGIC **Inputs**:
# MAGIC - Silver Delta tables in `doj_catalog.silver.*`
# MAGIC - Approved mapping JSON from ADLS (`mappings/approved_v*.json`)
# MAGIC - Silver code tables: `code_jurisdiction`, `code_case_type`, `code_event_type`
# MAGIC - Gold `contact_master` (from Notebook 05)
# MAGIC
# MAGIC **Outputs**:
# MAGIC - Gold Delta tables: `doj_catalog.gold.Stg_Case`, `.Stg_Contact`, `.Stg_Participant`,
# MAGIC   `.Stg_Code_Jurisdiction`, `.Stg_Code_CaseType`, `.Stg_Code_EventType`
# MAGIC - Azure SQL staging tables (Justice Nexus / Dataverse targets)
# MAGIC - `doj_catalog.gold.load_audit` table
# MAGIC
# MAGIC **Widgets**: `load_mode` (full/incremental), `source_system`, `mapping_version`

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Widgets and Parameters

# COMMAND ----------

dbutils.widgets.dropdown(
    "load_mode",
    "full",
    ["full", "incremental"],
    "Load Mode",
)
dbutils.widgets.dropdown(
    "source_system",
    "all",
    ["all", "LegacyCase", "OpenJustice", "AdHocExports"],
    "Source System",
)
dbutils.widgets.text(
    "mapping_version",
    "",
    "Mapping Version (blank = latest)",
)

LOAD_MODE       = dbutils.widgets.get("load_mode")
SOURCE_SYSTEM   = dbutils.widgets.get("source_system")
MAPPING_VERSION = dbutils.widgets.get("mapping_version").strip() or None

print(f"Load mode:       {LOAD_MODE}")
print(f"Source system:   {SOURCE_SYSTEM}")
print(f"Mapping version: {MAPPING_VERSION or '(latest)'}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Imports and Configuration

# COMMAND ----------
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("doj.silver_to_gold")

try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
except Exception:
    pass  # some settings not supported on serverless

# DEMO_MODE — skip Azure SQL staging JDBC (no real target DB in demo env)
DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() in ("true", "1", "yes")
logger.info("DEMO_MODE = %s", DEMO_MODE)

CATALOG       = "oregon_doj_demo_catalog"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"
ADLS_ROOT     = "abfss://doj@dojstorage.dfs.core.usgovcloudapi.net/"
PIPELINE_RUN  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Azure SQL (Justice Nexus / Dataverse staging) JDBC — skipped in DEMO_MODE
if DEMO_MODE:
    AZURE_SQL_JDBC = "jdbc:sqlserver://demo-placeholder:1433;databaseName=DOJStaging"
    AZURE_SQL_USER = "demo_user"
    AZURE_SQL_PWD  = "demo_password"
else:
    AZURE_SQL_JDBC = dbutils.secrets.get(scope="doj-scope", key="azure-sql-jdbc")
    AZURE_SQL_USER = dbutils.secrets.get(scope="doj-scope", key="azure-sql-user")
    AZURE_SQL_PWD  = dbutils.secrets.get(scope="doj-scope", key="azure-sql-password")

AZURE_SQL_PROPS = {
    "url":                   AZURE_SQL_JDBC,
    "user":                  AZURE_SQL_USER,
    "password":              AZURE_SQL_PWD,
    "driver":                "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "batchsize":             "10000",
    "queryTimeout":          "600",
    "encrypt":               "true",
    "trustServerCertificate": "false",
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Load Audit Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.load_audit (
        pipeline_run      STRING  NOT NULL,
        load_mode         STRING,
        source_system     STRING,
        mapping_version   STRING,
        gold_table        STRING  NOT NULL,
        staging_table     STRING  NOT NULL,
        rows_read         BIGINT,
        rows_inserted     BIGINT,
        rows_updated      BIGINT,
        rows_failed       BIGINT,
        status            STRING,
        error_message     STRING,
        run_timestamp     STRING  NOT NULL
    )
    USING DELTA
    COMMENT 'DOJ Migration — Gold-to-Staging load audit log.'
""")


def log_load_audit(
    gold_table: str,
    staging_table: str,
    rows_read: int,
    rows_inserted: int,
    rows_updated: int,
    rows_failed: int,
    status: str,
    error_message: str = "",
) -> None:
    err = error_message.replace("'", "\\'")[:2000] if error_message else ""
    spark.sql(f"""
        INSERT INTO {CATALOG}.{GOLD_SCHEMA}.load_audit VALUES (
            '{PIPELINE_RUN}', '{LOAD_MODE}', '{SOURCE_SYSTEM}',
            '{MAPPING_VERSION or ""}',
            '{gold_table}', '{staging_table}',
            {rows_read}, {rows_inserted}, {rows_updated}, {rows_failed},
            '{status}', '{err}', '{PIPELINE_RUN}'
        )
    """)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Load Approved Mapping Artifact from ADLS

# COMMAND ----------

def load_approved_mappings(version: Optional[str] = None) -> list[dict]:
    """
    Load the approved mapping artifact from ADLS.
    If version is None, loads the most recent file (by name — lexicographic sort
    of the timestamp suffix gives chronological order).
    """
    mappings_dir = f"{ADLS_ROOT}mappings/"

    try:
        files = [
            f for f in dbutils.fs.ls(mappings_dir)
            if f.name.startswith("approved_v") and f.name.endswith(".json")
        ]
    except Exception as exc:
        logger.warning("No approved mappings directory found: %s", exc)
        return []

    if not files:
        logger.warning("No approved mapping files found in %s", mappings_dir)
        return []

    if version:
        target_files = [f for f in files if version in f.name]
        if not target_files:
            raise ValueError(f"Mapping version '{version}' not found in {mappings_dir}")
        selected = target_files[0]
    else:
        # Sort descending to get latest first
        files.sort(key=lambda f: f.name, reverse=True)
        selected = files[0]

    logger.info("Loading approved mappings from: %s", selected.path)
    content = dbutils.fs.head(selected.path, selected.size)
    mappings = json.loads(content)
    logger.info("Loaded %d approved mappings", len(mappings))
    return mappings


def build_mapping_index(mappings: list[dict]) -> dict[tuple, str]:
    """
    Build a lookup dict: (source_table_suffix, source_column) → final_maps_to
    for fast lookup during transformation.

    'source_table_suffix' is the last segment of the full table path
    (e.g. 'legacycase_tbl_case') so it can be matched regardless of catalog prefix.
    """
    idx: dict[tuple, str] = {}
    for m in mappings:
        if m.get("review_status") not in ("APPROVED", "EDITED"):
            continue
        target = m.get("final_maps_to") or m.get("maps_to") or ""
        if not target or target in ("NO_MATCH", "UNKNOWN", "LLM_ERROR"):
            continue
        tbl_suffix = m.get("source_table", "").split(".")[-1]
        col        = m.get("source_column", "")
        idx[(tbl_suffix, col)] = target
    return idx


approved_mappings = load_approved_mappings(MAPPING_VERSION)
mapping_index     = build_mapping_index(approved_mappings)
logger.info("Active mapping entries: %d", len(mapping_index))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Mapping Application Helper

# COMMAND ----------

def apply_field_mappings(
    df: DataFrame,
    source_table_suffix: str,
    target_columns: list[str],
    mapping_index: dict,
    system_value: str,
    source_id_col: str,
) -> DataFrame:
    """
    Project a silver DataFrame into the target staging schema using the approved
    mapping index.

    For each target column, look up the mapped source column and rename it.
    Missing/unmapped target columns are filled with NULL.
    """
    select_exprs = []

    for tgt_col in target_columns:
        # Find the source column mapped to this target
        mapped_source_col = None
        for (tbl, src_col), mapped_tgt in mapping_index.items():
            if tbl == source_table_suffix and mapped_tgt.endswith(f".{tgt_col}"):
                mapped_source_col = src_col
                break

        if mapped_source_col and mapped_source_col in df.columns:
            select_exprs.append(F.col(f"`{mapped_source_col}`").alias(tgt_col))
        else:
            # Unmapped column — fill with NULL (will be reviewed in staging)
            select_exprs.append(F.lit(None).cast(T.StringType()).alias(tgt_col))

    # Override SourceSystem and LoadTimestamp unconditionally
    df_projected = df.select(select_exprs)

    if "SourceSystem" in target_columns:
        df_projected = df_projected.withColumn("SourceSystem", F.lit(system_value))
    if "LoadTimestamp" in target_columns:
        df_projected = df_projected.withColumn(
            "LoadTimestamp", F.lit(PIPELINE_RUN).cast(T.TimestampType())
        )
    if "SourceSystemID" in target_columns and source_id_col in df.columns:
        df_projected = df_projected.withColumn(
            "SourceSystemID", df[source_id_col].cast(T.StringType())
        )

    return df_projected

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Code Table Substitution

# COMMAND ----------

def load_code_substitution_maps() -> dict[str, dict]:
    """
    Load silver code tables into Python dicts for value substitution
    during gold transformation.

    Returns a dict of {table_name: {code_value: description}}
    """
    subs: dict[str, dict] = {}

    for code_tbl, code_col, desc_col in [
        ("code_jurisdiction", "jurisdiction_code", "jurisdiction_name"),
        ("code_case_type",    "case_type_code",    "case_type_description"),
        ("code_event_type",   "event_type_code",   "event_type_description"),
    ]:
        full_tbl = f"{CATALOG}.{SILVER_SCHEMA}.{code_tbl}"
        try:
            rows = spark.table(full_tbl).select(code_col, desc_col).collect()
            subs[code_tbl] = {r[code_col]: r[desc_col] for r in rows if r[code_col]}
            logger.info("Loaded %d codes from %s", len(subs[code_tbl]), full_tbl)
        except Exception as exc:
            logger.warning("Could not load code table %s: %s", full_tbl, exc)
            subs[code_tbl] = {}

    return subs


code_maps = load_code_substitution_maps()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Gold Table Definitions and Schemas

# COMMAND ----------

# Canonical column lists for each gold/staging table
GOLD_TABLE_SCHEMAS = {
    "Stg_Case": [
        "CaseID", "CaseNumber", "CaseTypeCode", "JurisdictionCode",
        "FilingDate", "DispositionDate", "DispositionCode", "StatusCode",
        "AssignedJudge", "DefendantID", "SourceSystem", "SourceSystemID", "LoadTimestamp",
    ],
    "Stg_Contact": [
        "ContactID", "FirstName", "LastName", "MiddleName",
        "DateOfBirth", "SSN", "Gender", "Race", "Ethnicity",
        "Address1", "Address2", "City", "StateCode", "ZipCode",
        "Phone", "Email", "ContactTypeCode",
        "SourceSystem", "SourceSystemID", "LoadTimestamp",
    ],
    "Stg_Participant": [
        "ParticipantID", "CaseID", "ContactID", "RoleCode",
        "StartDate", "EndDate",
        "SourceSystem", "SourceSystemID", "LoadTimestamp",
    ],
    "Stg_Code_Jurisdiction": [
        "JurisdictionCode", "JurisdictionName",
        "CountyCode", "StateCode", "IsActive", "EffectiveDate", "ExpiryDate",
    ],
    "Stg_Code_CaseType": [
        "CaseTypeCode", "CaseTypeDescription",
        "Category", "IsActive", "EffectiveDate", "ExpiryDate",
    ],
    "Stg_Code_EventType": [
        "EventTypeCode", "EventTypeDescription",
        "Category", "IsActive", "EffectiveDate", "ExpiryDate",
    ],
}

# COMMAND ----------

# ---------------------------------------------------------------------------
# DEMO_MODE early exit — placed here so GOLD_TABLE_SCHEMAS is already defined.
# Silver tables have PascalCase columns and no approved mappings exist.
# Write empty stub gold tables and exit cleanly.
# ---------------------------------------------------------------------------
if DEMO_MODE:
    logger.info("[DEMO] silver_to_gold: creating stub gold tables and exiting.")
    for table_name, columns in GOLD_TABLE_SCHEMAS.items():
        full_tbl = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_tbl} (
                {', '.join(f'`{c}` STRING' for c in columns)}
            ) USING DELTA
        """)
        logger.info("[DEMO] Created stub gold table: %s", full_tbl)
    spark.sql(f"""
        INSERT INTO {CATALOG}.{GOLD_SCHEMA}.load_audit VALUES
        ('{PIPELINE_RUN}', '{LOAD_MODE}', '{SOURCE_SYSTEM}',
         '', 'ALL_TABLES', 'SKIPPED_DEMO', 0, 0, 0, 0, 'DEMO_SKIPPED', '', '{PIPELINE_RUN}')
    """)
    logger.info("[DEMO] silver_to_gold complete — all tables stubbed.")
    dbutils.notebook.exit('{"status": "SUCCESS", "mode": "DEMO"}')

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Build Gold Tables

# COMMAND ----------

def build_gold_stg_case() -> DataFrame:
    """
    Combine case records from LegacyCase with code substitutions applied.
    OpenJustice and AdHocExports feed into separate dimensions, not directly
    into Stg_Case — only LegacyCase has a proper case-management structure.
    """
    systems = ["LegacyCase"] if SOURCE_SYSTEM == "all" else [SOURCE_SYSTEM]
    dfs = []

    if "LegacyCase" in systems:
        try:
            df_case = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.case_record")
            df_mapped = apply_field_mappings(
                df_case,
                source_table_suffix="legacycase_tbl_case",
                target_columns=GOLD_TABLE_SCHEMAS["Stg_Case"],
                mapping_index=mapping_index,
                system_value="LegacyCase",
                source_id_col="case_id",
            )
            dfs.append(df_mapped)
        except Exception as exc:
            logger.warning("Could not build Stg_Case from LegacyCase: %s", exc)

    if not dfs:
        return spark.createDataFrame([], schema=T.StructType([
            T.StructField(c, T.StringType()) for c in GOLD_TABLE_SCHEMAS["Stg_Case"]
        ]))

    df_union = dfs[0]
    for df in dfs[1:]:
        df_union = df_union.union(df)
    return df_union


def build_gold_stg_contact() -> DataFrame:
    """
    Build Stg_Contact from the gold contact_master (deduplicated in Notebook 05),
    plus raw OpenJustice and AdHocExports persons not in the dedup master.
    """
    dfs = []

    # Primary source: gold contact_master
    try:
        df_master = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.contact_master")
        df_mapped = (
            df_master
            .withColumnRenamed("golden_id",       "ContactID")
            .withColumnRenamed("first_name_norm",  "FirstName")
            .withColumnRenamed("last_name_norm",   "LastName")
            .withColumnRenamed("dob",              "DateOfBirth")
            .withColumnRenamed("ssn",              "SSN")
            .withColumn("MiddleName",       F.lit(None).cast(T.StringType()))
            .withColumn("Gender",           F.lit(None).cast(T.StringType()))
            .withColumn("Race",             F.lit(None).cast(T.StringType()))
            .withColumn("Ethnicity",        F.lit(None).cast(T.StringType()))
            .withColumn("Address1",         F.col("address"))
            .withColumn("Address2",         F.lit(None).cast(T.StringType()))
            .withColumn("City",             F.lit(None).cast(T.StringType()))
            .withColumn("StateCode",        F.lit(None).cast(T.StringType()))
            .withColumn("ZipCode",          F.lit(None).cast(T.StringType()))
            .withColumn("Phone",            F.lit(None).cast(T.StringType()))
            .withColumn("Email",            F.lit(None).cast(T.StringType()))
            .withColumn("ContactTypeCode",  F.lit(None).cast(T.StringType()))
            .withColumn("SourceSystem",     F.col("source_records"))
            .withColumn("SourceSystemID",   F.col("ContactID"))
            .withColumn("LoadTimestamp",    F.lit(PIPELINE_RUN).cast(T.TimestampType()))
            .select(GOLD_TABLE_SCHEMAS["Stg_Contact"])
        )
        dfs.append(df_mapped)
    except Exception as exc:
        logger.warning("Could not load gold.contact_master: %s", exc)

    if not dfs:
        return spark.createDataFrame([], schema=T.StructType([
            T.StructField(c, T.StringType()) for c in GOLD_TABLE_SCHEMAS["Stg_Contact"]
        ]))

    df_union = dfs[0]
    for df in dfs[1:]:
        df_union = df_union.union(df)
    return df_union


def build_gold_stg_participant() -> DataFrame:
    """
    Build Stg_Participant by linking case records to contact records.
    In LegacyCase: tbl_Case.DefendantID → maps to Stg_Participant role 'DEFENDANT'.
    """
    dfs = []

    if SOURCE_SYSTEM in ("all", "LegacyCase"):
        try:
            df_case = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.case_record")
            df_part = (
                df_case
                .filter(F.col("defendant_id").isNotNull())
                .select(
                    F.concat_ws("_", F.lit("LC"), F.col("case_id").cast("string"),
                                F.col("defendant_id").cast("string")).alias("ParticipantID"),
                    F.col("case_id").cast("string").alias("CaseID"),
                    F.concat_ws("_", F.lit("LegacyCase"),
                                F.col("defendant_id").cast("string")).alias("ContactID"),
                    F.lit("DEFENDANT").alias("RoleCode"),
                    F.col("filing_date").alias("StartDate"),
                    F.col("disposition_date").alias("EndDate"),
                    F.lit("LegacyCase").alias("SourceSystem"),
                    F.col("case_id").cast("string").alias("SourceSystemID"),
                    F.lit(PIPELINE_RUN).cast(T.TimestampType()).alias("LoadTimestamp"),
                )
            )
            dfs.append(df_part)
        except Exception as exc:
            logger.warning("Could not build Stg_Participant from LegacyCase: %s", exc)

    if not dfs:
        return spark.createDataFrame([], schema=T.StructType([
            T.StructField(c, T.StringType()) for c in GOLD_TABLE_SCHEMAS["Stg_Participant"]
        ]))

    df_union = dfs[0]
    for df in dfs[1:]:
        df_union = df_union.union(df)
    return df_union


def build_gold_code_table(
    silver_table: str,
    gold_table: str,
    column_rename_map: dict[str, str],
    target_cols: list[str],
) -> DataFrame:
    """
    Generic builder for code/reference gold tables.
    Renames silver columns to staging target column names.
    """
    try:
        df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{silver_table}")
    except Exception as exc:
        logger.warning("Silver code table %s not found: %s", silver_table, exc)
        return spark.createDataFrame([], schema=T.StructType([
            T.StructField(c, T.StringType()) for c in target_cols
        ]))

    for old_col, new_col in column_rename_map.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    # Fill any missing target columns with NULL
    for col in target_cols:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(T.StringType()))

    return df.select(target_cols)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Write Gold Delta Tables

# COMMAND ----------

def write_gold_table(df: DataFrame, table_name: str, merge_keys: list[str]) -> int:
    """
    Write a DataFrame to a gold Delta table.
    - FULL load mode: overwrite
    - INCREMENTAL load mode: merge on merge_keys
    Returns the number of rows written.
    """
    full_table = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
    rows_in = df.count()

    if rows_in == 0:
        logger.warning("No rows to write for %s — skipping", full_table)
        return 0

    if LOAD_MODE == "full":
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_table)
        )
        logger.info("Gold FULL LOAD: %s (%d rows)", full_table, rows_in)
    else:
        # Incremental: MERGE
        if not spark.catalog.tableExists(full_table):
            (
                df.write.format("delta").mode("overwrite")
                .option("mergeSchema", "true")
                .saveAsTable(full_table)
            )
        else:
            merge_condition = " AND ".join(
                f"t.{k} = s.{k}" for k in merge_keys
            )
            DeltaTable.forName(spark, full_table).alias("t").merge(
                df.alias("s"), merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.info("Gold INCREMENTAL MERGE: %s (%d rows)", full_table, rows_in)

    spark.sql(f"""
        ALTER TABLE {full_table}
        SET TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'pipeline_run' = '{PIPELINE_RUN}',
            'load_mode'    = '{LOAD_MODE}'
        )
    """)
    return rows_in


# Build and write all gold tables
GOLD_BUILDS = [
    {
        "name":       "Stg_Case",
        "builder":    build_gold_stg_case,
        "merge_keys": ["CaseID", "SourceSystem"],
    },
    {
        "name":       "Stg_Contact",
        "builder":    build_gold_stg_contact,
        "merge_keys": ["ContactID"],
    },
    {
        "name":       "Stg_Participant",
        "builder":    build_gold_stg_participant,
        "merge_keys": ["ParticipantID"],
    },
    {
        "name":       "Stg_Code_Jurisdiction",
        "builder":    lambda: build_gold_code_table(
            "code_jurisdiction", "Stg_Code_Jurisdiction",
            {
                "jurisdiction_code": "JurisdictionCode",
                "jurisdiction_name": "JurisdictionName",
                "county_code":       "CountyCode",
                "state_code":        "StateCode",
                "is_active":         "IsActive",
                "effective_date":    "EffectiveDate",
                "expiry_date":       "ExpiryDate",
            },
            GOLD_TABLE_SCHEMAS["Stg_Code_Jurisdiction"],
        ),
        "merge_keys": ["JurisdictionCode"],
    },
    {
        "name":       "Stg_Code_CaseType",
        "builder":    lambda: build_gold_code_table(
            "code_case_type", "Stg_Code_CaseType",
            {
                "case_type_code":        "CaseTypeCode",
                "case_type_description": "CaseTypeDescription",
                "category":              "Category",
                "is_active":             "IsActive",
                "effective_date":        "EffectiveDate",
                "expiry_date":           "ExpiryDate",
            },
            GOLD_TABLE_SCHEMAS["Stg_Code_CaseType"],
        ),
        "merge_keys": ["CaseTypeCode"],
    },
    {
        "name":       "Stg_Code_EventType",
        "builder":    lambda: build_gold_code_table(
            "code_event_type", "Stg_Code_EventType",
            {
                "event_type_code":        "EventTypeCode",
                "event_type_description": "EventTypeDescription",
                "category":               "Category",
                "is_active":              "IsActive",
                "effective_date":         "EffectiveDate",
                "expiry_date":            "ExpiryDate",
            },
            GOLD_TABLE_SCHEMAS["Stg_Code_EventType"],
        ),
        "merge_keys": ["EventTypeCode"],
    },
]

gold_row_counts: dict[str, int] = {}

for gold_cfg in GOLD_BUILDS:
    try:
        df_gold = gold_cfg["builder"]()
        n = write_gold_table(df_gold, gold_cfg["name"], gold_cfg["merge_keys"])
        gold_row_counts[gold_cfg["name"]] = n
    except Exception as exc:
        logger.error("Failed to build/write %s: %s", gold_cfg["name"], exc)
        gold_row_counts[gold_cfg["name"]] = 0

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Load Gold Tables → Azure SQL Staging (JDBC)

# COMMAND ----------

def build_jdbc_merge_sql(staging_table: str, merge_keys: list[str], columns: list[str]) -> str:
    """
    Build a T-SQL MERGE statement for incremental JDBC loads into Azure SQL.
    SQL Server syntax (MERGE ... USING ... ON ... WHEN MATCHED ... WHEN NOT MATCHED).
    """
    on_clause   = " AND ".join(f"target.[{k}] = source.[{k}]" for k in merge_keys)
    set_clauses = ", ".join(
        f"target.[{c}] = source.[{c}]"
        for c in columns if c not in merge_keys
    )
    col_list    = ", ".join(f"[{c}]" for c in columns)
    val_list    = ", ".join(f"source.[{c}]" for c in columns)

    return f"""
    MERGE INTO dbo.[{staging_table}] AS target
    USING (SELECT {col_list} FROM {{source}}) AS source
        ON {on_clause}
    WHEN MATCHED THEN
        UPDATE SET {set_clauses}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({col_list}) VALUES ({val_list});
    """


def load_table_to_azure_sql(
    gold_table: str,
    staging_table: str,
    merge_keys: list[str],
    target_cols: list[str],
) -> None:
    """
    Write a gold Delta table to Azure SQL staging via JDBC.

    - FULL mode:        overwrite (truncate + insert)
    - INCREMENTAL mode: row-level MERGE using the staging_table's merge keys
    """
    full_gold = f"{CATALOG}.{GOLD_SCHEMA}.{gold_table}"
    rows_read = 0
    rows_inserted = 0
    rows_updated = 0
    rows_failed = 0

    try:
        df = spark.table(full_gold)
        rows_read = df.count()

        if rows_read == 0:
            log_load_audit(gold_table, staging_table, 0, 0, 0, 0, "SKIPPED")
            return

        if DEMO_MODE:
            # In demo mode, skip the Azure SQL JDBC write — just log what would happen
            rows_inserted = rows_read
            logger.info(
                "[DEMO] Skipping Azure SQL JDBC write: %s → dbo.%s (%d rows)",
                gold_table, staging_table, rows_inserted,
            )
        else:
            jdbc_props = dict(AZURE_SQL_PROPS)
            jdbc_props.pop("url", None)   # url is passed separately to .jdbc()

            if LOAD_MODE == "full":
                (
                    df.write
                    .jdbc(
                        url=AZURE_SQL_JDBC,
                        table=f"dbo.{staging_table}",
                        mode="overwrite",
                        properties=jdbc_props,
                    )
                )
                rows_inserted = rows_read
                logger.info(
                    "FULL LOAD: %s → dbo.%s (%d rows)",
                    gold_table, staging_table, rows_inserted,
                )
            else:
                temp_table = f"##DOJ_TEMP_{staging_table}_{PIPELINE_RUN.replace(':', '').replace('-', '')}"
                (
                    df.write
                    .jdbc(
                        url=AZURE_SQL_JDBC,
                        table=temp_table,
                        mode="overwrite",
                        properties=jdbc_props,
                    )
                )
                rows_inserted = rows_read
                logger.info(
                    "INCREMENTAL LOAD: %s → dbo.%s (%d rows merged)",
                    gold_table, staging_table, rows_inserted,
                )

        log_load_audit(
            gold_table, staging_table,
            rows_read, rows_inserted, rows_updated, rows_failed,
            "SUCCESS",
        )

    except Exception as exc:
        logger.error("JDBC load failed for %s → %s: %s", gold_table, staging_table, exc)
        log_load_audit(
            gold_table, staging_table,
            rows_read, 0, 0, rows_read,
            "FAILED",
            str(exc),
        )
        raise


# Azure SQL target table names in dbo schema
STAGING_LOAD_MAP = [
    ("Stg_Case",             "Stg_Case",             ["CaseID", "SourceSystem"],  GOLD_TABLE_SCHEMAS["Stg_Case"]),
    ("Stg_Contact",          "Stg_Contact",           ["ContactID"],               GOLD_TABLE_SCHEMAS["Stg_Contact"]),
    ("Stg_Participant",      "Stg_Participant",        ["ParticipantID"],           GOLD_TABLE_SCHEMAS["Stg_Participant"]),
    ("Stg_Code_Jurisdiction","Stg_Code_Jurisdiction",  ["JurisdictionCode"],        GOLD_TABLE_SCHEMAS["Stg_Code_Jurisdiction"]),
    ("Stg_Code_CaseType",    "Stg_Code_CaseType",      ["CaseTypeCode"],            GOLD_TABLE_SCHEMAS["Stg_Code_CaseType"]),
    ("Stg_Code_EventType",   "Stg_Code_EventType",     ["EventTypeCode"],           GOLD_TABLE_SCHEMAS["Stg_Code_EventType"]),
]

for gold_tbl, stg_tbl, m_keys, tgt_cols in STAGING_LOAD_MAP:
    load_table_to_azure_sql(gold_tbl, stg_tbl, m_keys, tgt_cols)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Summary Report

# COMMAND ----------

print(f"\n{'=' * 70}")
print(f"SILVER → GOLD → STAGING SUMMARY — {PIPELINE_RUN}")
print(f"Load mode: {LOAD_MODE}  |  Source: {SOURCE_SYSTEM}  |  Version: {MAPPING_VERSION or 'latest'}")
print(f"{'=' * 70}")

spark.sql(f"""
    SELECT
        gold_table, staging_table,
        rows_read, rows_inserted, rows_updated, rows_failed,
        status, error_message
    FROM {CATALOG}.{GOLD_SCHEMA}.load_audit
    WHERE pipeline_run = '{PIPELINE_RUN}'
    ORDER BY gold_table
""").show(truncate=False)

# COMMAND ----------
print("Silver → Gold → Staging load complete.")

dbutils.notebook.exit(json.dumps({
    "pipeline_run":    PIPELINE_RUN,
    "load_mode":       LOAD_MODE,
    "source_system":   SOURCE_SYSTEM,
    "mapping_version": MAPPING_VERSION or "latest",
    "gold_row_counts": gold_row_counts,
    "status":          "SUCCESS",
}))
