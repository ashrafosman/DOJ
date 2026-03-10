# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # DOJ Data Migration — Bronze → Silver Transformation (Notebook 04)
# MAGIC
# MAGIC **Purpose**: Clean, validate, standardise, and type-cast all bronze data
# MAGIC into the silver layer. Separates code/reference tables from transactional
# MAGIC tables and implements an UPSERT pattern to keep silver idempotent.
# MAGIC
# MAGIC **Sources**: `doj_catalog.bronze.*` (LegacyCase, OpenJustice, AdHocExports)
# MAGIC **Targets**: `doj_catalog.silver.*`
# MAGIC **Metrics**: `doj_catalog.silver.pipeline_metrics`

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Imports and Configuration

# COMMAND ----------
import logging
import os
import re
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
logger = logging.getLogger("doj.bronze_to_silver")

# ---------------------------------------------------------------------------
# Spark optimisation
# ---------------------------------------------------------------------------
try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
except Exception:
    pass  # some settings not supported on serverless

# DEMO_MODE — use a simplified passthrough transformation that avoids
# column-name assumptions about the original SQL Server JDBC schema.
DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() in ("true", "1", "yes")
logger.info("DEMO_MODE = %s", DEMO_MODE)

CATALOG        = "oregon_doj_demo_catalog"
BRONZE_SCHEMA  = "bronze"
SILVER_SCHEMA  = "silver"
PIPELINE_RUN   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
TODAY          = datetime.now(timezone.utc).date()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# ---------------------------------------------------------------------------
# DEMO_MODE: simple passthrough — copy bronze tables to silver with a
# _silver_load_ts column and minimal renaming. No column-name assumptions.
# ---------------------------------------------------------------------------
if DEMO_MODE:
    BRONZE_SILVER_MAP = [
        ("legacycase_tbl_defendant",          "defendant"),
        ("legacycase_tbl_case",               "case_record"),
        ("legacycase_tbl_event",              "event"),
        ("openjustice_arrests",               "arrest"),
        ("openjustice_arrest_dispositions",   "arrest_disposition"),
        ("openjustice_crimes_clearances",     "crime_clearance"),
        ("adhoc_client",                      "client"),
        ("adhoc_incident",                    "incident"),
        ("adhoc_lookup",                      "lookup"),
    ]

    for bronze_tbl, silver_tbl in BRONZE_SILVER_MAP:
        src = f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_tbl}"
        tgt = f"{CATALOG}.{SILVER_SCHEMA}.{silver_tbl}"
        try:
            df = spark.table(src)
            row_count = df.count()
            df = df.withColumn("_silver_load_ts", F.lit(PIPELINE_RUN).cast(T.TimestampType()))
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(tgt)
            logger.info("[DEMO] bronze.%s → silver.%s (%d rows)", bronze_tbl, silver_tbl, row_count)
        except Exception as exc:
            logger.warning("[DEMO] Skipped %s → %s: %s", bronze_tbl, silver_tbl, exc)

    print(f"\n{'=' * 70}")
    print(f"BRONZE → SILVER PIPELINE SUMMARY — {PIPELINE_RUN} [DEMO MODE]")
    print(f"{'=' * 70}")
    for bronze_tbl, silver_tbl in BRONZE_SILVER_MAP:
        tgt = f"{CATALOG}.{SILVER_SCHEMA}.{silver_tbl}"
        try:
            cnt = spark.table(tgt).count()
            print(f"  silver.{silver_tbl:<35} {cnt:>8,} rows")
        except Exception:
            print(f"  silver.{silver_tbl:<35} (not found)")
    print("=" * 70)
    print("Bronze → Silver transformation complete.")
    dbutils.notebook.exit("SUCCESS")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Pipeline Metrics Infrastructure

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.pipeline_metrics (
        pipeline_run     STRING  NOT NULL,
        source_table     STRING  NOT NULL,
        target_table     STRING  NOT NULL,
        rows_read        BIGINT,
        rows_written     BIGINT,
        rows_rejected    BIGINT,
        rejection_rate   DOUBLE,
        run_timestamp    STRING  NOT NULL
    )
    USING DELTA
    COMMENT 'DOJ Migration — Bronze-to-Silver pipeline execution metrics.'
""")


def log_metrics(
    source_table: str,
    target_table: str,
    rows_read: int,
    rows_written: int,
    rows_rejected: int,
) -> None:
    rejection_rate = round(rows_rejected / rows_read, 6) if rows_read > 0 else 0.0
    spark.sql(f"""
        INSERT INTO {CATALOG}.{SILVER_SCHEMA}.pipeline_metrics
        VALUES (
            '{PIPELINE_RUN}',
            '{source_table}',
            '{target_table}',
            {rows_read},
            {rows_written},
            {rows_rejected},
            {rejection_rate},
            '{PIPELINE_RUN}'
        )
    """)
    logger.info(
        "Metrics logged: %s → %s | read=%d written=%d rejected=%d (%.2f%%)",
        source_table, target_table, rows_read, rows_written, rows_rejected,
        rejection_rate * 100,
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Common Transformation Functions

# COMMAND ----------

def standardise_date(col_expr, formats: list[str] | None = None) -> "Column":
    """
    Attempt to parse a date string using multiple common formats (US, ISO, etc.)
    and return a DateType column. Returns NULL for values that don't match any format.

    DOJ data contains dates in at least four different formats across the three systems.
    """
    default_formats = [
        "yyyy-MM-dd",
        "MM/dd/yyyy",
        "M/d/yyyy",
        "MM/dd/yy",
        "dd-MMM-yyyy",
        "yyyyMMdd",
        "yyyy-MM-dd HH:mm:ss",
        "MM/dd/yyyy HH:mm:ss",
    ]
    fmt_list = formats or default_formats

    # Cascade: try each format and coalesce to first non-null result
    parsed_cols = [F.to_date(col_expr, fmt) for fmt in fmt_list]
    return F.coalesce(*parsed_cols)


def clean_string(col_expr) -> "Column":
    """Trim leading/trailing whitespace and uppercase string fields."""
    return F.upper(F.trim(col_expr))


def null_invalid_age(col_expr, min_age: int = 0, max_age: int = 120) -> "Column":
    """
    Return NULL for ages outside a plausible human range.
    DOJ data has known data-entry errors with negative ages and ages > 120.
    """
    return F.when(
        (col_expr >= min_age) & (col_expr <= max_age),
        col_expr
    ).otherwise(F.lit(None).cast(T.IntegerType()))


def null_future_date(col_expr) -> "Column":
    """
    Return NULL for dates in the future.
    Future birthdates indicate data entry errors in source systems.
    """
    return F.when(col_expr <= F.current_date(), col_expr).otherwise(F.lit(None).cast(T.DateType()))


def to_snake_case(name: str) -> str:
    """
    Convert camelCase or PascalCase column names to snake_case.
    Example: 'FirstName' → 'first_name', 'DefendantID' → 'defendant_id'
    """
    # Insert underscore before uppercase sequences following lowercase
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Insert underscore before uppercase letter followed by lowercase (e.g. ABCDef → ABC_Def)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    return s.lower()


def rename_to_snake_case(df: DataFrame) -> DataFrame:
    """Rename all DataFrame columns to snake_case."""
    for col in df.columns:
        snake = to_snake_case(col)
        if snake != col:
            df = df.withColumnRenamed(col, snake)
    return df


def add_reject_flag(df: DataFrame, condition_col: "Column", reason: str) -> DataFrame:
    """
    Append or update a `_dq_reject_flag` column.
    Rows failing DQ assertions are flagged rather than dropped so they can be
    quarantined and reported without silently losing data.
    """
    if "_dq_reject_flag" not in df.columns:
        df = df.withColumn("_dq_reject_flag", F.lit(False))
    if "_dq_reject_reason" not in df.columns:
        df = df.withColumn("_dq_reject_reason", F.lit(None).cast(T.StringType()))

    df = df.withColumn(
        "_dq_reject_flag",
        F.col("_dq_reject_flag") | condition_col,
    ).withColumn(
        "_dq_reject_reason",
        F.when(
            condition_col,
            F.concat_ws("; ", F.col("_dq_reject_reason"), F.lit(reason)),
        ).otherwise(F.col("_dq_reject_reason")),
    )
    return df

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Silver MERGE (Upsert) Utility

# COMMAND ----------

def upsert_silver(
    df_new: DataFrame,
    target_table: str,
    merge_keys: list[str],
    comment: str = "",
) -> tuple[int, int]:
    """
    Perform a Delta MERGE (upsert) into a silver table.

    - If the target table does not exist, creates it and inserts all rows.
    - If it exists, updates matching rows and inserts new ones.
    - Returns (rows_written, rows_rejected) counts.

    `merge_keys` are the columns used to identify matching rows
    (e.g. ['source_id', 'source_system']).
    """
    full_table = f"{CATALOG}.{SILVER_SCHEMA}.{target_table}"

    # Split accepted vs rejected rows
    df_accepted = df_new.filter(~F.col("_dq_reject_flag")) if "_dq_reject_flag" in df_new.columns else df_new
    df_rejected = df_new.filter(F.col("_dq_reject_flag"))  if "_dq_reject_flag" in df_new.columns else df_new.filter(F.lit(False))

    rows_rejected = df_rejected.count()

    # Write rejected rows to a quarantine table for review
    if rows_rejected > 0:
        quarantine_table = f"{CATALOG}.{SILVER_SCHEMA}.{target_table}_quarantine"
        (
            df_rejected.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(quarantine_table)
        )

    if not spark.catalog.tableExists(full_table):
        (
            df_accepted.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(full_table)
        )
        rows_written = df_accepted.count()
    else:
        # Build MERGE ON condition from merge keys
        merge_condition = " AND ".join(
            f"target.{k} = source.{k}" for k in merge_keys
        )

        delta_table = DeltaTable.forName(spark, full_table)
        (
            delta_table.alias("target")
            .merge(df_accepted.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        rows_written = df_accepted.count()

    if comment:
        safe_comment = comment.replace("'", "\\'")
        spark.sql(f"COMMENT ON TABLE {full_table} IS '{safe_comment}'")

    return rows_written, rows_rejected

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. System A — LegacyCase Transformations

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.1 tbl_Defendant → silver.defendant

# COMMAND ----------

def transform_legacycase_defendant() -> None:
    src_table = f"{CATALOG}.{BRONZE_SCHEMA}.legacycase_tbl_defendant"
    tgt_table = "defendant"

    logger.info("Transforming: %s → silver.%s", src_table, tgt_table)
    df_raw = spark.table(src_table)
    rows_read = df_raw.count()

    # ---- Rename to snake_case ----------------------------------------
    df = rename_to_snake_case(df_raw)

    # ---- Type casting ---------------------------------------------------
    # DOJ note: DefendantID is the business key; SSN may be stored with or
    # without dashes — normalise to dashed format if length = 9.
    df = (
        df
        .withColumn("defendant_id",  F.col("defendantid").cast(T.LongType()))
        .withColumn("date_of_birth", standardise_date(F.col("dob")))
        .withColumn("ssn",
            F.when(
                F.length(F.regexp_replace(F.col("ssn"), "-", "")) == 9,
                F.regexp_replace(F.col("ssn"), r"(\d{3})(\d{2})(\d{4})", "$1-$2-$3"),
            ).otherwise(F.lit(None).cast(T.StringType()))
        )
        .withColumn("first_name",  clean_string(F.col("first_name")))
        .withColumn("last_name",   clean_string(F.col("last_name")))
        .withColumn("middle_name", clean_string(F.col("middle_name")))
    )

    # ---- DQ assertions (Great Expectations style) ----------------------
    # Expectation: defendant_id must not be null
    df = add_reject_flag(df, F.col("defendant_id").isNull(), "NULL defendant_id")
    # Expectation: date_of_birth must not be in the future
    df = df.withColumn("date_of_birth", null_future_date(F.col("date_of_birth")))
    # Expectation: SSN format is valid if present
    df = add_reject_flag(
        df,
        F.col("ssn").isNotNull() & ~F.col("ssn").rlike(r"^\d{3}-\d{2}-\d{4}$"),
        "Invalid SSN format",
    )

    # ---- Add silver metadata -------------------------------------------
    df = df.withColumn("source_system",  F.lit("LegacyCase"))
    df = df.withColumn("silver_load_ts", F.lit(PIPELINE_RUN).cast(T.TimestampType()))

    rows_written, rows_rejected = upsert_silver(
        df, tgt_table,
        merge_keys=["defendant_id", "source_system"],
        comment="Silver defendant records from LegacyCase. PII — access restricted.",
    )

    # Set lineage property
    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.{tgt_table}
        SET TBLPROPERTIES ('source' = '{src_table}')
    """)

    log_metrics(src_table, f"silver.{tgt_table}", rows_read, rows_written, rows_rejected)


if not DEMO_MODE:
    transform_legacycase_defendant()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.2 tbl_Case → silver.case_record

# COMMAND ----------

def transform_legacycase_case() -> None:
    src_table = f"{CATALOG}.{BRONZE_SCHEMA}.legacycase_tbl_case"
    tgt_table = "case_record"

    df_raw  = spark.table(src_table)
    rows_read = df_raw.count()
    df = rename_to_snake_case(df_raw)

    df = (
        df
        .withColumn("case_id",        F.col("caseid").cast(T.LongType()))
        .withColumn("defendant_id",   F.col("defendantid").cast(T.LongType()))
        .withColumn("filing_date",    standardise_date(F.col("filing_date")))
        .withColumn("disposition_date", standardise_date(F.col("disposition_date")))
        .withColumn("case_number",    clean_string(F.col("case_number")))
        .withColumn("case_type_code", clean_string(F.col("case_type_code")))
        .withColumn("status_code",    clean_string(F.col("status_code")))
        .withColumn("source_system",  F.lit("LegacyCase"))
        .withColumn("silver_load_ts", F.lit(PIPELINE_RUN).cast(T.TimestampType()))
    )

    df = add_reject_flag(df, F.col("case_id").isNull(), "NULL case_id")
    df = add_reject_flag(
        df,
        F.col("disposition_date").isNotNull() &
        F.col("filing_date").isNotNull() &
        (F.col("disposition_date") < F.col("filing_date")),
        "disposition_date before filing_date",
    )

    rows_written, rows_rejected = upsert_silver(
        df, tgt_table,
        merge_keys=["case_id", "source_system"],
        comment="Silver case master records from LegacyCase.",
    )

    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.{tgt_table}
        SET TBLPROPERTIES ('source' = '{src_table}')
    """)
    log_metrics(src_table, f"silver.{tgt_table}", rows_read, rows_written, rows_rejected)


if not DEMO_MODE:
    transform_legacycase_case()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.3 tbl_Event → silver.event

# COMMAND ----------

def transform_legacycase_event() -> None:
    src_table = f"{CATALOG}.{BRONZE_SCHEMA}.legacycase_tbl_event"
    tgt_table = "event"

    df_raw  = spark.table(src_table)
    rows_read = df_raw.count()
    df = rename_to_snake_case(df_raw)

    df = (
        df
        .withColumn("event_id",        F.col("eventid").cast(T.LongType()))
        .withColumn("case_id",         F.col("caseid").cast(T.LongType()))
        .withColumn("event_date",      standardise_date(F.col("event_date")))
        .withColumn("event_type_code", clean_string(F.col("event_type_code")))
        .withColumn("description",     F.trim(F.col("description")))
        .withColumn("source_system",   F.lit("LegacyCase"))
        .withColumn("silver_load_ts",  F.lit(PIPELINE_RUN).cast(T.TimestampType()))
    )

    df = add_reject_flag(df, F.col("event_id").isNull(), "NULL event_id")
    df = add_reject_flag(df, F.col("case_id").isNull(),  "NULL case_id")

    rows_written, rows_rejected = upsert_silver(
        df, tgt_table,
        merge_keys=["event_id", "source_system"],
        comment="Silver event/hearing records from LegacyCase.",
    )

    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.{tgt_table}
        SET TBLPROPERTIES ('source' = '{src_table}')
    """)
    log_metrics(src_table, f"silver.{tgt_table}", rows_read, rows_written, rows_rejected)


if not DEMO_MODE:
    transform_legacycase_event()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. System B — OpenJustice Transformations

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.1 openjustice_arrests → silver.arrest

# COMMAND ----------

def transform_openjustice_arrests() -> None:
    src_table = f"{CATALOG}.{BRONZE_SCHEMA}.openjustice_arrests"
    tgt_table = "arrest"

    df_raw  = spark.table(src_table)
    rows_read = df_raw.count()
    df = rename_to_snake_case(df_raw)

    # OpenJustice CSV columns are already in snake_case-ish format;
    # standardise known column names to DOJ canonical form.
    rename_map = {
        "arrest_date":     "arrest_date",
        "county_code":     "county_code",
        "gender":          "gender",
        "race_or_ethnicity": "race_code",
        "age":             "age",
        "charge_level":    "charge_level",
        "bcs_code":        "offense_code",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    df = (
        df
        .withColumn("arrest_date",    standardise_date(F.col("arrest_date")))
        .withColumn("age",            F.col("age").cast(T.IntegerType()))
        .withColumn("age",            null_invalid_age(F.col("age")))
        .withColumn("gender",         clean_string(F.col("gender")))
        .withColumn("county_code",    clean_string(F.col("county_code")))
        .withColumn("source_system",  F.lit("OpenJustice"))
        .withColumn("silver_load_ts", F.lit(PIPELINE_RUN).cast(T.TimestampType()))
        # Surrogate row key from source fields (no explicit PK in OpenJustice CSVs)
        .withColumn("arrest_id", F.md5(
            F.concat_ws("|",
                F.col("arrest_date").cast("string"),
                F.col("county_code"),
                F.col("age").cast("string"),
                F.col("gender"),
                F.col("offense_code"),
            )
        ))
    )

    df = add_reject_flag(df, F.col("arrest_date").isNull(), "NULL arrest_date")
    df = add_reject_flag(df, F.col("county_code").isNull(), "NULL county_code")

    rows_written, rows_rejected = upsert_silver(
        df, tgt_table,
        merge_keys=["arrest_id", "source_system"],
        comment="Silver arrest records from CA DOJ OpenJustice public dataset.",
    )

    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.{tgt_table}
        SET TBLPROPERTIES ('source' = '{src_table}')
    """)
    log_metrics(src_table, f"silver.{tgt_table}", rows_read, rows_written, rows_rejected)


if not DEMO_MODE:
    transform_openjustice_arrests()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.2 openjustice_arrest_dispositions → silver.arrest_disposition

# COMMAND ----------

def transform_openjustice_dispositions() -> None:
    src_table = f"{CATALOG}.{BRONZE_SCHEMA}.openjustice_arrest_dispositions"
    tgt_table = "arrest_disposition"

    df_raw  = spark.table(src_table)
    rows_read = df_raw.count()
    df = rename_to_snake_case(df_raw)

    df = (
        df
        .withColumn("arrest_date",       standardise_date(F.col("arrest_date")))
        .withColumn("disposition_date",  standardise_date(F.col("disposition_date")))
        .withColumn("disposition_code",  clean_string(F.col("disposition_code")))
        .withColumn("county_code",       clean_string(F.col("county_code")))
        .withColumn("source_system",     F.lit("OpenJustice"))
        .withColumn("silver_load_ts",    F.lit(PIPELINE_RUN).cast(T.TimestampType()))
        .withColumn("disp_id", F.md5(
            F.concat_ws("|",
                F.col("arrest_date").cast("string"),
                F.col("disposition_date").cast("string"),
                F.col("county_code"),
                F.col("disposition_code"),
            )
        ))
    )

    df = add_reject_flag(df, F.col("county_code").isNull(), "NULL county_code")
    df = add_reject_flag(
        df,
        F.col("disposition_date").isNotNull() &
        F.col("arrest_date").isNotNull() &
        (F.col("disposition_date") < F.col("arrest_date")),
        "disposition_date before arrest_date",
    )

    rows_written, rows_rejected = upsert_silver(
        df, tgt_table,
        merge_keys=["disp_id", "source_system"],
        comment="Silver arrest disposition records from CA DOJ OpenJustice.",
    )

    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.{tgt_table}
        SET TBLPROPERTIES ('source' = '{src_table}')
    """)
    log_metrics(src_table, f"silver.{tgt_table}", rows_read, rows_written, rows_rejected)


if not DEMO_MODE:
    transform_openjustice_dispositions()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.3 openjustice_crimes_clearances → silver.crime_clearance

# COMMAND ----------

def transform_openjustice_crimes_clearances() -> None:
    src_table = f"{CATALOG}.{BRONZE_SCHEMA}.openjustice_crimes_clearances"
    tgt_table = "crime_clearance"

    df_raw  = spark.table(src_table)
    rows_read = df_raw.count()
    df = rename_to_snake_case(df_raw)

    df = (
        df
        .withColumn("year",          F.col("year").cast(T.IntegerType()))
        .withColumn("county_code",   clean_string(F.col("county_code")))
        .withColumn("offense_code",  clean_string(F.col("offense_code")))
        .withColumn("crimes_total",  F.col("crimes_total").cast(T.LongType()))
        .withColumn("cleared_total", F.col("cleared_total").cast(T.LongType()))
        .withColumn("source_system", F.lit("OpenJustice"))
        .withColumn("silver_load_ts", F.lit(PIPELINE_RUN).cast(T.TimestampType()))
        .withColumn("clearance_id", F.md5(
            F.concat_ws("|",
                F.col("year").cast("string"),
                F.col("county_code"),
                F.col("offense_code"),
            )
        ))
    )

    df = add_reject_flag(
        df,
        F.col("year").isNull() | (F.col("year") < 1960) | (F.col("year") > 2030),
        "Invalid year",
    )

    rows_written, rows_rejected = upsert_silver(
        df, tgt_table,
        merge_keys=["clearance_id", "source_system"],
        comment="Silver crimes/clearances aggregate data from CA DOJ OpenJustice.",
    )

    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.{tgt_table}
        SET TBLPROPERTIES ('source' = '{src_table}')
    """)
    log_metrics(src_table, f"silver.{tgt_table}", rows_read, rows_written, rows_rejected)


if not DEMO_MODE:
    transform_openjustice_crimes_clearances()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. System C — AdHocExports Transformations

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6.1 adhoc_client → silver.client

# COMMAND ----------

def transform_adhoc_client() -> None:
    src_table = f"{CATALOG}.{BRONZE_SCHEMA}.adhoc_client"
    tgt_table = "client"

    df_raw  = spark.table(src_table)
    rows_read = df_raw.count()
    df = rename_to_snake_case(df_raw)

    df = (
        df
        .withColumn("client_id",    F.col("clientid").cast(T.LongType()))
        .withColumn("first_name",   clean_string(F.col("first_name")))
        .withColumn("last_name",    clean_string(F.col("last_name")))
        .withColumn("dob",          standardise_date(F.col("dob")))
        .withColumn("dob",          null_future_date(F.col("dob")))
        .withColumn("phone",        F.regexp_replace(F.col("phone"), r"[^0-9]", ""))
        .withColumn("email",        F.lower(F.trim(F.col("email"))))
        .withColumn("zip_code",     F.regexp_replace(F.col("zip_code"), r"[^0-9\-]", ""))
        .withColumn("source_system", F.lit("AdHocExports"))
        .withColumn("silver_load_ts", F.lit(PIPELINE_RUN).cast(T.TimestampType()))
    )

    # DQ: phone must be 10 digits if present
    df = add_reject_flag(
        df,
        F.col("phone").isNotNull() & (F.length(F.col("phone")) != 10),
        "Invalid phone length",
    )
    df = add_reject_flag(df, F.col("client_id").isNull(), "NULL client_id")

    rows_written, rows_rejected = upsert_silver(
        df, tgt_table,
        merge_keys=["client_id", "source_system"],
        comment="Silver client records from AdHocExports. Contains PII.",
    )

    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.{tgt_table}
        SET TBLPROPERTIES ('source' = '{src_table}')
    """)
    log_metrics(src_table, f"silver.{tgt_table}", rows_read, rows_written, rows_rejected)


if not DEMO_MODE:
    transform_adhoc_client()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6.2 adhoc_incident → silver.incident

# COMMAND ----------

def transform_adhoc_incident() -> None:
    src_table = f"{CATALOG}.{BRONZE_SCHEMA}.adhoc_incident"
    tgt_table = "incident"

    df_raw  = spark.table(src_table)
    rows_read = df_raw.count()
    df = rename_to_snake_case(df_raw)

    df = (
        df
        .withColumn("incident_id",    F.col("incidentid").cast(T.LongType()))
        .withColumn("client_id",      F.col("clientid").cast(T.LongType()))
        .withColumn("incident_date",  standardise_date(F.col("incident_date")))
        .withColumn("incident_type",  clean_string(F.col("incident_type")))
        .withColumn("location",       F.trim(F.col("location")))
        .withColumn("source_system",  F.lit("AdHocExports"))
        .withColumn("silver_load_ts", F.lit(PIPELINE_RUN).cast(T.TimestampType()))
    )

    df = add_reject_flag(df, F.col("incident_id").isNull(), "NULL incident_id")

    rows_written, rows_rejected = upsert_silver(
        df, tgt_table,
        merge_keys=["incident_id", "source_system"],
        comment="Silver incident records from AdHocExports.",
    )

    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.{tgt_table}
        SET TBLPROPERTIES ('source' = '{src_table}')
    """)
    log_metrics(src_table, f"silver.{tgt_table}", rows_read, rows_written, rows_rejected)


if not DEMO_MODE:
    transform_adhoc_incident()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Reference / Code Tables — Separated into Dedicated Silver Tables

# COMMAND ----------
# MAGIC %md
# MAGIC ### 7.1 adhoc_lookup → silver.code_jurisdiction, silver.code_case_type, silver.code_event_type

# COMMAND ----------

def transform_adhoc_lookup_tables() -> None:
    """
    The AdHocExports Lookup_* tables contain mixed reference data.
    We split them by LookupType into dedicated silver code tables to align
    with the staging target schema (Stg_Code_*).
    """
    src_table = f"{CATALOG}.{BRONZE_SCHEMA}.adhoc_lookup"

    try:
        df_raw = spark.table(src_table)
    except Exception as exc:
        logger.warning("Lookup table not found, skipping: %s", exc)
        return

    rows_read = df_raw.count()
    df = rename_to_snake_case(df_raw)

    # Ensure we have the expected columns; fill with NULL if absent
    for col_name in ["lookup_type", "lookup_code", "lookup_description",
                     "is_active", "effective_date", "expiry_date"]:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(T.StringType()))

    df = (
        df
        .withColumn("lookup_code",        clean_string(F.col("lookup_code")))
        .withColumn("lookup_description", F.trim(F.col("lookup_description")))
        .withColumn("is_active",          F.col("is_active").cast(T.BooleanType()))
        .withColumn("effective_date",     standardise_date(F.col("effective_date")))
        .withColumn("expiry_date",        standardise_date(F.col("expiry_date")))
        .withColumn("source_system",      F.lit("AdHocExports"))
        .withColumn("silver_load_ts",     F.lit(PIPELINE_RUN).cast(T.TimestampType()))
    )

    # ------------ Jurisdiction codes -------------------------------------------
    df_jurisdiction = df.filter(F.upper(F.col("lookup_type")) == "JURISDICTION").select(
        F.col("lookup_code").alias("jurisdiction_code"),
        F.col("lookup_description").alias("jurisdiction_name"),
        F.lit(None).cast(T.StringType()).alias("county_code"),
        F.lit("CA").alias("state_code"),
        F.col("is_active"),
        F.col("effective_date"),
        F.col("expiry_date"),
        F.col("source_system"),
        F.col("silver_load_ts"),
    )
    if df_jurisdiction.count() > 0:
        rows_written, rows_rejected = upsert_silver(
            df_jurisdiction, "code_jurisdiction",
            merge_keys=["jurisdiction_code"],
            comment="Silver jurisdiction reference codes.",
        )
        log_metrics(src_table, "silver.code_jurisdiction", df_jurisdiction.count(), rows_written, rows_rejected)

    # ------------ Case type codes -----------------------------------------------
    df_case_type = df.filter(F.upper(F.col("lookup_type")) == "CASETYPE").select(
        F.col("lookup_code").alias("case_type_code"),
        F.col("lookup_description").alias("case_type_description"),
        F.lit(None).cast(T.StringType()).alias("category"),
        F.col("is_active"),
        F.col("effective_date"),
        F.col("expiry_date"),
        F.col("source_system"),
        F.col("silver_load_ts"),
    )
    if df_case_type.count() > 0:
        rows_written, rows_rejected = upsert_silver(
            df_case_type, "code_case_type",
            merge_keys=["case_type_code"],
            comment="Silver case type reference codes.",
        )
        log_metrics(src_table, "silver.code_case_type", df_case_type.count(), rows_written, rows_rejected)

    # ------------ Event type codes -----------------------------------------------
    df_event_type = df.filter(F.upper(F.col("lookup_type")) == "EVENTTYPE").select(
        F.col("lookup_code").alias("event_type_code"),
        F.col("lookup_description").alias("event_type_description"),
        F.lit(None).cast(T.StringType()).alias("category"),
        F.col("is_active"),
        F.col("effective_date"),
        F.col("expiry_date"),
        F.col("source_system"),
        F.col("silver_load_ts"),
    )
    if df_event_type.count() > 0:
        rows_written, rows_rejected = upsert_silver(
            df_event_type, "code_event_type",
            merge_keys=["event_type_code"],
            comment="Silver event type reference codes.",
        )
        log_metrics(src_table, "silver.code_event_type", df_event_type.count(), rows_written, rows_rejected)

    logger.info("Lookup table split complete. rows_read=%d", rows_read)


if not DEMO_MODE:
    transform_adhoc_lookup_tables()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Silver Pipeline Metrics Summary

# COMMAND ----------

print(f"\n{'=' * 70}")
print(f"BRONZE → SILVER PIPELINE SUMMARY — {PIPELINE_RUN}")
print(f"{'=' * 70}")

spark.sql(f"""
    SELECT
        source_table,
        target_table,
        rows_read,
        rows_written,
        rows_rejected,
        ROUND(rejection_rate * 100, 2) AS rejection_pct
    FROM {CATALOG}.{SILVER_SCHEMA}.pipeline_metrics
    WHERE pipeline_run = '{PIPELINE_RUN}'
    ORDER BY source_table
""").show(truncate=False)

# COMMAND ----------
print("Bronze → Silver transformation complete.")
