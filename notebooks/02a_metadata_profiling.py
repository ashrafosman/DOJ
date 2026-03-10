# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # DOJ Data Migration — Bronze Metadata Profiling (Notebook 02a)
# MAGIC
# MAGIC **Purpose**: Deterministic Phase 1 profiling of all bronze tables.
# MAGIC Produces a structured `column_profiles` Delta table consumed by Notebook 02b
# MAGIC (LLM schema mapping) and by SMEs during review.
# MAGIC
# MAGIC **Output table**: `doj_catalog.bronze.column_profiles`
# MAGIC
# MAGIC **Idempotency**: Results are merged by `(system, table_name, column_name)` so
# MAGIC re-runs update existing rows rather than duplicating them.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Imports and Configuration

# COMMAND ----------
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("doj.profiling")

# ---------------------------------------------------------------------------
# Spark settings
# ---------------------------------------------------------------------------
try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
except Exception:
    pass  # some settings not supported on serverless

CATALOG       = "doj_catalog"
BRONZE_SCHEMA = "bronze"
PROFILE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.column_profiles"
PROFILE_TS    = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# ---------------------------------------------------------------------------
# All bronze tables across the three source systems
# ---------------------------------------------------------------------------
BRONZE_TABLE_REGISTRY = {
    "LegacyCase": [
        f"{CATALOG}.{BRONZE_SCHEMA}.legacycase_tbl_defendant",
        f"{CATALOG}.{BRONZE_SCHEMA}.legacycase_tbl_case",
        f"{CATALOG}.{BRONZE_SCHEMA}.legacycase_tbl_event",
    ],
    "OpenJustice": [
        f"{CATALOG}.{BRONZE_SCHEMA}.openjustice_arrests",
        f"{CATALOG}.{BRONZE_SCHEMA}.openjustice_arrest_dispositions",
        f"{CATALOG}.{BRONZE_SCHEMA}.openjustice_crimes_clearances",
    ],
    "AdHocExports": [
        f"{CATALOG}.{BRONZE_SCHEMA}.adhoc_client",
        f"{CATALOG}.{BRONZE_SCHEMA}.adhoc_incident",
        f"{CATALOG}.{BRONZE_SCHEMA}.adhoc_lookup",
    ],
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Schema: column_profiles Output Table

# COMMAND ----------

PROFILES_SCHEMA = T.StructType([
    T.StructField("system",            T.StringType(),  False),
    T.StructField("table_name",        T.StringType(),  False),
    T.StructField("column_name",       T.StringType(),  False),
    T.StructField("dtype",             T.StringType(),  True),
    T.StructField("null_rate",         T.DoubleType(),  True),
    T.StructField("uniqueness_ratio",  T.DoubleType(),  True),
    T.StructField("cardinality",       T.LongType(),    True),
    T.StructField("top_values",        T.StringType(),  True),   # JSON array string
    T.StructField("min_value",         T.StringType(),  True),
    T.StructField("max_value",         T.StringType(),  True),
    T.StructField("avg_value",         T.DoubleType(),  True),
    T.StructField("detected_pattern",  T.StringType(),  True),
    T.StructField("fk_candidate",      T.StringType(),  True),   # JSON object or null
    T.StructField("profile_timestamp", T.StringType(),  False),
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Pattern Detection

# COMMAND ----------

# ---------------------------------------------------------------------------
# Compiled regex patterns for common DOJ data patterns.
# Ordered from most-specific to least-specific so the first match wins.
# ---------------------------------------------------------------------------
REGEX_PATTERNS = [
    ("SSN",        re.compile(r"^\d{3}-\d{2}-\d{4}$")),
    ("SSN_NODASH",  re.compile(r"^\d{9}$")),
    ("DATE_ISO",    re.compile(r"^\d{4}-\d{2}-\d{2}$")),
    ("DATE_US",     re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")),
    ("DATETIME",    re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}")),
    ("PHONE",       re.compile(r"^[\(]?\d{3}[\)\-\s]?\d{3}[\-\s]?\d{4}$")),
    ("EMAIL",       re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")),
    ("ZIPCODE",     re.compile(r"^\d{5}(-\d{4})?$")),
    ("CODE_ALPHA",  re.compile(r"^[A-Z]{1,6}$")),
    ("CODE_ALNUM",  re.compile(r"^[A-Z0-9]{2,10}$")),
    ("ID_NUMERIC",  re.compile(r"^\d{1,20}$")),
    ("ID_UUID",     re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)),
    ("CURRENCY",    re.compile(r"^\$?\d{1,3}(,\d{3})*(\.\d{2})?$")),
]


def detect_pattern(sample_values: list[str]) -> str:
    """
    Analyse up to 100 non-null sample values and return the most-prevalent
    named pattern, or 'FREE_TEXT' if no pattern matches a majority.
    """
    if not sample_values:
        return "UNKNOWN"

    # Strip whitespace for matching, but keep empty strings as evidence of nulls
    cleaned = [v.strip() for v in sample_values if v and v.strip()]
    if not cleaned:
        return "EMPTY"

    pattern_counts: dict[str, int] = {}
    for value in cleaned[:200]:      # Cap at 200 samples for speed
        for name, regex in REGEX_PATTERNS:
            if regex.match(value):
                pattern_counts[name] = pattern_counts.get(name, 0) + 1
                break

    if not pattern_counts:
        return "FREE_TEXT"

    best_pattern, best_count = max(pattern_counts.items(), key=lambda x: x[1])
    # Only report a pattern if it covers >50 % of sampled values
    if best_count / len(cleaned) >= 0.50:
        return best_pattern

    return "FREE_TEXT"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. FK Candidate Detection

# COMMAND ----------

def build_pk_universe(tables: list[str]) -> dict[str, set]:
    """
    For each table in the registry, collect the distinct values of the
    first (and most likely PK) integer or string column as a reference set.

    Returns a dict mapping full_table_name → frozenset of distinct values.

    We limit each PK set to 500 k values to avoid driver OOM on large tables.
    """
    pk_universe: dict[str, set] = {}
    SAMPLE_LIMIT = 500_000

    for full_table in tables:
        try:
            df = spark.table(full_table)
            # Heuristic: column named *ID or the first column
            id_cols = [c for c in df.columns if c.upper().endswith("ID")]
            pk_col = id_cols[0] if id_cols else df.columns[0]
            vals = (
                df.select(F.col(pk_col).cast("string").alias("v"))
                .dropna()
                .distinct()
                .limit(SAMPLE_LIMIT)
                .toPandas()["v"]
                .tolist()
            )
            pk_universe[full_table] = set(vals)
            logger.info("PK universe for %s.%s: %d distinct values",
                        full_table, pk_col, len(vals))
        except Exception as exc:
            logger.warning("Could not build PK universe for %s: %s", full_table, exc)

    return pk_universe


def find_fk_candidate(
    col_values: set,
    pk_universe: dict[str, set],
    min_overlap_ratio: float = 0.80,
) -> dict | None:
    """
    Check whether the given column value set is a subset (≥80 %) of any
    known PK universe.  If so, return metadata indicating the FK relationship.
    """
    if not col_values:
        return None

    best: dict | None = None
    best_ratio = 0.0

    for ref_table, ref_values in pk_universe.items():
        if not ref_values:
            continue
        overlap = len(col_values & ref_values)
        ratio = overlap / len(col_values)
        if ratio >= min_overlap_ratio and ratio > best_ratio:
            best_ratio = ratio
            best = {
                "references_table":  ref_table,
                "overlap_ratio":     round(ratio, 4),
                "sample_overlap":    overlap,
                "col_distinct_vals": len(col_values),
            }

    return best

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Column Profiling Core

# COMMAND ----------

def profile_column(
    df: DataFrame,
    col_name: str,
    total_rows: int,
    pk_universe: dict[str, set],
    max_top_values: int = 10,
) -> dict[str, Any]:
    """
    Compute all profiling metrics for a single column.

    Returns a dict matching the PROFILES_SCHEMA structure (minus system/table).
    """
    col_expr = F.col(f"`{col_name}`")  # Back-tick quoting handles reserved words
    dtype = str(df.schema[col_name].dataType)

    # ---- Null rate --------------------------------------------------------
    null_count_row = df.select(
        F.count(F.when(col_expr.isNull() | (col_expr.cast("string") == ""), None).otherwise(1))
        .alias("non_null")
    ).collect()[0]
    non_null = null_count_row["non_null"] or 0
    null_rate = round(1.0 - (non_null / total_rows), 6) if total_rows > 0 else 1.0

    # ---- Cardinality and uniqueness ---------------------------------------
    cardinality_row = df.select(
        F.countDistinct(col_expr).alias("cardinality")
    ).collect()[0]
    cardinality = cardinality_row["cardinality"] or 0
    uniqueness_ratio = round(cardinality / total_rows, 6) if total_rows > 0 else 0.0

    # ---- Top 10 values ----------------------------------------------------
    top_values_rows = (
        df.groupBy(col_expr.alias("value"))
        .count()
        .orderBy(F.desc("count"))
        .limit(max_top_values)
        .collect()
    )
    top_values = json.dumps([
        {"value": str(r["value"]), "count": r["count"]}
        for r in top_values_rows
    ])

    # ---- Min / Max / Avg (numeric columns only) ----------------------------
    min_val = max_val = None
    avg_val = None

    is_numeric = isinstance(
        df.schema[col_name].dataType,
        (T.IntegerType, T.LongType, T.DoubleType, T.FloatType, T.DecimalType, T.ShortType)
    )
    if is_numeric:
        stats_row = df.select(
            F.min(col_expr).cast("string").alias("min_v"),
            F.max(col_expr).cast("string").alias("max_v"),
            F.avg(col_expr.cast("double")).alias("avg_v"),
        ).collect()[0]
        min_val = stats_row["min_v"]
        max_val = stats_row["max_v"]
        avg_val = float(stats_row["avg_v"]) if stats_row["avg_v"] is not None else None
    else:
        # For string/date columns, min/max are lexicographic
        stats_row = df.select(
            F.min(col_expr.cast("string")).alias("min_v"),
            F.max(col_expr.cast("string")).alias("max_v"),
        ).collect()[0]
        min_val = stats_row["min_v"]
        max_val = stats_row["max_v"]

    # ---- Pattern detection (sample 500 values) ----------------------------
    sample_vals = (
        df.select(col_expr.cast("string").alias("v"))
        .dropna()
        .sample(fraction=min(500.0 / max(total_rows, 1), 1.0), seed=42)
        .limit(500)
        .toPandas()["v"]
        .tolist()
    )
    detected_pattern = detect_pattern(sample_vals)

    # ---- FK candidate detection -------------------------------------------
    if uniqueness_ratio < 0.95 and cardinality < 200_000:
        # Only test FK for columns that look like foreign keys (low-ish uniqueness)
        col_value_set = set(str(v["value"]) for v in top_values_rows if v["value"] is not None)
        # Supplement with sample for better coverage
        col_value_set.update(str(v) for v in sample_vals[:1000] if v)
        fk_candidate = find_fk_candidate(col_value_set, pk_universe)
    else:
        fk_candidate = None

    return {
        "dtype":            dtype,
        "null_rate":        null_rate,
        "uniqueness_ratio": uniqueness_ratio,
        "cardinality":      cardinality,
        "top_values":       top_values,
        "min_value":        str(min_val) if min_val is not None else None,
        "max_value":        str(max_val) if max_val is not None else None,
        "avg_value":        avg_val,
        "detected_pattern": detected_pattern,
        "fk_candidate":     json.dumps(fk_candidate) if fk_candidate else None,
        "profile_timestamp": PROFILE_TS,
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Main Profiling Loop

# COMMAND ----------

def profile_table(
    system: str,
    full_table: str,
    pk_universe: dict[str, set],
) -> list[dict]:
    """
    Profile every column in a bronze Delta table and return a list of row dicts.
    Skips internal metadata columns (_ingest_timestamp, _source_system, _source_file).
    """
    table_short = full_table.split(".")[-1]
    logger.info("Profiling table: %s (system=%s)", full_table, system)

    try:
        df = spark.table(full_table)
    except Exception as exc:
        logger.error("Cannot read table %s: %s", full_table, exc)
        return []

    # Exclude ingestion metadata columns from profiling
    skip_cols = {"_ingest_timestamp", "_source_system", "_source_file", "_raw_file_path"}
    profile_cols = [c for c in df.columns if c not in skip_cols]

    total_rows = df.count()
    logger.info("Table %s: %d rows, %d columns to profile", table_short, total_rows, len(profile_cols))

    if total_rows == 0:
        logger.warning("Table %s is empty — skipping profiling", full_table)
        return []

    rows: list[dict] = []
    for col_name in profile_cols:
        try:
            metrics = profile_column(df, col_name, total_rows, pk_universe)
            rows.append({
                "system":      system,
                "table_name":  full_table,
                "column_name": col_name,
                **metrics,
            })
        except Exception as exc:
            logger.error("Failed to profile column %s.%s: %s", full_table, col_name, exc)
            rows.append({
                "system":            system,
                "table_name":        full_table,
                "column_name":       col_name,
                "dtype":             str(df.schema[col_name].dataType),
                "null_rate":         None,
                "uniqueness_ratio":  None,
                "cardinality":       None,
                "top_values":        None,
                "min_value":         None,
                "max_value":         None,
                "avg_value":         None,
                "detected_pattern":  "ERROR",
                "fk_candidate":      None,
                "profile_timestamp": PROFILE_TS,
            })

    logger.info("Profiled %d columns from %s", len(rows), full_table)
    return rows

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Collect All Tables and Build PK Universe

# COMMAND ----------

all_tables_flat = [
    tbl
    for tables in BRONZE_TABLE_REGISTRY.values()
    for tbl in tables
]

# Build PK reference sets once — used by all FK candidate checks
pk_universe = build_pk_universe(all_tables_flat)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Run Profiling and Write Results

# COMMAND ----------

all_profile_rows: list[dict] = []

for system_name, table_list in BRONZE_TABLE_REGISTRY.items():
    for full_table_name in table_list:
        table_rows = profile_table(system_name, full_table_name, pk_universe)
        all_profile_rows.extend(table_rows)

logger.info("Total column profiles collected: %d", len(all_profile_rows))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Create / Merge Profile Results into Delta Table

# COMMAND ----------

if all_profile_rows:
    df_profiles = spark.createDataFrame(all_profile_rows, schema=PROFILES_SCHEMA)

    # Create the target table if it does not yet exist
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {PROFILE_TABLE} (
            system            STRING  NOT NULL,
            table_name        STRING  NOT NULL,
            column_name       STRING  NOT NULL,
            dtype             STRING,
            null_rate         DOUBLE,
            uniqueness_ratio  DOUBLE,
            cardinality       BIGINT,
            top_values        STRING,
            min_value         STRING,
            max_value         STRING,
            avg_value         DOUBLE,
            detected_pattern  STRING,
            fk_candidate      STRING,
            profile_timestamp STRING  NOT NULL
        )
        USING DELTA
        COMMENT 'DOJ Migration — Bronze column profiling results. Updated on each pipeline run.'
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    # ---------------------------------------------------------------------------
    # Idempotent MERGE: update existing profiles, insert new ones.
    # Key is (system, table_name, column_name).
    # ---------------------------------------------------------------------------
    df_profiles.createOrReplaceTempView("new_profiles")

    spark.sql(f"""
        MERGE INTO {PROFILE_TABLE} AS target
        USING new_profiles AS source
            ON  target.system      = source.system
            AND target.table_name  = source.table_name
            AND target.column_name = source.column_name
        WHEN MATCHED THEN
            UPDATE SET
                target.dtype             = source.dtype,
                target.null_rate         = source.null_rate,
                target.uniqueness_ratio  = source.uniqueness_ratio,
                target.cardinality       = source.cardinality,
                target.top_values        = source.top_values,
                target.min_value         = source.min_value,
                target.max_value         = source.max_value,
                target.avg_value         = source.avg_value,
                target.detected_pattern  = source.detected_pattern,
                target.fk_candidate      = source.fk_candidate,
                target.profile_timestamp = source.profile_timestamp
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    logger.info("Profiles merged into %s", PROFILE_TABLE)
else:
    logger.warning("No profile rows to write — all source tables may be empty or unavailable")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Profiling Summary Report

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
        system,
        COUNT(DISTINCT table_name)  AS tables_profiled,
        COUNT(*)                    AS columns_profiled,
        ROUND(AVG(null_rate), 4)    AS avg_null_rate,
        COUNT(CASE WHEN fk_candidate IS NOT NULL THEN 1 END) AS fk_candidates_found,
        COUNT(CASE WHEN detected_pattern = 'SSN' THEN 1 END) AS ssn_columns_detected
    FROM {PROFILE_TABLE}
    WHERE profile_timestamp = '{PROFILE_TS}'
    GROUP BY system
    ORDER BY system
""")

print("\n" + "=" * 70)
print(f"PROFILING SUMMARY — {PROFILE_TS}")
print("=" * 70)
summary.show(truncate=False)

# COMMAND ----------
# Show top-10 columns with highest null rates (potential data quality issues)
print("Top 10 columns with highest null rates:")
spark.sql(f"""
    SELECT system, table_name, column_name, dtype,
           ROUND(null_rate, 4) AS null_rate,
           detected_pattern
    FROM {PROFILE_TABLE}
    WHERE profile_timestamp = '{PROFILE_TS}'
    ORDER BY null_rate DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------
# Show detected FK relationships
print("FK Candidate Relationships:")
spark.sql(f"""
    SELECT system, table_name, column_name, fk_candidate
    FROM {PROFILE_TABLE}
    WHERE fk_candidate IS NOT NULL
      AND profile_timestamp = '{PROFILE_TS}'
    ORDER BY system, table_name
""").show(truncate=False)

# COMMAND ----------
print("Metadata profiling complete.")
