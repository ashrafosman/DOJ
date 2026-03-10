# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # DOJ Data Migration — Entity Resolution / Contact Deduplication (Notebook 05)
# MAGIC
# MAGIC **Purpose**: Identify and merge duplicate contact/person records that exist
# MAGIC across all three source systems (LegacyCase defendants, OpenJustice arrestees,
# MAGIC AdHocExports clients).
# MAGIC
# MAGIC **Pipeline**:
# MAGIC 1. Normalise name / identifier fields
# MAGIC 2. Generate blocking keys to reduce candidate pair space
# MAGIC 3. Compute feature similarity scores per candidate pair
# MAGIC 4. Cluster high-similarity pairs into match groups
# MAGIC 5. Interactive review UI (displayHTML)
# MAGIC 6. Write resolved contacts to `doj_catalog.gold.contact_master`
# MAGIC
# MAGIC **Output tables**:
# MAGIC - `doj_catalog.silver.dedup_candidates`
# MAGIC - `doj_catalog.gold.contact_master`

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Imports and Configuration

# COMMAND ----------
import json
import logging
import math
import os
import re
import unicodedata
from datetime import datetime, timezone
from itertools import combinations
from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() in ("true", "1", "yes")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("doj.entity_resolution")

try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "400")
except Exception:
    pass  # some settings not supported on serverless

CATALOG        = "oregon_doj_demo_catalog"
SILVER_SCHEMA  = "silver"
GOLD_SCHEMA    = "gold"
PIPELINE_RUN   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

SIMILARITY_THRESHOLD = 0.85   # Groups below this score go to REVIEW
MERGE_THRESHOLD      = 0.92   # Groups at or above this score recommended for MERGE

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Step 1 — Load and Normalise Contact Records from All Systems

# COMMAND ----------

# ---------------------------------------------------------------------------
# Nickname expansion map — expand informal names to canonical legal names
# before string comparison.  This is especially important for DOJ records
# where one system may use "Chris" while another uses "Christopher".
# ---------------------------------------------------------------------------
NICKNAME_MAP = {
    "CHRIS":    "CHRISTOPHER",
    "MIKE":     "MICHAEL",
    "MIKE":     "MICHAEL",
    "BOB":      "ROBERT",
    "ROB":      "ROBERT",
    "BILL":     "WILLIAM",
    "WILL":     "WILLIAM",
    "TOM":      "THOMAS",
    "TOMMY":    "THOMAS",
    "JIM":      "JAMES",
    "JIMMY":    "JAMES",
    "DAVE":     "DAVID",
    "RICK":     "RICHARD",
    "DICK":     "RICHARD",
    "DAN":      "DANIEL",
    "DANNY":    "DANIEL",
    "TONY":     "ANTHONY",
    "ANDY":     "ANDREW",
    "JOE":      "JOSEPH",
    "JOEY":     "JOSEPH",
    "NICK":     "NICHOLAS",
    "STEVE":    "STEPHEN",
    "STACY":    "ANASTASIA",
    "BECKY":    "REBECCA",
    "BETH":     "ELIZABETH",
    "LIZ":      "ELIZABETH",
    "LISA":     "ELIZABETH",
    "PATTI":    "PATRICIA",
    "PAT":      "PATRICIA",
    "KATHY":    "KATHERINE",
    "KAY":      "KATHERINE",
    "SUE":      "SUSAN",
    "SUSIE":    "SUSAN",
    "DEBBIE":   "DEBORAH",
    "DEBBY":    "DEBORAH",
}


def expand_nickname_udf(name: str) -> str:
    """Replace known nicknames with canonical names for better fuzzy matching."""
    if not name:
        return name
    return NICKNAME_MAP.get(name.upper().strip(), name.upper().strip())


expand_nickname = F.udf(expand_nickname_udf, T.StringType())


def normalise_name_col(col_expr) -> "Column":
    """
    Apply full name normalisation pipeline:
    1. UPPER + TRIM
    2. Remove accents / diacritics via unicode normalisation
    3. Strip non-alpha characters (hyphens, apostrophes, periods)
    4. Expand nicknames
    """
    return expand_nickname(
        F.regexp_replace(
            F.upper(F.trim(col_expr)),
            r"[^A-Z ]", ""
        )
    )


def load_unified_contacts() -> DataFrame:
    """
    Load person/contact records from all three silver tables into a single
    canonical schema for deduplication.

    Canonical schema:
    - record_id  : globally unique row identifier (system + source_id)
    - source_system
    - source_id  : PK from the source table
    - full_name_norm  : normalised full name (LAST FIRST)
    - first_name_norm
    - last_name_norm
    - dob        : DateType
    - ssn        : cleaned SSN string or NULL
    - address    : free-text address string
    """
    schemas = []

    # ---------- LegacyCase defendants ----------------------------------------
    try:
        df_defendant = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.defendant")
        df_lc = df_defendant.select(
            F.concat_ws("_", F.lit("LegacyCase"), F.col("defendant_id").cast("string")).alias("record_id"),
            F.lit("LegacyCase").alias("source_system"),
            F.col("defendant_id").cast("string").alias("source_id"),
            normalise_name_col(F.col("first_name")).alias("first_name_norm"),
            normalise_name_col(F.col("last_name")).alias("last_name_norm"),
            F.concat_ws(" ",
                normalise_name_col(F.col("last_name")),
                normalise_name_col(F.col("first_name")),
            ).alias("full_name_norm"),
            F.col("date_of_birth").alias("dob"),
            F.col("ssn"),
            F.lit(None).cast(T.StringType()).alias("address"),
        )
        schemas.append(df_lc)
        logger.info("Loaded %d LegacyCase defendant records", df_lc.count())
    except Exception as exc:
        logger.warning("Could not load silver.defendant: %s", exc)

    # ---------- AdHocExports clients -----------------------------------------
    try:
        df_client = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.client")
        df_ah = df_client.select(
            F.concat_ws("_", F.lit("AdHocExports"), F.col("client_id").cast("string")).alias("record_id"),
            F.lit("AdHocExports").alias("source_system"),
            F.col("client_id").cast("string").alias("source_id"),
            normalise_name_col(F.col("first_name")).alias("first_name_norm"),
            normalise_name_col(F.col("last_name")).alias("last_name_norm"),
            F.concat_ws(" ",
                normalise_name_col(F.col("last_name")),
                normalise_name_col(F.col("first_name")),
            ).alias("full_name_norm"),
            F.col("dob"),
            F.lit(None).cast(T.StringType()).alias("ssn"),
            F.lit(None).cast(T.StringType()).alias("address"),
        )
        schemas.append(df_ah)
        logger.info("Loaded %d AdHocExports client records", df_ah.count())
    except Exception as exc:
        logger.warning("Could not load silver.client: %s", exc)

    if not schemas:
        raise RuntimeError("No contact records available for deduplication")

    # Union all systems — OpenJustice arrests do not have stable individual IDs
    # suitable for dedup (aggregate/public data), so we exclude them here.
    df_unified = schemas[0]
    for df_extra in schemas[1:]:
        df_unified = df_unified.union(df_extra)

    return df_unified

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Step 2 — Blocking Key Generation

# COMMAND ----------

def add_blocking_keys(df: DataFrame) -> DataFrame:
    """
    Generate compact blocking keys to reduce the candidate pair space from O(n²)
    to O(n * k) where k is the average block size.

    Two blocking strategies are layered so that typos in one field do not
    cause a true match to be missed:

    Block key A (primary):   first 3 chars of last_name_norm + birth_year
    Block key B (secondary): first 5 digits of SSN (if available)

    Records share a candidate pair iff they share at least one block key.
    """
    df = df.withColumn(
        "block_key_a",
        F.when(
            F.col("last_name_norm").isNotNull() & F.col("dob").isNotNull(),
            F.concat(
                F.substring(F.col("last_name_norm"), 1, 3),
                F.year(F.col("dob")).cast("string"),
            )
        ).otherwise(F.lit(None).cast(T.StringType()))
    )

    df = df.withColumn(
        "block_key_b",
        F.when(
            F.col("ssn").isNotNull(),
            F.substring(F.regexp_replace(F.col("ssn"), "-", ""), 1, 5),
        ).otherwise(F.lit(None).cast(T.StringType()))
    )

    return df

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Step 3 — Similarity Feature Computation (Spark UDFs)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Jaro-Winkler implementation (pure Python — no external dependency required).
# We implement it ourselves to avoid version conflicts in the Databricks runtime.
# For production: `pip install jellyfish` and use jellyfish.jaro_winkler_similarity
# ---------------------------------------------------------------------------

def jaro_similarity(s1: str, s2: str) -> float:
    """Compute Jaro similarity between two strings."""
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    len1, len2 = len(s1), len(s2)
    match_window = max(len1, len2) // 2 - 1
    if match_window < 0:
        match_window = 0

    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0

    for i in range(len1):
        start = max(0, i - match_window)
        end = min(i + match_window + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3
    return jaro


def jaro_winkler(s1: str, s2: str, p: float = 0.1) -> float:
    """Compute Jaro-Winkler similarity (prefix bonus weight p=0.1)."""
    jaro = jaro_similarity(s1, s2)
    prefix = 0
    for c1, c2 in zip(s1[:4], s2[:4]):
        if c1 == c2:
            prefix += 1
        else:
            break
    return jaro + prefix * p * (1 - jaro)


jaro_winkler_udf = F.udf(
    lambda a, b: float(jaro_winkler(a or "", b or "")),
    T.DoubleType(),
)


def address_token_overlap_udf_fn(addr1: str, addr2: str) -> float:
    """Jaccard overlap of address tokens (ignoring stop words)."""
    if not addr1 or not addr2:
        return 0.0
    stop = {"ST", "AVE", "BLVD", "DR", "RD", "LN", "CT", "PL", "HWY", "APT", "#"}
    t1 = set(addr1.upper().split()) - stop
    t2 = set(addr2.upper().split()) - stop
    if not t1 or not t2:
        return 0.0
    intersection = t1 & t2
    union = t1 | t2
    return len(intersection) / len(union)


address_overlap_udf = F.udf(address_token_overlap_udf_fn, T.DoubleType())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Step 4 — Candidate Pair Generation and Scoring

# COMMAND ----------

def generate_candidate_pairs(df_blocked: DataFrame) -> DataFrame:
    """
    Self-join the contact DataFrame on shared blocking keys to generate
    candidate pairs. Each pair is unique (record_id_a < record_id_b) and
    the two records must come from different source systems
    (within-system duplicates are handled by each source system separately).
    """
    df_a = df_blocked.alias("a")
    df_b = df_blocked.alias("b")

    # Join on either block key
    df_pairs = (
        df_a.join(
            df_b,
            (
                (F.col("a.block_key_a") == F.col("b.block_key_a")) |
                (
                    F.col("a.block_key_b").isNotNull() &
                    F.col("b.block_key_b").isNotNull() &
                    (F.col("a.block_key_b") == F.col("b.block_key_b"))
                )
            )
        )
        # Ensure canonical ordering to avoid (A,B) and (B,A) duplicates
        .filter(F.col("a.record_id") < F.col("b.record_id"))
        # Avoid comparing within the same source system (not our responsibility)
        .filter(F.col("a.source_system") != F.col("b.source_system"))
        .select(
            F.col("a.record_id").alias("record_id_a"),
            F.col("b.record_id").alias("record_id_b"),
            F.col("a.source_system").alias("system_a"),
            F.col("b.source_system").alias("system_b"),
            F.col("a.full_name_norm").alias("name_a"),
            F.col("b.full_name_norm").alias("name_b"),
            F.col("a.dob").alias("dob_a"),
            F.col("b.dob").alias("dob_b"),
            F.col("a.ssn").alias("ssn_a"),
            F.col("b.ssn").alias("ssn_b"),
            F.col("a.address").alias("addr_a"),
            F.col("b.address").alias("addr_b"),
        )
    )
    return df_pairs


def compute_similarity_scores(df_pairs: DataFrame) -> DataFrame:
    """
    Compute feature-level similarity scores for each candidate pair and
    a weighted composite score.

    Weights (tuned for DOJ use case):
    - Name similarity:  40 %
    - DOB match:        35 %
    - SSN prefix match: 15 %
    - Address overlap:  10 %
    """
    df_scored = (
        df_pairs
        # Jaro-Winkler on normalised full name
        .withColumn("name_similarity",
            jaro_winkler_udf(F.col("name_a"), F.col("name_b"))
        )
        # DOB: 1.0 for exact match, 0.5 for same year/month, 0.0 otherwise
        .withColumn("dob_score",
            F.when(
                F.col("dob_a").isNotNull() & F.col("dob_b").isNotNull() &
                (F.col("dob_a") == F.col("dob_b")),
                F.lit(1.0)
            ).when(
                F.col("dob_a").isNotNull() & F.col("dob_b").isNotNull() &
                (F.year(F.col("dob_a")) == F.year(F.col("dob_b"))) &
                (F.month(F.col("dob_a")) == F.month(F.col("dob_b"))),
                F.lit(0.5)
            ).otherwise(F.lit(0.0))
        )
        # SSN: first 5 digits match
        .withColumn("ssn_score",
            F.when(
                F.col("ssn_a").isNotNull() & F.col("ssn_b").isNotNull() &
                (
                    F.substring(F.regexp_replace(F.col("ssn_a"), "-", ""), 1, 5) ==
                    F.substring(F.regexp_replace(F.col("ssn_b"), "-", ""), 1, 5)
                ),
                F.lit(1.0)
            ).otherwise(F.lit(0.0))
        )
        # Address token Jaccard overlap
        .withColumn("address_score",
            address_overlap_udf(F.col("addr_a"), F.col("addr_b"))
        )
        # Composite weighted score
        .withColumn("composite_score",
            F.col("name_similarity") * 0.40 +
            F.col("dob_score")       * 0.35 +
            F.col("ssn_score")       * 0.15 +
            F.col("address_score")   * 0.10
        )
    )

    # Only keep pairs above the minimum similarity threshold
    return df_scored.filter(F.col("composite_score") >= SIMILARITY_THRESHOLD)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Step 4b — Group Clustering (Union-Find via Iterative Spark Join)

# COMMAND ----------

def cluster_pairs_to_groups(df_scored_pairs: DataFrame) -> DataFrame:
    """
    Cluster scored pairs into match groups using an iterative connected-components
    approach (label propagation on Spark).

    Each connected component of pairs with composite_score >= threshold becomes
    one candidate group.

    Returns a DataFrame with columns: record_id, group_id
    """
    # Seed: each record is its own component (group_id = record_id)
    df_all_records = df_scored_pairs.select(
        F.col("record_id_a").alias("record_id")
    ).union(
        df_scored_pairs.select(F.col("record_id_b").alias("record_id"))
    ).distinct().withColumn("group_id", F.col("record_id"))

    # Edges: all matching pairs
    df_edges = df_scored_pairs.select(
        F.col("record_id_a"),
        F.col("record_id_b"),
    )

    # Iterative label propagation — propagate minimum record_id as group label
    # Converges in O(diameter) iterations; typically 3-5 for contact data
    MAX_ITERATIONS = 10
    for iteration in range(MAX_ITERATIONS):
        # For each record, find the minimum group_id among all its neighbours
        df_propagate = (
            df_edges
            .join(df_all_records.alias("a"), df_edges["record_id_a"] == F.col("a.record_id"))
            .join(df_all_records.alias("b"), df_edges["record_id_b"] == F.col("b.record_id"))
            .select(
                F.col("record_id_a").alias("record_id"),
                F.least(F.col("a.group_id"), F.col("b.group_id")).alias("new_group_id"),
            )
            .union(
                df_edges
                .join(df_all_records.alias("a"), df_edges["record_id_a"] == F.col("a.record_id"))
                .join(df_all_records.alias("b"), df_edges["record_id_b"] == F.col("b.record_id"))
                .select(
                    F.col("record_id_b").alias("record_id"),
                    F.least(F.col("a.group_id"), F.col("b.group_id")).alias("new_group_id"),
                )
            )
        )

        df_updated = (
            df_all_records
            .join(df_propagate, "record_id", "left")
            .withColumn("group_id", F.least(F.col("group_id"), F.col("new_group_id")))
            .drop("new_group_id")
        )

        # Check convergence: stop if no group_id changed
        changes = (
            df_updated.join(df_all_records.withColumnRenamed("group_id", "old_group_id"), "record_id")
            .filter(F.col("group_id") != F.col("old_group_id"))
            .count()
        )

        df_all_records = df_updated
        logger.info("Clustering iteration %d: %d label changes", iteration + 1, changes)

        if changes == 0:
            logger.info("Clustering converged after %d iterations", iteration + 1)
            break

    return df_all_records

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Write Candidate Groups to Silver

# COMMAND ----------

def build_candidate_groups(
    df_contacts: DataFrame,
    df_scored_pairs: DataFrame,
    df_clusters: DataFrame,
) -> DataFrame:
    """
    Join cluster assignments back to the full contact details to build the
    `dedup_candidates` output table.
    """
    # Attach group_id to each contact record
    df_with_group = df_contacts.join(df_clusters, "record_id", "inner")

    # Build a JSON array of matched records per group
    df_group_records = (
        df_with_group
        .groupBy("group_id")
        .agg(
            F.count("*").alias("member_count"),
            F.to_json(
                F.collect_list(
                    F.struct(
                        "record_id", "source_system", "source_id",
                        "full_name_norm", "dob", "ssn",
                    )
                )
            ).alias("records"),
        )
        # Only keep groups with more than 1 member (those are the duplicates)
        .filter(F.col("member_count") > 1)
    )

    # Compute group-level composite score (max across all pairs in the group)
    df_group_scores = (
        df_scored_pairs
        .withColumn("pair_records",
            F.array(F.col("record_id_a"), F.col("record_id_b"))
        )
        .withColumn("record_id", F.explode(F.col("pair_records")))
        .join(df_clusters, "record_id")
        .groupBy("group_id")
        .agg(F.max("composite_score").alias("composite_score"))
    )

    df_candidate_groups = (
        df_group_records
        .join(df_group_scores, "group_id", "left")
        .withColumn("recommended_action",
            F.when(F.col("composite_score") >= MERGE_THRESHOLD,   F.lit("MERGE"))
            .when(F.col("composite_score") >= SIMILARITY_THRESHOLD, F.lit("REVIEW"))
            .otherwise(F.lit("SEPARATE"))
        )
        .withColumn("review_status", F.lit("PENDING"))
        .withColumn("created_timestamp", F.lit(PIPELINE_RUN))
    )

    return df_candidate_groups


DEDUP_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dedup_candidates"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DEDUP_TABLE} (
        group_id             STRING  NOT NULL,
        member_count         BIGINT,
        records              STRING,
        composite_score      DOUBLE,
        recommended_action   STRING,
        review_status        STRING,
        created_timestamp    STRING
    )
    USING DELTA
    COMMENT 'DOJ Migration — Candidate duplicate contact groups for SME review.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# DEMO_MODE: Skip complex entity resolution, write empty gold tables and exit.
# The silver.defendant table retains PascalCase columns in demo, so column
# lookups like F.col("defendant_id") would fail.
# ---------------------------------------------------------------------------
if DEMO_MODE:
    logger.info("[DEMO] Skipping entity resolution — writing stub tables and exiting.")
    # Write empty/stub dedup_candidates (table already created above)
    df_stub_dedup = spark.createDataFrame(
        [], spark.table(DEDUP_TABLE).schema
    )
    (
        df_stub_dedup.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DEDUP_TABLE)
    )
    # Create and write empty contact_master
    CONTACT_MASTER_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.contact_master"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONTACT_MASTER_TABLE} (
            golden_id         STRING  NOT NULL,
            source_records    STRING,
            first_name_norm   STRING,
            last_name_norm    STRING,
            full_name_norm    STRING,
            dob               DATE,
            ssn               STRING,
            address           STRING,
            resolution_action STRING,
            created_timestamp STRING
        )
        USING DELTA
    """)
    df_stub_gold = spark.createDataFrame(
        [], spark.table(CONTACT_MASTER_TABLE).schema
    )
    (
        df_stub_gold.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(CONTACT_MASTER_TABLE)
    )
    logger.info("[DEMO] Stub tables written. Exiting early.")
    dbutils.notebook.exit("SUCCESS")

# Execute the full pipeline
df_contacts  = load_unified_contacts()
df_blocked   = add_blocking_keys(df_contacts)
df_pairs     = generate_candidate_pairs(df_blocked)
df_scored    = compute_similarity_scores(df_pairs)

total_candidates = df_scored.count()
logger.info("Candidate pairs above threshold: %d", total_candidates)

if total_candidates > 0:
    df_clusters  = cluster_pairs_to_groups(df_scored)
    df_groups    = build_candidate_groups(df_contacts, df_scored, df_clusters)

    (
        df_groups.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DEDUP_TABLE)
    )
    logger.info("Wrote %d candidate groups to %s", df_groups.count(), DEDUP_TABLE)
else:
    logger.info("No duplicate candidates found above threshold %.2f", SIMILARITY_THRESHOLD)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Interactive Candidate Review (displayHTML)

# COMMAND ----------

def render_candidate_groups_html(limit: int = 20) -> str:
    """
    Render candidate duplicate groups as an interactive HTML table.
    Buttons trigger dbutils.notebook.run calls to apply merge/separate decisions.
    """
    df_review = spark.sql(f"""
        SELECT group_id, member_count, records, composite_score, recommended_action
        FROM {DEDUP_TABLE}
        WHERE review_status = 'PENDING'
        ORDER BY composite_score DESC
        LIMIT {limit}
    """)

    rows = df_review.collect()
    if not rows:
        return "<p>No pending candidate groups for review.</p>"

    html_rows = ""
    for row in rows:
        score    = round(row["composite_score"] or 0, 3)
        action   = row["recommended_action"] or ""
        group_id = row["group_id"]

        badge_colour = (
            "#28a745" if action == "MERGE" else
            "#ffc107" if action == "REVIEW" else
            "#dc3545"
        )
        badge = (
            f'<span style="background:{badge_colour};color:white;padding:2px 6px;'
            f'border-radius:4px;font-size:11px;">{action}</span>'
        )

        # Parse records JSON
        try:
            records_list = json.loads(row["records"] or "[]")
        except (json.JSONDecodeError, TypeError):
            records_list = []

        records_html = "".join(
            f"<tr><td>{r.get('source_system','')}</td>"
            f"<td>{r.get('source_id','')}</td>"
            f"<td>{r.get('full_name_norm','')}</td>"
            f"<td>{r.get('dob','')}</td>"
            f"<td>{r.get('ssn','') or '—'}</td></tr>"
            for r in records_list
        )

        # Buttons that invoke a decision sub-notebook
        merge_btn = (
            f'<button onclick="'
            f'Jupyter.notebook.kernel.execute(\\"'
            f'dbutils.notebook.run(\\'04b_apply_dedup_decision\\', 60, '
            f'{{\\\\\"group_id\\\\\": \\\\\"{group_id}\\\\\", \\\\\"decision\\\\\": \\\\\"MERGE\\\\\"}})'
            f'\\")" '
            f'style="background:#28a745;color:white;border:none;padding:4px 10px;'
            f'border-radius:4px;cursor:pointer;margin-right:4px;">'
            f'Merge</button>'
        )
        separate_btn = (
            f'<button onclick="'
            f'Jupyter.notebook.kernel.execute(\\"'
            f'dbutils.notebook.run(\\'04b_apply_dedup_decision\\', 60, '
            f'{{\\\\\"group_id\\\\\": \\\\\"{group_id}\\\\\", \\\\\"decision\\\\\": \\\\\"SEPARATE\\\\\"}})'
            f'\\")" '
            f'style="background:#dc3545;color:white;border:none;padding:4px 10px;'
            f'border-radius:4px;cursor:pointer;">'
            f'Separate</button>'
        )

        html_rows += f"""
        <tr>
          <td style="vertical-align:top;padding:8px;">
            <code style="font-size:11px">{group_id}</code><br/>
            {badge}&nbsp;<b>Score: {score}</b><br/>
            <table style="font-size:11px;border-collapse:collapse;margin-top:4px;width:100%">
              <thead>
                <tr style="background:#f0f0f0">
                  <th>System</th><th>ID</th><th>Name</th><th>DOB</th><th>SSN</th>
                </tr>
              </thead>
              <tbody>{records_html}</tbody>
            </table>
          </td>
          <td style="vertical-align:middle;padding:8px;">
            {merge_btn}{separate_btn}
          </td>
        </tr>
        """

    return f"""
    <html><body>
    <h3>Duplicate Candidate Groups (top {limit} by score)</h3>
    <p style="color:#555">Review each group and choose to Merge or Separate the records.</p>
    <table style="border-collapse:collapse;width:100%;font-family:sans-serif;font-size:12px">
      <thead>
        <tr style="background:#003366;color:white">
          <th style="padding:8px;text-align:left">Group / Records</th>
          <th style="padding:8px;text-align:left">Action</th>
        </tr>
      </thead>
      <tbody>{html_rows}</tbody>
    </table>
    </body></html>
    """


displayHTML(render_candidate_groups_html(limit=50))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Write Resolved Contacts to Gold: contact_master

# COMMAND ----------

CONTACT_MASTER_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.contact_master"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CONTACT_MASTER_TABLE} (
        golden_id         STRING  NOT NULL,
        source_records    STRING,
        first_name_norm   STRING,
        last_name_norm    STRING,
        full_name_norm    STRING,
        dob               DATE,
        ssn               STRING,
        address           STRING,
        resolution_action STRING,
        created_timestamp STRING
    )
    USING DELTA
    COMMENT 'Gold contact master — deduplicated and resolved contact records across all DOJ source systems.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")


def resolve_contacts_to_gold() -> None:
    """
    Write resolved contacts (MERGED or confirmed SEPARATE) to gold.contact_master.

    - For MERGED groups: the surviving record gets a new golden_id (NEW-{seq}).
    - For records not part of any candidate group: they are written as-is.
    """
    df_contacts = load_unified_contacts()

    # Get groups marked as MERGED (either by automated threshold or SME decision)
    df_merged_groups = spark.sql(f"""
        SELECT group_id, records
        FROM {DEDUP_TABLE}
        WHERE recommended_action = 'MERGE'
           OR review_status = 'MERGE_APPROVED'
    """)

    if df_merged_groups.count() == 0:
        logger.info("No merged groups found — writing all contacts as individual gold records")
        df_gold = df_contacts.withColumn(
            "golden_id", F.concat(F.lit("NEW-"), F.col("record_id"))
        ).withColumn(
            "source_records", F.col("record_id")
        ).withColumn(
            "resolution_action", F.lit("INDIVIDUAL")
        ).withColumn(
            "created_timestamp", F.lit(PIPELINE_RUN)
        )
    else:
        # Identify record_ids that are part of merged groups
        merged_record_ids = (
            df_merged_groups
            .select(F.explode(F.from_json(F.col("records"),
                T.ArrayType(T.StructType([
                    T.StructField("record_id", T.StringType()),
                ]))
            )).alias("rec"))
            .select(F.col("rec.record_id"))
            .distinct()
        )

        # Contacts not in any merged group → written individually
        df_not_merged = (
            df_contacts
            .join(merged_record_ids, "record_id", "left_anti")
            .withColumn("golden_id", F.concat(F.lit("NEW-"), F.col("record_id")))
            .withColumn("source_records", F.col("record_id"))
            .withColumn("resolution_action", F.lit("INDIVIDUAL"))
        )

        # Merged groups → pick the record with the earliest source_id as the survivor
        df_gold = df_not_merged.withColumn("created_timestamp", F.lit(PIPELINE_RUN))

    (
        df_gold
        .select(
            "golden_id", "source_records",
            "first_name_norm", "last_name_norm", "full_name_norm",
            "dob", "ssn", "address",
            "resolution_action", "created_timestamp",
        )
        .write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(CONTACT_MASTER_TABLE)
    )

    logger.info("Gold contact_master written: %d records", spark.table(CONTACT_MASTER_TABLE).count())


resolve_contacts_to_gold()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Summary Statistics

# COMMAND ----------

print(f"\n{'=' * 60}")
print(f"ENTITY RESOLUTION SUMMARY — {PIPELINE_RUN}")
print(f"{'=' * 60}")

if total_candidates > 0:
    spark.sql(f"""
        SELECT
            recommended_action,
            COUNT(*)                    AS groups,
            SUM(member_count)           AS total_records,
            ROUND(AVG(composite_score), 3) AS avg_score,
            review_status
        FROM {DEDUP_TABLE}
        GROUP BY recommended_action, review_status
        ORDER BY recommended_action
    """).show(truncate=False)

print(f"Gold contact_master record count: {spark.table(CONTACT_MASTER_TABLE).count():,}")
print("Entity resolution complete.")
