# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # DOJ Data Migration — Bronze Ingestion (Notebook 01)
# MAGIC
# MAGIC **Purpose**: Ingest raw data from all three source systems into the Unity Catalog bronze layer.
# MAGIC
# MAGIC | Source System | Type | Tables |
# MAGIC |---|---|---|
# MAGIC | LegacyCase (System A) | SQL Server via JDBC | tbl_Defendant, tbl_Case, tbl_Event |
# MAGIC | OpenJustice (System B) | HTTP CSV download → Auto Loader | arrests, arrest_dispositions, crimes_clearances |
# MAGIC | AdHocExports (System C) | ADLS Excel/CSV → Auto Loader | Client, Incident, Lookup_* |
# MAGIC
# MAGIC **Target**: `doj_catalog.bronze.*`
# MAGIC **ADLS Root**: `abfss://doj@dojstorage.dfs.core.usgovcloudapi.net/`

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Imports and Configuration

# COMMAND ----------
import logging
import os
import time
import random
import string
from datetime import datetime, timezone, timedelta
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, TimestampType, IntegerType, DateType, DoubleType,
    StructType, StructField
)

# ---------------------------------------------------------------------------
# Logging — surface messages in the Databricks driver log and notebook output
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("doj.bronze_ingest")

# ---------------------------------------------------------------------------
# Spark / Unity Catalog optimisation settings
# ---------------------------------------------------------------------------
try:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
except Exception:
    pass  # some settings not supported on serverless

# ---------------------------------------------------------------------------
# DEMO_MODE — when True, use synthetic in-memory data instead of live sources.
# This allows the workflow to run in demo/dev environments without real
# credentials or ADLS access.
# ---------------------------------------------------------------------------
DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() in ("true", "1", "yes")
logger.info("DEMO_MODE = %s", DEMO_MODE)

# ---------------------------------------------------------------------------
# LEGACYCASE_SOURCE — controls where LegacyCase data is read from:
#   "demo"     — generate synthetic in-memory data (default / DEMO_MODE)
#   "lakebase" — read from the real Lakebase PostgreSQL database
#   "jdbc"     — read from the original SQL Server via JDBC (requires secrets)
# Setting LEGACYCASE_SOURCE=lakebase overrides DEMO_MODE for System A only.
# ---------------------------------------------------------------------------
LEGACYCASE_SOURCE = os.getenv("LEGACYCASE_SOURCE", "lakebase").lower()
logger.info("LEGACYCASE_SOURCE = %s", LEGACYCASE_SOURCE)

# Lakebase (PostgreSQL) connection details for the real LegacyCase source
LAKEBASE_HOST = os.getenv(
    "LAKEBASE_HOST",
    "instance-18144f19-a9b9-447a-bb21-51dc465e9969.database.cloud.databricks.com",
)
LAKEBASE_DB = os.getenv("LAKEBASE_DB", "legacycase")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "oregon_doj_demo_catalog"
BRONZE_SCHEMA = "bronze"
ADLS_ROOT = "abfss://doj@dojstorage.dfs.core.usgovcloudapi.net/"
LANDING_ROOT = f"{ADLS_ROOT}landing/"
CHECKPOINTS_ROOT = f"{ADLS_ROOT}_checkpoints/bronze/"

# Ingest timestamp used as a stable batch marker for this run
INGEST_TS = datetime.now(timezone.utc)
INGEST_TS_STR = INGEST_TS.strftime("%Y-%m-%dT%H:%M:%SZ")

# DOJ OpenJustice public data portal base URL
OPENJUSTICE_BASE = "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/"

# PII columns that must be tagged for data governance
PII_TAG_MAP = {
    "tbl_defendant":       ["DOB", "FIRST_NAME", "LAST_NAME"],
    "tbl_case":            [],
    "tbl_event":           [],
    "arrests":             ["RACE", "SEX"],
    "arrest_dispositions": [],
    "crimes_clearances":   [],
    "client":              [],
    "incident":            [],
}

# Seed for reproducible synthetic data
random.seed(42)

# ---------------------------------------------------------------------------
# Helper: ensure database and table exist in Unity Catalog
# ---------------------------------------------------------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Shared Utility Functions

# COMMAND ----------

def add_metadata_columns(df: DataFrame, source_system: str, source_file: str = "") -> DataFrame:
    """
    Append DOJ-standard metadata columns to any ingested DataFrame.

    Columns added
    -------------
    _ingest_timestamp : TimestampType — wall-clock time this row was ingested
    _source_system    : StringType    — logical source system name (e.g. 'LegacyCase')
    _source_file      : StringType    — originating file path or JDBC table identifier
    """
    return (
        df
        .withColumn("_ingest_timestamp", F.lit(INGEST_TS_STR).cast(TimestampType()))
        .withColumn("_source_system", F.lit(source_system).cast(StringType()))
        .withColumn("_source_file", F.lit(source_file).cast(StringType()))
    )


def write_bronze_delta(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
    comment: str = "",
) -> None:
    """
    Write a DataFrame to a Unity Catalog bronze Delta table.

    Parameters
    ----------
    df         : Spark DataFrame with metadata columns already added.
    table_name : Unqualified table name (will be prefixed with doj_catalog.bronze).
    mode       : 'append' (default) or 'overwrite'.
    comment    : Table-level COMMENT string registered in Unity Catalog.
    """
    full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    logger.info("Writing %d rows to %s (mode=%s)", df.count(), full_table, mode)

    writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(full_table)

    # Register comment in Unity Catalog
    if comment:
        safe_comment = comment.replace("'", "\\'")
        spark.sql(f"COMMENT ON TABLE {full_table} IS '{safe_comment}'")

    logger.info("Successfully wrote table %s", full_table)


def tag_pii_columns(table_name: str, pii_columns: list) -> None:
    """
    Apply Unity Catalog column-level PII tags so that downstream data-access
    policies can be enforced by column masking and row filters.
    """
    full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    for col in pii_columns:
        try:
            spark.sql(
                f"""
                ALTER TABLE {full_table}
                ALTER COLUMN {col}
                SET TAGS ('doj_pii' = 'true', 'doj_pii_category' = 'DOJ_SENSITIVE')
                """
            )
            logger.info("Tagged PII column %s.%s", full_table, col)
        except Exception as exc:
            # Column may not exist for all source shapes — log and continue
            logger.warning("Could not tag column %s on %s: %s", col, full_table, exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. System A — LegacyCase (SQL Server JDBC)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.1 LegacyCase Table Definitions

# COMMAND ----------

LEGACYCASE_TABLES = [
    {
        "source_table":   "dbo.tbl_Defendant",
        "lakebase_table": "public.tbl_defendant",
        "bronze_table":   "legacycase_tbl_defendant",
        "partition_col":  "DefendantID",
        "num_partitions": 20,
        "pushdown_query": None,
        "comment": (
            "Raw LegacyCase defendant records. "
            "Contains PII — access restricted to authorised DOJ personnel."
        ),
    },
    {
        "source_table":   "dbo.tbl_Case",
        "lakebase_table": "public.tbl_case",
        "bronze_table":   "legacycase_tbl_case",
        "partition_col":  "CaseID",
        "num_partitions": 10,
        "pushdown_query": None,
        "comment": "Raw LegacyCase case master records.",
    },
    {
        "source_table":   "dbo.tbl_Event",
        "lakebase_table": "public.tbl_event",
        "bronze_table":   "legacycase_tbl_event",
        "partition_col":  "EventID",
        "num_partitions": 10,
        "pushdown_query": None,
        "comment": "Raw LegacyCase event/hearing records.",
    },
]

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.2 LegacyCase Ingestion (JDBC or Synthetic Demo Data)

# COMMAND ----------

def _rand_date(start_year=1970, end_year=2005):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")

def _rand_case_date(start_year=2015, end_year=2024):
    return _rand_date(start_year, end_year)

def generate_legacycase_defendants(n=500):
    first_names = ["James","Maria","Robert","Linda","Michael","Barbara","William","Patricia","David","Jennifer","Carlos","Ana","Marcus","Tanya","Kevin","Denise","Anthony","Sharon","Eric","Angela"]
    last_names  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez","Hernandez","Lopez","Wilson","Anderson","Taylor","Thomas","Moore","Jackson","Martin","Lee"]
    race_codes  = ["W","B","H","A","I","P","OTH","UNK"]
    gender_codes = ["M","F"]
    charge_codes = ["PC187","PC211","PC459","PC288","PC245","HS11352","PC664","PC496","PC530.5","PC207"]
    disp_codes   = ["CONV","ACQ","DISM","PLEA","NOLO","PEND","DIVER"]
    county_codes = ["MULT","WASH","CLAC","LANE","JACK","DOUG","BENT","LINC","YAMI","COOS"]
    courts       = ["C001","C002","C003","C004","C005"]
    rows = []
    for i in range(1, n + 1):
        case_id = f"LC-{random.randint(2015,2024)}-{random.randint(10000,99999)}"
        rows.append((
            case_id,
            f"DEF-{i:06d}",
            random.choice(last_names),
            random.choice(first_names),
            random.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + [""]),
            _rand_date(1960, 2000),
            random.choice(race_codes),
            random.choice(gender_codes),
            random.choice(charge_codes),
            f"Charge description for {random.choice(charge_codes)}",
            random.choice(courts),
            f"JDG-{random.randint(100,999)}",
            _rand_case_date(2015, 2023),
            _rand_case_date(2016, 2024) if random.random() > 0.3 else None,
            random.choice(disp_codes) if random.random() > 0.3 else None,
            f"SEN-{random.randint(1,20):02d}" if random.random() > 0.4 else None,
            random.choice(county_codes),
            random.choice(["OPEN","CLOSED","PENDING","APPEAL"]),
            random.randint(0, 5),
            random.choice(["Y","N"]),
        ))
    schema = StructType([
        StructField("CASE_ID", StringType()),
        StructField("DefendantID", StringType()),
        StructField("LAST_NAME", StringType()),
        StructField("FIRST_NAME", StringType()),
        StructField("MIDDLE_INIT", StringType()),
        StructField("DOB", StringType()),
        StructField("RACE_CD", StringType()),
        StructField("GENDER_CD", StringType()),
        StructField("CHARGE_CD", StringType()),
        StructField("CHARGE_DESC", StringType()),
        StructField("COURT_CD", StringType()),
        StructField("JUDGE_ID", StringType()),
        StructField("ARRAIGNMENT_DT", StringType()),
        StructField("DISPOSITION_DT", StringType()),
        StructField("DISPOSITION_CD", StringType()),
        StructField("SENTENCE_CD", StringType()),
        StructField("COUNTY_CD", StringType()),
        StructField("CASE_STATUS_CD", StringType()),
        StructField("PRIOR_OFFENSES", IntegerType()),
        StructField("PUBLIC_DEFENDER_FLG", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


def generate_legacycase_cases(n=300):
    case_types = ["FELONY","MISDEMEANOR","INFRACTION","JUVENILE"]
    statuses   = ["OPEN","CLOSED","PENDING","APPEAL","TRANSFERRED"]
    rows = []
    for i in range(1, n + 1):
        rows.append((
            f"LC-{random.randint(2015,2024)}-{random.randint(10000,99999)}",
            random.choice(case_types),
            _rand_case_date(2015, 2024),
            random.choice(statuses),
            f"COURT-{random.randint(1,5):03d}",
            f"JUDGE-{random.randint(100,999)}",
            f"DA-{random.randint(1000,9999)}",
        ))
    schema = StructType([
        StructField("CaseID", StringType()),
        StructField("CASE_TYPE", StringType()),
        StructField("FILING_DATE", StringType()),
        StructField("STATUS", StringType()),
        StructField("COURT_ID", StringType()),
        StructField("JUDGE_ID", StringType()),
        StructField("DA_ID", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


def generate_legacycase_events(n=800):
    event_types = ["ARRAIGNMENT","HEARING","TRIAL","SENTENCING","PLEA","CONTINUANCE","MOTION","VERDICT"]
    rows = []
    for i in range(1, n + 1):
        rows.append((
            f"EVT-{i:07d}",
            f"LC-{random.randint(2015,2024)}-{random.randint(10000,99999)}",
            random.choice(event_types),
            _rand_case_date(2015, 2024),
            f"COURT-{random.randint(1,5):03d}",
            random.choice(["COMPLETED","SCHEDULED","CANCELLED","CONTINUED"]),
            f"Note for event {i}" if random.random() > 0.6 else None,
        ))
    schema = StructType([
        StructField("EventID", StringType()),
        StructField("CASE_ID", StringType()),
        StructField("EVENT_TYPE", StringType()),
        StructField("EVENT_DATE", StringType()),
        StructField("COURT_ID", StringType()),
        StructField("EVENT_STATUS", StringType()),
        StructField("NOTES", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


LEGACYCASE_GENERATORS = {
    "legacycase_tbl_defendant": generate_legacycase_defendants,
    "legacycase_tbl_case":      generate_legacycase_cases,
    "legacycase_tbl_event":     generate_legacycase_events,
}


LAKEBASE_INSTANCE = os.getenv("LAKEBASE_INSTANCE", "oregon-doj-lakebase")


def _generate_lakebase_credential(workspace_host: str, notebook_token: str) -> str:
    """
    Call the Databricks REST API to generate a short-lived Lakebase credential.

    Equivalent to:
        databricks database generate-database-credential \\
            --json '{"instance_names": ["<name>"], "request_id": "<uuid>"}'
    """
    import urllib.request as _urllib
    import json as _json
    import uuid as _uuid

    payload = _json.dumps({
        "instance_names": [LAKEBASE_INSTANCE],
        "request_id": str(_uuid.uuid4()),
    }).encode("utf-8")

    url = f"https://{workspace_host}/api/2.0/database/credentials"
    req = _urllib.Request(
        url,
        method="POST",
        headers={
            "Authorization": f"Bearer {notebook_token}",
            "Content-Type": "application/json",
        },
        data=payload,
    )
    with _urllib.urlopen(req, timeout=30) as resp:
        data = _json.loads(resp.read())

    credential = data.get("token") or ""
    if not credential:
        raise RuntimeError(
            f"generate-database-credential returned no token. Response: {data}"
        )
    logger.info("[LAKEBASE] Generated database credential for instance %s", LAKEBASE_INSTANCE)
    return credential


def _read_from_lakebase(lakebase_table: str) -> DataFrame:
    """
    Read a table from the Lakebase PostgreSQL source database and return a
    Spark DataFrame.

    Authentication uses a short-lived credential obtained via the Databricks
    generate-database-credential API (equivalent to the CLI command
    `databricks database generate-database-credential`).
    """
    # Install psycopg2 if not already available (safe to call on every run)
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        import subprocess as _sp
        _sp.check_call(["pip", "install", "psycopg2-binary", "-q"])

    import psycopg2
    import pandas as _pd

    # Get notebook context token (used to call the credential API)
    try:
        nb_token = (
            dbutils.notebook.entry_point
            .getDbutils().notebook().getContext()
            .apiToken().get()
        )
    except Exception:
        nb_token = os.getenv("DATABRICKS_TOKEN", "")

    # Get workspace host for the credential API call
    try:
        workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        workspace_host = os.getenv(
            "DATABRICKS_HOST",
            "fevm-oregon-doj-demo.cloud.databricks.com",
        ).lstrip("https://")

    # Generate a Lakebase-specific credential
    pg_token = _generate_lakebase_credential(workspace_host, nb_token)
    pg_user  = spark.sql("SELECT current_user()").collect()[0][0]

    logger.info(
        "[LAKEBASE] Connecting to %s/%s as %s, table=%s",
        LAKEBASE_HOST, LAKEBASE_DB, pg_user, lakebase_table,
    )

    conn = psycopg2.connect(
        host=LAKEBASE_HOST,
        port=5432,
        dbname=LAKEBASE_DB,
        user=pg_user,
        password=pg_token,
        sslmode="require",
    )
    try:
        df_pd = _pd.read_sql(f'SELECT * FROM {lakebase_table}', conn)
    finally:
        conn.close()

    logger.info("[LAKEBASE] Fetched %d rows from %s", len(df_pd), lakebase_table)
    return spark.createDataFrame(df_pd)


def ingest_legacycase_table(cfg: dict) -> None:
    if LEGACYCASE_SOURCE == "lakebase":
        logger.info("[LAKEBASE] Reading %s from Lakebase table %s",
                    cfg["bronze_table"], cfg["lakebase_table"])
        df_raw   = _read_from_lakebase(cfg["lakebase_table"])
        source_id = f"lakebase:{LAKEBASE_HOST}/{LAKEBASE_DB}/{cfg['lakebase_table']}"

    elif LEGACYCASE_SOURCE == "jdbc" or not DEMO_MODE:
        # Live SQL Server JDBC path — only runs when real credentials exist
        LEGACYCASE_JDBC_URL  = dbutils.secrets.get(scope="doj-scope", key="legacycase-jdbc-url")
        LEGACYCASE_JDBC_USER = dbutils.secrets.get(scope="doj-scope", key="legacycase-jdbc-user")
        LEGACYCASE_JDBC_PWD  = dbutils.secrets.get(scope="doj-scope", key="legacycase-jdbc-password")
        LEGACYCASE_JDBC_PROPS = {
            "user": LEGACYCASE_JDBC_USER, "password": LEGACYCASE_JDBC_PWD,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "fetchsize": "10000", "queryTimeout": "600",
            "encrypt": "true", "trustServerCertificate": "false",
        }
        source_id = f"jdbc:{cfg['source_table']}"
        read_opts = {
            "url":             LEGACYCASE_JDBC_URL,
            "dbtable":         cfg["pushdown_query"] if cfg["pushdown_query"] else cfg["source_table"],
            "partitionColumn": cfg["partition_col"],
            "lowerBound":      "1",
            "upperBound":      "9999999",
            "numPartitions":   str(cfg["num_partitions"]),
        }
        read_opts.update(LEGACYCASE_JDBC_PROPS)
        df_raw = spark.read.format("jdbc").options(**read_opts).load()

    else:
        # DEMO_MODE — use synthetic in-memory data
        logger.info("[DEMO] Generating synthetic data for %s", cfg["bronze_table"])
        gen_fn   = LEGACYCASE_GENERATORS[cfg["bronze_table"]]
        df_raw   = gen_fn()
        source_id = f"demo:synthetic:{cfg['bronze_table']}"

    df_meta = add_metadata_columns(df_raw, source_system="LegacyCase", source_file=source_id)
    write_bronze_delta(df_meta, cfg["bronze_table"], mode="overwrite", comment=cfg["comment"])

    pii_cols = PII_TAG_MAP.get(cfg["bronze_table"].replace("legacycase_", ""), [])
    if pii_cols:
        tag_pii_columns(cfg["bronze_table"], pii_cols)

    logger.info("Completed LegacyCase ingest: %s", cfg["bronze_table"])


for table_cfg in LEGACYCASE_TABLES:
    ingest_legacycase_table(table_cfg)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. System B — OpenJustice (HTTP CSV or Synthetic Data)

# COMMAND ----------

OPENJUSTICE_YEAR = "2022"

OPENJUSTICE_FILES = [
    {
        "filename":       f"OnlineArrestData{OPENJUSTICE_YEAR}.csv",
        "bronze_table":   "openjustice_arrests",
        "landing_subdir": "openjustice/arrests/",
        "comment":        f"CA DOJ OpenJustice arrest data — annual export {OPENJUSTICE_YEAR}.",
    },
    {
        "filename":       f"OnlineArrestDispositionData{OPENJUSTICE_YEAR}.csv",
        "bronze_table":   "openjustice_arrest_dispositions",
        "landing_subdir": "openjustice/arrest_dispositions/",
        "comment":        f"CA DOJ OpenJustice arrest disposition data — annual export {OPENJUSTICE_YEAR}.",
    },
    {
        "filename":       f"CrimesClearancesData{OPENJUSTICE_YEAR}.csv",
        "bronze_table":   "openjustice_crimes_clearances",
        "landing_subdir": "openjustice/crimes_clearances/",
        "comment":        f"CA DOJ OpenJustice crimes/clearances aggregate data — annual export {OPENJUSTICE_YEAR}.",
    },
]


def generate_openjustice_arrests(n=600):
    agencies = ["PORTLAND PD","MULTNOMAH SO","OREGON STATE POLICE","SALEM PD","EUGENE PD","GRESHAM PD","BEAVERTON PD","HILLSBORO PD","MEDFORD PD","BEND PD"]
    races   = ["WHITE","BLACK","HISPANIC","ASIAN","AMERICAN INDIAN","PACIFIC ISLANDER","OTHER","UNKNOWN"]
    charges = ["ASSAULT","THEFT","DUI","DRUG POSSESSION","BURGLARY","ROBBERY","FRAUD","VANDALISM","TRESPASS","DISORDERLY CONDUCT"]
    rows = []
    for i in range(n):
        rows.append((
            str(random.randint(2015,2024)),
            random.choice(agencies),
            random.choice(races),
            random.choice(["M","F"]),
            str(random.randint(18,75)),
            random.choice(charges),
            str(random.randint(1,500)),
            str(random.randint(0,50)),
        ))
    schema = StructType([
        StructField("YEAR", StringType()),
        StructField("AGENCY", StringType()),
        StructField("RACE", StringType()),
        StructField("SEX", StringType()),
        StructField("AGE_GROUP", StringType()),
        StructField("CHARGE_CATEGORY", StringType()),
        StructField("TOTAL_ARRESTS", StringType()),
        StructField("FELONY_ARRESTS", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


def generate_openjustice_dispositions(n=500):
    dispositions = ["CONVICTED","ACQUITTED","DISMISSED","PLEA BARGAIN","DIVERTED","PENDING"]
    rows = []
    for i in range(n):
        rows.append((
            str(random.randint(2015,2024)),
            f"AGENCY-{random.randint(1,20):02d}",
            random.choice(dispositions),
            str(random.randint(1,300)),
        ))
    schema = StructType([
        StructField("YEAR", StringType()),
        StructField("AGENCY_CODE", StringType()),
        StructField("DISPOSITION", StringType()),
        StructField("COUNT", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


def generate_openjustice_clearances(n=200):
    crime_types = ["VIOLENT","PROPERTY","DRUG","OTHER"]
    rows = []
    for i in range(n):
        reported = random.randint(100, 5000)
        cleared  = int(reported * random.uniform(0.2, 0.8))
        rows.append((
            str(random.randint(2015,2024)),
            f"COUNTY-{random.randint(1,36):02d}",
            random.choice(crime_types),
            str(reported),
            str(cleared),
            f"{cleared/reported*100:.1f}",
        ))
    schema = StructType([
        StructField("YEAR", StringType()),
        StructField("COUNTY_CODE", StringType()),
        StructField("CRIME_TYPE", StringType()),
        StructField("CRIMES_REPORTED", StringType()),
        StructField("CRIMES_CLEARED", StringType()),
        StructField("CLEARANCE_RATE_PCT", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


OPENJUSTICE_GENERATORS = {
    "openjustice_arrests":              generate_openjustice_arrests,
    "openjustice_arrest_dispositions":  generate_openjustice_dispositions,
    "openjustice_crimes_clearances":    generate_openjustice_clearances,
}


def ingest_openjustice_table(cfg: dict) -> None:
    if DEMO_MODE:
        logger.info("[DEMO] Generating synthetic OpenJustice data for %s", cfg["bronze_table"])
        gen_fn = OPENJUSTICE_GENERATORS[cfg["bronze_table"]]
        df_raw = gen_fn()
        source_id = f"demo:synthetic:{cfg['bronze_table']}"
    else:
        import requests
        url = f"{OPENJUSTICE_BASE}{cfg['filename']}"
        adls_path = f"{LANDING_ROOT}{cfg['landing_subdir']}{cfg['filename']}"
        local_tmp = f"/tmp/{cfg['filename']}"
        logger.info("Downloading OpenJustice file: %s", url)
        with requests.get(url, stream=True, timeout=300) as resp:
            resp.raise_for_status()
            with open(local_tmp, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    fh.write(chunk)
        dbutils.fs.cp(f"file://{local_tmp}", adls_path, recurse=False)
        landing_dir = f"{LANDING_ROOT}{cfg['landing_subdir']}"
        checkpoint_path = f"{CHECKPOINTS_ROOT}{cfg['bronze_table']}/"
        df_raw = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}schema/")
            .option("header", "true").option("inferSchema", "true")
            .load(landing_dir)
            .withColumn("_source_file_raw", F.col("_metadata.file_path"))
        )
        df_meta = add_metadata_columns(df_raw, source_system="OpenJustice", source_file=adls_path).drop("_source_file_raw")
        full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{cfg['bronze_table']}"
        query = (df_meta.writeStream.format("delta").outputMode("append")
                 .option("checkpointLocation", checkpoint_path)
                 .option("mergeSchema", "true").trigger(availableNow=True).toTable(full_table))
        query.awaitTermination(timeout=1800)
        safe_comment = cfg["comment"].replace("'", "\\'")
        spark.sql(f"COMMENT ON TABLE {full_table} IS '{safe_comment}'")
        pii_cols = PII_TAG_MAP.get(cfg["bronze_table"].replace("openjustice_", ""), [])
        if pii_cols:
            tag_pii_columns(cfg["bronze_table"], pii_cols)
        return

    df_meta = add_metadata_columns(df_raw, source_system="OpenJustice", source_file=source_id)
    write_bronze_delta(df_meta, cfg["bronze_table"], mode="overwrite", comment=cfg["comment"])

    pii_cols = PII_TAG_MAP.get(cfg["bronze_table"].replace("openjustice_", ""), [])
    if pii_cols:
        tag_pii_columns(cfg["bronze_table"], pii_cols)

    logger.info("Completed OpenJustice ingest: %s", cfg["bronze_table"])


for oj_cfg in OPENJUSTICE_FILES:
    ingest_openjustice_table(oj_cfg)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. System C — AdHocExports (ADLS or Synthetic Data)

# COMMAND ----------

ADHOC_LANDING_ROOT = f"{LANDING_ROOT}adhoc_exports/"

ADHOC_TABLES = [
    {
        "landing_subdir": "client/",
        "bronze_table":   "adhoc_client",
        "format":         "csv",
        "comment":        "AdHocExports — Client records exported from case management system.",
    },
    {
        "landing_subdir": "incident/",
        "bronze_table":   "adhoc_incident",
        "format":         "csv",
        "comment":        "AdHocExports — Incident reports.",
    },
    {
        "landing_subdir": "lookup/",
        "bronze_table":   "adhoc_lookup",
        "format":         "csv",
        "comment":        "AdHocExports — Reference/lookup tables (Lookup_*).",
    },
]


def generate_adhoc_clients(n=400):
    programs = ["VICTIM SERVICES","WITNESS PROTECTION","REENTRY","DIVERSION","COMMUNITY SUPERVISION"]
    statuses = ["ACTIVE","INACTIVE","PENDING","CLOSED","TRANSFERRED"]
    counties = ["MULTNOMAH","WASHINGTON","CLACKAMAS","LANE","JACKSON","DOUGLAS","BENTON","LINCOLN","YAMHILL","COOS"]
    rows = []
    for i in range(1, n + 1):
        rows.append((
            f"CLT-{i:06d}",
            f"DEF-{random.randint(1,500):06d}",
            random.choice(programs),
            random.choice(statuses),
            random.choice(counties),
            _rand_case_date(2015, 2024),
            _rand_case_date(2020, 2025) if random.random() > 0.4 else None,
            str(random.randint(18,75)),
            random.choice(["HIGH","MEDIUM","LOW"]),
        ))
    schema = StructType([
        StructField("ClientID", StringType()),
        StructField("DEFENDANT_REF", StringType()),
        StructField("PROGRAM", StringType()),
        StructField("STATUS", StringType()),
        StructField("COUNTY", StringType()),
        StructField("ENROLLMENT_DATE", StringType()),
        StructField("EXIT_DATE", StringType()),
        StructField("AGE_AT_ENROLLMENT", StringType()),
        StructField("RISK_LEVEL", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


def generate_adhoc_incidents(n=350):
    incident_types = ["DOMESTIC VIOLENCE","ASSAULT","THEFT","FRAUD","DRUG RELATED","PROPERTY CRIME","TRAFFIC","OTHER"]
    statuses = ["OPEN","CLOSED","UNDER INVESTIGATION","REFERRED","RESOLVED"]
    rows = []
    for i in range(1, n + 1):
        rows.append((
            f"INC-{i:06d}",
            random.choice(incident_types),
            _rand_case_date(2018, 2024),
            random.choice(statuses),
            f"COUNTY-{random.randint(1,36):02d}",
            str(random.randint(0,5)),
            random.choice(["YES","NO"]),
        ))
    schema = StructType([
        StructField("IncidentID", StringType()),
        StructField("INCIDENT_TYPE", StringType()),
        StructField("INCIDENT_DATE", StringType()),
        StructField("STATUS", StringType()),
        StructField("COUNTY_CODE", StringType()),
        StructField("VICTIM_COUNT", StringType()),
        StructField("ARREST_MADE", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


def generate_adhoc_lookups(n=150):
    lookup_domains = [
        ("RACE", ["WHITE","BLACK","HISPANIC","ASIAN","NATIVE AMERICAN","PACIFIC ISLANDER","OTHER","UNKNOWN"]),
        ("GENDER", ["MALE","FEMALE","NON-BINARY","UNKNOWN"]),
        ("DISPOSITION", ["CONVICTED","ACQUITTED","DISMISSED","PLEA","DIVERTED","PENDING"]),
        ("CASE_STATUS", ["OPEN","CLOSED","PENDING","APPEAL","TRANSFERRED"]),
        ("COUNTY", ["MULTNOMAH","WASHINGTON","CLACKAMAS","LANE","JACKSON"]),
    ]
    rows = []
    code_counter = 1
    for domain, values in lookup_domains:
        for i, val in enumerate(values):
            rows.append((
                f"LKP-{code_counter:04d}",
                domain,
                f"{domain[:3]}{i+1:02d}",
                val,
                "ACTIVE",
            ))
            code_counter += 1
    schema = StructType([
        StructField("LookupCode", StringType()),
        StructField("DOMAIN", StringType()),
        StructField("CODE", StringType()),
        StructField("DESCRIPTION", StringType()),
        StructField("STATUS", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


ADHOC_GENERATORS = {
    "adhoc_client":   generate_adhoc_clients,
    "adhoc_incident": generate_adhoc_incidents,
    "adhoc_lookup":   generate_adhoc_lookups,
}


def convert_xlsx_to_csv_in_landing(landing_dir: str) -> None:
    """
    Scan the landing directory for .xlsx files and convert each to CSV.
    Only runs in live mode (not DEMO_MODE).
    """
    import io
    import pandas as pd

    try:
        files = dbutils.fs.ls(landing_dir)
    except Exception:
        logger.warning("Landing directory not found, skipping xlsx conversion: %s", landing_dir)
        return

    xlsx_files = [f for f in files if f.path.endswith(".xlsx")]
    if not xlsx_files:
        return

    for file_info in xlsx_files:
        adls_path = file_info.path
        filename = file_info.name
        logger.info("Converting Excel workbook: %s", adls_path)
        raw_bytes = dbutils.fs.head(adls_path, file_info.size)
        buf = io.BytesIO(raw_bytes.encode("latin-1"))
        try:
            xl = pd.ExcelFile(buf, engine="openpyxl")
        except Exception as exc:
            logger.error("Failed to open Excel file %s: %s", adls_path, exc)
            continue
        for sheet_name in xl.sheet_names:
            df_sheet = xl.parse(sheet_name)
            csv_filename = filename.replace(".xlsx", f"_{sheet_name}.csv")
            csv_path = f"{landing_dir}{csv_filename}"
            csv_buf = io.StringIO()
            df_sheet.to_csv(csv_buf, index=False)
            dbutils.fs.put(csv_path, csv_buf.getvalue(), overwrite=True)
            logger.info("Wrote sheet '%s' → %s", sheet_name, csv_path)
        archive_path = adls_path.replace(landing_dir, f"{landing_dir}archived/")
        dbutils.fs.mv(adls_path, archive_path)


def ingest_adhoc_table(cfg: dict) -> None:
    if DEMO_MODE:
        logger.info("[DEMO] Generating synthetic AdHoc data for %s", cfg["bronze_table"])
        gen_fn = ADHOC_GENERATORS[cfg["bronze_table"]]
        df_raw = gen_fn()
        source_id = f"demo:synthetic:{cfg['bronze_table']}"
        df_meta = add_metadata_columns(df_raw, source_system="AdHocExports", source_file=source_id)
        write_bronze_delta(df_meta, cfg["bronze_table"], mode="overwrite", comment=cfg["comment"])
        pii_cols = PII_TAG_MAP.get(cfg["bronze_table"].replace("adhoc_", ""), [])
        if pii_cols:
            tag_pii_columns(cfg["bronze_table"], pii_cols)
        logger.info("Completed AdHoc ingest: %s", cfg["bronze_table"])
        return

    # Live ADLS Auto Loader path
    landing_dir = f"{ADHOC_LANDING_ROOT}{cfg['landing_subdir']}"
    checkpoint_path = f"{CHECKPOINTS_ROOT}{cfg['bronze_table']}/"
    convert_xlsx_to_csv_in_landing(landing_dir)

    logger.info("Starting Auto Loader ingest: %s from %s", cfg["bronze_table"], landing_dir)
    df_raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", cfg["format"])
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}schema/")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("header", "true").option("inferSchema", "true")
        .option("encoding", "UTF-8").option("multiLine", "true")
        .option("escape", '"')
        .option("badRecordsPath", f"{ADLS_ROOT}_bad_records/{cfg['bronze_table']}/")
        .load(landing_dir)
        .withColumn("_raw_file_path", F.col("_metadata.file_path"))
    )
    df_meta = add_metadata_columns(df_raw, source_system="AdHocExports",
                                   source_file=f"autoloader:{landing_dir}")
    full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{cfg['bronze_table']}"
    query = (
        df_meta.writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true").trigger(availableNow=True).toTable(full_table)
    )
    query.awaitTermination(timeout=1800)
    safe_comment = cfg["comment"].replace("'", "\\'")
    spark.sql(f"COMMENT ON TABLE {full_table} IS '{safe_comment}'")
    pii_cols = PII_TAG_MAP.get(cfg["bronze_table"].replace("adhoc_", ""), [])
    if pii_cols:
        tag_pii_columns(cfg["bronze_table"], pii_cols)
    logger.info("Completed Auto Loader ingest: %s", cfg["bronze_table"])


for adhoc_cfg in ADHOC_TABLES:
    ingest_adhoc_table(adhoc_cfg)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Post-Ingestion Validation and Summary

# COMMAND ----------

def log_bronze_table_summary() -> None:
    """
    Print row counts for every bronze table ingested in this run.
    """
    all_tables = (
        [c["bronze_table"] for c in LEGACYCASE_TABLES]
        + [c["bronze_table"] for c in OPENJUSTICE_FILES]
        + [c["bronze_table"] for c in ADHOC_TABLES]
    )

    print("\n" + "=" * 70)
    print(f"BRONZE INGESTION SUMMARY — {INGEST_TS_STR}")
    print(f"MODE: {'DEMO (synthetic data)' if DEMO_MODE else 'LIVE (real sources)'}")
    print("=" * 70)
    print(f"{'Table':<45} {'Row Count':>12}")
    print("-" * 70)

    for tbl in all_tables:
        full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{tbl}"
        try:
            cnt = spark.table(full_table).count()
            print(f"{full_table:<45} {cnt:>12,}")
        except Exception as exc:
            print(f"{full_table:<45} ERROR: {exc}")

    print("=" * 70)


log_bronze_table_summary()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Optimize Bronze Delta Tables

# COMMAND ----------

def optimize_bronze_tables() -> None:
    """
    Run OPTIMIZE with ZORDER on high-cardinality join keys for each bronze table.
    """
    optimize_specs = {
        "legacycase_tbl_defendant":            "DefendantID",
        "legacycase_tbl_case":                 "CaseID",
        "legacycase_tbl_event":                "EventID",
        "openjustice_arrests":                 "YEAR",
        "openjustice_arrest_dispositions":     "YEAR",
        "openjustice_crimes_clearances":       "YEAR",
        "adhoc_client":                        "ClientID",
        "adhoc_incident":                      "IncidentID",
        "adhoc_lookup":                        "LookupCode",
    }

    for tbl, zorder_col in optimize_specs.items():
        full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{tbl}"
        try:
            spark.sql(f"OPTIMIZE {full_table} ZORDER BY ({zorder_col})")
            logger.info("Optimized %s ZORDER BY %s", full_table, zorder_col)
        except Exception as exc:
            logger.warning("OPTIMIZE failed for %s: %s", full_table, exc)


optimize_bronze_tables()

# COMMAND ----------
print("Bronze ingestion complete.")
