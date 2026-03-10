# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # DOJ Data Migration — LLM Schema Mapping (Notebook 02b)
# MAGIC
# MAGIC **Purpose**: Use an LLM to reason over column-level profiling metadata and
# MAGIC propose source→target field mappings for the DOJ staging schema.
# MAGIC
# MAGIC **Inputs**:
# MAGIC - `doj_catalog.bronze.column_profiles` (from Notebook 02a)
# MAGIC - Hardcoded staging schema definitions (Stg_Case, Stg_Contact, etc.)
# MAGIC
# MAGIC **Outputs**:
# MAGIC - `doj_catalog.bronze.schema_mappings` Delta table
# MAGIC - `abfss://doj@dojstorage.dfs.core.usgovcloudapi.net/mappings/llm_mappings_v{ts}.json`
# MAGIC
# MAGIC **LLM**: `databricks-meta-llama-3-3-70b-instruct` (fallback: `databricks-claude-sonnet-4-6`)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Imports and Configuration

# COMMAND ----------
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("doj.llm_mapping")

try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
except Exception:
    pass  # some settings not supported on serverless

# ---------------------------------------------------------------------------
# DEMO_MODE — bypass secrets; use notebook context credentials instead
# ---------------------------------------------------------------------------
DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() in ("true", "1", "yes")
logger.info("DEMO_MODE = %s", DEMO_MODE)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG        = "doj_catalog"
BRONZE_SCHEMA  = "bronze"
PROFILE_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.column_profiles"
MAPPING_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.schema_mappings"
ADLS_ROOT      = "abfss://doj@dojstorage.dfs.core.usgovcloudapi.net/"
MAPPING_VERSION = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Retrieve workspace URL and token — use notebook context in DEMO_MODE
if DEMO_MODE:
    try:
        _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        _host = _ctx.browserHostName().get()
        WORKSPACE_URL   = f"https://{_host}"
        WORKSPACE_TOKEN = _ctx.apiToken().get()
    except Exception as _e:
        logger.warning("Could not get notebook context credentials: %s. Falling back to env.", _e)
        WORKSPACE_URL   = os.getenv("DATABRICKS_HOST", "https://fevm-oregon-doj-demo.cloud.databricks.com")
        WORKSPACE_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
else:
    WORKSPACE_URL   = dbutils.secrets.get(scope="doj-scope", key="databricks-workspace-url")
    WORKSPACE_TOKEN = dbutils.secrets.get(scope="doj-scope", key="databricks-workspace-token")

# Primary model; fallback used if primary returns errors or is unavailable
PRIMARY_MODEL  = "databricks-meta-llama-3-3-70b-instruct"
FALLBACK_MODEL = "databricks-claude-sonnet-4-6"

# Rate-limit / retry settings
MAX_RETRIES   = 5
INITIAL_DELAY = 2.0   # seconds
MAX_DELAY     = 60.0  # seconds

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Destination Staging Schema Definitions

# COMMAND ----------

# ---------------------------------------------------------------------------
# These are the authoritative target column lists for each staging table.
# The LLM uses these as the candidate destination for each source column.
# Update this dict if the staging schema evolves.
# ---------------------------------------------------------------------------
STAGING_SCHEMA = {
    "Stg_Case": [
        "CaseID",
        "CaseNumber",
        "CaseTypeCode",
        "JurisdictionCode",
        "FilingDate",
        "DispositionDate",
        "DispositionCode",
        "StatusCode",
        "AssignedJudge",
        "DefendantID",
        "SourceSystem",
        "SourceSystemID",
        "LoadTimestamp",
    ],
    "Stg_Contact": [
        "ContactID",
        "FirstName",
        "LastName",
        "MiddleName",
        "DateOfBirth",
        "SSN",
        "Gender",
        "Race",
        "Ethnicity",
        "Address1",
        "Address2",
        "City",
        "StateCode",
        "ZipCode",
        "Phone",
        "Email",
        "ContactTypeCode",
        "SourceSystem",
        "SourceSystemID",
        "LoadTimestamp",
    ],
    "Stg_Participant": [
        "ParticipantID",
        "CaseID",
        "ContactID",
        "RoleCode",
        "StartDate",
        "EndDate",
        "SourceSystem",
        "SourceSystemID",
        "LoadTimestamp",
    ],
    "Stg_Code_Jurisdiction": [
        "JurisdictionCode",
        "JurisdictionName",
        "CountyCode",
        "StateCode",
        "IsActive",
        "EffectiveDate",
        "ExpiryDate",
    ],
    "Stg_Code_CaseType": [
        "CaseTypeCode",
        "CaseTypeDescription",
        "Category",
        "IsActive",
        "EffectiveDate",
        "ExpiryDate",
    ],
    "Stg_Code_EventType": [
        "EventTypeCode",
        "EventTypeDescription",
        "Category",
        "IsActive",
        "EffectiveDate",
        "ExpiryDate",
    ],
}

# Build a flat list of all target columns for use in prompts
ALL_TARGET_COLUMNS = [
    f"{tbl}.{col}"
    for tbl, cols in STAGING_SCHEMA.items()
    for col in cols
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. LLM Client with Exponential Backoff

# COMMAND ----------

def call_llm(
    prompt: str,
    model: str = PRIMARY_MODEL,
    max_tokens: int = 512,
    temperature: float = 0.0,
) -> dict:
    """
    Call the Databricks Foundation Model REST API.

    Uses exponential backoff on HTTP 429 (rate limited) and 503 (unavailable)
    responses. Raises RuntimeError after MAX_RETRIES exhausted.

    Returns the parsed JSON response body.
    """
    endpoint_url = f"{WORKSPACE_URL}/serving-endpoints/{model}/invocations"
    headers = {
        "Authorization": f"Bearer {WORKSPACE_TOKEN}",
        "Content-Type":  "application/json",
    }
    payload = {
        "messages": [
            {
                "role":    "system",
                "content": (
                    "You are a data migration expert assisting the California Department of Justice. "
                    "Respond ONLY with valid JSON. Do not include markdown code fences."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    delay = INITIAL_DELAY
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(endpoint_url, headers=headers, json=payload, timeout=60)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code in (429, 503):
                # Rate limited or service unavailable — back off and retry
                retry_after = float(resp.headers.get("Retry-After", delay))
                sleep_secs = min(retry_after, MAX_DELAY)
                logger.warning(
                    "LLM rate limited (attempt %d/%d). Retrying in %.1fs.",
                    attempt, MAX_RETRIES, sleep_secs,
                )
                time.sleep(sleep_secs)
                delay = min(delay * 2, MAX_DELAY)
                continue

            # Non-retryable error
            resp.raise_for_status()

        except requests.Timeout as exc:
            last_exc = exc
            logger.warning("LLM request timed out (attempt %d/%d)", attempt, MAX_RETRIES)
            time.sleep(min(delay, MAX_DELAY))
            delay = min(delay * 2, MAX_DELAY)

        except requests.RequestException as exc:
            last_exc = exc
            logger.error("LLM request error (attempt %d/%d): %s", attempt, MAX_RETRIES, exc)
            time.sleep(min(delay, MAX_DELAY))
            delay = min(delay * 2, MAX_DELAY)

    raise RuntimeError(
        f"LLM call failed after {MAX_RETRIES} attempts. Last error: {last_exc}"
    )


def parse_llm_mapping_response(raw_response: dict) -> dict:
    """
    Extract the mapping JSON from the LLM response structure.

    Expected LLM output (as a JSON object in the message content):
    {
        "maps_to":   "Stg_Contact.FirstName",
        "confidence": 0.95,
        "rationale":  "Column 'FIRST_NAME' in arrests table contains..."
    }
    """
    try:
        content = raw_response["choices"][0]["message"]["content"].strip()
        # Strip any accidental markdown code fences
        content = content.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        return json.loads(content)
    except (KeyError, IndexError, json.JSONDecodeError) as exc:
        logger.warning("Could not parse LLM response: %s | Raw: %.200s", exc, str(raw_response))
        return {"maps_to": "UNKNOWN", "confidence": 0.0, "rationale": f"Parse error: {exc}"}

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Prompt Construction

# COMMAND ----------

def build_mapping_prompt(profile_row: dict, all_target_columns: list[str]) -> str:
    """
    Construct a concise but information-rich prompt for the LLM to reason about
    the appropriate staging target for a single source column.

    The prompt includes:
    - Source metadata (system, table, column, dtype)
    - Statistical profile (null_rate, cardinality, uniqueness)
    - Top sample values (truncated)
    - Detected regex pattern
    - FK candidate hint
    - Full list of candidate destination columns
    """
    top_vals = profile_row.get("top_values", "[]") or "[]"
    try:
        top_vals_parsed = json.loads(top_vals)
        sample_str = ", ".join(
            f"\"{v['value']}\" ({v['count']}x)"
            for v in top_vals_parsed[:5]
        )
    except (json.JSONDecodeError, KeyError):
        sample_str = top_vals[:200]

    fk_info = ""
    if profile_row.get("fk_candidate"):
        try:
            fk = json.loads(profile_row["fk_candidate"])
            fk_info = (
                f"\n- FK candidate: references {fk.get('references_table')} "
                f"(overlap {fk.get('overlap_ratio', 0):.0%})"
            )
        except (json.JSONDecodeError, TypeError):
            pass

    target_list = "\n".join(f"  - {c}" for c in all_target_columns)

    prompt = f"""You are mapping source database columns to a DOJ staging schema.

SOURCE COLUMN:
- System:    {profile_row['system']}
- Table:     {profile_row['table_name']}
- Column:    {profile_row['column_name']}
- Data type: {profile_row['dtype']}
- Null rate: {profile_row.get('null_rate', 'N/A')}
- Cardinality: {profile_row.get('cardinality', 'N/A')} distinct values
- Uniqueness ratio: {profile_row.get('uniqueness_ratio', 'N/A')}
- Detected pattern: {profile_row.get('detected_pattern', 'UNKNOWN')}
- Top values: {sample_str}{fk_info}

CANDIDATE TARGET COLUMNS (format: StagingTable.ColumnName):
{target_list}

TASK: Select the single best-matching target column for this source column.
If no reasonable match exists, use "NO_MATCH".

Respond with ONLY a JSON object in this exact format:
{{
    "maps_to":    "<StagingTable.ColumnName or NO_MATCH>",
    "confidence": <0.0-1.0>,
    "rationale":  "<one sentence explaining the match>"
}}"""

    return prompt

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Main Mapping Loop

# COMMAND ----------

def generate_mappings_for_system(
    system: str,
    profile_rows: list[dict],
    model: str = PRIMARY_MODEL,
) -> list[dict]:
    """
    For each profiled column in a system, call the LLM and accumulate
    mapping result rows.
    """
    results: list[dict] = []
    total = len(profile_rows)

    for idx, row in enumerate(profile_rows, 1):
        logger.info(
            "[%s] Mapping column %d/%d: %s.%s",
            system, idx, total, row["table_name"], row["column_name"]
        )

        prompt = build_mapping_prompt(row, ALL_TARGET_COLUMNS)

        # Try primary model; fall back to secondary on failure
        llm_response = None
        used_model = model
        try:
            llm_response = call_llm(prompt, model=model)
        except RuntimeError as primary_err:
            logger.warning("Primary LLM failed, trying fallback: %s", primary_err)
            try:
                llm_response = call_llm(prompt, model=FALLBACK_MODEL)
                used_model = FALLBACK_MODEL
            except RuntimeError as fallback_err:
                logger.error(
                    "Both models failed for %s.%s: %s",
                    row["table_name"], row["column_name"], fallback_err
                )
                llm_response = None

        if llm_response:
            parsed = parse_llm_mapping_response(llm_response)
        else:
            parsed = {
                "maps_to":    "LLM_ERROR",
                "confidence": 0.0,
                "rationale":  "LLM call failed — manual review required",
            }

        results.append({
            "system":           row["system"],
            "source_table":     row["table_name"],
            "source_column":    row["column_name"],
            "maps_to":          parsed.get("maps_to", "UNKNOWN"),
            "confidence":       float(parsed.get("confidence", 0.0)),
            "rationale":        parsed.get("rationale", ""),
            "llm_model":        used_model,
            "mapping_version":  MAPPING_VERSION,
            "review_status":    "PENDING",
            "reviewer_name":    None,
            "review_timestamp": None,
            "reviewer_note":    None,
            "final_maps_to":    None,
            "created_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    return results

# COMMAND ----------

# ---------------------------------------------------------------------------
# DEMO_MODE: generate deterministic mappings without calling the LLM.
# This produces realistic-looking high-confidence mappings instantly.
# ---------------------------------------------------------------------------
DEMO_MAPPINGS = {
    # LegacyCase — tbl_Defendant
    ("LegacyCase", "legacycase_tbl_defendant", "DefendantID"):    ("Stg_Contact.ContactID",         0.97, "Direct ID mapping from LegacyCase defendant to unified contact entity."),
    ("LegacyCase", "legacycase_tbl_defendant", "LAST_NAME"):      ("Stg_Contact.LastName",           0.99, "Last name field maps directly to contact last name."),
    ("LegacyCase", "legacycase_tbl_defendant", "FIRST_NAME"):     ("Stg_Contact.FirstName",          0.99, "First name field maps directly to contact first name."),
    ("LegacyCase", "legacycase_tbl_defendant", "MIDDLE_INIT"):    ("Stg_Contact.MiddleName",         0.88, "Middle initial maps to MiddleName with truncation note."),
    ("LegacyCase", "legacycase_tbl_defendant", "DOB"):            ("Stg_Contact.DateOfBirth",        0.97, "Date of birth — PII field, direct mapping."),
    ("LegacyCase", "legacycase_tbl_defendant", "RACE_CD"):        ("Stg_Contact.Race",               0.92, "Race code requires lookup table translation to standard values."),
    ("LegacyCase", "legacycase_tbl_defendant", "GENDER_CD"):      ("Stg_Contact.Gender",             0.94, "Gender code needs normalisation (M/F/MALE/FEMALE → standard)."),
    ("LegacyCase", "legacycase_tbl_defendant", "CASE_ID"):        ("Stg_Case.CaseID",                0.96, "Case ID links defendant to case record."),
    ("LegacyCase", "legacycase_tbl_defendant", "CHARGE_CD"):      ("Stg_Case.CaseTypeCode",          0.78, "Charge code loosely maps to case type; may need separate charge table."),
    ("LegacyCase", "legacycase_tbl_defendant", "CHARGE_DESC"):    ("NO_MATCH",                       0.45, "Free-text description — no direct staging column; candidate for notes field."),
    ("LegacyCase", "legacycase_tbl_defendant", "COURT_CD"):       ("Stg_Code_Jurisdiction.JurisdictionCode", 0.85, "Court code maps to jurisdiction code after lookup translation."),
    ("LegacyCase", "legacycase_tbl_defendant", "JUDGE_ID"):       ("Stg_Case.AssignedJudge",         0.90, "Judge ID maps to assigned judge on case record."),
    ("LegacyCase", "legacycase_tbl_defendant", "ARRAIGNMENT_DT"): ("Stg_Case.FilingDate",            0.73, "Arraignment date closest to filing date; verify with SME."),
    ("LegacyCase", "legacycase_tbl_defendant", "DISPOSITION_DT"): ("Stg_Case.DispositionDate",       0.96, "Disposition date direct mapping."),
    ("LegacyCase", "legacycase_tbl_defendant", "DISPOSITION_CD"): ("Stg_Case.DispositionCode",       0.95, "Disposition code maps directly with lookup translation."),
    ("LegacyCase", "legacycase_tbl_defendant", "COUNTY_CD"):      ("Stg_Code_Jurisdiction.CountyCode", 0.91, "County code maps to jurisdiction county."),
    ("LegacyCase", "legacycase_tbl_defendant", "CASE_STATUS_CD"): ("Stg_Case.StatusCode",            0.94, "Case status code maps directly."),
    ("LegacyCase", "legacycase_tbl_defendant", "PRIOR_OFFENSES"): ("NO_MATCH",                       0.35, "Prior offense count has no equivalent staging column; recommend new field."),
    ("LegacyCase", "legacycase_tbl_defendant", "PUBLIC_DEFENDER_FLG"): ("NO_MATCH",                  0.40, "Public defender flag not in staging schema; may need extension."),
    # LegacyCase — tbl_Case
    ("LegacyCase", "legacycase_tbl_case", "CaseID"):              ("Stg_Case.CaseID",                0.99, "Primary key direct mapping."),
    ("LegacyCase", "legacycase_tbl_case", "CASE_TYPE"):           ("Stg_Case.CaseTypeCode",          0.93, "Case type maps to staging case type code."),
    ("LegacyCase", "legacycase_tbl_case", "FILING_DATE"):         ("Stg_Case.FilingDate",            0.98, "Filing date direct mapping."),
    ("LegacyCase", "legacycase_tbl_case", "STATUS"):              ("Stg_Case.StatusCode",            0.95, "Status maps directly to staging status code."),
    ("LegacyCase", "legacycase_tbl_case", "COURT_ID"):            ("Stg_Code_Jurisdiction.JurisdictionCode", 0.86, "Court ID maps to jurisdiction code."),
    ("LegacyCase", "legacycase_tbl_case", "JUDGE_ID"):            ("Stg_Case.AssignedJudge",         0.91, "Judge ID maps to assigned judge."),
    ("LegacyCase", "legacycase_tbl_case", "DA_ID"):               ("NO_MATCH",                       0.38, "District attorney ID has no direct staging column."),
    # LegacyCase — tbl_Event
    ("LegacyCase", "legacycase_tbl_event", "EventID"):            ("NO_MATCH",                       0.52, "Event ID — no direct staging table for events; consider new table."),
    ("LegacyCase", "legacycase_tbl_event", "CASE_ID"):            ("Stg_Case.CaseID",                0.97, "Case ID foreign key links event to case."),
    ("LegacyCase", "legacycase_tbl_event", "EVENT_TYPE"):         ("Stg_Code_EventType.EventTypeCode", 0.89, "Event type maps to EventType code table."),
    ("LegacyCase", "legacycase_tbl_event", "EVENT_DATE"):         ("NO_MATCH",                       0.48, "Event date — needs dedicated event staging table."),
    ("LegacyCase", "legacycase_tbl_event", "COURT_ID"):           ("Stg_Code_Jurisdiction.JurisdictionCode", 0.82, "Court ID maps to jurisdiction."),
    ("LegacyCase", "legacycase_tbl_event", "EVENT_STATUS"):       ("NO_MATCH",                       0.44, "Event status has no direct staging column."),
    ("LegacyCase", "legacycase_tbl_event", "NOTES"):              ("NO_MATCH",                       0.30, "Free-text notes field — no staging column."),
    # OpenJustice — arrests
    ("OpenJustice", "openjustice_arrests", "YEAR"):               ("NO_MATCH",                       0.55, "Year dimension — typically handled as date partition."),
    ("OpenJustice", "openjustice_arrests", "AGENCY"):             ("Stg_Code_Jurisdiction.JurisdictionName", 0.79, "Agency name maps to jurisdiction name."),
    ("OpenJustice", "openjustice_arrests", "RACE"):               ("Stg_Contact.Race",               0.93, "Race field maps to contact race."),
    ("OpenJustice", "openjustice_arrests", "SEX"):                ("Stg_Contact.Gender",             0.91, "Sex/gender field maps to contact gender."),
    ("OpenJustice", "openjustice_arrests", "AGE_GROUP"):          ("NO_MATCH",                       0.42, "Age group — computed field, not stored in staging."),
    ("OpenJustice", "openjustice_arrests", "CHARGE_CATEGORY"):    ("Stg_Case.CaseTypeCode",          0.76, "Charge category loosely maps to case type."),
    ("OpenJustice", "openjustice_arrests", "TOTAL_ARRESTS"):      ("NO_MATCH",                       0.35, "Aggregate count — no staging column for aggregate metrics."),
    ("OpenJustice", "openjustice_arrests", "FELONY_ARRESTS"):     ("NO_MATCH",                       0.35, "Aggregate count — no staging column."),
    # OpenJustice — arrest_dispositions
    ("OpenJustice", "openjustice_arrest_dispositions", "YEAR"):         ("NO_MATCH",           0.55, "Year dimension."),
    ("OpenJustice", "openjustice_arrest_dispositions", "AGENCY_CODE"):  ("Stg_Code_Jurisdiction.JurisdictionCode", 0.81, "Agency code maps to jurisdiction code."),
    ("OpenJustice", "openjustice_arrest_dispositions", "DISPOSITION"):  ("Stg_Case.DispositionCode", 0.88, "Disposition value maps to staging disposition code."),
    ("OpenJustice", "openjustice_arrest_dispositions", "COUNT"):        ("NO_MATCH",           0.32, "Aggregate count — no direct staging field."),
    # OpenJustice — crimes_clearances
    ("OpenJustice", "openjustice_crimes_clearances", "YEAR"):                ("NO_MATCH",      0.55, "Year dimension."),
    ("OpenJustice", "openjustice_crimes_clearances", "COUNTY_CODE"):         ("Stg_Code_Jurisdiction.CountyCode", 0.88, "County code direct mapping."),
    ("OpenJustice", "openjustice_crimes_clearances", "CRIME_TYPE"):          ("Stg_Case.CaseTypeCode", 0.77, "Crime type maps to case type code."),
    ("OpenJustice", "openjustice_crimes_clearances", "CRIMES_REPORTED"):     ("NO_MATCH",      0.30, "Aggregate count."),
    ("OpenJustice", "openjustice_crimes_clearances", "CRIMES_CLEARED"):      ("NO_MATCH",      0.30, "Aggregate count."),
    ("OpenJustice", "openjustice_crimes_clearances", "CLEARANCE_RATE_PCT"):  ("NO_MATCH",      0.30, "Derived metric — no staging column."),
    # AdHocExports — client
    ("AdHocExports", "adhoc_client", "ClientID"):               ("Stg_Contact.ContactID",      0.91, "Client ID maps to unified contact ID."),
    ("AdHocExports", "adhoc_client", "DEFENDANT_REF"):          ("Stg_Participant.ContactID",  0.87, "Defendant reference maps to participant contact."),
    ("AdHocExports", "adhoc_client", "PROGRAM"):                ("Stg_Participant.RoleCode",   0.72, "Program type loosely maps to participant role code."),
    ("AdHocExports", "adhoc_client", "STATUS"):                 ("Stg_Participant.EndDate",    0.58, "Status INACTIVE/CLOSED could derive end date; low confidence."),
    ("AdHocExports", "adhoc_client", "COUNTY"):                 ("Stg_Code_Jurisdiction.JurisdictionName", 0.83, "County name maps to jurisdiction name."),
    ("AdHocExports", "adhoc_client", "ENROLLMENT_DATE"):        ("Stg_Participant.StartDate",  0.94, "Enrollment date maps to participant start date."),
    ("AdHocExports", "adhoc_client", "EXIT_DATE"):              ("Stg_Participant.EndDate",    0.93, "Exit date maps to participant end date."),
    ("AdHocExports", "adhoc_client", "AGE_AT_ENROLLMENT"):      ("NO_MATCH",                   0.41, "Age at enrollment — derived field, not in staging."),
    ("AdHocExports", "adhoc_client", "RISK_LEVEL"):             ("NO_MATCH",                   0.38, "Risk level — no staging column; recommend extension."),
    # AdHocExports — incident
    ("AdHocExports", "adhoc_incident", "IncidentID"):           ("NO_MATCH",                   0.50, "Incident ID — no dedicated incident staging table."),
    ("AdHocExports", "adhoc_incident", "INCIDENT_TYPE"):        ("Stg_Case.CaseTypeCode",      0.75, "Incident type maps to case type code."),
    ("AdHocExports", "adhoc_incident", "INCIDENT_DATE"):        ("Stg_Case.FilingDate",        0.68, "Incident date proxies filing date; verify with SME."),
    ("AdHocExports", "adhoc_incident", "STATUS"):               ("Stg_Case.StatusCode",        0.82, "Incident status maps to case status code."),
    ("AdHocExports", "adhoc_incident", "COUNTY_CODE"):          ("Stg_Code_Jurisdiction.CountyCode", 0.90, "County code direct mapping."),
    ("AdHocExports", "adhoc_incident", "VICTIM_COUNT"):         ("NO_MATCH",                   0.33, "Victim count — no staging column."),
    ("AdHocExports", "adhoc_incident", "ARREST_MADE"):          ("NO_MATCH",                   0.36, "Arrest flag — no direct staging column."),
    # AdHocExports — lookup
    ("AdHocExports", "adhoc_lookup", "LookupCode"):             ("Stg_Code_Jurisdiction.JurisdictionCode", 0.60, "Generic lookup code — jurisdiction code is closest match."),
    ("AdHocExports", "adhoc_lookup", "DOMAIN"):                 ("NO_MATCH",                   0.45, "Lookup domain — no direct staging column."),
    ("AdHocExports", "adhoc_lookup", "CODE"):                   ("Stg_Code_CaseType.CaseTypeCode", 0.62, "Generic code value — case type code is candidate."),
    ("AdHocExports", "adhoc_lookup", "DESCRIPTION"):            ("Stg_Code_CaseType.CaseTypeDescription", 0.65, "Description maps to code table description."),
    ("AdHocExports", "adhoc_lookup", "STATUS"):                 ("Stg_Code_CaseType.IsActive", 0.71, "Status ACTIVE/INACTIVE maps to IsActive flag."),
}


def generate_demo_mappings(profile_rows: list) -> list:
    """Return deterministic mappings for all profiled columns in DEMO_MODE."""
    results = []
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for row in profile_rows:
        key = (row["system"], row["table_name"], row["column_name"])
        maps_to, confidence, rationale = DEMO_MAPPINGS.get(
            key, ("NO_MATCH", 0.50, "Column not in demo mapping table — manual review required.")
        )
        results.append({
            "system":            row["system"],
            "source_table":      row["table_name"],
            "source_column":     row["column_name"],
            "maps_to":           maps_to,
            "confidence":        confidence,
            "rationale":         rationale,
            "llm_model":         "DEMO_SYNTHETIC",
            "mapping_version":   MAPPING_VERSION,
            "review_status":     "PENDING",
            "reviewer_name":     None,
            "review_timestamp":  None,
            "reviewer_note":     None,
            "final_maps_to":     None,
            "created_timestamp": ts,
        })
    return results


# Read profiling results
df_profiles = spark.table(PROFILE_TABLE)

# Exclude metadata columns from mapping (they are system columns, not data fields)
SKIP_COLS = {"_ingest_timestamp", "_source_system", "_source_file", "_raw_file_path"}

df_profiles_filtered = df_profiles.filter(
    ~F.col("column_name").isin(list(SKIP_COLS))
)

# Group profiles by system for orderly processing
all_mapping_rows: list = []

for system_name in ["LegacyCase", "OpenJustice", "AdHocExports"]:
    system_rows = (
        df_profiles_filtered
        .filter(F.col("system") == system_name)
        .orderBy("table_name", "column_name")
        .collect()
    )

    profile_dicts = [row.asDict() for row in system_rows]
    logger.info("Generating mappings for %s: %d columns (DEMO_MODE=%s)", system_name, len(profile_dicts), DEMO_MODE)

    if DEMO_MODE:
        system_mappings = generate_demo_mappings(profile_dicts)
    else:
        system_mappings = generate_mappings_for_system(system_name, profile_dicts)
    all_mapping_rows.extend(system_mappings)

logger.info("Total mapping rows generated: %d", len(all_mapping_rows))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Write Mapping Results to Delta Table

# COMMAND ----------

MAPPING_SCHEMA = T.StructType([
    T.StructField("system",            T.StringType(),  False),
    T.StructField("source_table",      T.StringType(),  False),
    T.StructField("source_column",     T.StringType(),  False),
    T.StructField("maps_to",           T.StringType(),  True),
    T.StructField("confidence",        T.DoubleType(),  True),
    T.StructField("rationale",         T.StringType(),  True),
    T.StructField("llm_model",         T.StringType(),  True),
    T.StructField("mapping_version",   T.StringType(),  False),
    T.StructField("review_status",     T.StringType(),  True),
    T.StructField("reviewer_name",     T.StringType(),  True),
    T.StructField("review_timestamp",  T.StringType(),  True),
    T.StructField("reviewer_note",     T.StringType(),  True),
    T.StructField("final_maps_to",     T.StringType(),  True),
    T.StructField("created_timestamp", T.StringType(),  False),
])

# Create target table if needed
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MAPPING_TABLE} (
        system            STRING  NOT NULL,
        source_table      STRING  NOT NULL,
        source_column     STRING  NOT NULL,
        maps_to           STRING,
        confidence        DOUBLE,
        rationale         STRING,
        llm_model         STRING,
        mapping_version   STRING  NOT NULL,
        review_status     STRING,
        reviewer_name     STRING,
        review_timestamp  STRING,
        reviewer_note     STRING,
        final_maps_to     STRING,
        created_timestamp STRING  NOT NULL
    )
    USING DELTA
    COMMENT 'DOJ Migration — LLM-generated schema mapping proposals awaiting SME review.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

if all_mapping_rows:
    df_mappings = spark.createDataFrame(all_mapping_rows, schema=MAPPING_SCHEMA)

    df_mappings.createOrReplaceTempView("new_mappings")

    # Idempotent MERGE: update same mapping_version rows, insert new ones
    spark.sql(f"""
        MERGE INTO {MAPPING_TABLE} AS target
        USING new_mappings AS source
            ON  target.system          = source.system
            AND target.source_table    = source.source_table
            AND target.source_column   = source.source_column
            AND target.mapping_version = source.mapping_version
        WHEN MATCHED THEN
            UPDATE SET
                target.maps_to           = source.maps_to,
                target.confidence        = source.confidence,
                target.rationale         = source.rationale,
                target.llm_model         = source.llm_model,
                target.review_status     = source.review_status,
                target.created_timestamp = source.created_timestamp
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    logger.info("Mappings merged into %s (version=%s)", MAPPING_TABLE, MAPPING_VERSION)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Export Mapping Artifact to ADLS

# COMMAND ----------

# Write the full mapping JSON to ADLS so it can be consumed by downstream
# notebooks (Silver→Gold, Load Staging) without requiring Delta table reads.
adls_mappings_path = (
    f"abfss://doj@dojstorage.dfs.core.usgovcloudapi.net/"
    f"mappings/llm_mappings_v{MAPPING_VERSION}.json"
)

mappings_json = json.dumps(
    [
        {k: v for k, v in row.items()}
        for row in all_mapping_rows
    ],
    indent=2,
    default=str,
)

if DEMO_MODE:
    logger.info("[DEMO] Skipping ADLS export (no storage connectivity). Mapping count: %d", len(all_mapping_rows))
else:
    dbutils.fs.put(adls_mappings_path, mappings_json, overwrite=True)
    logger.info("Mapping artifact written to ADLS: %s", adls_mappings_path)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Mapping Summary Report

# COMMAND ----------

print(f"\n{'=' * 70}")
print(f"LLM MAPPING SUMMARY — version {MAPPING_VERSION}")
print(f"{'=' * 70}")

spark.sql(f"""
    SELECT
        system,
        COUNT(*)                                          AS total_columns,
        COUNT(CASE WHEN maps_to != 'NO_MATCH'
                    AND maps_to != 'UNKNOWN'
                    AND maps_to != 'LLM_ERROR' THEN 1 END)  AS matched,
        COUNT(CASE WHEN maps_to = 'NO_MATCH' THEN 1 END) AS no_match,
        COUNT(CASE WHEN maps_to = 'LLM_ERROR' THEN 1 END) AS errors,
        ROUND(AVG(confidence), 3)                        AS avg_confidence,
        COUNT(CASE WHEN confidence >= 0.85 THEN 1 END)   AS high_confidence,
        COUNT(CASE WHEN confidence  < 0.75 THEN 1 END)   AS low_confidence
    FROM {MAPPING_TABLE}
    WHERE mapping_version = '{MAPPING_VERSION}'
    GROUP BY system
    ORDER BY system
""").show(truncate=False)

# COMMAND ----------
# Show low-confidence mappings that need SME attention
print("Low-confidence mappings requiring SME review (confidence < 0.75):")
spark.sql(f"""
    SELECT system, source_table, source_column, maps_to,
           ROUND(confidence, 3) AS confidence, rationale
    FROM {MAPPING_TABLE}
    WHERE mapping_version = '{MAPPING_VERSION}'
      AND confidence < 0.75
    ORDER BY confidence ASC
    LIMIT 20
""").show(truncate=False)

# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "mapping_version": MAPPING_VERSION,
    "total_mappings":  len(all_mapping_rows),
    "adls_path":       adls_mappings_path if not DEMO_MODE else "N/A (demo mode)",
    "status":          "SUCCESS",
}))
