"""
DOJ Data Migration Pipeline — FastAPI application entry point.

Exposes REST endpoints for file upload, job monitoring, reconciliation review,
and pipeline observability.  Azure AD / Entra ID JWT auth is wired in but
controlled by the ``AUTH_ENABLED`` environment variable so it can be disabled
for local development.

Environment variables
---------------------
DATABRICKS_HOST      Databricks workspace URL (required)
DATABRICKS_TOKEN     Databricks PAT or service-principal secret (required)
WAREHOUSE_ID         SQL warehouse ID for statement execution (required)
DATABRICKS_JOB_ID    Numeric ID of the migration orchestration job (required)
ADLS_UPLOAD_PATH     DBFS/ADLS path prefix for uploaded files
                     (default: /mnt/doj-landing/uploads)
AUTH_ENABLED         Set to "true" to enforce Azure AD JWT validation
                     (default: false)
AZURE_TENANT_ID      Required when AUTH_ENABLED=true
AZURE_CLIENT_ID      Required when AUTH_ENABLED=true
DEMO_MODE            Set to "true" to return mock data instead of live queries
ALLOWED_ORIGINS      Comma-separated list of CORS origins
                     (default: http://localhost:5173,http://localhost:3000)
"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, AsyncGenerator, Optional

from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    File,
    HTTPException,
    Query,
    UploadFile,
    status,
)
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from databricks_client import DatabricksClient, DatabricksError
from models import (
    IssueDecisionRequest,
    IssueStatus,
    IssueType,
    JobStatus,
    JobStatusValue,
    PipelineStage,
    ReconciliationIssue,
    SourceSystem,
    StageMetrics,
    UploadResponse,
)
from routes.pipeline import router as pipeline_router

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AUTH_ENABLED: bool = os.getenv("AUTH_ENABLED", "false").lower() == "true"
DEMO_MODE: bool = os.getenv("DEMO_MODE", "false").lower() == "true"
DATABRICKS_JOB_ID: int = int(os.getenv("DATABRICKS_JOB_ID", "0"))
ADLS_UPLOAD_PATH: str = os.getenv("ADLS_UPLOAD_PATH", "/mnt/doj-landing/uploads")
CATALOG: str = os.getenv("CATALOG", "")
_PIPELINE_TABLE: str = f"{CATALOG}.pipeline.job_status" if CATALOG else ""
ALLOWED_ORIGINS: list[str] = [
    o.strip()
    for o in os.getenv(
        "ALLOWED_ORIGINS", "http://localhost:5173,http://localhost:3000"
    ).split(",")
]

ALLOWED_EXTENSIONS = {".xlsx", ".csv"}
ALLOWED_MIME_TYPES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "text/csv",
    "application/csv",
    "application/octet-stream",  # some browsers send this for xlsx
}

# ---------------------------------------------------------------------------
# Pipeline stage constants
# ---------------------------------------------------------------------------

# Display names for the 7 stages the frontend shows (index matches position)
_STAGE_NAMES = ["Upload", "Bronze", "Mapping", "Silver", "Gold", "Staging", "Complete"]

# Maps Databricks task_key → (PipelineStage enum, stage_index in _STAGE_NAMES)
_TASK_TO_STAGE: dict[str, tuple[PipelineStage, int]] = {
    "ingest_bronze":      (PipelineStage.BRONZE,  1),
    "metadata_profiling": (PipelineStage.BRONZE,  1),
    "llm_schema_mapping": (PipelineStage.MAPPING, 2),
    "bronze_to_silver":   (PipelineStage.SILVER,  3),
    "entity_resolution":  (PipelineStage.SILVER,  3),
    "silver_to_gold":     (PipelineStage.GOLD,    4),
}

_STAGE_ENUM_TO_IDX: dict[str, int] = {
    "UPLOAD":   0,
    "BRONZE":   1,
    "MAPPING":  2,
    "SILVER":   3,
    "GOLD":     4,
    "STAGING":  5,
    "COMPLETE": 6,
    "FAILED":   6,
}

# ---------------------------------------------------------------------------
# Enum/string helper
# ---------------------------------------------------------------------------


def _val(e: Any) -> str:
    """
    Return the plain string value of an enum member OR pass through a string.

    JobStatus uses ``model_config = {"use_enum_values": True}`` which causes
    Pydantic to store enum fields as their string values after model creation.
    Any code that calls ``.value`` on those fields will raise AttributeError.
    Use ``_val()`` everywhere instead of ``.value``.
    """
    return e.value if hasattr(e, "value") else str(e)


def _escape_sql(s: str) -> str:
    """Escape single-quotes for SQL string literals."""
    if not s:
        return ""
    return s.replace("\\", "\\\\").replace("'", "\\'")


async def _ensure_pipeline_table() -> None:
    """
    Create the ``pipeline`` schema and ``job_status`` table in Unity Catalog
    if they don't already exist.  Called once on startup before job restore.
    """
    if not _db_client or not _PIPELINE_TABLE:
        logger.warning(
            "_ensure_pipeline_table: skipped (CATALOG not set or client unavailable)"
        )
        return
    try:
        await _db_client.execute_sql(
            f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.pipeline"
        )
    except Exception as exc:
        logger.info("CREATE SCHEMA note (may already exist): %s", exc)

    try:
        await _db_client.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS {_PIPELINE_TABLE} (
                job_id             STRING  NOT NULL,
                file_name          STRING,
                source_system      STRING,
                uploaded_at        TIMESTAMP,
                current_stage      STRING,
                status             STRING,
                rows_processed     BIGINT,
                issue_count        INT,
                error_message      STRING,
                databricks_run_id  STRING,
                stage_timings_json STRING,
                last_updated       TIMESTAMP
            )
            USING DELTA
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """)
        logger.info("Pipeline status table ready: %s", _PIPELINE_TABLE)
    except Exception as exc:
        logger.warning("Could not create pipeline table: %s", exc)


async def _upsert_job_status(job: JobStatus) -> None:
    """
    Write or update a single job's status in the Delta table.
    Uses MERGE to handle both inserts and updates idempotently.
    Fire-and-forget: errors are logged but do NOT raise.
    """
    if not _db_client or not _PIPELINE_TABLE:
        return

    import json as _json

    run_id = getattr(job, "_run_id", None) or ""
    stage_timings = _job_stage_timings.get(job.job_id, [])
    timings_json = _json.dumps(stage_timings)

    err_val = (
        f"'{_escape_sql(job.error_message)}'"
        if job.error_message
        else "NULL"
    )
    run_id_val = f"'{_escape_sql(run_id)}'" if run_id else "NULL"

    merge_sql = f"""
    MERGE INTO {_PIPELINE_TABLE} AS t
    USING (
      SELECT
        '{_escape_sql(job.job_id)}'                  AS job_id,
        '{_escape_sql(job.file_name)}'               AS file_name,
        '{_val(job.source_system)}'                  AS source_system,
        CAST('{job.uploaded_at.strftime("%Y-%m-%d %H:%M:%S")}' AS TIMESTAMP)
                                                     AS uploaded_at,
        '{_val(job.current_stage)}'                  AS current_stage,
        '{_val(job.status)}'                         AS status,
        {job.rows_processed}                         AS rows_processed,
        {job.issue_count}                            AS issue_count,
        {err_val}                                    AS error_message,
        {run_id_val}                                 AS databricks_run_id,
        '{_escape_sql(timings_json)}'                AS stage_timings_json,
        CURRENT_TIMESTAMP()                          AS last_updated
    ) AS s ON t.job_id = s.job_id
    WHEN MATCHED THEN
      UPDATE SET
        current_stage      = s.current_stage,
        status             = s.status,
        rows_processed     = s.rows_processed,
        issue_count        = s.issue_count,
        error_message      = s.error_message,
        databricks_run_id  = s.databricks_run_id,
        stage_timings_json = s.stage_timings_json,
        last_updated       = s.last_updated
    WHEN NOT MATCHED THEN
      INSERT (job_id, file_name, source_system, uploaded_at, current_stage,
              status, rows_processed, issue_count, error_message,
              databricks_run_id, stage_timings_json, last_updated)
      VALUES (s.job_id, s.file_name, s.source_system, s.uploaded_at,
              s.current_stage, s.status, s.rows_processed, s.issue_count,
              s.error_message, s.databricks_run_id, s.stage_timings_json,
              s.last_updated)
    """
    try:
        await _db_client.execute_sql(merge_sql)
        logger.info("Upserted job %s → %s/%s", job.job_id,
                    _val(job.current_stage), _val(job.status))
    except Exception as exc:
        logger.error(
            "UPSERT FAILED for job %s (table=%s): %s",
            job.job_id, _PIPELINE_TABLE, exc, exc_info=True
        )


# Source system display name mapping
_SYSTEM_DISPLAY: dict[str, str] = {
    "LEGACY_CASE":    "LegacyCase",
    "OPEN_JUSTICE":   "OpenJustice",
    "AD_HOC_EXPORTS": "AdHocExports",
}

# ---------------------------------------------------------------------------
# In-memory stores (replace with Delta table reads in production)
# ---------------------------------------------------------------------------

_jobs: dict[str, JobStatus] = {}
_issues: dict[str, ReconciliationIssue] = {}
# Stores task-level timing data per job_id for the frontend stage_timings field
_job_stage_timings: dict[str, list[dict]] = {}

# ---------------------------------------------------------------------------
# Databricks client singleton
# ---------------------------------------------------------------------------

_db_client: Optional[DatabricksClient] = None


def get_databricks_client() -> DatabricksClient:
    """FastAPI dependency that returns the initialised Databricks client."""
    if _db_client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Databricks client failed to initialise (check DATABRICKS_HOST/TOKEN env vars).",
        )
    if not _db_client._host:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DATABRICKS_HOST is not configured.",
        )
    return _db_client


# ---------------------------------------------------------------------------
# Azure AD / JWT auth
# ---------------------------------------------------------------------------

_bearer_scheme = HTTPBearer(auto_error=False)


async def verify_token(
    credentials: Annotated[
        Optional[HTTPAuthorizationCredentials], Depends(_bearer_scheme)
    ],
) -> dict[str, Any]:
    """
    Validate the bearer token when AUTH_ENABLED is true.

    When disabled, returns an empty claims dict so downstream code does not
    need to branch on whether auth is active.

    In production, replace the stub validation with a call to
    ``fastapi_azure_auth.SingleTenantAzureAuthorizationCodeBearer`` or
    ``python-jose`` JWKS verification against the Entra ID well-known endpoint.
    """
    if not AUTH_ENABLED:
        return {}

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # --- Production stub: replace with real JWKS validation ---
    token = credentials.credentials
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Example fastapi-azure-auth integration (uncomment when ready):
    # from fastapi_azure_auth import SingleTenantAzureAuthorizationCodeBearer
    # azure_scheme = SingleTenantAzureAuthorizationCodeBearer(
    #     app_client_id=os.environ["AZURE_CLIENT_ID"],
    #     tenant_id=os.environ["AZURE_TENANT_ID"],
    # )
    # return await azure_scheme(token)

    return {"sub": "unknown", "token": token}


# ---------------------------------------------------------------------------
# Background job status poller
# ---------------------------------------------------------------------------

_poll_task: Optional[asyncio.Task[None]] = None


def _build_stage_timings(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Convert a Databricks tasks array (from the runs/get API) into the 7-element
    stage_timings list expected by the frontend.

    Each element: {"stage": str, "status": str, "duration": int|None,
                   "rows_in": None, "rows_out": None}
    """
    import time as _time

    timings: list[dict[str, Any]] = [
        {"stage": s, "status": "idle", "duration": None, "rows_in": None, "rows_out": None}
        for s in _STAGE_NAMES
    ]
    # Upload is always complete by the time the job runs.
    timings[0]["status"] = "complete"
    timings[0]["duration"] = 0

    # Aggregate per-stage data from tasks
    stage_tasks: dict[int, list[dict]] = {}
    for task in tasks:
        key = task.get("task_key", "")
        if key not in _TASK_TO_STAGE:
            continue
        _, idx = _TASK_TO_STAGE[key]
        stage_tasks.setdefault(idx, []).append(task)

    for idx, task_list in stage_tasks.items():
        total_duration = 0
        stage_status = "idle"
        status_rank = {"idle": 0, "complete": 1, "running": 2, "failed": 3}

        for task in task_list:
            t_lifecycle = task.get("state", {}).get("life_cycle_state", "")
            t_result = task.get("state", {}).get("result_state", "")
            start_ms = task.get("start_time", 0) or 0
            end_ms = task.get("end_time", 0) or 0

            if t_result == "SUCCESS":
                t_status = "complete"
                if start_ms and end_ms and end_ms > start_ms:
                    total_duration += int((end_ms - start_ms) / 1000)
            elif t_lifecycle == "RUNNING":
                t_status = "running"
                if start_ms:
                    total_duration += int(_time.time() - start_ms / 1000)
            elif t_result in ("FAILED", "TIMEDOUT", "CANCELED"):
                t_status = "failed"
            elif t_lifecycle in ("PENDING", "WAITING_FOR_RETRY", "BLOCKED", "QUEUED"):
                t_status = "idle"
            else:
                t_status = "idle"

            if status_rank.get(t_status, 0) > status_rank.get(stage_status, 0):
                stage_status = t_status

        timings[idx]["status"] = stage_status
        timings[idx]["duration"] = total_duration if total_duration > 0 else None

    return timings


def _to_frontend_job(job: JobStatus) -> dict[str, Any]:
    """Transform a JobStatus into the dict format the frontend expects."""
    # Use _val() to handle both enum members and plain strings (use_enum_values=True)
    stage_str = _val(job.current_stage)
    stage_idx = _STAGE_ENUM_TO_IDX.get(stage_str, 0)
    stage_name = _STAGE_NAMES[stage_idx] if 0 <= stage_idx < len(_STAGE_NAMES) else "Upload"

    raw_status = _val(job.status)
    status_map = {
        "RUNNING": "running",
        "COMPLETE": "complete",
        "FAILED": "failed",
        "REVIEW_NEEDED": "review",
    }
    fe_status = status_map.get(raw_status, "running")
    if stage_str == "UPLOAD":
        fe_status = "upload"

    system_raw = _val(job.source_system)
    system_display = _SYSTEM_DISPLAY.get(system_raw, system_raw)

    stage_timings = _job_stage_timings.get(job.job_id, [
        {"stage": s, "status": "idle", "duration": None, "rows_in": None, "rows_out": None}
        for s in _STAGE_NAMES
    ])
    # Always mark Upload complete since the job was triggered
    if stage_timings and stage_timings[0]["status"] == "idle":
        stage_timings[0]["status"] = "complete"

    return {
        "id": job.job_id,
        "file_name": job.file_name,
        "system": system_display,
        "uploaded_at": job.uploaded_at.isoformat(),
        "stage": stage_name,
        "stage_index": stage_idx,
        "status": fe_status,
        "rows": job.rows_processed,
        "issues": job.issue_count,
        "stage_timings": stage_timings,
        "error_message": job.error_message,
        "databricks_run_id": getattr(job, "_run_id", None),
    }


async def _load_recent_jobs() -> None:
    """
    On startup, restore recent jobs into _jobs.

    Strategy:
    1. Load from the Delta table (fast, full fidelity incl. stage timings).
    2. ALWAYS also check the Databricks runs API for recent pipeline runs
       that may not be in the Delta table (e.g. trigger-based runs with no
       notebook params, or runs triggered before the app was first deployed).
       For parameterless runs, create one virtual JobStatus per source system.
       Only the most recent parameterless run is materialised (plus any that
       are still active) to avoid cluttering the UI with stale history.
    """
    import json as _json

    # --- Step 1: Delta table ---
    if _db_client and _PIPELINE_TABLE:
        try:
            rows = await _db_client.execute_sql(
                f"SELECT * FROM {_PIPELINE_TABLE} ORDER BY uploaded_at DESC LIMIT 100"
            )
            loaded = 0
            for row in rows:
                job_id = row.get("job_id")
                if not job_id or job_id in _jobs:
                    continue

                try:
                    source_system = SourceSystem(row.get("source_system", "LEGACY_CASE"))
                except ValueError:
                    source_system = SourceSystem.LEGACY_CASE

                try:
                    current_stage = PipelineStage(row.get("current_stage", "UPLOAD"))
                except ValueError:
                    current_stage = PipelineStage.UPLOAD

                try:
                    job_status = JobStatusValue(row.get("status", "RUNNING"))
                except ValueError:
                    job_status = JobStatusValue.RUNNING

                raw_ts = row.get("uploaded_at")
                if isinstance(raw_ts, str):
                    try:
                        uploaded_at = datetime.fromisoformat(raw_ts.replace("Z", ""))
                    except ValueError:
                        uploaded_at = datetime.utcnow()
                elif isinstance(raw_ts, datetime):
                    uploaded_at = raw_ts
                else:
                    uploaded_at = datetime.utcnow()

                job = JobStatus(
                    job_id=job_id,
                    file_name=row.get("file_name", job_id),
                    source_system=source_system,
                    uploaded_at=uploaded_at,
                    current_stage=current_stage,
                    status=job_status,
                    rows_processed=int(row.get("rows_processed") or 0),
                    issue_count=int(row.get("issue_count") or 0),
                    error_message=row.get("error_message"),
                )
                run_id = row.get("databricks_run_id") or ""
                if run_id:
                    job._run_id = run_id  # type: ignore[attr-defined]
                _jobs[job_id] = job

                timings_json = row.get("stage_timings_json") or "[]"
                try:
                    timings = _json.loads(timings_json)
                    if timings:
                        _job_stage_timings[job_id] = timings
                except Exception:
                    pass

                loaded += 1

            if loaded:
                logger.info("Restored %d job(s) from Delta table %s.", loaded, _PIPELINE_TABLE)
            # NOTE: Do NOT return here — always also check the runs API below
            # so that trigger-based pipeline runs show up even if they weren't
            # persisted to the Delta table.
        except Exception as exc:
            logger.warning("Could not load from Delta table (%s): %s", _PIPELINE_TABLE, exc)

    # --- Step 2: Databricks runs API (always runs) ---
    if not _db_client or not DATABRICKS_JOB_ID:
        return

    # Collect run IDs already tracked (from Delta table) so we can skip them.
    tracked_run_ids: set[str] = {
        getattr(j, "_run_id", None) or ""
        for j in _jobs.values()
    }
    tracked_run_ids.discard("")

    try:
        runs = await _db_client.get_job_runs(DATABRICKS_JOB_ID, limit=20)
        loaded = 0
        pipeline_runs_added = 0  # how many parameterless runs we've materialised

        for run in runs:
            overriding = run.get("overriding_parameters") or {}
            params = overriding.get("notebook_params") or run.get("notebook_params") or {}
            run_id_str = str(run.get("run_id", ""))

            start_ms = run.get("start_time") or 0
            uploaded_at = (
                datetime.utcfromtimestamp(start_ms / 1000)
                if start_ms else datetime.utcnow()
            )

            lifecycle = run.get("state", {}).get("life_cycle_state", "RUNNING")
            result = run.get("state", {}).get("result_state", "")

            is_terminal = lifecycle in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED")
            is_success  = lifecycle == "TERMINATED" and result == "SUCCESS"

            if is_success:
                job_status   = JobStatusValue.COMPLETE
                current_stage = PipelineStage.COMPLETE
                error_msg: Optional[str] = None
            elif is_terminal:
                job_status   = JobStatusValue.FAILED
                current_stage = PipelineStage.FAILED
                error_msg    = run.get("state", {}).get("state_message") or "Run failed"
            else:
                job_status   = JobStatusValue.RUNNING
                current_stage = PipelineStage.BRONZE
                error_msg    = None

            tasks = run.get("tasks", [])
            job_id = params.get("job_id")

            if job_id:
                # ── Upload-flow job (has an explicit job_id param) ──────────
                if job_id in _jobs:
                    continue
                file_name = params.get("file_name", f"run_{run_id_str}")
                source_system_raw = (params.get("source_system") or "LEGACY_CASE").upper()
                try:
                    source_system = SourceSystem(source_system_raw)
                except ValueError:
                    source_system = SourceSystem.LEGACY_CASE

                job = JobStatus(
                    job_id=job_id,
                    file_name=file_name,
                    source_system=source_system,
                    uploaded_at=uploaded_at,
                    current_stage=current_stage,
                    status=job_status,
                    error_message=error_msg,
                )
                job._run_id = run_id_str  # type: ignore[attr-defined]
                _jobs[job_id] = job
                if tasks:
                    _job_stage_timings[job_id] = _build_stage_timings(tasks)
                loaded += 1

            elif run_id_str and run_id_str not in tracked_run_ids:
                # ── Parameterless pipeline run ─────────────────────────────
                # Create virtual jobs per source system.
                # Only materialise: (a) any currently active run, or
                # (b) the single most-recent completed/failed run.
                is_active = not is_terminal
                if is_terminal and pipeline_runs_added >= 1:
                    continue  # skip older completed pipeline runs

                tracked_run_ids.add(run_id_str)
                pipeline_runs_added += 1

                _VIRT_SOURCES = [
                    (SourceSystem.LEGACY_CASE,    "LC", "LegacyCase DB Ingestion"),
                    (SourceSystem.OPEN_JUSTICE,   "OJ", "OpenJustice CSV Batch"),
                    (SourceSystem.AD_HOC_EXPORTS, "AH", "AdHocExports Files"),
                ]
                for source, code, fname in _VIRT_SOURCES:
                    virt_id = f"pipeline-{run_id_str}-{code}"
                    if virt_id in _jobs:
                        continue
                    job = JobStatus(
                        job_id=virt_id,
                        file_name=fname,
                        source_system=source,
                        uploaded_at=uploaded_at,
                        current_stage=current_stage,
                        status=job_status,
                        error_message=error_msg,
                    )
                    job._run_id = run_id_str  # type: ignore[attr-defined]
                    _jobs[virt_id] = job
                    if tasks:
                        _job_stage_timings[virt_id] = _build_stage_timings(tasks)
                    loaded += 1

        if loaded:
            logger.info("Restored %d job(s) from Databricks runs API.", loaded)
            for jid in list(_jobs.keys()):
                await _upsert_job_status(_jobs[jid])
    except Exception as exc:
        logger.warning("Could not load recent jobs on startup: %s", exc)


async def _poll_job_statuses() -> None:
    """
    Background coroutine that polls Databricks every 10 seconds and updates
    the in-memory job store with the latest run state and task-level timings.
    """
    while True:
        try:
            running_jobs = [
                j for j in _jobs.values() if _val(j.status) == "RUNNING"
            ]
            for job in running_jobs:
                if not hasattr(job, "_run_id") or not job._run_id:  # type: ignore[attr-defined]
                    continue
                try:
                    assert _db_client is not None
                    run = await _db_client.get_run_status(job._run_id)  # type: ignore[attr-defined]
                    lifecycle = run.get("state", {}).get("life_cycle_state", "")
                    result = run.get("state", {}).get("result_state", "")

                    # Parse task-level status to build stage_timings
                    tasks = run.get("tasks", [])
                    if tasks:
                        _job_stage_timings[job.job_id] = _build_stage_timings(tasks)
                        # Advance current_stage to the furthest running task
                        for task in tasks:
                            task_key = task.get("task_key", "")
                            t_lifecycle = task.get("state", {}).get("life_cycle_state", "")
                            t_result = task.get("state", {}).get("result_state", "")
                            if task_key in _TASK_TO_STAGE and t_lifecycle == "RUNNING":
                                stage, _ = _TASK_TO_STAGE[task_key]
                                job.current_stage = stage

                    if lifecycle == "TERMINATED":
                        if result == "SUCCESS":
                            job.status = JobStatusValue.COMPLETE
                            job.current_stage = PipelineStage.COMPLETE
                        else:
                            job.status = JobStatusValue.FAILED
                            job.current_stage = PipelineStage.FAILED
                            job.error_message = (
                                run.get("state", {}).get("state_message", "Unknown error")
                            )
                        logger.info(
                            "Job %s transitioned to %s", job.job_id, _val(job.status)
                        )
                    # Persist every poll cycle so the table stays current.
                    await _upsert_job_status(job)
                except DatabricksError as exc:
                    logger.warning("Failed to poll run for job %s: %s", job.job_id, exc)
        except Exception as exc:
            logger.error("Unexpected error in poll loop: %s", exc, exc_info=True)

        await asyncio.sleep(10)


# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Initialise the Databricks client and start the background poller on
    startup; shut them down cleanly on exit.
    """
    global _db_client, _poll_task

    logger.info("Starting DOJ Migration API …")
    try:
        _db_client = DatabricksClient()
        logger.info(
            "DatabricksClient initialised (host=%s, token_set=%s)",
            _db_client._host or "<empty>",
            bool(_db_client._token),
        )
    except Exception as exc:
        logger.error("Failed to initialise DatabricksClient: %s", exc)
        _db_client = None

    # Run DB init in the background so the app can serve requests immediately.
    # The warehouse may need to warm up (minutes), blocking here causes
    # Databricks Apps to report "App Not Available" during startup.
    asyncio.create_task(_ensure_pipeline_table())
    asyncio.create_task(_load_recent_jobs())

    _poll_task = asyncio.create_task(_poll_job_statuses())
    logger.info("Background job-status poller started.")

    yield  # application is running

    logger.info("Shutting down DOJ Migration API …")
    if _poll_task:
        _poll_task.cancel()
        try:
            await _poll_task
        except asyncio.CancelledError:
            pass
    if _db_client:
        await _db_client.close()
    logger.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="DOJ Data Migration API",
    description=(
        "REST API for the Justice Nexus data migration pipeline. "
        "Handles file ingestion, Databricks job orchestration, and "
        "SME-driven reconciliation review."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pipeline-specific router
app.include_router(pipeline_router)

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _validate_upload_file(file: UploadFile) -> None:
    """
    Validate the uploaded file extension and MIME type.

    Raises
    ------
    HTTPException (422)
        When the file type is not permitted.
    """
    filename = file.filename or ""
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"File extension '{ext}' is not permitted. "
                f"Allowed: {sorted(ALLOWED_EXTENSIONS)}"
            ),
        )

    content_type = file.content_type or mimetypes.guess_type(filename)[0] or ""
    if content_type not in ALLOWED_MIME_TYPES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Content-Type '{content_type}' is not permitted for this endpoint."
            ),
        )


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get(
    "/api/health",
    tags=["Operations"],
    summary="Health check",
    response_model=dict,
)
async def health_check() -> dict[str, Any]:
    """Return a liveness payload with Databricks connectivity diagnostics."""
    db_host = _db_client._host if _db_client else ""
    db_token_set = bool(_db_client and _db_client._get_token()) if _db_client else False

    # Test Databricks connectivity by fetching the current user.
    db_connectivity: Optional[str] = None
    db_error: Optional[str] = None
    if _db_client and db_token_set:
        try:
            resp = await _db_client._request("GET", "/api/2.0/preview/scim/v2/Me")
            db_connectivity = resp.get("userName") or "ok"
        except Exception as exc:
            db_error = str(exc)

    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "auth_enabled": AUTH_ENABLED,
        "demo_mode": DEMO_MODE,
        "databricks_host": db_host or None,
        "databricks_token_set": db_token_set,
        "databricks_user": db_connectivity,
        "databricks_error": db_error,
        "job_id_configured": DATABRICKS_JOB_ID or None,
        "catalog": CATALOG or None,
        "pipeline_table": _PIPELINE_TABLE or None,
        "jobs_in_memory": len(_jobs),
    }


# ---------------------------------------------------------------------------
# Debug — test job connectivity (remove before production)
# ---------------------------------------------------------------------------


@app.get("/api/test-job", tags=["Operations"], response_model=dict)
async def test_job() -> dict[str, Any]:
    """Verify the configured job ID exists and the SP has permission to run it."""
    if _db_client is None:
        return {"ok": False, "error": "Databricks client not initialised"}
    try:
        resp = await _db_client._request(
            "GET", "/api/2.1/jobs/get", params={"job_id": DATABRICKS_JOB_ID}
        )
        return {
            "ok": True,
            "job_id": DATABRICKS_JOB_ID,
            "job_name": resp.get("settings", {}).get("name"),
            "creator": resp.get("creator_user_name"),
        }
    except Exception as exc:
        return {"ok": False, "job_id": DATABRICKS_JOB_ID, "error": str(exc)}


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------


@app.post(
    "/api/upload",
    tags=["Pipeline"],
    summary="Upload a source file and trigger the migration pipeline",
    response_model=UploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def upload_file(
    file: UploadFile = File(..., description="Excel (.xlsx) or CSV file to ingest."),
    source_system: SourceSystem = Query(
        ..., description="Source system that produced the file."
    ),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    _claims: dict = Depends(verify_token),
) -> UploadResponse:
    """
    Accept a multipart file upload, persist it to ADLS, and trigger the
    Databricks migration orchestration job.

    The endpoint returns immediately with a ``job_id`` that can be polled via
    ``GET /api/jobs/{job_id}``.
    """
    _validate_upload_file(file)

    job_id = str(uuid.uuid4())
    filename = file.filename or f"upload_{job_id}.bin"
    adls_path = f"{ADLS_UPLOAD_PATH}/{job_id}/{filename}"

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Uploaded file is empty.",
        )

    # Register job in the in-memory store immediately so GET /jobs returns it.
    job = JobStatus(
        job_id=job_id,
        file_name=filename,
        source_system=source_system,
        current_stage=PipelineStage.UPLOAD,
        status=JobStatusValue.RUNNING,
    )
    _jobs[job_id] = job
    asyncio.create_task(_upsert_job_status(job))

    # In DEMO_MODE skip the real ADLS upload and job trigger entirely.
    if DEMO_MODE:
        logger.info("[DEMO] Skipping ADLS upload and job trigger for job %s", job_id)
        return UploadResponse(
            job_id=job_id,
            file_name=filename,
            adls_path=adls_path,
            databricks_run_id=f"DEMO-RUN-{job_id[:8]}",
            message="Upload successful. Pipeline triggered (DEMO MODE).",
        )

    # Only resolve the Databricks client when not in DEMO_MODE.
    db = get_databricks_client()

    run_id: Optional[str] = None

    # Upload the file to ADLS/DBFS.  Non-fatal: if the path isn't writable
    # (e.g. /FileStore not configured) we log and proceed so the pipeline
    # job can still be triggered with the other params.
    try:
        await db.upload_to_adls(content, adls_path)
        logger.info("File uploaded to ADLS: %s", adls_path)
    except DatabricksError as exc:
        logger.warning(
            "ADLS upload failed for job %s (continuing to job trigger): %s",
            job_id, exc,
        )
        adls_path = ""  # signal to downstream that file isn't on ADLS

    # Trigger the Databricks migration job.  This is the critical step.
    try:
        run_id = await db.trigger_job(
            DATABRICKS_JOB_ID,
            {
                "job_id": job_id,
                "source_system": source_system,
                "adls_path": adls_path,
                "file_name": filename,
            },
        )
        job._run_id = run_id  # type: ignore[attr-defined]
        logger.info("Pipeline triggered: job_id=%s run_id=%s", job_id, run_id)

    except DatabricksError as exc:
        logger.error("Job trigger failed for job %s: %s", job_id, exc)
        job.status = JobStatusValue.FAILED
        job.current_stage = PipelineStage.FAILED
        job.error_message = str(exc)
        asyncio.create_task(_upsert_job_status(job))
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Pipeline trigger failed: {exc}",
        ) from exc

    return UploadResponse(
        job_id=job_id,
        file_name=filename,
        adls_path=adls_path,
        databricks_run_id=run_id,
    )


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


@app.get(
    "/api/jobs",
    tags=["Pipeline"],
    summary="List all migration jobs",
    response_model=list[dict],
)
async def list_jobs(
    source_system: Optional[SourceSystem] = Query(
        default=None, description="Filter by source system."
    ),
    status_filter: Optional[JobStatusValue] = Query(
        default=None, alias="status", description="Filter by job status."
    ),
    _claims: dict = Depends(verify_token),
) -> list[dict]:
    """
    Return all known migration jobs in the frontend-compatible format,
    optionally filtered by source system or status.
    """
    jobs = list(_jobs.values())
    if source_system:
        sys_str = _val(source_system)
        jobs = [j for j in jobs if _val(j.source_system) == sys_str]
    if status_filter:
        status_str = _val(status_filter)
        jobs = [j for j in jobs if _val(j.status) == status_str]
    return [
        _to_frontend_job(j)
        for j in sorted(jobs, key=lambda j: j.uploaded_at, reverse=True)
    ]


@app.get(
    "/api/jobs/{job_id}",
    tags=["Pipeline"],
    summary="Get details for a single migration job",
    response_model=dict,
)
async def get_job(
    job_id: str,
    _claims: dict = Depends(verify_token),
) -> dict:
    """Return the current state of a specific migration job in frontend-compatible format."""
    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found.",
        )
    return _to_frontend_job(job)


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


@app.get(
    "/api/reconciliation/issues",
    tags=["Reconciliation"],
    summary="List reconciliation issues pending SME review",
    response_model=list[ReconciliationIssue],
)
async def list_issues(
    issue_type: Optional[IssueType] = Query(
        default=None, alias="type", description="Filter by issue type."
    ),
    source_system: Optional[SourceSystem] = Query(
        default=None, alias="system", description="Filter by source system."
    ),
    job_id: Optional[str] = Query(
        default=None, description="Filter to a specific job."
    ),
    issue_status: Optional[IssueStatus] = Query(
        default=None, alias="status", description="Filter by issue lifecycle status."
    ),
    _claims: dict = Depends(verify_token),
) -> list[ReconciliationIssue]:
    """
    Return reconciliation issues, supporting optional filtering on type,
    source system, job, and status.
    """
    results = list(_issues.values())
    if issue_type:
        results = [i for i in results if i.issue_type == issue_type]
    if source_system:
        results = [i for i in results if i.source_system == source_system]
    if job_id:
        results = [i for i in results if i.job_id == job_id]
    if issue_status:
        results = [i for i in results if i.status == issue_status]
    return results


@app.put(
    "/api/reconciliation/issues/{issue_id}",
    tags=["Reconciliation"],
    summary="Submit an SME decision for a reconciliation issue",
    response_model=ReconciliationIssue,
)
async def decide_issue(
    issue_id: str,
    decision: IssueDecisionRequest,
    _claims: dict = Depends(verify_token),
) -> ReconciliationIssue:
    """
    Update the lifecycle status of a reconciliation issue with an SME decision.
    Optionally overrides the proposed value before approval.
    """
    issue = _issues.get(issue_id)
    if issue is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Issue '{issue_id}' not found.",
        )

    issue.status = decision.status
    issue.reviewer = decision.reviewer
    issue.reviewer_note = decision.reviewer_note
    issue.review_timestamp = datetime.utcnow()
    if decision.proposed_value is not None:
        issue.proposed_value = decision.proposed_value

    logger.info(
        "Issue %s → %s by %s", issue_id, decision.status, decision.reviewer
    )
    return issue


@app.get(
    "/api/reconciliation/report/{job_id}",
    tags=["Reconciliation"],
    summary="Get the reconciliation report for a specific job",
    response_model=dict,
)
async def reconciliation_report(
    job_id: str,
    _claims: dict = Depends(verify_token),
) -> dict[str, Any]:
    """
    Return a summary reconciliation report for a migration job, broken down
    by issue type and status.
    """
    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found.",
        )

    job_issues = [i for i in _issues.values() if i.job_id == job_id]

    by_type: dict[str, int] = {}
    by_status: dict[str, int] = {}
    for issue in job_issues:
        by_type[issue.issue_type] = by_type.get(issue.issue_type, 0) + 1
        by_status[issue.status] = by_status.get(issue.status, 0) + 1

    return {
        "job_id": job_id,
        "file_name": job.file_name,
        "source_system": job.source_system,
        "total_issues": len(job_issues),
        "by_type": by_type,
        "by_status": by_status,
        "issues": job_issues,
        "generated_at": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Stage summary (also available via pipeline router)
# ---------------------------------------------------------------------------


@app.get(
    "/api/stages/summary",
    tags=["Pipeline"],
    summary="Aggregate stage metrics for the pipeline flow canvas",
    response_model=dict,
)
async def stage_summary(
    _claims: dict = Depends(verify_token),
) -> dict[str, Any]:
    """
    Return a summary dict for the PipelineFlowCanvas frontend component.

    Shape::

        {
          "stages":          [{id, label, job_count, status, avg_duration_s, sla_ok}],
          "active_jobs":     [{id, system, stage_index, status, file_name, rows}],
          "connector_flows": [{from, to, job_count, systems}],
        }
    """
    status_map = {
        "RUNNING": "running",
        "COMPLETE": "complete",
        "FAILED": "failed",
        "REVIEW_NEEDED": "review",
    }
    # SLA thresholds in seconds per stage (Upload, Bronze, Mapping, Silver, Gold, Staging, Complete)
    _SLA_THRESHOLDS = [60, 300, 400, 600, 600, 200, 10]

    # Per-stage counters: idx → {statuses, systems, job_count}
    stage_data: dict[int, dict[str, Any]] = {
        i: {"job_count": 0, "statuses": [], "systems": []}
        for i in range(len(_STAGE_NAMES))
    }

    active_jobs_out: list[dict[str, Any]] = []

    today = datetime.utcnow().date()

    for job in _jobs.values():
        # Only count jobs that started today
        if job.uploaded_at.date() != today:
            continue

        stage_str = _val(job.current_stage)
        stage_idx = _STAGE_ENUM_TO_IDX.get(stage_str, 0)
        fe_status = status_map.get(_val(job.status), "running")
        system_display = _SYSTEM_DISPLAY.get(_val(job.source_system), _val(job.source_system))

        stage_data[stage_idx]["job_count"] += 1
        stage_data[stage_idx]["statuses"].append(fe_status)
        stage_data[stage_idx]["systems"].append(system_display)

        active_jobs_out.append({
            "id": job.job_id,
            "system": system_display,
            "stage_index": stage_idx,
            "status": fe_status,
            "file_name": job.file_name,
            "rows": job.rows_processed,
        })

    # Build stages list
    stages_out: list[dict[str, Any]] = []
    for i, label in enumerate(_STAGE_NAMES):
        statuses = stage_data[i]["statuses"]
        if "running" in statuses:
            stage_status = "running"
        elif "review" in statuses:
            stage_status = "review"
        elif "failed" in statuses:
            stage_status = "failed"
        elif "complete" in statuses:
            stage_status = "complete"
        else:
            stage_status = "idle"

        # Compute avg duration from stored stage timings
        durations = [
            t[i]["duration"]
            for t in _job_stage_timings.values()
            if i < len(t) and t[i].get("duration")
        ]
        avg_dur = int(sum(durations) / len(durations)) if durations else None

        stages_out.append({
            "id": label.lower(),
            "label": label,
            "job_count": stage_data[i]["job_count"],
            "status": stage_status,
            "avg_duration_s": avg_dur,
            "sla_ok": avg_dur is None or avg_dur <= _SLA_THRESHOLDS[i],
        })

    # Build connector flows: upload→bronze is active when jobs are RUNNING at bronze, etc.
    connector_flows: list[dict[str, Any]] = []
    for i in range(len(_STAGE_NAMES) - 1):
        flowing = [
            j for j in _jobs.values()
            if _STAGE_ENUM_TO_IDX.get(_val(j.current_stage), 0) == i + 1
            and _val(j.status) == "RUNNING"
            and j.uploaded_at.date() == today
        ]
        if flowing:
            systems = list({
                _SYSTEM_DISPLAY.get(_val(j.source_system), _val(j.source_system))
                for j in flowing
            })
            connector_flows.append({
                "from": _STAGE_NAMES[i].lower(),
                "to": _STAGE_NAMES[i + 1].lower(),
                "job_count": len(flowing),
                "systems": systems,
            })

    return {
        "stages": stages_out,
        "active_jobs": active_jobs_out,
        "connector_flows": connector_flows,
    }


@app.post(
    "/api/pipeline/trigger",
    tags=["Pipeline"],
    summary="Trigger a new ingestion run for all 3 datasources",
)
async def trigger_pipeline(
    _claims: dict = Depends(verify_token),
) -> dict[str, Any]:
    """
    Trigger the Databricks ingestion job and register one virtual
    ``JobStatus`` per source system (LC / OJ / AH), all tied to the
    same Databricks run ID so the poller can advance them together.
    """
    if not _db_client:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Databricks client not available.",
        )
    if not DATABRICKS_JOB_ID:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DATABRICKS_JOB_ID is not configured.",
        )

    try:
        run_id = await _db_client.trigger_job(DATABRICKS_JOB_ID, {})
    except Exception as exc:
        logger.error("trigger_pipeline failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Pipeline trigger failed: {exc}",
        ) from exc
    now = datetime.utcnow()

    source_configs = [
        (SourceSystem.LEGACY_CASE,    "LC", "LegacyCase DB Ingestion"),
        (SourceSystem.OPEN_JUSTICE,   "OJ", "OpenJustice CSV Batch"),
        (SourceSystem.AD_HOC_EXPORTS, "AH", "AdHocExports Files"),
    ]

    job_ids: list[str] = []
    for source, code, file_name in source_configs:
        job_id = f"pipeline-{run_id}-{code}"
        job = JobStatus(
            job_id=job_id,
            file_name=file_name,
            source_system=source,
            uploaded_at=now,
            current_stage=PipelineStage.BRONZE,
            status=JobStatusValue.RUNNING,
        )
        job._run_id = run_id  # type: ignore[attr-defined]
        _jobs[job_id] = job
        await _upsert_job_status(job)
        job_ids.append(job_id)

    logger.info("Pipeline triggered: run_id=%s  jobs=%s", run_id, job_ids)
    return {
        "run_id": run_id,
        "job_ids": job_ids,
        "message": "Pipeline ingestion triggered for all 3 datasources.",
    }


# ---------------------------------------------------------------------------
# Stage detail endpoints
# ---------------------------------------------------------------------------

def _compute_stage_metrics(
    stage_id: str,
    stage_job_ids: set,
    jobs_at_stage: list,
) -> dict[str, Any]:
    """Compute stage-specific KPIs from in-memory job and reconciliation data."""
    # Return empty dict when no jobs are at this stage — frontend will show demo data.
    if not jobs_at_stage:
        return {}

    total_rows = sum(j.get("rows") or 0 for j in jobs_at_stage)
    error_jobs = [j for j in jobs_at_stage if j.get("status") in ("failed", "review")]
    durations = [j["time_in_stage"] for j in jobs_at_stage if j.get("time_in_stage")]
    avg_dur = int(sum(durations) / len(durations)) if durations else None

    # If no rows have been processed yet (pipeline just started), fall back to demo.
    has_row_data = total_rows > 0

    # Reconciliation issues linked to these jobs
    recon = [i for i in _issues.values() if i.job_id in stage_job_ids]
    low_conf = sum(1 for i in recon if _val(i.issue_type) == "LOW_CONFIDENCE_MAPPING")
    unmapped = sum(1 for i in recon if _val(i.issue_type) == "UNMAPPED_CODE")
    duplicates = sum(1 for i in recon if _val(i.issue_type) == "DUPLICATE_CONTACT")
    mapped_cols = len({i.source_column for i in recon if i.proposed_value})
    total_issue_count = sum(j.get("issues") or 0 for j in jobs_at_stage)

    # Return {} if we have no meaningful data — triggers demo fallback in the frontend.
    has_any_data = has_row_data or total_issue_count > 0 or len(recon) > 0 or avg_dur is not None
    if not has_any_data:
        return {}

    if stage_id == "upload":
        return {
            "files_received": len(jobs_at_stage),
            "total_rows": total_rows if has_row_data else None,
            "file_size_mb": None,
            "schema_detected": any(j["status"] == "complete" for j in jobs_at_stage),
        }
    if stage_id == "bronze":
        return {
            "rows_landed": total_rows if has_row_data else None,
            "parse_errors": len(error_jobs),
            "schema_cols": None,
            "autoloader_lag_s": avg_dur,
        }
    if stage_id == "mapping":
        return {
            "columns_mapped": mapped_cols if mapped_cols > 0 else None,
            "avg_confidence": None,
            "low_confidence_count": low_conf,
            "unmapped_count": unmapped,
        }
    if stage_id == "silver":
        return {
            "rows_cleaned": total_rows if has_row_data else None,
            "rows_rejected": total_issue_count,
            "quality_rule_failures": low_conf + unmapped,
            "null_rate_pct": None,
        }
    if stage_id == "gold":
        return {
            "duplicates_resolved": duplicates if duplicates > 0 else None,
            "entities_merged": None,
            "rows_conformed": total_rows if has_row_data else None,
            "merge_conflicts": len(error_jobs),
        }
    if stage_id == "staging":
        return {
            "rows_inserted": total_rows if has_row_data else None,
            "upserts": None,
            "jdbc_errors": len(error_jobs),
            "commit_time_s": avg_dur,
        }
    if stage_id == "complete":
        return {
            "final_row_count": total_rows if has_row_data else None,
            "tables_populated": len(jobs_at_stage),
            "total_duration_s": avg_dur,
            "pipeline_status": "SUCCESS" if not error_jobs else "PARTIAL",
        }
    return {}


@app.get(
    "/api/stages/{stage_id}/jobs",
    tags=["Pipeline"],
    summary="Jobs, metrics, and issues for a pipeline stage",
)
async def get_stage_jobs(
    stage_id: str,
    _claims: dict = Depends(verify_token),
) -> dict[str, Any]:
    """Return live jobs, aggregated metrics, and reconciliation issues for a stage."""
    stage_id_lower = stage_id.lower()
    stage_idx = next(
        (i for i, name in enumerate(_STAGE_NAMES) if name.lower() == stage_id_lower),
        None,
    )
    if stage_idx is None:
        raise HTTPException(status_code=404, detail=f"Stage '{stage_id}' not found.")

    jobs_at_stage: list[dict] = []
    stage_job_ids: set = set()

    for job in _jobs.values():
        job_stage_str = _val(job.current_stage)
        job_stage_idx = _STAGE_ENUM_TO_IDX.get(job_stage_str, 0)
        if job_stage_idx != stage_idx:
            continue

        fj = _to_frontend_job(job)
        timings = _job_stage_timings.get(job.job_id, [])
        time_in_stage: Optional[int] = None
        if timings and stage_idx < len(timings):
            time_in_stage = timings[stage_idx].get("duration")

        jobs_at_stage.append({
            "id": fj["id"],
            "file": fj["file_name"],
            "system": fj["system"],
            "rows": fj["rows"],
            "time_in_stage": time_in_stage,
            "status": fj["status"],
            "issues": fj["issues"],
            "error_message": fj["error_message"],
        })
        stage_job_ids.add(job.job_id)

    metrics = _compute_stage_metrics(stage_id_lower, stage_job_ids, jobs_at_stage)

    # Build issues list from reconciliation queue + job error messages
    issues_out: list[dict] = []
    _issue_type_labels = {
        "LOW_CONFIDENCE_MAPPING": "Low Confidence",
        "DUPLICATE_CONTACT": "Duplicate",
        "UNMAPPED_CODE": "Unmapped Code",
        "SCHEMA_DRIFT": "Schema Drift",
    }
    _issue_severity = {
        "LOW_CONFIDENCE_MAPPING": "warn",
        "UNMAPPED_CODE": "warn",
        "DUPLICATE_CONTACT": "warn",
        "SCHEMA_DRIFT": "error",
    }
    for iss in _issues.values():
        if iss.job_id not in stage_job_ids:
            continue
        if _val(iss.status) not in ("PENDING", "ESCALATED"):
            continue
        itype = _val(iss.issue_type)
        desc = iss.source_column
        if iss.current_value:
            desc += f": {iss.current_value}"
        if iss.proposed_value:
            desc += f" → {iss.proposed_value}"
        issues_out.append({
            "id": iss.issue_id[:8].upper(),
            "type": _issue_type_labels.get(itype, itype),
            "desc": desc[:150],
            "severity": _issue_severity.get(itype, "warn"),
        })
    # Surface job-level errors as issues
    for job in _jobs.values():
        if job.job_id not in stage_job_ids:
            continue
        if job.error_message:
            issues_out.append({
                "id": job.job_id[:8].upper(),
                "type": "Pipeline Error",
                "desc": job.error_message[:150],
                "severity": "error",
            })

    return {"jobs": jobs_at_stage, "metrics": metrics, "issues": issues_out}


@app.get(
    "/api/stages/{stage_id}/logs",
    tags=["Pipeline"],
    summary="Execution logs for a pipeline stage",
)
async def get_stage_logs(
    stage_id: str,
    _claims: dict = Depends(verify_token),
) -> dict[str, Any]:
    """Return execution logs for a pipeline stage, sourced from Databricks run data."""
    import time as _time

    stage_id_lower = stage_id.lower()
    stage_idx = next(
        (i for i, name in enumerate(_STAGE_NAMES) if name.lower() == stage_id_lower),
        None,
    )
    if stage_idx is None:
        raise HTTPException(status_code=404, detail=f"Stage '{stage_id}' not found.")

    logs: list[dict] = []

    if not _db_client:
        return {"logs": logs}

    # Collect unique run_ids across all jobs (including jobs that passed through this stage)
    run_ids: set = set()
    for job in _jobs.values():
        run_id = getattr(job, "_run_id", None)
        if run_id:
            run_ids.add(run_id)

    # Limit to 5 most-recently-added run IDs to avoid excessive API calls
    recent_run_ids = list(run_ids)[-5:]

    for run_id in recent_run_ids:
        try:
            run_data = await _db_client.get_run_status(run_id)
        except Exception as exc:
            logger.debug("get_run_status(%s) failed: %s", run_id, exc)
            continue

        tasks = run_data.get("tasks", [])
        for task in tasks:
            task_key = task.get("task_key", "")
            if task_key not in _TASK_TO_STAGE:
                continue
            _, task_stage_idx = _TASK_TO_STAGE[task_key]
            if task_stage_idx != stage_idx:
                continue

            state = task.get("state", {})
            lifecycle = state.get("life_cycle_state", "")
            result = state.get("result_state", "")
            start_ms = task.get("start_time", 0) or 0
            end_ms = task.get("end_time", 0) or 0

            def _ms_to_time(ms: int) -> str:
                from datetime import timezone
                if not ms:
                    return "—"
                return datetime.utcfromtimestamp(ms / 1000).strftime("%H:%M:%S")

            # Task lifecycle log entries
            if start_ms:
                logs.append({
                    "level": "INFO",
                    "time": _ms_to_time(start_ms),
                    "msg": f"[{task_key}] Task started",
                })

            # Attempt to fetch notebook cell output
            task_run_id = str(task.get("run_id", ""))
            if task_run_id and result in ("SUCCESS", "FAILED", "TIMEDOUT"):
                try:
                    output = await _db_client.get_run_output(task_run_id)
                    notebook_out = output.get("notebook_output", {}).get("result", "")
                    error_trace = output.get("error_trace", "")

                    if notebook_out:
                        for line in notebook_out.splitlines()[-30:]:
                            line = line.strip()
                            if not line:
                                continue
                            lvl = "INFO"
                            if any(w in line.upper() for w in ("ERROR", "EXCEPTION", "TRACEBACK", "FAIL")):
                                lvl = "ERROR"
                            elif any(w in line.upper() for w in ("WARN", "WARNING")):
                                lvl = "WARN"
                            logs.append({
                                "level": lvl,
                                "time": _ms_to_time(end_ms or start_ms),
                                "msg": f"[{task_key}] {line[:200]}",
                            })
                    if error_trace:
                        for line in error_trace.splitlines()[:10]:
                            line = line.strip()
                            if line:
                                logs.append({
                                    "level": "ERROR",
                                    "time": _ms_to_time(end_ms or start_ms),
                                    "msg": f"[{task_key}] {line[:200]}",
                                })
                except Exception as exc:
                    logger.debug("get_run_output(%s) failed: %s", task_run_id, exc)

            # Terminal status entry
            if end_ms or lifecycle in ("TERMINATED", "SKIPPED"):
                level = "INFO" if result == "SUCCESS" else ("ERROR" if result in ("FAILED", "TIMEDOUT") else "INFO")
                duration_s = int((end_ms - start_ms) / 1000) if end_ms and start_ms else None
                msg = f"[{task_key}] {lifecycle} — {result}"
                if duration_s is not None:
                    msg += f" ({duration_s}s)"
                logs.append({"level": level, "time": _ms_to_time(end_ms), "msg": msg})
            elif lifecycle == "RUNNING":
                elapsed = int(_time.time() - start_ms / 1000) if start_ms else 0
                logs.append({
                    "level": "INFO",
                    "time": _ms_to_time(int(_time.time() * 1000)),
                    "msg": f"[{task_key}] RUNNING — {elapsed}s elapsed",
                })

    # Sort by time string (HH:MM:SS) ascending
    logs.sort(key=lambda e: e.get("time", ""))
    return {"logs": logs}


# ---------------------------------------------------------------------------
# Data Quality / Quarantine endpoints (silver tables)
# ---------------------------------------------------------------------------

_QUALITY_CATALOG = os.getenv("CATALOG", "oregon_doj_demo_catalog")


async def _query_silver(sql: str) -> list[dict]:
    """Run a SQL query against silver quality tables and return rows as dicts."""
    if not _db_client:
        return []
    try:
        return await _db_client.execute_sql(sql)
    except Exception as exc:
        logger.warning("Quality query failed: %s", exc)
        return []


async def _exec_silver(sql: str) -> None:
    """Execute a DML/DDL statement against the silver schema; raises on failure."""
    if not _db_client:
        raise HTTPException(status_code=503, detail="Database client not available")
    await _db_client.execute_sql(sql)


@app.get(
    "/api/quality/summary",
    tags=["Quality"],
    summary="High-level summary of all data quality issues from silver quarantine tables",
)
async def quality_summary(
    _claims: dict = Depends(verify_token),
) -> dict[str, Any]:
    """Return aggregate counts by issue type and severity from silver.quarantine_log."""
    c = _QUALITY_CATALOG
    rows = await _query_silver(f"""
        SELECT issue_type, severity, SUM(affected_record_count) AS total_affected,
               COUNT(*) AS issue_categories
        FROM {c}.silver.quarantine_log
        GROUP BY issue_type, severity
        ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END
    """)
    totals = {"total_issues": 0, "critical": 0, "high": 0, "medium": 0}
    by_type: dict[str, dict] = {}
    for r in rows:
        sev = (r.get("severity") or "MEDIUM").lower()
        cnt = int(r.get("total_affected") or 0)
        totals["total_issues"] += cnt
        if sev in totals:
            totals[sev] += cnt
        t = r.get("issue_type", "UNKNOWN")
        by_type.setdefault(t, {"total": 0, "critical": 0, "high": 0, "medium": 0})
        by_type[t]["total"] += cnt
        if sev in by_type[t]:
            by_type[t][sev] += cnt
    return {"totals": totals, "by_type": by_type, "rows": rows}


@app.get(
    "/api/quality/low-confidence",
    tags=["Quality"],
    summary="Schema mappings flagged as low-confidence by the LLM",
)
async def quality_low_confidence(
    limit: int = Query(default=50, ge=1, le=500),
    _claims: dict = Depends(verify_token),
) -> list[dict]:
    c = _QUALITY_CATALOG
    return await _query_silver(f"""
        SELECT m.source_system, m.source_table, m.source_column,
               m.suggested_target, CAST(m.mapping_confidence AS DOUBLE) AS mapping_confidence,
               m.priority, m.review_status, m.llm_rationale, m.reviewer_note, m.mapping_date
        FROM {c}.silver.low_confidence_mappings m
        LEFT JOIN {c}.silver.review_decisions rd
               ON rd.issue_key = CONCAT('mapping::', m.source_system, '::', m.source_column)
        WHERE rd.decision IS NULL
        ORDER BY m.mapping_confidence ASC
        LIMIT {int(limit)}
    """)


@app.get(
    "/api/quality/unmapped-codes",
    tags=["Quality"],
    summary="Records referencing codes not found in the canonical lookup table",
)
async def quality_unmapped_codes(
    limit: int = Query(default=100, ge=1, le=1000),
    _claims: dict = Depends(verify_token),
) -> list[dict]:
    c = _QUALITY_CATALOG
    return await _query_silver(f"""
        SELECT u.source_table, u.field_name, u.code_value, u.lookup_domain,
               COUNT(*) AS record_count,
               MIN(u.record_identifier) AS example_record
        FROM {c}.silver.unmapped_codes u
        LEFT JOIN {c}.silver.review_decisions rd
               ON rd.issue_key = CONCAT('unmapped::', u.source_table, '::', u.field_name, '::', u.code_value)
        WHERE rd.decision IS NULL
        GROUP BY u.source_table, u.field_name, u.code_value, u.lookup_domain
        ORDER BY record_count DESC
        LIMIT {int(limit)}
    """)


@app.get(
    "/api/quality/duplicates",
    tags=["Quality"],
    summary="Duplicate contact records detected via name+DOB fingerprinting",
)
async def quality_duplicates(
    limit: int = Query(default=50, ge=1, le=200),
    _claims: dict = Depends(verify_token),
) -> list[dict]:
    c = _QUALITY_CATALOG
    return await _query_silver(f"""
        SELECT d.duplicate_group_id, d.last_name, d.first_name, d.date_of_birth,
               d.total_records, d.distinct_defendant_ids, d.severity,
               d.all_defendant_ids, d.earliest_seen, d.latest_seen
        FROM {c}.silver.duplicate_contacts d
        LEFT JOIN {c}.silver.review_decisions rd
               ON rd.issue_key = CONCAT('duplicate::', d.duplicate_group_id)
        WHERE rd.decision IS NULL
        ORDER BY CASE d.severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
                 d.total_records DESC
        LIMIT {int(limit)}
    """)


@app.get(
    "/api/quality/schema-drift",
    tags=["Quality"],
    summary="Records with field values that deviate from the expected schema format",
)
async def quality_schema_drift(
    limit: int = Query(default=100, ge=1, le=500),
    _claims: dict = Depends(verify_token),
) -> list[dict]:
    c = _QUALITY_CATALOG
    return await _query_silver(f"""
        SELECT s.source_table, s.field_name, s.drift_type,
               COUNT(*) AS record_count,
               MIN(s.field_value) AS example_value,
               s.drift_description
        FROM {c}.silver.schema_drift_records s
        LEFT JOIN {c}.silver.review_decisions rd
               ON rd.issue_key = CONCAT('drift::', s.source_table, '::', s.field_name, '::', s.drift_type)
        WHERE rd.decision IS NULL
        GROUP BY s.source_table, s.field_name, s.drift_type, s.drift_description
        ORDER BY record_count DESC
        LIMIT {int(limit)}
    """)


class QualityDecisionPayload(BaseModel):
    issue_type: str   # "mapping" | "duplicate" | "unmapped" | "drift"
    issue_key: str    # stable compound key, e.g. "mapping::LegacyCase::RACE_CD"
    decision: str     # "approved" | "rejected" | "merged" | "kept_separate" | ...
    reviewer: str = "SME"
    note: str = ""


@app.put(
    "/api/quality/decision",
    tags=["Quality"],
    summary="Record an SME review decision and persist it to silver.review_decisions",
)
async def record_quality_decision(
    payload: QualityDecisionPayload,
    _claims: dict = Depends(verify_token),
) -> dict[str, str]:
    c = _QUALITY_CATALOG

    # Escape single quotes to prevent SQL injection
    def esc(s: str) -> str:
        return str(s).replace("'", "''")

    await _exec_silver(f"""
        INSERT INTO {c}.silver.review_decisions
            (issue_type, issue_key, decision, reviewer, note, decided_at)
        VALUES
            ('{esc(payload.issue_type)}', '{esc(payload.issue_key)}',
             '{esc(payload.decision)}', '{esc(payload.reviewer)}', '{esc(payload.note)}',
             current_timestamp())
    """)

    logger.info(
        "Quality decision recorded: %s / %s → %s by %s",
        payload.issue_type, payload.issue_key, payload.decision, payload.reviewer,
    )
    return {"status": "ok", "issue_key": payload.issue_key, "decision": payload.decision}


# ---------------------------------------------------------------------------
# Cases / Defendant 360 endpoints
# ---------------------------------------------------------------------------

# Re-use _QUALITY_CATALOG — both sets of tables live in the same catalog
_BRONZE_CAT = _QUALITY_CATALOG  # oregon_doj_demo_catalog



@app.get(
    "/api/cases/search",
    tags=["Cases"],
    summary="Search defendants by name or ID",
)
async def search_defendants(
    q: str = Query(..., min_length=1, description="Partial name or defendant ID"),
    limit: int = Query(20, ge=1, le=100),
    _claims: dict = Depends(verify_token),
) -> list[dict]:
    c = _BRONZE_CAT

    def esc(s: str) -> str:
        return str(s).replace("'", "''")

    safe_q = esc(q.upper())
    return await _query_silver(f"""
        SELECT DEFENDANT_ID, FIRST_NAME, LAST_NAME, MIDDLE_INIT,
               DOB, RACE_CD, GENDER_CD, COUNTY_CD, CASE_STATUS_CD,
               CHARGE_CD, CHARGE_DESC, SOURCE
        FROM (
            SELECT DefendantID AS DEFENDANT_ID, FIRST_NAME, LAST_NAME, MIDDLE_INIT,
                   DOB, RACE_CD, GENDER_CD, COUNTY_CD, CASE_STATUS_CD,
                   CHARGE_CD, CHARGE_DESC, 'bronze' AS SOURCE
            FROM {c}.bronze.legacycase_tbl_defendant
            WHERE UPPER(LAST_NAME) LIKE '%{safe_q}%'
               OR UPPER(FIRST_NAME) LIKE '%{safe_q}%'
               OR UPPER(DefendantID) LIKE '%{safe_q}%'

            UNION ALL

            SELECT duplicate_group_id AS DEFENDANT_ID,
                   first_name AS FIRST_NAME, last_name AS LAST_NAME,
                   NULL AS MIDDLE_INIT, NULL AS DOB,
                   NULL AS RACE_CD, NULL AS GENDER_CD, NULL AS COUNTY_CD,
                   'DUPLICATE' AS CASE_STATUS_CD, NULL AS CHARGE_CD,
                   CONCAT('Duplicate group — ', CAST(total_records AS STRING), ' records') AS CHARGE_DESC,
                   'duplicate_contacts' AS SOURCE
            FROM {c}.silver.duplicate_contacts
            WHERE UPPER(last_name) LIKE '%{safe_q}%'
               OR UPPER(first_name) LIKE '%{safe_q}%'
        )
        ORDER BY SOURCE, LAST_NAME, FIRST_NAME
        LIMIT {int(limit)}
    """)


@app.get(
    "/api/cases/{defendant_id}/profile",
    tags=["Cases"],
    summary="Full 360 profile: LegacyCase + AdHocExports + OpenJustice context",
)
async def get_defendant_profile(
    defendant_id: str,
    _claims: dict = Depends(verify_token),
) -> dict:
    c = _BRONZE_CAT

    def esc(s: str) -> str:
        return str(s).replace("'", "''")

    safe_id = esc(defendant_id)

    # ── Check if this is a duplicate_group_id (from silver.duplicate_contacts) ──
    # These IDs don't exist in bronze; fetch their profile from the silver table.
    dup_group_rows = await _query_silver(f"""
        SELECT duplicate_group_id, first_name, last_name, date_of_birth,
               severity, total_records, distinct_defendant_ids,
               all_defendant_ids, all_case_ids, earliest_seen, latest_seen
        FROM {c}.silver.duplicate_contacts
        WHERE duplicate_group_id = '{safe_id}'
        LIMIT 1
    """)
    if dup_group_rows:
        row = dup_group_rows[0]
        # Build a synthetic defendant record from the duplicate group
        defendant_record = {
            "DEFENDANT_ID": row.get("duplicate_group_id"),
            "FIRST_NAME": row.get("first_name"),
            "LAST_NAME": row.get("last_name"),
            "DOB": row.get("date_of_birth"),
            "CASE_STATUS_CD": "DUPLICATE",
            "SOURCE": "duplicate_contacts",
        }
        quality_flag = {
            "duplicate_group_id": row.get("duplicate_group_id"),
            "severity": row.get("severity"),
            "total_records": row.get("total_records"),
            "all_defendant_ids": row.get("all_defendant_ids"),
        }
        return {
            "defendant": defendant_record,
            "cases": [],
            "events": [],
            "programs": [],
            "quality_flags": [quality_flag],
            "oj_context": [],
        }

    # ── Parallel queries ────────────────────────────────────────────────────

    defendant_task = _query_silver(f"""
        SELECT DefendantID AS DEFENDANT_ID, CASE_ID, FIRST_NAME, LAST_NAME, MIDDLE_INIT,
               DOB, RACE_CD, GENDER_CD, CHARGE_CD, CHARGE_DESC, COURT_CD, JUDGE_ID,
               ARRAIGNMENT_DT, DISPOSITION_DT, DISPOSITION_CD, SENTENCE_CD,
               COUNTY_CD, CASE_STATUS_CD, PRIOR_OFFENSES, PUBLIC_DEFENDER_FLG
        FROM {c}.bronze.legacycase_tbl_defendant
        WHERE DefendantID = '{safe_id}'
        LIMIT 1
    """)

    cases_task = _query_silver(f"""
        SELECT c.* FROM {c}.bronze.legacycase_tbl_case c
        JOIN {c}.bronze.legacycase_tbl_defendant d
          ON c.CaseID = d.CASE_ID
        WHERE d.DefendantID = '{safe_id}'
        ORDER BY c.FILING_DATE DESC
    """)

    events_task = _query_silver(f"""
        SELECT e.* FROM {c}.bronze.legacycase_tbl_event e
        JOIN {c}.bronze.legacycase_tbl_defendant d
          ON e.CASE_ID = d.CASE_ID
        WHERE d.DefendantID = '{safe_id}'
        ORDER BY e.EVENT_DATE DESC
    """)

    programs_task = _query_silver(f"""
        SELECT ClientID, DEFENDANT_REF, PROGRAM, STATUS, COUNTY,
               ENROLLMENT_DATE, EXIT_DATE, AGE_AT_ENROLLMENT, RISK_LEVEL
        FROM {c}.bronze.adhoc_client
        WHERE DEFENDANT_REF = '{safe_id}'
        ORDER BY ENROLLMENT_DATE DESC
    """)

    quality_task = _query_silver(f"""
        SELECT duplicate_group_id, severity, total_records, all_defendant_ids
        FROM {c}.silver.duplicate_contacts
        WHERE ARRAY_CONTAINS(all_defendant_ids, '{safe_id}')
        LIMIT 5
    """)

    results = await asyncio.gather(
        defendant_task, cases_task, events_task, programs_task, quality_task,
        return_exceptions=True,
    )

    def safe(r: Any, default: list) -> list:
        return r if isinstance(r, list) else default

    defendant_info = safe(results[0], [])
    cases         = safe(results[1], [])
    events        = safe(results[2], [])
    programs      = safe(results[3], [])
    quality_flags = safe(results[4], [])

    # Log any errors from parallel queries (non-fatal)
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            logger.warning("Parallel query %d for defendant %s failed: %s", i, safe_id, r)

    # ── OpenJustice aggregate context (sequential — depends on defendant data) ─
    oj_context: list[dict] = []
    if defendant_info:
        charge_cd = defendant_info[0].get("CHARGE_CD") or ""
        charge_desc = defendant_info[0].get("CHARGE_DESC") or ""
        # Map charge to an OJ category via partial match on description
        charge_filter = ""
        if charge_desc:
            safe_charge = esc(charge_desc.upper().split()[0] if charge_desc.split() else "")
            if safe_charge:
                charge_filter = f"WHERE UPPER(CHARGE_CATEGORY) LIKE '%{safe_charge}%'"
        oj_rows = await _query_silver(f"""
            SELECT YEAR, CHARGE_CATEGORY,
                   SUM(TOTAL_ARRESTS) AS TOTAL_ARRESTS,
                   SUM(FELONY_ARRESTS) AS FELONY_ARRESTS
            FROM {c}.bronze.openjustice_arrests
            {charge_filter}
            GROUP BY YEAR, CHARGE_CATEGORY
            ORDER BY YEAR DESC, TOTAL_ARRESTS DESC
            LIMIT 10
        """)
        oj_context = oj_rows if isinstance(oj_rows, list) else []

    return {
        "defendant": defendant_info[0] if defendant_info else None,
        "cases": cases,
        "events": events,
        "programs": programs,
        "quality_flags": quality_flags,
        "oj_context": oj_context,
    }


# ---------------------------------------------------------------------------
# Serve React frontend (production build)
# IMPORTANT: This mount must come AFTER all API route definitions.
# StaticFiles mounted at "/" matches every path and returns 405 for non-GET
# requests, which would shadow POST /api/upload and other write endpoints.
# ---------------------------------------------------------------------------

_frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"
if _frontend_dist.exists():
    # Serve static assets (JS, CSS, images) from /assets directly via StaticFiles
    # for efficient cache-header handling.
    _assets_dir = _frontend_dist / "assets"
    if _assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(_assets_dir)), name="assets")

    # Catch-all: serve any existing file, or fall back to index.html so React
    # Router can handle client-side paths like /pipeline, /upload, /review.
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str) -> FileResponse:
        candidate = _frontend_dist / full_path
        if candidate.exists() and candidate.is_file():
            return FileResponse(candidate)
        index_html = _frontend_dist / "index.html"
        if index_html.exists():
            return FileResponse(index_html)
        raise HTTPException(status_code=404, detail="Frontend not built")

    logger.info("Serving frontend from %s", _frontend_dist)
else:
    logger.warning(
        "Frontend dist not found at %s — UI will not be served.", _frontend_dist
    )
