"""
Pipeline-specific FastAPI router for the DOJ data migration API.

Provides endpoints for stage-level metrics, per-job execution traces,
stage log retrieval, and job retry.  When ``DEMO_MODE=true`` the endpoints
return realistic synthetic data so the UI can be developed and demoed
without a live Databricks workspace.
"""

from __future__ import annotations

import logging
import os
import random
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Path, Query, status

from models import (
    JobStatusValue,
    JobTrace,
    LogEntry,
    LogLevel,
    PipelineStage,
    SourceSystem,
    StageMetrics,
    StageTrace,
)

logger = logging.getLogger(__name__)

DEMO_MODE: bool = os.getenv("DEMO_MODE", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api", tags=["Pipeline"])

# ---------------------------------------------------------------------------
# Demo / mock data helpers
# ---------------------------------------------------------------------------

_DEMO_SOURCES = [
    SourceSystem.LEGACY_CASE,
    SourceSystem.OPEN_JUSTICE,
    SourceSystem.AD_HOC_EXPORTS,
]

_ORDERED_STAGES = [
    PipelineStage.UPLOAD,
    PipelineStage.BRONZE,
    PipelineStage.SILVER,
    PipelineStage.MAPPING,
    PipelineStage.GOLD,
    PipelineStage.STAGING,
    PipelineStage.COMPLETE,
]

_STAGE_AVG_SECONDS: dict[PipelineStage, float] = {
    PipelineStage.UPLOAD: 5.2,
    PipelineStage.BRONZE: 42.0,
    PipelineStage.SILVER: 118.5,
    PipelineStage.MAPPING: 77.3,
    PipelineStage.GOLD: 95.1,
    PipelineStage.STAGING: 61.8,
    PipelineStage.COMPLETE: 0.0,
    PipelineStage.FAILED: 0.0,
}

# Deterministic fake job metadata keyed by short prefix
_DEMO_JOBS: list[dict[str, Any]] = [
    {
        "job_id": "demo-lc-001",
        "file_name": "legacy_case_q1_2024.xlsx",
        "source_system": SourceSystem.LEGACY_CASE,
        "current_stage": PipelineStage.GOLD,
        "status": JobStatusValue.RUNNING,
        "rows_processed": 143_820,
        "issue_count": 34,
    },
    {
        "job_id": "demo-lc-002",
        "file_name": "legacy_case_q2_2024.xlsx",
        "source_system": SourceSystem.LEGACY_CASE,
        "current_stage": PipelineStage.COMPLETE,
        "status": JobStatusValue.COMPLETE,
        "rows_processed": 98_412,
        "issue_count": 12,
    },
    {
        "job_id": "demo-oj-001",
        "file_name": "open_justice_arrests_2023.csv",
        "source_system": SourceSystem.OPEN_JUSTICE,
        "current_stage": PipelineStage.MAPPING,
        "status": JobStatusValue.REVIEW_NEEDED,
        "rows_processed": 55_001,
        "issue_count": 89,
    },
    {
        "job_id": "demo-oj-002",
        "file_name": "open_justice_dispositions_2023.csv",
        "source_system": SourceSystem.OPEN_JUSTICE,
        "current_stage": PipelineStage.SILVER,
        "status": JobStatusValue.RUNNING,
        "rows_processed": 31_980,
        "issue_count": 7,
    },
    {
        "job_id": "demo-ah-001",
        "file_name": "adhoc_clients_export_nov2023.xlsx",
        "source_system": SourceSystem.AD_HOC_EXPORTS,
        "current_stage": PipelineStage.FAILED,
        "status": JobStatusValue.FAILED,
        "rows_processed": 4_200,
        "issue_count": 3,
    },
    {
        "job_id": "demo-ah-002",
        "file_name": "adhoc_incidents_export_dec2023.xlsx",
        "source_system": SourceSystem.AD_HOC_EXPORTS,
        "current_stage": PipelineStage.STAGING,
        "status": JobStatusValue.RUNNING,
        "rows_processed": 19_634,
        "issue_count": 21,
    },
]


def _build_demo_stage_metrics() -> list[StageMetrics]:
    """
    Synthesise realistic aggregate metrics for each pipeline stage based on
    the demo job list above.
    """
    metrics: dict[str, StageMetrics] = {
        s.value: StageMetrics(
            stage=s,
            avg_time_seconds=_STAGE_AVG_SECONDS[s],
        )
        for s in PipelineStage
    }

    for job in _DEMO_JOBS:
        stage: PipelineStage = job["current_stage"]
        m = metrics[stage.value if isinstance(stage, PipelineStage) else stage]
        job_status: JobStatusValue = job["status"]
        if job_status == JobStatusValue.RUNNING:
            m.job_count_active += 1
        elif job_status == JobStatusValue.COMPLETE:
            m.job_count_complete += 1
        elif job_status == JobStatusValue.FAILED:
            m.job_count_failed += 1
        m.rows_processed += job.get("rows_processed", 0)
        m.issues_count += job.get("issue_count", 0)

    return list(metrics.values())


def _build_demo_trace(job_id: str) -> JobTrace:
    """
    Build a synthetic stage timeline for the requested demo job.

    Stages up to and including the job's current stage are marked complete;
    any stage after that is omitted (not yet started).
    """
    job_meta = next(
        (j for j in _DEMO_JOBS if j["job_id"] == job_id), None
    )
    if job_meta is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Demo job '{job_id}' not found.",
        )

    current_stage: PipelineStage = job_meta["current_stage"]
    job_status: JobStatusValue = job_meta["status"]

    try:
        current_idx = _ORDERED_STAGES.index(current_stage)
    except ValueError:
        current_idx = len(_ORDERED_STAGES) - 1

    traces: list[StageTrace] = []
    base_time = datetime.utcnow() - timedelta(hours=2)
    cursor = base_time

    for idx, stage in enumerate(_ORDERED_STAGES):
        if idx > current_idx:
            break

        duration = _STAGE_AVG_SECONDS[stage] * random.uniform(0.85, 1.15)
        start = cursor
        end = start + timedelta(seconds=duration)
        cursor = end

        is_current = idx == current_idx
        if is_current and job_status == JobStatusValue.RUNNING:
            stage_status = JobStatusValue.RUNNING
            end = None  # type: ignore[assignment]
            duration_val = None
        elif is_current and job_status == JobStatusValue.FAILED:
            stage_status = JobStatusValue.FAILED
            duration_val = duration
        elif is_current and job_status == JobStatusValue.REVIEW_NEEDED:
            stage_status = JobStatusValue.REVIEW_NEEDED
            duration_val = duration
        else:
            stage_status = JobStatusValue.COMPLETE
            duration_val = duration

        rows_in = job_meta.get("rows_processed", 10000) + random.randint(-1000, 1000)
        rows_out = int(rows_in * random.uniform(0.98, 1.0))

        traces.append(
            StageTrace(
                stage_name=stage,
                start_time=start,
                end_time=end,
                duration_seconds=duration_val,
                rows_in=max(0, rows_in),
                rows_out=max(0, rows_out),
                issue_count=random.randint(0, 15) if stage == PipelineStage.MAPPING else 0,
                status=stage_status,
            )
        )

    return JobTrace(job_id=job_id, stages=traces)


def _build_demo_logs(stage_id: str, limit: int = 50) -> list[LogEntry]:
    """
    Generate synthetic log lines for a pipeline stage.

    Parameters
    ----------
    stage_id:
        Stage name string (case-insensitive).
    limit:
        Maximum number of log lines to return.
    """
    try:
        stage = PipelineStage(stage_id.upper())
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Stage '{stage_id}' is not a valid pipeline stage.",
        )

    templates: list[tuple[LogLevel, str]] = [
        (LogLevel.INFO, "Stage initialised. Reading partition metadata."),
        (LogLevel.INFO, "Schema validation passed. 0 drift violations detected."),
        (LogLevel.INFO, "Partition pruning applied. Reading 12 of 47 files."),
        (LogLevel.INFO, "Spark job submitted. Application ID: app-20240312-001."),
        (LogLevel.INFO, "Processed 10,000 rows in 4.2s (2,381 rows/sec)."),
        (LogLevel.INFO, "Processed 50,000 rows in 18.9s (2,645 rows/sec)."),
        (LogLevel.WARN, "Column RACE_CODE contains 43 unmapped values — escalating to review queue."),
        (LogLevel.INFO, "Processed 100,000 rows in 39.1s (2,558 rows/sec)."),
        (LogLevel.WARN, "Duplicate contact fingerprint detected for SSN_HASH group 4a9f3b."),
        (LogLevel.INFO, "Confidence threshold 0.85 applied. 127 low-confidence mappings queued."),
        (LogLevel.INFO, "Delta table write completed. OPTIMIZE triggered."),
        (LogLevel.INFO, "ZORDER BY (SourceId, SourceSystem) complete in 8.4s."),
        (LogLevel.INFO, "Audit row written to doj_catalog.gold.load_audit."),
        (LogLevel.INFO, "Stage complete. Output: 143,820 rows written to target layer."),
        (LogLevel.ERROR, "Transient network error on attempt 1/3. Retrying in 5s."),
        (LogLevel.INFO, "Retry succeeded on attempt 2/3."),
        (LogLevel.WARN, "AGE_GROUP values 'JUVENILE' not in code reference table — added to UNMAPPED_CODE queue."),
        (LogLevel.INFO, "Reconciliation issue batch flushed. 34 issues persisted to Delta."),
    ]

    base_time = datetime.utcnow() - timedelta(minutes=45)
    logs: list[LogEntry] = []
    for i, (level, msg) in enumerate(templates[:limit]):
        logs.append(
            LogEntry(
                timestamp=base_time + timedelta(seconds=i * 30),
                level=level,
                stage=stage,
                message=msg,
            )
        )
    return logs


# NOTE: /api/stages/summary is handled by the endpoint in main.py
# (which builds the response directly from the in-memory _jobs store).


# ---------------------------------------------------------------------------
# Endpoint: Job trace
# ---------------------------------------------------------------------------


@router.get(
    "/jobs/{job_id}/trace",
    summary="Full stage timeline for a migration job",
    response_model=JobTrace,
)
async def get_job_trace(
    job_id: str = Path(..., description="Migration job ID to trace."),
) -> JobTrace:
    """
    Return the ordered list of stage-level execution traces for a single
    migration job, including start/end timestamps, row throughput, and issue
    counts per stage.

    In production, this reads from ``doj_catalog.gold.load_audit``.
    When ``DEMO_MODE=true``, a synthetic trace is generated.
    """
    if DEMO_MODE:
        return _build_demo_trace(job_id)

    from main import get_databricks_client  # noqa: PLC0415

    try:
        db = get_databricks_client()
        rows = await db.read_delta_table(
            "doj_catalog.gold.load_audit",
            filters={"job_id": job_id},
        )
        if not rows:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No trace data found for job '{job_id}'.",
            )
        stages = [StageTrace(**row) for row in rows]
        return JobTrace(job_id=job_id, stages=stages)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to read load_audit for job %s: %s", job_id, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to retrieve job trace from Databricks.",
        ) from exc


# ---------------------------------------------------------------------------
# Endpoint: Stage logs
# ---------------------------------------------------------------------------


@router.get(
    "/stages/{stage_id}/logs",
    summary="Recent log entries for a pipeline stage",
    response_model=list[LogEntry],
)
async def get_stage_logs(
    stage_id: str = Path(..., description="Stage name (e.g. BRONZE, SILVER)."),
    limit: int = Query(
        default=50,
        ge=1,
        le=200,
        description="Maximum number of log lines to return.",
    ),
    job_id: Optional[str] = Query(
        default=None, description="Optionally filter logs to a specific job."
    ),
) -> list[LogEntry]:
    """
    Return the most recent log entries emitted by a pipeline stage.
    Results are capped at 200 lines and ordered newest-first.

    In DEMO_MODE, synthetic log lines are returned.
    """
    if DEMO_MODE:
        return _build_demo_logs(stage_id, limit=limit)

    from main import get_databricks_client  # noqa: PLC0415

    try:
        db = get_databricks_client()
        filters: dict[str, Any] = {"stage": stage_id.upper()}
        if job_id:
            filters["job_id"] = job_id
        rows = await db.read_delta_table(
            "doj_catalog.gold.pipeline_logs",
            filters=filters,
            limit=limit,
        )
        return [LogEntry(**row) for row in rows]
    except Exception as exc:
        logger.error("Failed to read logs for stage %s: %s", stage_id, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to retrieve logs from Databricks.",
        ) from exc


# ---------------------------------------------------------------------------
# Endpoint: Retry failed job
# ---------------------------------------------------------------------------


@router.post(
    "/jobs/{job_id}/retry",
    summary="Retry a failed migration job at its current stage",
    response_model=dict,
    status_code=status.HTTP_202_ACCEPTED,
)
async def retry_job(
    job_id: str = Path(..., description="Failed migration job ID to retry."),
) -> dict[str, Any]:
    """
    Re-trigger the Databricks orchestration job for a migration job that is
    currently in FAILED status, starting from the last failed stage.

    In DEMO_MODE, the job is transitioned back to RUNNING and a fake run ID
    is returned without making any real Databricks API calls.
    """
    if DEMO_MODE:
        demo_job = next(
            (j for j in _DEMO_JOBS if j["job_id"] == job_id), None
        )
        if demo_job is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Demo job '{job_id}' not found.",
            )
        fake_run_id = f"demo-retry-{uuid.uuid4().hex[:8]}"
        logger.info("DEMO: Retrying job %s → fake run_id=%s", job_id, fake_run_id)
        return {
            "job_id": job_id,
            "new_run_id": fake_run_id,
            "message": "Retry triggered successfully (DEMO_MODE).",
            "timestamp": datetime.utcnow().isoformat(),
        }

    # Production path
    from main import _jobs, get_databricks_client  # noqa: PLC0415

    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found.",
        )
    if job.status != JobStatusValue.FAILED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Job '{job_id}' is not in FAILED status (current: {job.status}).",
        )

    try:
        from main import DATABRICKS_JOB_ID  # noqa: PLC0415

        db = get_databricks_client()
        run_id = await db.trigger_job(
            DATABRICKS_JOB_ID,
            {
                "job_id": job_id,
                "source_system": job.source_system,
                "retry": "true",
                "retry_from_stage": job.current_stage,
            },
        )
        job.status = JobStatusValue.RUNNING
        job._run_id = run_id  # type: ignore[attr-defined]
        job.error_message = None
        logger.info("Retrying job %s → run_id=%s", job_id, run_id)
        return {
            "job_id": job_id,
            "new_run_id": run_id,
            "message": "Retry triggered successfully.",
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as exc:
        logger.error("Failed to retry job %s: %s", job_id, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to trigger retry: {exc}",
        ) from exc
