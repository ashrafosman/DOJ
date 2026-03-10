"""
Pydantic v2 data models for the DOJ data migration pipeline.

Covers job tracking, reconciliation review, mapping decisions,
stage metrics, pipeline tracing, and log entries.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class SourceSystem(str, Enum):
    """Identifies which upstream source system produced a file."""

    LEGACY_CASE = "LEGACY_CASE"
    OPEN_JUSTICE = "OPEN_JUSTICE"
    AD_HOC_EXPORTS = "AD_HOC_EXPORTS"


class PipelineStage(str, Enum):
    """Ordered stages in the medallion-architecture migration pipeline."""

    UPLOAD = "UPLOAD"
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    MAPPING = "MAPPING"
    GOLD = "GOLD"
    STAGING = "STAGING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class JobStatusValue(str, Enum):
    """High-level execution status of a migration job."""

    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    REVIEW_NEEDED = "REVIEW_NEEDED"


class IssueType(str, Enum):
    """Classification of a reconciliation issue surfaced during mapping."""

    LOW_CONFIDENCE_MAPPING = "LOW_CONFIDENCE_MAPPING"
    DUPLICATE_CONTACT = "DUPLICATE_CONTACT"
    UNMAPPED_CODE = "UNMAPPED_CODE"
    SCHEMA_DRIFT = "SCHEMA_DRIFT"


class IssueStatus(str, Enum):
    """Lifecycle state of an individual reconciliation issue."""

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    ESCALATED = "ESCALATED"


class LogLevel(str, Enum):
    """Severity levels for pipeline log entries."""

    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


# ---------------------------------------------------------------------------
# Core domain models
# ---------------------------------------------------------------------------


class JobStatus(BaseModel):
    """
    Tracks the current state of a single migration job as it moves through
    the pipeline stages from upload through to the Azure SQL staging tables.
    """

    job_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for the migration job.",
    )
    file_name: str = Field(..., description="Original filename submitted by the user.")
    source_system: SourceSystem = Field(
        ..., description="Source system that produced the uploaded file."
    )
    uploaded_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp when the file was received.",
    )
    current_stage: PipelineStage = Field(
        default=PipelineStage.UPLOAD,
        description="Stage the job is currently executing or last completed.",
    )
    status: JobStatusValue = Field(
        default=JobStatusValue.RUNNING,
        description="Overall execution status of the job.",
    )
    rows_processed: int = Field(
        default=0,
        ge=0,
        description="Cumulative count of rows processed so far.",
    )
    issue_count: int = Field(
        default=0,
        ge=0,
        description="Number of reconciliation issues raised for this job.",
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Human-readable error description when status is FAILED.",
    )

    model_config = {"use_enum_values": True}


class ReconciliationIssue(BaseModel):
    """
    Represents a single data-quality or mapping ambiguity that requires
    subject-matter-expert (SME) review before the record can be promoted
    to the gold / staging layer.
    """

    issue_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for this reconciliation issue.",
    )
    job_id: str = Field(..., description="Parent migration job identifier.")
    issue_type: IssueType = Field(..., description="Classification of the issue.")
    source_system: SourceSystem = Field(
        ..., description="Source system from which the issue originated."
    )
    source_table: str = Field(
        ..., description="Table name in the source system (e.g., tbl_Defendant)."
    )
    source_column: str = Field(
        ..., description="Column name in the source system (e.g., RACE_CODE)."
    )
    current_value: Optional[str] = Field(
        default=None,
        description="Raw value as it exists in the source data.",
    )
    proposed_value: Optional[str] = Field(
        default=None,
        description="Proposed target value after transformation/mapping.",
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="ML/heuristic confidence score for the proposed mapping (0–1).",
    )
    rationale: str = Field(
        ...,
        description="Explanation of why this issue was raised and how the proposed value was derived.",
    )
    status: IssueStatus = Field(
        default=IssueStatus.PENDING,
        description="Current lifecycle state of the issue.",
    )
    reviewer: Optional[str] = Field(
        default=None,
        description="Username or email of the SME who reviewed the issue.",
    )
    review_timestamp: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp when the review decision was submitted.",
    )
    reviewer_note: Optional[str] = Field(
        default=None,
        description="Optional free-text note from the reviewer.",
    )

    @field_validator("confidence")
    @classmethod
    def round_confidence(cls, v: float) -> float:
        """Normalise confidence to four decimal places."""
        return round(v, 4)

    model_config = {"use_enum_values": True}


class MappingDecision(BaseModel):
    """
    Records a finalised field-level mapping decision, typically produced by
    an SME approving or overriding an AI-suggested mapping.
    """

    source_table: str = Field(..., description="Source table name.")
    source_column: str = Field(..., description="Source column name.")
    final_maps_to: str = Field(
        ...,
        description="Fully-qualified target column (e.g., Stg_Contact.RaceCode).",
    )
    reviewer: str = Field(
        ..., description="Username or email of the reviewer who approved the mapping."
    )
    note: Optional[str] = Field(
        default=None,
        description="Optional clarification note from the reviewer.",
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp when the decision was recorded.",
    )

    model_config = {"use_enum_values": True}


# ---------------------------------------------------------------------------
# Metrics and observability models
# ---------------------------------------------------------------------------


class StageMetrics(BaseModel):
    """
    Aggregate statistics for a single pipeline stage across all jobs,
    used to populate the monitoring dashboard stage cards.
    """

    stage: PipelineStage = Field(..., description="The pipeline stage being measured.")
    job_count_active: int = Field(
        default=0, ge=0, description="Number of jobs currently executing this stage."
    )
    job_count_complete: int = Field(
        default=0, ge=0, description="Number of jobs that have completed this stage."
    )
    job_count_failed: int = Field(
        default=0, ge=0, description="Number of jobs that failed at this stage."
    )
    avg_time_seconds: float = Field(
        default=0.0,
        ge=0.0,
        description="Average wall-clock time in seconds for this stage.",
    )
    rows_processed: int = Field(
        default=0,
        ge=0,
        description="Total rows processed across all jobs at this stage.",
    )
    issues_count: int = Field(
        default=0,
        ge=0,
        description="Total reconciliation issues raised at this stage.",
    )

    model_config = {"use_enum_values": True}


class StageTrace(BaseModel):
    """
    Timing and throughput information for one stage within a single job trace.
    """

    stage_name: PipelineStage = Field(..., description="Name of this pipeline stage.")
    start_time: Optional[datetime] = Field(
        default=None, description="UTC timestamp when the stage started."
    )
    end_time: Optional[datetime] = Field(
        default=None, description="UTC timestamp when the stage ended (None if still running)."
    )
    duration_seconds: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Elapsed time in seconds (None if stage has not completed).",
    )
    rows_in: int = Field(
        default=0, ge=0, description="Row count entering this stage."
    )
    rows_out: int = Field(
        default=0, ge=0, description="Row count exiting this stage after transformation."
    )
    issue_count: int = Field(
        default=0, ge=0, description="Reconciliation issues raised during this stage."
    )
    status: JobStatusValue = Field(
        ..., description="Execution status of this specific stage."
    )

    model_config = {"use_enum_values": True}


class JobTrace(BaseModel):
    """
    Full end-to-end timeline for a migration job, composed of ordered stage traces.
    Used to populate the job-detail drill-down view.
    """

    job_id: str = Field(..., description="Identifier of the job being traced.")
    stages: list[StageTrace] = Field(
        default_factory=list,
        description="Ordered list of stage-level traces from UPLOAD through COMPLETE/FAILED.",
    )


class LogEntry(BaseModel):
    """
    A single structured log line emitted by the pipeline, surfaced in the
    log viewer on the stage-detail panel.
    """

    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp of the log event.",
    )
    level: LogLevel = Field(..., description="Severity level of the log entry.")
    stage: PipelineStage = Field(
        ..., description="Pipeline stage that emitted this log entry."
    )
    message: str = Field(..., description="Human-readable log message.")

    model_config = {"use_enum_values": True}


# ---------------------------------------------------------------------------
# Request / response helpers
# ---------------------------------------------------------------------------


class IssueDecisionRequest(BaseModel):
    """
    Payload submitted by an SME when approving, rejecting, or editing a
    reconciliation issue via the PUT /api/reconciliation/issues/{issue_id} endpoint.
    """

    status: IssueStatus = Field(
        ..., description="New lifecycle state for the issue (APPROVED/REJECTED/ESCALATED)."
    )
    reviewer: str = Field(..., description="Username or email of the reviewer.")
    reviewer_note: Optional[str] = Field(
        default=None, description="Optional free-text justification for the decision."
    )
    proposed_value: Optional[str] = Field(
        default=None,
        description="Overridden proposed value if the reviewer is editing the mapping.",
    )

    model_config = {"use_enum_values": True}


class UploadResponse(BaseModel):
    """Response body returned after a successful file upload."""

    job_id: str = Field(..., description="Newly-created job identifier.")
    file_name: str = Field(..., description="Stored filename on ADLS.")
    adls_path: str = Field(..., description="Full ADLS path where the file was written.")
    databricks_run_id: Optional[str] = Field(
        default=None,
        description="Databricks job run ID that was triggered (None if trigger failed).",
    )
    message: str = Field(default="Upload successful. Pipeline triggered.")
