"""Saved report and report run models.

Persistence flows through Postgres semantic layer only — no file exports.
"""

from __future__ import annotations

import copy
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dev_health_ops.models.git import GUID, Base

if TYPE_CHECKING:
    from .settings import ScheduledJob


class ReportRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class SavedReport(Base):
    __tablename__ = "saved_reports"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(
        Text, nullable=False, index=True, server_default=""
    )
    name: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Display name for this report"
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    report_plan: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=False,
        default=dict,
        comment="Serialized ReportPlan dataclass as JSON",
    )

    is_template: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    template_source_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID,
        ForeignKey("saved_reports.id", ondelete="SET NULL"),
        nullable=True,
        comment="ID of the report this was cloned from",
    )

    parameters: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        default=dict,
        comment="Parameterized fields: team, repo, date_range overrides",
    )

    schedule_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID,
        ForeignKey("scheduled_jobs.id", ondelete="SET NULL"),
        nullable=True,
        comment="FK to scheduled_jobs for recurring execution",
    )
    schedule: Mapped[ScheduledJob | None] = relationship(
        "ScheduledJob", foreign_keys=[schedule_id]
    )

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_run_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_run_status: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Last execution status"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    created_by: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="User or system that created this"
    )

    runs: Mapped[list[ReportRun]] = relationship(
        "ReportRun",
        back_populates="report",
        cascade="all, delete-orphan",
        lazy="raise",
    )
    template_source: Mapped[SavedReport | None] = relationship(
        "SavedReport",
        remote_side="SavedReport.id",
        foreign_keys=[template_source_id],
        lazy="raise",
    )

    __table_args__ = (
        Index("ix_saved_reports_org_name", "org_id", "name"),
        Index("ix_saved_reports_org_template", "org_id", "is_template"),
    )

    def __init__(
        self,
        name: str,
        org_id: str = "",
        description: str | None = None,
        report_plan: dict | None = None,
        is_template: bool = False,
        template_source_id: uuid.UUID | None = None,
        parameters: dict | None = None,
        schedule_id: uuid.UUID | None = None,
        is_active: bool = True,
        created_by: str | None = None,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.name = name
        self.description = description
        self.report_plan = report_plan or {}
        self.is_template = is_template
        self.template_source_id = template_source_id
        self.parameters = parameters or {}
        self.schedule_id = schedule_id
        self.is_active = is_active
        self.created_by = created_by
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def clone(
        self,
        new_name: str | None = None,
        parameter_overrides: dict[str, Any] | None = None,
    ) -> SavedReport:
        """Deep copy this report with a new ID. Sets template_source_id to self.id."""
        cloned_plan = copy.deepcopy(self.report_plan)
        cloned_params: dict[str, Any] = copy.deepcopy(self.parameters or {})
        if parameter_overrides:
            cloned_params.update(parameter_overrides)

        return SavedReport(
            name=new_name or f"{self.name} (Copy)",
            org_id=self.org_id,
            description=self.description,
            report_plan=cloned_plan,
            is_template=False,
            template_source_id=self.id,
            parameters=cloned_params,
            is_active=True,
            created_by=self.created_by,
        )


class ReportRun(Base):
    __tablename__ = "report_runs"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    report_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("saved_reports.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    report: Mapped[SavedReport] = relationship("SavedReport", back_populates="runs")

    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=ReportRunStatus.PENDING.value,
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)

    rendered_markdown: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Rendered report markdown"
    )
    artifact_url: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="URL to externally stored artifact (future use)",
    )

    provenance_records: Mapped[list[dict[str, Any]] | None] = mapped_column(
        JSON,
        nullable=True,
        default=list,
        comment="List of provenance records for this run",
    )

    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_traceback: Mapped[str | None] = mapped_column(Text, nullable=True)

    triggered_by: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="manual",
        comment="What triggered this run: scheduler, manual, api",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_report_runs_report_created", "report_id", "created_at"),
        Index("ix_report_runs_status", "status"),
    )

    def __init__(
        self,
        report_id: uuid.UUID,
        triggered_by: str = "manual",
        status: str = ReportRunStatus.PENDING.value,
    ):
        self.id = uuid.uuid4()
        self.report_id = report_id
        self.triggered_by = triggered_by
        self.status = status
        self.created_at = datetime.now(timezone.utc)
