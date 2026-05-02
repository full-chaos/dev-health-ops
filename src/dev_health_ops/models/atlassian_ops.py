from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import Base


@dataclass(frozen=True)
class AtlassianOpsIncident:
    id: str
    url: str | None
    summary: str
    description: str | None
    status: str
    severity: str
    created_at: datetime
    provider_id: str | None = None
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class AtlassianOpsAlert:
    id: str
    status: str
    priority: str
    created_at: datetime
    acknowledged_at: datetime | None = None
    snoozed_at: datetime | None = None
    closed_at: datetime | None = None
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class AtlassianOpsSchedule:
    id: str
    name: str
    timezone: str | None = None
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class AtlassianOpsIncidentModel(Base):
    __tablename__ = "atlassian_ops_incidents"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    provider_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_synced: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class AtlassianOpsAlertModel(Base):
    __tablename__ = "atlassian_ops_alerts"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    snoozed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    closed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_synced: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class AtlassianOpsScheduleModel(Base):
    __tablename__ = "atlassian_ops_schedules"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    timezone: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_synced: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
