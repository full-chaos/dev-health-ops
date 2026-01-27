from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Column, DateTime, Text
from dev_health_ops.models.git import Base


@dataclass(frozen=True)
class AtlassianOpsIncident:
    id: str
    url: Optional[str]
    summary: str
    description: Optional[str]
    status: str
    severity: str
    created_at: datetime
    provider_id: Optional[str] = None
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class AtlassianOpsAlert:
    id: str
    status: str
    priority: str
    created_at: datetime
    acknowledged_at: Optional[datetime] = None
    snoozed_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class AtlassianOpsSchedule:
    id: str
    name: str
    timezone: Optional[str] = None
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class AtlassianOpsIncidentModel(Base):
    __tablename__ = "atlassian_ops_incidents"

    id = Column(Text, primary_key=True)
    url = Column(Text, nullable=True)
    summary = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    status = Column(Text, nullable=False)
    severity = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    provider_id = Column(Text, nullable=True)
    last_synced = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class AtlassianOpsAlertModel(Base):
    __tablename__ = "atlassian_ops_alerts"

    id = Column(Text, primary_key=True)
    status = Column(Text, nullable=False)
    priority = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    acknowledged_at = Column(DateTime(timezone=True), nullable=True)
    snoozed_at = Column(DateTime(timezone=True), nullable=True)
    closed_at = Column(DateTime(timezone=True), nullable=True)
    last_synced = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class AtlassianOpsScheduleModel(Base):
    __tablename__ = "atlassian_ops_schedules"

    id = Column(Text, primary_key=True)
    name = Column(Text, nullable=False)
    timezone = Column(Text, nullable=True)
    last_synced = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
