"""Settings and configuration models for Enterprise Edition.

This module defines the database models for storing application settings,
integration credentials, sync configurations, and scheduled jobs.

All sensitive data (API tokens, secrets) should be stored encrypted
using the SettingsService encryption utilities.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from dev_health_ops.models.git import Base, GUID


class SettingCategory(str, Enum):
    """Categories for grouping settings."""

    GENERAL = "general"
    GITHUB = "github"
    GITLAB = "gitlab"
    JIRA = "jira"
    LINEAR = "linear"
    ATLASSIAN = "atlassian"
    LLM = "llm"
    SYNC = "sync"
    NOTIFICATIONS = "notifications"
    RETENTION = "retention"


class IntegrationProvider(str, Enum):
    """Supported integration providers."""

    GITHUB = "github"
    GITLAB = "gitlab"
    JIRA = "jira"
    LINEAR = "linear"
    ATLASSIAN = "atlassian"


class JobStatus(int, Enum):
    ACTIVE = 0
    PAUSED = 1
    DISABLED = 2


class JobRunStatus(int, Enum):
    PENDING = 0
    RUNNING = 1
    SUCCESS = 2
    FAILED = 3
    CANCELLED = 4


class Setting(Base):
    """Key-value store for application settings.

    Settings are scoped to an organization (org_id) for multi-tenancy.
    For single-tenant deployments, org_id can be a default value like 'default'.

    Sensitive values should be stored encrypted (is_encrypted=True).
    """

    __tablename__ = "settings"

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    org_id = Column(Text, nullable=False, default="default", index=True)
    category = Column(Text, nullable=False, index=True)
    key = Column(Text, nullable=False)
    value = Column(Text, nullable=True, comment="Setting value (may be encrypted)")
    is_encrypted = Column(Boolean, nullable=False, default=False)
    description = Column(Text, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id", "category", "key", name="uq_settings_org_category_key"
        ),
        Index("ix_settings_org_category", "org_id", "category"),
    )

    def __init__(
        self,
        key: str,
        category: str = SettingCategory.GENERAL.value,
        value: Optional[str] = None,
        org_id: str = "default",
        is_encrypted: bool = False,
        description: Optional[str] = None,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.category = category
        self.key = key
        self.value = value
        self.is_encrypted = is_encrypted
        self.description = description
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)


class IntegrationCredential(Base):
    """Encrypted storage for integration credentials.

    Each provider (GitHub, GitLab, Jira, etc.) can have one set of credentials
    per organization. All sensitive fields are encrypted at rest.
    """

    __tablename__ = "integration_credentials"

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    org_id = Column(Text, nullable=False, default="default", index=True)
    provider = Column(Text, nullable=False, index=True)
    name = Column(Text, nullable=False, comment="Display name for this credential set")
    is_active = Column(Boolean, nullable=False, default=True)

    credentials_encrypted = Column(
        Text,
        nullable=True,
        comment="Encrypted JSON containing provider-specific credentials",
    )

    config = Column(
        JSON,
        nullable=True,
        default=dict,
        comment="Non-sensitive provider configuration (base URLs, options)",
    )

    last_test_at = Column(DateTime(timezone=True), nullable=True)
    last_test_success = Column(Boolean, nullable=True)
    last_test_error = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id", "provider", "name", name="uq_credentials_org_provider_name"
        ),
        Index("ix_credentials_org_provider", "org_id", "provider"),
    )

    def __init__(
        self,
        provider: str,
        name: str,
        org_id: str = "default",
        credentials_encrypted: Optional[str] = None,
        config: Optional[dict] = None,
        is_active: bool = True,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.provider = provider
        self.name = name
        self.is_active = is_active
        self.credentials_encrypted = credentials_encrypted
        self.config = config or {}
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)


class SyncConfiguration(Base):
    """Configuration for data sync jobs.

    Defines what to sync (repos, projects, teams) and how (filters, options).
    Links to IntegrationCredential for authentication.
    """

    __tablename__ = "sync_configurations"

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    org_id = Column(Text, nullable=False, default="default", index=True)
    name = Column(Text, nullable=False, comment="Display name for this sync config")
    provider = Column(Text, nullable=False, index=True)

    credential_id = Column(
        GUID,
        ForeignKey("integration_credentials.id", ondelete="SET NULL"),
        nullable=True,
    )
    credential = relationship("IntegrationCredential")

    sync_targets = Column(
        JSON,
        nullable=False,
        default=list,
        comment="List of targets to sync (repos, projects, etc.)",
    )

    sync_options = Column(
        JSON,
        nullable=False,
        default=dict,
        comment="Provider-specific sync options",
    )

    is_active = Column(Boolean, nullable=False, default=True)
    last_sync_at = Column(DateTime(timezone=True), nullable=True)
    last_sync_success = Column(Boolean, nullable=True)
    last_sync_error = Column(Text, nullable=True)
    last_sync_stats = Column(
        JSON,
        nullable=True,
        comment="Stats from last sync (items synced, duration, etc.)",
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("org_id", "name", name="uq_sync_config_org_name"),
        Index("ix_sync_config_org_provider", "org_id", "provider"),
    )

    def __init__(
        self,
        name: str,
        provider: str,
        org_id: str = "default",
        credential_id: Optional[uuid.UUID] = None,
        sync_targets: Optional[list] = None,
        sync_options: Optional[dict] = None,
        is_active: bool = True,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.name = name
        self.provider = provider
        self.credential_id = credential_id
        self.sync_targets = sync_targets or []
        self.sync_options = sync_options or {}
        self.is_active = is_active
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)


class ScheduledJob(Base):
    """Scheduled job definitions.

    Defines recurring jobs (syncs, metrics computation, etc.) with
    cron-like scheduling and execution tracking.
    """

    __tablename__ = "scheduled_jobs"

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    org_id = Column(Text, nullable=False, default="default", index=True)
    name = Column(Text, nullable=False, comment="Display name for this job")
    job_type = Column(
        Text, nullable=False, index=True, comment="Type of job (sync, metrics, etc.)"
    )

    schedule_cron = Column(
        Text,
        nullable=False,
        comment="Cron expression for scheduling (e.g., '0 * * * *' for hourly)",
    )
    timezone = Column(Text, nullable=False, default="UTC")

    job_config = Column(
        JSON,
        nullable=False,
        default=dict,
        comment="Job-specific configuration",
    )

    sync_config_id = Column(
        GUID,
        ForeignKey("sync_configurations.id", ondelete="SET NULL"),
        nullable=True,
    )
    sync_config = relationship("SyncConfiguration")

    status = Column(Integer, nullable=False, default=JobStatus.ACTIVE.value)
    is_running = Column(Boolean, nullable=False, default=False)

    last_run_at = Column(DateTime(timezone=True), nullable=True)
    last_run_status = Column(Integer, nullable=True)
    last_run_duration_seconds = Column(Integer, nullable=True)
    last_run_error = Column(Text, nullable=True)
    next_run_at = Column(DateTime(timezone=True), nullable=True)
    run_count = Column(Integer, nullable=False, default=0)
    failure_count = Column(Integer, nullable=False, default=0)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("org_id", "name", name="uq_scheduled_job_org_name"),
        Index("ix_scheduled_job_org_type", "org_id", "job_type"),
        Index("ix_scheduled_job_next_run", "next_run_at"),
    )

    def __init__(
        self,
        name: str,
        job_type: str,
        schedule_cron: str,
        org_id: str = "default",
        job_config: Optional[dict] = None,
        sync_config_id: Optional[uuid.UUID] = None,
        tz: str = "UTC",
        status: int = JobStatus.ACTIVE.value,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.name = name
        self.job_type = job_type
        self.schedule_cron = schedule_cron
        self.timezone = tz
        self.job_config = job_config or {}
        self.sync_config_id = sync_config_id
        self.status = status
        self.is_running = False
        self.run_count = 0
        self.failure_count = 0
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)


class JobRun(Base):
    """Individual job execution records.

    Tracks each execution of a scheduled job with timing and results.
    """

    __tablename__ = "job_runs"

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    job_id = Column(
        GUID,
        ForeignKey("scheduled_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    job = relationship("ScheduledJob")

    status = Column(Integer, nullable=False, default=JobRunStatus.PENDING.value)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Integer, nullable=True)

    result = Column(JSON, nullable=True, comment="Job execution results/stats")
    error = Column(Text, nullable=True)
    error_traceback = Column(Text, nullable=True)

    triggered_by = Column(
        Text,
        nullable=False,
        default="scheduler",
        comment="What triggered this run (scheduler, manual, webhook)",
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_job_runs_job_created", "job_id", "created_at"),
        Index("ix_job_runs_status", "status"),
    )

    def __init__(
        self,
        job_id: uuid.UUID,
        triggered_by: str = "scheduler",
        status: int = JobRunStatus.PENDING.value,
    ):
        self.id = uuid.uuid4()
        self.job_id = job_id
        self.triggered_by = triggered_by
        self.status = status
        self.created_at = datetime.now(timezone.utc)


class IdentityMapping(Base):
    """Identity mappings for correlating users across providers.

    Maps provider-specific identities (GitHub usernames, Jira account IDs, etc.)
    to a canonical identity for unified reporting.
    """

    __tablename__ = "identity_mappings"

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    org_id = Column(Text, nullable=False, default="default", index=True)

    canonical_id = Column(
        Text,
        nullable=False,
        index=True,
        comment="Canonical identity (usually email or unique ID)",
    )
    display_name = Column(Text, nullable=True)
    email = Column(Text, nullable=True, index=True)

    provider_identities = Column(
        JSON,
        nullable=False,
        default=dict,
        comment="Map of provider -> [identities] (e.g., {'github': ['user1'], 'jira': ['accountId']})",
    )

    team_ids = Column(
        JSON,
        nullable=False,
        default=list,
        comment="List of team IDs this identity belongs to",
    )

    is_active = Column(Boolean, nullable=False, default=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("org_id", "canonical_id", name="uq_identity_org_canonical"),
        Index("ix_identity_org_email", "org_id", "email"),
    )

    def __init__(
        self,
        canonical_id: str,
        org_id: str = "default",
        display_name: Optional[str] = None,
        email: Optional[str] = None,
        provider_identities: Optional[dict] = None,
        team_ids: Optional[list] = None,
        is_active: bool = True,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.canonical_id = canonical_id
        self.display_name = display_name
        self.email = email
        self.provider_identities = provider_identities or {}
        self.team_ids = team_ids or []
        self.is_active = is_active
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)


class TeamMapping(Base):
    """Team mappings for organizing identities and work scopes.

    Replaces YAML-based team configuration with database storage.
    """

    __tablename__ = "team_mappings"

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    org_id = Column(Text, nullable=False, default="default", index=True)

    team_id = Column(Text, nullable=False, comment="Unique team identifier (slug)")
    name = Column(Text, nullable=False, comment="Team display name")
    description = Column(Text, nullable=True)

    repo_patterns = Column(
        JSON,
        nullable=False,
        default=list,
        comment="List of repo patterns (glob) this team owns",
    )
    project_keys = Column(
        JSON,
        nullable=False,
        default=list,
        comment="List of Jira/Linear project keys",
    )

    extra_data = Column(
        JSON,
        nullable=True,
        default=dict,
        comment="Additional team data (cost center, manager, etc.)",
    )
    managed_fields = Column(
        JSON,
        nullable=False,
        default=list,
        comment="Fields the provider owns (e.g. name, repo_patterns)",
    )
    sync_policy = Column(
        Integer,
        nullable=False,
        default=1,
        comment="0=merge (auto-apply), 1=flag (review), 2=manual_only",
    )
    flagged_changes = Column(
        JSON,
        nullable=True,
        comment="Pending provider-suggested changes for admin review",
    )
    last_drift_sync_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Last time this team was checked for drift",
    )

    is_active = Column(Boolean, nullable=False, default=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("org_id", "team_id", name="uq_team_mapping_org_team"),
    )

    def __init__(
        self,
        team_id: str,
        name: str,
        org_id: str = "default",
        description: Optional[str] = None,
        repo_patterns: Optional[list] = None,
        project_keys: Optional[list] = None,
        extra_data: Optional[dict] = None,
        managed_fields: Optional[list] = None,
        sync_policy: int = 1,
        flagged_changes: Optional[dict] = None,
        is_active: bool = True,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.team_id = team_id
        self.name = name
        self.description = description
        self.repo_patterns = repo_patterns or []
        self.project_keys = project_keys or []
        self.extra_data = extra_data or {}
        self.managed_fields = managed_fields or []
        self.sync_policy = sync_policy
        self.flagged_changes = flagged_changes
        self.is_active = is_active
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)
