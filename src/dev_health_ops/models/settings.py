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
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dev_health_ops.models.git import GUID, Base


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
    For single-tenant deployments, org_id identifies the tenant organization.

    Sensitive values should be stored encrypted (is_encrypted=True).
    """

    __tablename__ = "settings"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(
        Text, nullable=False, index=True, server_default=""
    )
    category: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    key: Mapped[str] = mapped_column(Text, nullable=False)
    value: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Setting value (may be encrypted)"
    )
    is_encrypted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
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
        value: str | None = None,
        org_id: str | None = None,
        is_encrypted: bool = False,
        description: str | None = None,
    ) -> None:
        self.id = uuid.uuid4()
        self.org_id = org_id or ""
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

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(
        Text, nullable=False, index=True, server_default=""
    )
    provider: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    name: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Display name for this credential set"
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    credentials_encrypted: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Encrypted JSON containing provider-specific credentials",
    )

    config: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        default=dict,
        comment="Non-sensitive provider configuration (base URLs, options)",
    )

    last_test_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_test_success: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    last_test_error: Mapped[str | None] = mapped_column(Text, nullable=True)

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
        org_id: str | None = None,
        credentials_encrypted: str | None = None,
        config: dict[str, Any] | None = None,
        is_active: bool = True,
    ) -> None:
        self.id = uuid.uuid4()
        self.org_id = org_id or ""
        self.provider = provider
        self.name = name
        self.is_active = is_active
        self.credentials_encrypted = credentials_encrypted
        self.config = config or {}
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)


class ProviderOAuthCredential(Base):
    """Encrypted OAuth payload with an optimistic-lock version for token rotation."""

    __tablename__ = "provider_oauth_credentials"

    org_id: Mapped[str] = mapped_column(Text, primary_key=True)
    provider: Mapped[str] = mapped_column(Text, primary_key=True)
    credential_name: Mapped[str] = mapped_column(Text, primary_key=True)
    token_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    binding_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    granted_scopes: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    has_refresh_token: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    account_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    account_display: Mapped[str | None] = mapped_column(Text, nullable=True)


class ProviderOAuthRevocation(Base):
    """Encrypted remote OAuth revocation work retained until PagerDuty accepts it."""

    __tablename__ = "provider_oauth_revocations"

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    credential_name: Mapped[str] = mapped_column(Text, nullable=False)
    purpose: Mapped[str] = mapped_column(Text, nullable=False)
    token_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    token_key_version: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class PagerDutyOAuthAuthorizationRequest(Base):
    """Server-side PKCE authorization-request state for PagerDuty OAuth setup.

    Persists the one-time authorization context between ``authorize`` and the
    OAuth ``callback``. Keyed by the SHA-256 hash of the opaque ``state`` token
    sent to PagerDuty (never the plaintext state), so a database reader cannot
    forge a callback. Only the PKCE ``code_verifier`` is encrypted; it is used
    server-side for the code exchange and never travels through the browser.
    Rows are single-use (consumed on callback) and expire via ``expires_at``.
    """

    __tablename__ = "pagerduty_oauth_authorization_requests"

    state_hash: Mapped[str] = mapped_column(Text, primary_key=True)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    code_verifier_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )


class GithubAppInstallation(Base):
    """Mapping of a GitHub App installation to a Dev Health organization.

    Captures the installation lifecycle (created / deleted / suspend /
    unsuspend) reported via the ``installation`` webhook, plus the link from
    ``installation_id`` to an org established by the signed-state install
    callback. The App-mode credential the sync pipeline consumes is written
    separately to ``integration_credentials`` using the server-held App
    private key (never an end-user secret).
    """

    __tablename__ = "github_app_installations"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    installation_id: Mapped[int] = mapped_column(
        BigInteger, nullable=False, unique=True, index=True
    )
    account_login: Mapped[str | None] = mapped_column(Text, nullable=True)
    account_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    org_id: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    suspended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
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


class SyncConfiguration(Base):
    """Configuration for data sync jobs.

    Defines what to sync (repos, projects, teams) and how (filters, options).

    Authentication is NOT configured here. ``credential_id`` was removed
    (CHAOS-2762): it was a second, unfrozen mirror of ``Integration
    .credential_id`` that the auth-resolution path (``sync/planner.py``,
    ``workers/sync_bootstrap.py``) never read. The sanctioned surface is
    ``Integration.credential_id``, reached via this config's
    ``integration_id`` FK and frozen onto ``sync_runs`` at plan time
    (CHAOS-2755's ``resolve_run_auth``).
    """

    __tablename__ = "sync_configurations"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(
        Text, nullable=False, index=True, server_default=""
    )
    name: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Display name for this sync config"
    )
    provider: Mapped[str] = mapped_column(Text, nullable=False, index=True)

    sync_targets: Mapped[list[str]] = mapped_column(
        JSON,
        nullable=False,
        default=list,
        comment="List of targets to sync (repos, projects, etc.)",
    )

    sync_options: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        default=dict,
        comment="Provider-specific sync options",
    )

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    planner_managed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    last_sync_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_sync_success: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    last_sync_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_sync_stats: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Stats from last sync (items synced, duration, etc.)",
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

    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID,
        ForeignKey("sync_configurations.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    parent: Mapped[SyncConfiguration | None] = relationship(
        "SyncConfiguration",
        remote_side="SyncConfiguration.id",
        back_populates="children",
        foreign_keys=[parent_id],
        lazy="raise",
    )
    children: Mapped[list[SyncConfiguration]] = relationship(
        "SyncConfiguration",
        back_populates="parent",
        cascade="all, delete-orphan",
        foreign_keys="SyncConfiguration.parent_id",
        lazy="raise",
    )
    integration_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID,
        ForeignKey("integrations.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID,
        ForeignKey("integration_sources.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id", "provider", "name", name="uq_sync_config_org_provider_name"
        ),
        Index("ix_sync_config_org_provider", "org_id", "provider"),
    )

    def __init__(
        self,
        name: str,
        provider: str,
        org_id: str | None = None,
        sync_targets: list[str] | None = None,
        sync_options: dict[str, Any] | None = None,
        is_active: bool = True,
        parent_id: uuid.UUID | None = None,
        integration_id: uuid.UUID | None = None,
        source_id: uuid.UUID | None = None,
        planner_managed: bool = False,
    ) -> None:
        self.id = uuid.uuid4()
        self.org_id = org_id or ""
        self.name = name
        self.provider = provider
        self.sync_targets = sync_targets or []
        self.sync_options = sync_options or {}
        self.is_active = is_active
        self.parent_id = parent_id
        self.integration_id = integration_id
        self.source_id = source_id
        self.planner_managed = planner_managed
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)


class ScheduledJob(Base):
    """Scheduled job definitions.

    Defines recurring jobs (syncs, metrics computation, etc.) with
    cron-like scheduling and execution tracking.
    """

    __tablename__ = "scheduled_jobs"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(
        Text, nullable=False, index=True, server_default=""
    )
    name: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Display name for this job"
    )
    job_type: Mapped[str] = mapped_column(
        Text, nullable=False, index=True, comment="Type of job (sync, metrics, etc.)"
    )

    provider: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default="",
        comment="Provider this job belongs to (empty string for non-provider jobs)",
    )

    schedule_cron: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Cron expression for scheduling (e.g., '0 * * * *' for hourly)",
    )
    timezone: Mapped[str] = mapped_column(Text, nullable=False, default="UTC")

    job_config: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        default=dict,
        comment="Job-specific configuration",
    )

    sync_config_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID,
        ForeignKey("sync_configurations.id", ondelete="SET NULL"),
        nullable=True,
    )
    sync_config: Mapped[SyncConfiguration | None] = relationship("SyncConfiguration")

    status: Mapped[int] = mapped_column(
        Integer, nullable=False, default=JobStatus.ACTIVE.value
    )
    is_running: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    last_run_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_run_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_run_duration_seconds: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    last_run_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    run_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

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

    __table_args__ = (
        UniqueConstraint(
            "org_id", "provider", "name", name="uq_scheduled_job_org_provider_name"
        ),
        UniqueConstraint(
            "org_id",
            "sync_config_id",
            "job_type",
            name="uq_scheduled_job_org_sync_config_type",
        ),
        Index("ix_scheduled_job_org_type", "org_id", "job_type"),
        Index("ix_scheduled_job_next_run", "next_run_at"),
    )

    def __init__(
        self,
        name: str,
        job_type: str,
        schedule_cron: str,
        org_id: str | None = None,
        provider: str = "",
        job_config: dict[str, Any] | None = None,
        sync_config_id: uuid.UUID | None = None,
        tz: str = "UTC",
        status: int = JobStatus.ACTIVE.value,
    ) -> None:
        self.id = uuid.uuid4()
        self.org_id = org_id or ""
        self.name = name
        self.job_type = job_type
        self.provider = provider
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

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("scheduled_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    job: Mapped[ScheduledJob] = relationship("ScheduledJob")

    status: Mapped[int] = mapped_column(
        Integer, nullable=False, default=JobRunStatus.PENDING.value
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    duration_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)

    result: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True, comment="Job execution results/stats"
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_traceback: Mapped[str | None] = mapped_column(Text, nullable=True)

    triggered_by: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="scheduler",
        comment="What triggered this run (scheduler, manual, webhook)",
    )

    created_at: Mapped[datetime] = mapped_column(
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
    ) -> None:
        self.id = uuid.uuid4()
        self.job_id = job_id
        self.triggered_by = triggered_by
        self.status = status
        self.created_at = datetime.now(timezone.utc)


class SyncWatermark(Base):
    """Per-source/dataset sync watermarks for incremental sync.

    Legacy repo_id/target remain populated during the transition from
    repo-scoped to source/dataset-scoped sync state.
    """

    __tablename__ = "sync_watermarks"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    repo_id: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="owner/repo for GitHub, project_id for GitLab",
    )
    source_id: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Integration source identifier for generalized sync state",
    )
    target: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Sync target: git, prs, cicd, deployments, incidents, work-items",
    )
    dataset_key: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Dataset key for generalized sync state",
    )
    last_synced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of last successful sync for this target",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id", "repo_id", "target", name="uq_sync_watermark_org_repo_target"
        ),
        UniqueConstraint(
            "org_id",
            "source_id",
            "dataset_key",
            name="uq_sync_watermark_org_source_dataset",
        ),
        Index("ix_sync_watermark_org_repo", "org_id", "repo_id"),
    )

    def __init__(
        self,
        repo_id: str,
        target: str,
        org_id: str | None = None,
        source_id: str | None = None,
        dataset_key: str | None = None,
        last_synced_at: datetime | None = None,
    ) -> None:
        self.id = uuid.uuid4()
        self.org_id = org_id or ""
        self.repo_id = repo_id
        self.source_id = source_id or repo_id
        self.target = target
        self.dataset_key = dataset_key or target
        self.last_synced_at = last_synced_at
        self.updated_at = datetime.now(timezone.utc)
