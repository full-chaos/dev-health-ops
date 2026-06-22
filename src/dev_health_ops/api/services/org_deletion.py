from __future__ import annotations

import asyncio
import logging
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.db import get_clickhouse_uri
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.billing_audit import BillingAuditLog
from dev_health_ops.models.checkpoints import MetricCheckpoint
from dev_health_ops.models.impersonation import ImpersonationSession
from dev_health_ops.models.invoices import Invoice, InvoiceLineItem
from dev_health_ops.models.ip_allowlist import OrgIPAllowlist
from dev_health_ops.models.licensing import OrgFeatureOverride, OrgLicense
from dev_health_ops.models.org_invite import OrgInvite
from dev_health_ops.models.refresh_token import RefreshToken
from dev_health_ops.models.refunds import Refund
from dev_health_ops.models.reports import ReportRun, SavedReport
from dev_health_ops.models.retention import OrgRetentionPolicy
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    JobStatus,
    ScheduledJob,
    Setting,
    SyncConfiguration,
    SyncWatermark,
)
from dev_health_ops.models.sso import SSOProvider
from dev_health_ops.models.subscriptions import Subscription, SubscriptionEvent
from dev_health_ops.models.users import Membership, Organization

logger = logging.getLogger(__name__)

_CLICKHOUSE_MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[2] / "migrations" / "clickhouse"
)
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(?P<table>[A-Za-z_][\w]*)`?\s*\(",
    re.IGNORECASE,
)
_ALTER_ORG_ID_RE = re.compile(
    r"ALTER\s+TABLE\s+`?(?P<table>[A-Za-z_][\w]*)`?\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+org_id\b",
    re.IGNORECASE,
)
_PY_TABLE_RE = re.compile(r'["\'](?P<table>[A-Za-z_][\w]*)["\']\s*:\s*["\']\(org_id\b')


@dataclass(slots=True)
class DeletionScopeResult:
    total: int = 0
    tables: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"total": self.total, "tables": dict(self.tables)}


@dataclass(slots=True)
class DeletionResult:
    organization_id: str
    dry_run: bool
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    postgres: DeletionScopeResult = field(default_factory=DeletionScopeResult)
    clickhouse: DeletionScopeResult = field(default_factory=DeletionScopeResult)
    disabled_jobs: int = 0
    credentials_deleted: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        timestamp = self.timestamp.astimezone(timezone.utc).isoformat()
        return {
            "organization_id": self.organization_id,
            "dry_run": self.dry_run,
            "timestamp": timestamp.replace("+00:00", "Z"),
            "postgres": self.postgres.to_dict(),
            "clickhouse": self.clickhouse.to_dict(),
            "disabled_jobs": self.disabled_jobs,
            "credentials_deleted": self.credentials_deleted,
            "warnings": list(self.warnings),
        }

    def dict(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True, slots=True)
class PostgresDeletionTarget:
    table: str
    model: Any
    predicate: Callable[[uuid.UUID, str], Any]


@lru_cache(maxsize=1)
def _clickhouse_tables_from_migrations() -> tuple[str, ...]:
    tables: set[str] = set()
    if not _CLICKHOUSE_MIGRATIONS_DIR.exists():
        return ()

    for path in sorted(_CLICKHOUSE_MIGRATIONS_DIR.glob("*")):
        if path.suffix not in {".py", ".sql"}:
            continue
        text = path.read_text(encoding="utf-8")
        for match in _ALTER_ORG_ID_RE.finditer(text):
            tables.add(match.group("table"))
        for match in _PY_TABLE_RE.finditer(text):
            tables.add(match.group("table"))
        for statement in text.split(";"):
            if not re.search(r"\borg_id\b", statement):
                continue
            create_match = _CREATE_TABLE_RE.search(statement)
            if create_match:
                tables.add(create_match.group("table"))

    return tuple(sorted(tables))


def _uuid_org_id(org_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(org_id))
    except ValueError as exc:
        raise ValueError("Invalid organization id") from exc


def _postgres_targets() -> list[PostgresDeletionTarget]:
    def scheduled_job_ids(_org_uuid: uuid.UUID, org_id: str) -> Any:
        return select(ScheduledJob.id).where(ScheduledJob.org_id == org_id)

    def saved_report_ids(_org_uuid: uuid.UUID, org_id: str) -> Any:
        return select(SavedReport.id).where(SavedReport.org_id == org_id)

    def invoice_ids(org_uuid: uuid.UUID, _org_id: str) -> Any:
        return select(Invoice.id).where(Invoice.org_id == org_uuid)

    def subscription_ids(org_uuid: uuid.UUID, _org_id: str) -> Any:
        return select(Subscription.id).where(Subscription.org_id == org_uuid)

    return [
        PostgresDeletionTarget(
            "report_runs",
            ReportRun,
            lambda org_uuid, org_id: ReportRun.report_id.in_(
                saved_report_ids(org_uuid, org_id)
            ),
        ),
        PostgresDeletionTarget(
            "saved_reports",
            SavedReport,
            lambda _org_uuid, org_id: SavedReport.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "job_runs",
            JobRun,
            lambda org_uuid, org_id: JobRun.job_id.in_(
                scheduled_job_ids(org_uuid, org_id)
            ),
        ),
        PostgresDeletionTarget(
            "backfill_jobs",
            BackfillJob,
            lambda _org_uuid, org_id: BackfillJob.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "refunds", Refund, lambda org_uuid, _org_id: Refund.org_id == org_uuid
        ),
        PostgresDeletionTarget(
            "invoice_line_items",
            InvoiceLineItem,
            lambda org_uuid, org_id: InvoiceLineItem.invoice_id.in_(
                invoice_ids(org_uuid, org_id)
            ),
        ),
        PostgresDeletionTarget(
            "invoices", Invoice, lambda org_uuid, _org_id: Invoice.org_id == org_uuid
        ),
        PostgresDeletionTarget(
            "subscription_events",
            SubscriptionEvent,
            lambda org_uuid, org_id: SubscriptionEvent.subscription_id.in_(
                subscription_ids(org_uuid, org_id)
            ),
        ),
        PostgresDeletionTarget(
            "subscriptions",
            Subscription,
            lambda org_uuid, _org_id: Subscription.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "metric_checkpoints",
            MetricCheckpoint,
            lambda _org_uuid, org_id: MetricCheckpoint.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "sync_watermarks",
            SyncWatermark,
            lambda _org_uuid, org_id: SyncWatermark.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "scheduled_jobs",
            ScheduledJob,
            lambda _org_uuid, org_id: ScheduledJob.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "sync_configurations",
            SyncConfiguration,
            lambda _org_uuid, org_id: SyncConfiguration.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "integration_credentials",
            IntegrationCredential,
            lambda _org_uuid, org_id: IntegrationCredential.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "settings", Setting, lambda _org_uuid, org_id: Setting.org_id == org_id
        ),
        PostgresDeletionTarget(
            "sso_providers",
            SSOProvider,
            lambda org_uuid, _org_id: SSOProvider.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "org_ip_allowlist",
            OrgIPAllowlist,
            lambda org_uuid, _org_id: OrgIPAllowlist.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "org_feature_overrides",
            OrgFeatureOverride,
            lambda org_uuid, _org_id: OrgFeatureOverride.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "org_licenses",
            OrgLicense,
            lambda org_uuid, _org_id: OrgLicense.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "org_retention_policies",
            OrgRetentionPolicy,
            lambda org_uuid, _org_id: OrgRetentionPolicy.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "org_invites",
            OrgInvite,
            lambda org_uuid, _org_id: OrgInvite.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "refresh_tokens",
            RefreshToken,
            lambda org_uuid, _org_id: RefreshToken.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "impersonation_sessions",
            ImpersonationSession,
            lambda org_uuid, _org_id: ImpersonationSession.target_org_id == org_uuid,
        ),
        # NOTE: team + identity catalogs are ClickHouse-native (CH `teams` /
        # `identities` tables), purged org-scoped via `_purge_clickhouse`. The
        # Postgres `team_mappings` / `identity_mappings` tables were dropped in
        # CHAOS-2600 CS6, so there are no Postgres deletion targets for them.
        PostgresDeletionTarget(
            "memberships",
            Membership,
            lambda org_uuid, _org_id: Membership.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "audit_logs",
            AuditLog,
            lambda org_uuid, _org_id: AuditLog.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "billing_audit_log",
            BillingAuditLog,
            lambda org_uuid, _org_id: BillingAuditLog.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "organizations",
            Organization,
            lambda org_uuid, _org_id: Organization.id == org_uuid,
        ),
    ]


class OrganizationDeletionService:
    def __init__(self, session: AsyncSession, *, clickhouse_client: Any | None = None):
        self.session = session
        self.clickhouse_client = clickhouse_client

    async def delete(self, org_id: str, *, dry_run: bool = False) -> DeletionResult:
        org_uuid = _uuid_org_id(org_id)
        org_id_str = str(org_uuid)
        result = DeletionResult(organization_id=org_id_str, dry_run=dry_run)

        result.disabled_jobs = await self._count_where(
            ScheduledJob, ScheduledJob.org_id == org_id_str
        )
        result.credentials_deleted = await self._credential_count(org_uuid, org_id_str)

        for target in _postgres_targets():
            predicate = target.predicate(org_uuid, org_id_str)
            count = await self._count_where(target.model, predicate)
            result.postgres.tables[target.table] = count
            result.postgres.total += count

        if not dry_run:
            await self._disable_scheduled_jobs(org_id_str)
            for target in _postgres_targets():
                count = result.postgres.tables[target.table]
                if count == 0:
                    continue
                await self.session.execute(
                    delete(target.model)
                    .where(target.predicate(org_uuid, org_id_str))
                    .execution_options(synchronize_session=False)
                )
            await self.session.flush()

        await self._purge_clickhouse(org_id_str, dry_run=dry_run, result=result)

        logger.info(
            "Organization deletion finished org_id=%s dry_run=%s postgres_rows=%s clickhouse_rows=%s",
            org_id_str,
            "True" if dry_run else "False",
            result.postgres.total,
            result.clickhouse.total,
        )
        return result

    async def _count_where(self, model: Any, predicate: Any) -> int:
        stmt = select(func.count()).select_from(model).where(predicate)
        count = await self.session.scalar(stmt)
        return int(count or 0)

    async def _credential_count(self, org_uuid: uuid.UUID, org_id: str) -> int:
        credential_rows = await self._count_where(
            IntegrationCredential, IntegrationCredential.org_id == org_id
        )
        encrypted_settings = await self._count_where(
            Setting,
            (Setting.org_id == org_id) & (Setting.is_encrypted == True),  # noqa: E712
        )
        sso_secret_rows = await self._count_where(
            SSOProvider,
            (SSOProvider.org_id == org_uuid)
            & (SSOProvider.encrypted_secrets.is_not(None)),
        )
        return credential_rows + encrypted_settings + sso_secret_rows

    async def _disable_scheduled_jobs(self, org_id: str) -> None:
        await self.session.execute(
            update(ScheduledJob)
            .where(ScheduledJob.org_id == org_id)
            .values(
                status=JobStatus.DISABLED.value,
                is_running=False,
                next_run_at=None,
                updated_at=datetime.now(timezone.utc),
            )
            .execution_options(synchronize_session=False)
        )
        await self.session.flush()

    async def _purge_clickhouse(
        self, org_id: str, *, dry_run: bool, result: DeletionResult
    ) -> None:
        tables = _clickhouse_tables_from_migrations()
        if not tables:
            result.warnings.append("ClickHouse migration table catalog is empty.")
            return

        client, close_client = self._resolve_clickhouse_client(result)
        if client is None:
            return

        try:
            for table in tables:
                org_id_type = await self._clickhouse_org_id_type(client, table)
                if org_id_type is None:
                    result.warnings.append(
                        f"ClickHouse table {table} missing or has no org_id column; skipped."
                    )
                    continue

                condition = self._clickhouse_org_id_condition(org_id_type)
                count = await self._clickhouse_count(client, table, condition, org_id)
                result.clickhouse.tables[table] = count
                result.clickhouse.total += count
                if dry_run or count == 0:
                    continue
                await self._clickhouse_delete(client, table, condition, org_id)
        finally:
            if close_client is not None:
                close_client()

    def _resolve_clickhouse_client(
        self, result: DeletionResult
    ) -> tuple[Any | None, Callable[[], None] | None]:
        if self.clickhouse_client is not None:
            client = getattr(self.clickhouse_client, "client", self.clickhouse_client)
            return client, None

        uri = get_clickhouse_uri()
        if not uri:
            result.warnings.append(
                "ClickHouse URI not configured; analytics tables were not verified."
            )
            return None, None

        sink = ClickHouseMetricsSink(dsn=uri)
        return sink.client, sink.close

    async def _clickhouse_org_id_type(self, client: Any, table: str) -> str | None:
        try:
            response = await asyncio.to_thread(
                client.query,
                "SELECT type FROM system.columns "
                "WHERE database = currentDatabase() "
                "AND table = {table:String} AND name = 'org_id'",
                parameters={"table": table},
            )
        except Exception as exc:
            logger.warning(
                "Unable to verify ClickHouse table org_id column org_id table=%s error=%s",
                table,
                exc,
            )
            return None
        rows = list(getattr(response, "result_rows", []) or [])
        if not rows:
            return None
        return str(rows[0][0])

    def _clickhouse_org_id_condition(self, org_id_type: str) -> str:
        if "UUID" in org_id_type.upper():
            return "org_id = toUUID({org_id:String})"
        return "org_id = {org_id:String}"

    async def _clickhouse_count(
        self, client: Any, table: str, condition: str, org_id: str
    ) -> int:
        try:
            response = await asyncio.to_thread(
                client.query,
                f"SELECT count() FROM `{table}` WHERE {condition}",
                parameters={"org_id": org_id},
            )
        except Exception as exc:
            logger.warning(
                "Unable to count ClickHouse table for org deletion org_id=%s table=%s error=%s",
                org_id,
                table,
                exc,
            )
            return 0
        rows = list(getattr(response, "result_rows", []) or [])
        return int(rows[0][0]) if rows else 0

    async def _clickhouse_delete(
        self, client: Any, table: str, condition: str, org_id: str
    ) -> None:
        try:
            await asyncio.to_thread(
                client.command,
                f"ALTER TABLE `{table}` DELETE WHERE {condition}",
                parameters={"org_id": org_id},
            )
        except Exception as exc:
            logger.warning(
                "Unable to delete ClickHouse table for org deletion org_id=%s table=%s error=%s",
                org_id,
                table,
                exc,
            )


__all__ = [
    "DeletionResult",
    "DeletionScopeResult",
    "OrganizationDeletionService",
]
