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

from dev_health_ops.api.services.derived_store_registry import (
    EXTERNAL_DERIVED_STORES,
    unregistered_clickhouse_tables,
)
from dev_health_ops.core.encryption import decrypt_value
from dev_health_ops.db import get_clickhouse_uri
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.billing_audit import BillingAuditLog
from dev_health_ops.models.checkpoints import MetricCheckpoint, SyncComputeCheckpoint
from dev_health_ops.models.dev_persistence import (
    DevConversation,
    DevFeedback,
    DevMessage,
    DevRun,
    DevToolCall,
)
from dev_health_ops.models.impersonation import ImpersonationSession
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunPostDispatch,
    SyncRunReferenceDiscovery,
    SyncRunUnit,
)
from dev_health_ops.models.invoices import Invoice, InvoiceLineItem
from dev_health_ops.models.ip_allowlist import OrgIPAllowlist
from dev_health_ops.models.licensing import OrgFeatureOverride, OrgLicense
from dev_health_ops.models.org_invite import OrgInvite
from dev_health_ops.models.pagerduty_webhook_binding import PagerDutyWebhookBinding
from dev_health_ops.models.refresh_token import RefreshToken
from dev_health_ops.models.refunds import Refund
from dev_health_ops.models.reports import ReportRun, SavedReport
from dev_health_ops.models.retention import OrgRetentionPolicy
from dev_health_ops.models.settings import (
    GithubAppInstallation,
    IntegrationCredential,
    JobRun,
    JobStatus,
    PagerDutyOAuthAuthorizationRequest,
    ProviderOAuthCredential,
    ProviderOAuthRevocation,
    ScheduledJob,
    Setting,
    SyncConfiguration,
    SyncWatermark,
)
from dev_health_ops.models.sso import SSOProvider
from dev_health_ops.models.subscriptions import Subscription, SubscriptionEvent
from dev_health_ops.models.users import Membership, Organization
from dev_health_ops.providers.pagerduty.oauth import (
    OAuthTokens,
    PagerDutyOAuthConfig,
    revoke_token,
)

logger = logging.getLogger(__name__)

_CLICKHOUSE_MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[2] / "migrations" / "clickhouse"
)
#: PR #1602 round-3 review D (LOW, pre-existing): anchored to the START of
#: a LINE (`^` + re.MULTILINE) -- without this, the unanchored version
#: matched PROSE inside a comment in migration 027's real source ("Regex:
#: table name in CREATE TABLE statement (handles...)"), discovering a
#: phantom `statement` table and emitting a spurious "missing or has no
#: org_id column; skipped." warning on EVERY production org deletion. A
#: real `CREATE TABLE` statement always starts its own line (possibly
#: indented); prose referencing "CREATE TABLE" mid-sentence does not.
_CREATE_TABLE_RE = re.compile(
    r"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(?P<table>[A-Za-z_][\w]*)`?\s*\(",
    re.IGNORECASE | re.MULTILINE,
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
    #: PR #1602 review F8 (CONFIRMED): a successful `EXTERNAL_DERIVED_STORES`
    #: visit is a result, not a problem -- it belongs in a typed bucket
    #: parallel to `postgres`/`clickhouse` (keyed by store name), not in
    #: `warnings`, which stays reserved for genuine problems (a store
    #: registered without a `visit` callable, or a visit that raised).
    external: DeletionScopeResult = field(default_factory=DeletionScopeResult)
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
            "external": self.external.to_dict(),
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
            # Self-discovered while adding migration 074's second table
            # (PR #1602 round-2 review NEW-1): `.search()` finds only the
            # FIRST `CREATE TABLE` in this chunk. Two CREATE TABLE
            # statements defined back-to-back as Python triple-quoted
            # string constants share a chunk (neither has a literal `;`),
            # so a second org_id-bearing table in the same migration file
            # was silently never discovered. `.finditer()` catches all of
            # them; a name without a REAL org_id column is still safely
            # filtered out downstream by `_purge_clickhouse`'s own
            # `system.columns` probe (with a warning), so over-discovery
            # here is harmless -- under-discovery is not.
            for create_match in _CREATE_TABLE_RE.finditer(statement):
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
            "dev_feedback",
            DevFeedback,
            lambda org_uuid, _org_id: DevFeedback.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "dev_tool_calls",
            DevToolCall,
            lambda org_uuid, _org_id: DevToolCall.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "dev_runs",
            DevRun,
            lambda org_uuid, _org_id: DevRun.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "dev_messages",
            DevMessage,
            lambda org_uuid, _org_id: DevMessage.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "dev_conversations",
            DevConversation,
            lambda org_uuid, _org_id: DevConversation.org_id == org_uuid,
        ),
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
            "sync_compute_checkpoints",
            SyncComputeCheckpoint,
            lambda _org_uuid, org_id: SyncComputeCheckpoint.org_id == org_id,
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
            "sync_run_reference_discoveries",
            SyncRunReferenceDiscovery,
            lambda _org_uuid, org_id: SyncRunReferenceDiscovery.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "sync_dispatch_outbox",
            SyncDispatchOutbox,
            lambda _org_uuid, org_id: SyncDispatchOutbox.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "sync_run_post_dispatches",
            SyncRunPostDispatch,
            lambda _org_uuid, org_id: SyncRunPostDispatch.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "sync_run_units",
            SyncRunUnit,
            lambda _org_uuid, org_id: SyncRunUnit.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "sync_runs",
            SyncRun,
            lambda _org_uuid, org_id: SyncRun.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "pagerduty_webhook_bindings",
            PagerDutyWebhookBinding,
            lambda org_uuid, _org_id: PagerDutyWebhookBinding.org_id == org_uuid,
        ),
        PostgresDeletionTarget(
            "pagerduty_oauth_authorization_requests",
            PagerDutyOAuthAuthorizationRequest,
            lambda _org_uuid, org_id: (
                PagerDutyOAuthAuthorizationRequest.org_id == org_id
            ),
        ),
        PostgresDeletionTarget(
            "provider_oauth_credentials",
            ProviderOAuthCredential,
            lambda _org_uuid, org_id: ProviderOAuthCredential.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "provider_oauth_revocations",
            ProviderOAuthRevocation,
            lambda _org_uuid, org_id: ProviderOAuthRevocation.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "integration_datasets",
            IntegrationDataset,
            lambda _org_uuid, org_id: IntegrationDataset.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "integration_sources",
            IntegrationSource,
            lambda _org_uuid, org_id: IntegrationSource.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "integrations",
            Integration,
            lambda _org_uuid, org_id: Integration.org_id == org_id,
        ),
        PostgresDeletionTarget(
            "github_app_installations",
            GithubAppInstallation,
            lambda _org_uuid, org_id: GithubAppInstallation.org_id == org_id,
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

        if not dry_run:
            await self._revoke_pagerduty_oauth_before_delete(org_id_str)

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
        await self._purge_external_stores(org_id_str, dry_run=dry_run, result=result)

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

    async def _revoke_pagerduty_oauth_before_delete(self, org_id: str) -> None:
        """Revoke remote PagerDuty grants before their encrypted local copies are deleted."""
        credentials = list(
            (
                await self.session.execute(
                    select(ProviderOAuthCredential).where(
                        ProviderOAuthCredential.org_id == org_id,
                        ProviderOAuthCredential.provider == "pagerduty",
                    )
                )
            )
            .scalars()
            .all()
        )
        pending_revocations = list(
            (
                await self.session.execute(
                    select(ProviderOAuthRevocation).where(
                        ProviderOAuthRevocation.org_id == org_id,
                        ProviderOAuthRevocation.provider == "pagerduty",
                    )
                )
            )
            .scalars()
            .all()
        )
        if not credentials and not pending_revocations:
            return
        config = PagerDutyOAuthConfig.from_env()
        if config is None:
            raise RuntimeError(
                "PagerDuty OAuth configuration is unavailable for deletion"
            )
        for credential in credentials:
            tokens = OAuthTokens.model_validate_json(
                decrypt_value(credential.token_encrypted)
            )
            await revoke_token(config, tokens.refresh_token or tokens.access_token)
        for pending in pending_revocations:
            await revoke_token(config, decrypt_value(pending.token_encrypted))
            await self.session.delete(pending)

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

        # CHAOS-3566: this scan stays the deletion-time source of truth (no
        # behavior change below), but drift against the explicit, reviewed
        # derived_store_registry is surfaced rather than silently swallowed.
        #
        # PR #1602 review F7 (CONFIRMED): the drift warning below is now
        # appended AFTER the purge outcome is known, with per-outcome
        # wording -- it used to be appended HERE, before the client was even
        # resolved, unconditionally claiming "this run still purged them"
        # even on a dry run (which never issues a single `ALTER TABLE
        # DELETE` -- see the per-table `if dry_run or count == 0: continue`
        # guard below) or when the client never resolved at all (this
        # method returns immediately afterward, having purged nothing).
        unregistered = unregistered_clickhouse_tables(tables)

        client, close_client = self._resolve_clickhouse_client(result)
        if client is None:
            if unregistered:
                result.warnings.append(
                    "Org deletion has no reviewed deletion-completeness "
                    f"decision for: {sorted(unregistered)}. Record one in "
                    "api/services/derived_store_registry.py's "
                    "CLICKHOUSE_DERIVED_STORES (the ClickHouse client could "
                    "not be resolved this run -- these tables were not "
                    "verified or purged; the registry is out of date)."
                )
            return

        # PR #1602 round-2 review C3 (CONFIRMED): the drift warning's outcome
        # claim used to be derived ONLY from `dry_run`, blanket-applied to
        # EVERY unregistered table -- a table with no org_id column
        # (skipped), a count query that itself failed (silently folded into
        # "0 rows", indistinguishable from a genuinely empty table), or a
        # DELETE that raised (caught and logged, but the loop moved on as if
        # nothing happened) all still got reported as "purged them" /
        # "would purge them". Track each table's REAL outcome here and
        # report per-table below, for the unregistered subset specifically.
        outcomes: dict[str, str] = {}
        try:
            for table in tables:
                org_id_type = await self._clickhouse_org_id_type(client, table)
                if org_id_type is None:
                    result.warnings.append(
                        f"ClickHouse table {table} missing or has no org_id column; skipped."
                    )
                    outcomes[table] = "skipped (no org_id column)"
                    continue

                condition = self._clickhouse_org_id_condition(org_id_type)
                count = await self._clickhouse_count(client, table, condition, org_id)
                if count is None:
                    # The count query itself failed (already logged by
                    # _clickhouse_count) -- this table was never verified,
                    # NOT confirmed empty. Recorded as 0 in the response
                    # totals only because DeletionScopeResult has no
                    # "unknown" slot; the per-table outcome text below is
                    # what actually distinguishes it from a real zero.
                    result.clickhouse.tables[table] = 0
                    outcomes[table] = "not verified (count query failed)"
                    continue
                result.clickhouse.tables[table] = count
                result.clickhouse.total += count
                if dry_run:
                    outcomes[table] = (
                        f"would purge ({count} row(s))"
                        if count
                        else "would purge (0 rows)"
                    )
                    continue
                if count == 0:
                    outcomes[table] = "purged (0 rows)"
                    continue
                deleted = await self._clickhouse_delete(
                    client, table, condition, org_id
                )
                outcomes[table] = (
                    f"purged ({count} row(s))" if deleted else "failed (delete raised)"
                )
        finally:
            if close_client is not None:
                close_client()

        # PR #1602 round-3 review C (CONFIRMED, proven live on `repos`): the
        # per-table outcome tracking above only ever surfaced through the
        # unregistered-drift warning below, which by construction only
        # iterates the UNREGISTERED subset. A REGISTERED table's failed
        # count query or failed DELETE produced NO warning at all -- its
        # `result.clickhouse.tables[t]` entry (whatever count was captured
        # before the failure, or 0 for a failed count) read exactly like a
        # genuine success, indistinguishable from one. Any bad outcome,
        # registered or not, must be surfaced.
        for table, outcome in outcomes.items():
            if outcome.startswith("failed") or outcome.startswith("not verified"):
                result.warnings.append(
                    f"ClickHouse table {table}: {outcome} during org deletion."
                )

        if unregistered:
            per_table = "; ".join(
                f"{table}: {outcomes.get(table, 'not verified (not reached)')}"
                for table in sorted(unregistered)
            )
            result.warnings.append(
                "Org deletion has no reviewed deletion-completeness decision "
                f"for: {sorted(unregistered)}. Record one in "
                "api/services/derived_store_registry.py's "
                "CLICKHOUSE_DERIVED_STORES (the registry is out of date). "
                f"Per-table outcome this run: {per_table}."
            )

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
    ) -> int | None:
        """Returns ``None`` on a failed count query -- PR #1602 round-2
        review C3 (CONFIRMED): this used to return ``0`` on failure,
        silently indistinguishable from a genuinely empty table (which
        also legitimately skips the DELETE below). The caller now reports
        those two cases with different wording ("not verified" vs.
        "purged (0 rows)").
        """
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
            return None
        rows = list(getattr(response, "result_rows", []) or [])
        return int(rows[0][0]) if rows else 0

    async def _clickhouse_delete(
        self, client: Any, table: str, condition: str, org_id: str
    ) -> bool:
        """Returns whether the DELETE actually succeeded -- PR #1602
        round-2 review C3 (CONFIRMED): this used to return ``None``
        unconditionally, so a raised exception (caught and only logged)
        was indistinguishable to the caller from a genuine success.
        """
        try:
            await asyncio.to_thread(
                client.command,
                f"ALTER TABLE `{table}` DELETE WHERE {condition}",
                parameters={"org_id": org_id},
            )
            return True
        except Exception as exc:
            logger.warning(
                "Unable to delete ClickHouse table for org deletion org_id=%s table=%s error=%s",
                org_id,
                table,
                exc,
            )
            return False

    async def _purge_external_stores(
        self, org_id: str, *, dry_run: bool, result: DeletionResult
    ) -> None:
        """CHAOS-3566: visit every registered non-ClickHouse derived store.

        `EXTERNAL_DERIVED_STORES` is empty in production today (no such store
        exists yet), so this is a no-op there. It exists so a future derived
        store (e.g. the CHAOS-3499/3500 discovery-lane shadow store) only has
        to register a `DerivedStore(kind=EXTERNAL, visit=...)` entry -- this
        method, and the rest of `delete()`, do not need to change.
        """
        for store in EXTERNAL_DERIVED_STORES:
            if store.visit is None:
                result.warnings.append(
                    f"Derived store '{store.name}' is registered but not wired "
                    "for deletion (no visit callable)."
                )
                continue
            try:
                count = await store.visit(org_id, dry_run)
            except Exception as exc:
                logger.warning(
                    "Unable to visit derived store for org deletion org_id=%s store=%s error=%s",
                    org_id,
                    store.name,
                    exc,
                )
                result.warnings.append(
                    f"Derived store '{store.name}' deletion failed: {exc}"
                )
                continue
            # PR #1602 review F8 (CONFIRMED): a successful visit is a
            # result, not a problem -- record it in the typed `external`
            # bucket (parallel to `postgres`/`clickhouse`), never in
            # `warnings`, which a caller reasonably reads as "something
            # needs attention."
            result.external.tables[store.name] = count
            result.external.total += count


__all__ = [
    "DeletionResult",
    "DeletionScopeResult",
    "OrganizationDeletionService",
]
