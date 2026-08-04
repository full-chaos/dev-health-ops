"""User-safe Ask Dev data-health and source-freshness projection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol

from sqlalchemy import select

from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.api.services.sync_coverage import (
    STALE_FALLBACK_GRACE,
    STALE_MINIMUM_GRACE,
    _schedule_interval,
)
from dev_health_ops.models.settings import (
    JobStatus,
    ScheduledJob,
    SyncConfiguration,
)

from .entitlement import AskDevEntitlementAuthorizer
from .evidence_service import EvidenceScopeAuthorizer
from .native_evidence import SourceFreshnessPolicy, default_native_freshness_policies
from .native_status_change import PROJECT_REPOSITORIES_SQL
from .scope_service import (
    EntityKind,
    ScopeResolution,
    ScopeResolutionOutcome,
    ScopeResolveRequest,
)

DATA_HEALTH_VERSION = "data-health.v1"
MAX_HEALTH_SOURCES = 25
NATIVE_EVIDENCE_SOURCES = (
    "work_items",
    "work_units",
    "pull_requests",
    "reviews",
    "commits",
    "ci_runs",
    "deployments",
    "incidents",
)


class DataHealthState(StrEnum):
    COMPLETE = "complete"
    NO_DATA = "no_data"
    UNAVAILABLE = "unavailable"
    UNCONFIGURED = "unconfigured"
    STALE = "stale"
    UNAUTHORIZED = "unauthorized"


@dataclass(frozen=True, slots=True)
class SourceHealthObservation:
    source_system: str
    configured: bool | None
    required: bool
    last_successful_at: datetime | None = None
    watermark: datetime | None = None
    active_failure: bool = False
    covered_repository_ids: tuple[str, ...] = ()
    relevant_repository_ids: tuple[str, ...] = ()
    maximum_age: timedelta | None = None
    freshness_policy_version: str | None = None
    warning: str | None = None


@dataclass(frozen=True, slots=True)
class DataHealthSource:
    source_system: str
    state: DataHealthState
    required: bool
    last_successful_at: datetime | None
    watermark: datetime | None
    missing_repository_ids: tuple[str, ...]
    missing_entity_ids: tuple[str, ...]
    coverage: float
    confidence_impact: str | None
    freshness_policy_version: str | None
    warning: str | None = None


@dataclass(frozen=True, slots=True)
class DataHealthResult:
    sources: tuple[DataHealthSource, ...]
    complete_eligible: bool
    query_version: str = DATA_HEALTH_VERSION


class DataHealthReader(Protocol):
    async def read(
        self,
        *,
        org_id: str,
        scope: ScopeResolution,
        source_systems: Sequence[str],
    ) -> tuple[SourceHealthObservation, ...]: ...


class DataHealthService:
    def __init__(
        self,
        *,
        entitlement: AskDevEntitlementAuthorizer,
        authorizer: EvidenceScopeAuthorizer,
        reader: DataHealthReader,
        policies: Mapping[str, SourceFreshnessPolicy] | None = None,
        now: datetime | None = None,
    ) -> None:
        self._entitlement = entitlement
        self._authorizer = authorizer
        self._reader = reader
        self._policies = dict(policies or default_native_freshness_policies())
        self._now = now

    async def inspect(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope_request: ScopeResolveRequest,
        required_sources: Sequence[str] = NATIVE_EVIDENCE_SOURCES,
    ) -> DataHealthResult:
        await self._entitlement.require(org_id)
        sources = tuple(dict.fromkeys(required_sources))
        if not sources or len(sources) > MAX_HEALTH_SOURCES:
            raise ValueError(f"Data health requires 1 to {MAX_HEALTH_SOURCES} sources")
        resolution = await self._authorizer.resolve(
            org_id, permission_fingerprint, scope_request
        )
        if resolution.outcome not in {
            ScopeResolutionOutcome.EXACT,
            ScopeResolutionOutcome.FILTERED,
            ScopeResolutionOutcome.INHERITED,
            ScopeResolutionOutcome.ORGANIZATION_FALLBACK,
        }:
            denied = tuple(
                DataHealthSource(
                    source_system=source,
                    state=DataHealthState.UNAUTHORIZED,
                    required=True,
                    last_successful_at=None,
                    watermark=None,
                    missing_repository_ids=(),
                    missing_entity_ids=(),
                    coverage=0.0,
                    confidence_impact="insufficient_evidence",
                    freshness_policy_version=None,
                    warning="not_found",
                )
                for source in sources
            )
            return DataHealthResult(denied, False)

        try:
            observations = await self._reader.read(
                org_id=org_id,
                scope=resolution,
                source_systems=sources,
            )
        except Exception:
            observations = tuple(
                SourceHealthObservation(
                    source, None, True, warning="health_unavailable"
                )
                for source in sources
            )
        by_source = {item.source_system: item for item in observations}
        scoped_repositories = _repository_ids(resolution)
        requested_entities = _entity_ids(resolution)
        now = self._now or datetime.now(UTC)
        results: list[DataHealthSource] = []
        for source in sources:
            observation = by_source.get(source) or SourceHealthObservation(
                source, None, True, warning="health_unavailable"
            )
            policy = self._policies.get(source)
            effective_policy = policy
            if observation.freshness_policy_version:
                effective_policy = SourceFreshnessPolicy(
                    source,
                    observation.freshness_policy_version,
                    observation.maximum_age,
                )
            freshness = (
                effective_policy.classify(observation.watermark, now=now)
                if effective_policy
                else None
            )
            requested_repositories = scoped_repositories or set(
                observation.relevant_repository_ids
            )
            missing_repositories = tuple(
                sorted(requested_repositories - set(observation.covered_repository_ids))
            )
            if observation.configured is False:
                state = DataHealthState.UNCONFIGURED
            elif observation.configured is None:
                state = DataHealthState.UNAVAILABLE
            elif observation.active_failure:
                state = DataHealthState.UNAVAILABLE
            elif observation.watermark is None:
                state = DataHealthState.NO_DATA
            elif freshness is not None and freshness.value == "stale":
                state = DataHealthState.STALE
            elif missing_repositories:
                state = DataHealthState.NO_DATA
            else:
                state = DataHealthState.COMPLETE
            covered = len(requested_repositories) - len(missing_repositories)
            coverage = (
                covered / len(requested_repositories)
                if requested_repositories
                else (1.0 if observation.watermark else 0.0)
            )
            impact = None
            if observation.required and state is not DataHealthState.COMPLETE:
                impact = (
                    "degraded"
                    if state in {DataHealthState.UNAVAILABLE, DataHealthState.STALE}
                    else "insufficient_evidence"
                )
            results.append(
                DataHealthSource(
                    source_system=source,
                    state=state,
                    required=observation.required,
                    last_successful_at=observation.last_successful_at,
                    watermark=observation.watermark,
                    missing_repository_ids=missing_repositories,
                    missing_entity_ids=requested_entities
                    if state in {DataHealthState.NO_DATA, DataHealthState.UNCONFIGURED}
                    else (),
                    coverage=max(0.0, min(1.0, coverage)),
                    confidence_impact=impact,
                    freshness_policy_version=(
                        effective_policy.policy_version if effective_policy else None
                    ),
                    warning=observation.warning,
                )
            )
        complete_eligible = all(
            not item.required or item.state is DataHealthState.COMPLETE
            for item in results
        )
        return DataHealthResult(tuple(results), complete_eligible)


class NativeDataHealthReader:
    """Reconcile existing sync configuration with native source watermarks."""

    def __init__(self, clickhouse_client: Any, postgres_session: Any | None) -> None:
        self._client = clickhouse_client
        self._session = postgres_session

    async def read(
        self,
        *,
        org_id: str,
        scope: ScopeResolution,
        source_systems: Sequence[str],
    ) -> tuple[SourceHealthObservation, ...]:
        configuration_rows = await self._configurations(org_id)
        configurations = configuration_rows or []
        schedules = await self._schedules(org_id)
        repository_ids = sorted(_repository_ids(scope))
        # A committed PROJECT subject carries no repository dimension (the
        # catalog resolves projects with ``NULL AS repository_id``), so
        # ``_repository_ids`` is empty for one and the ``empty(array) OR ...``
        # arm in ``_watermark`` below then disables repository filtering
        # entirely -- measuring the whole organization and reporting it as the
        # project's coverage. Because source health is a *mandatory* source for
        # the project status plan, unrelated healthy repositories could make a
        # project's evidence coverage read complete. Resolve the same
        # repository set the status/change reader derives, from the same shared
        # query, or fail closed. Codex adversarial review (HIGH, 2026-08-03).
        project_scope_unresolved = False
        if not repository_ids:
            project_ids = [
                entity.canonical_id
                for entity in scope.entities
                if entity.kind is EntityKind.PROJECT
            ]
            if project_ids:
                if scope.team_filters:
                    # No data-health query applies a team filter either, so a
                    # team-filtered project must not be answered with the whole
                    # project's repository set.
                    project_scope_unresolved = True
                else:
                    repository_ids = await self._project_repositories(
                        org_id, project_ids[0]
                    )
                    project_scope_unresolved = not repository_ids
        observations: list[SourceHealthObservation] = []
        for source in source_systems:
            if source == "acr":
                observations.append(
                    SourceHealthObservation(
                        source, False, False, warning="acr_optional"
                    )
                )
                continue
            if project_scope_unresolved:
                # ``configured=None`` maps to DataHealthState.UNAVAILABLE --
                # an explicit "this could not be measured for this subject",
                # never a silent organization-wide substitute.
                observations.append(
                    SourceHealthObservation(
                        source,
                        None,
                        False,
                        warning="project_repository_scope_unavailable",
                    )
                )
                continue
            configured: bool | None = (
                _source_configured(source, configurations)
                if configuration_rows is not None
                else None
            )
            failure = any(
                config.last_sync_success is False
                and _provider_supports(source, config.provider)
                for config in configurations
            )
            last_success = max(
                (
                    config.last_sync_at
                    for config in configurations
                    if config.last_sync_success is not False
                    and config.last_sync_at is not None
                    and _provider_supports(source, config.provider)
                ),
                default=None,
            )
            watermark, covered = await self._watermark(source, org_id, repository_ids)
            relevant = set(repository_ids) or await self._repositories(org_id)
            if watermark is not None:
                configured = True
            intervals = [
                interval
                for provider, interval in schedules
                if _provider_supports(source, provider)
            ]
            maximum_age = (
                max(min(intervals) * 2, STALE_MINIMUM_GRACE)
                if intervals
                else STALE_FALLBACK_GRACE
            )
            freshness_version = (
                f"{source}-sync-schedule.v1"
                if intervals
                else f"{source}-sync-fallback.v1"
            )
            observations.append(
                SourceHealthObservation(
                    source_system=source,
                    configured=configured,
                    required=True,
                    last_successful_at=_utc(last_success),
                    watermark=_utc(watermark),
                    active_failure=failure,
                    covered_repository_ids=tuple(sorted(covered)),
                    relevant_repository_ids=tuple(sorted(relevant)),
                    maximum_age=maximum_age,
                    freshness_policy_version=freshness_version,
                    warning="active_sync_failure" if failure else None,
                )
            )
        return tuple(observations)

    async def _project_repositories(self, org_id: str, project_id: str) -> list[str]:
        """The project's repository set, from the one shared derivation.

        Deliberately the same ``PROJECT_REPOSITORIES_SQL`` the status/change
        reader uses rather than a second query with the same intent: two
        independently-drifting notions of "which repositories is this project
        in" is exactly the class of bug this fix exists to close. A failed or
        empty derivation returns ``[]`` and the caller fails closed -- an
        unreadable attribution source is never "this project spans no
        repositories", and never an excuse to widen to the organization.
        """

        if not project_id:
            return []
        try:
            rows = await query_dicts(
                self._client,
                PROJECT_REPOSITORIES_SQL,
                {
                    "org_id": org_id,
                    "entity_id": project_id,
                    # Data health is a "right now" measurement, unlike a
                    # snapshot's caller-supplied as_of.
                    "as_of": datetime.now(UTC),
                },
            )
        except Exception:
            return []
        return sorted(
            {str(row["repository_id"]) for row in rows if row.get("repository_id")}
        )

    async def _repositories(self, org_id: str) -> set[str]:
        rows = await query_dicts(
            self._client,
            """
            SELECT groupUniqArray(toString(id)) AS repository_ids
            FROM repos FINAL WHERE org_id = {org_id:String}
            """,
            {"org_id": org_id},
        )
        if not rows:
            return set()
        return {str(value) for value in (rows[0].get("repository_ids") or [])}

    async def _schedules(self, org_id: str) -> list[tuple[str, timedelta]]:
        if self._session is None:
            return []
        rows = await self._session.execute(
            select(ScheduledJob).where(
                ScheduledJob.org_id == org_id,
                ScheduledJob.job_type == "sync",
                ScheduledJob.status == JobStatus.ACTIVE.value,
            )
        )
        result: list[tuple[str, timedelta]] = []
        for job in rows.scalars().all():
            interval = _schedule_interval(job, datetime.now(UTC))
            if interval is not None:
                result.append((job.provider, interval))
        return result

    async def _configurations(self, org_id: str) -> list[SyncConfiguration] | None:
        if self._session is None:
            return None
        rows = await self._session.execute(
            select(SyncConfiguration).where(
                SyncConfiguration.org_id == org_id,
                SyncConfiguration.is_active.is_(True),
            )
        )
        return list(rows.scalars().all())

    async def _watermark(
        self, source: str, org_id: str, repository_ids: Sequence[str]
    ) -> tuple[datetime | None, set[str]]:
        table, repo_column, watermark_column = _SOURCE_TABLES[source]
        repo_projection = (
            f"groupUniqArray(toString({repo_column}))" if repo_column else "[]"
        )
        repo_filter = (
            f"AND (empty({{repository_ids:Array(String)}}) OR toString({repo_column}) IN {{repository_ids:Array(String)}})"
            if repo_column
            else ""
        )
        rows = await query_dicts(
            self._client,
            f"""
            SELECT maxOrNull({watermark_column}) AS watermark,
                   {repo_projection} AS covered_repository_ids
            FROM {table} FINAL
            WHERE org_id = {{org_id:String}} {repo_filter}
            """,
            {"org_id": org_id, "repository_ids": list(repository_ids)},
        )
        if not rows:
            return None, set()
        return _utc(rows[0].get("watermark")), {
            str(value) for value in (rows[0].get("covered_repository_ids") or [])
        }


_SOURCE_TABLES = {
    "work_items": ("work_items", "repo_id", "last_synced"),
    "work_units": ("work_unit_investments", "repo_id", "computed_at"),
    "pull_requests": ("git_pull_requests", "repo_id", "last_synced"),
    "reviews": ("git_pull_request_reviews", "repo_id", "last_synced"),
    "commits": ("git_commits", "repo_id", "last_synced"),
    "ci_runs": ("ci_pipeline_runs", "repo_id", "last_synced"),
    "deployments": ("deployments", "repo_id", "last_synced"),
    "incidents": ("operational_incidents", None, "last_synced"),
}


def _provider_supports(source: str, provider: str) -> bool:
    provider = provider.casefold()
    if source in {"work_items", "work_units"}:
        return provider in {"jira", "linear", "github", "gitlab"}
    if source == "incidents":
        return provider in {"pagerduty", "opsgenie", "incident"}
    return provider in {"github", "gitlab", "local", "git"}


def _source_configured(source: str, configs: Sequence[SyncConfiguration]) -> bool:
    return any(_provider_supports(source, config.provider) for config in configs)


def _repository_ids(scope: ScopeResolution) -> set[str]:
    result: set[str] = set()
    for entity in scope.entities:
        if entity.kind is EntityKind.REPOSITORY:
            result.add(entity.canonical_id)
        if entity.repository_id:
            result.add(entity.repository_id)
    return result


def _entity_ids(scope: ScopeResolution) -> tuple[str, ...]:
    return tuple(
        sorted(
            entity.canonical_id
            for entity in scope.entities
            if entity.kind not in {EntityKind.ORGANIZATION, EntityKind.REPOSITORY}
        )
    )


def _utc(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return (
            value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        )
    if value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return (
                parsed.replace(tzinfo=UTC)
                if parsed.tzinfo is None
                else parsed.astimezone(UTC)
            )
        except ValueError:
            return None
    return None
