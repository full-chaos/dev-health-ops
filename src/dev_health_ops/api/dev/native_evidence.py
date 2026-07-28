"""ClickHouse-backed native evidence adapters for Ask Dev.

The adapters project existing structured source tables into ``EvidenceRecord``.
They do not add a document index, embeddings, or a second evidence store.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.api.services.sync_coverage import STALE_FALLBACK_GRACE

from .contracts import DevEvidenceRef, FreshnessState
from .evidence_service import (
    EvidenceAvailability,
    EvidenceRecord,
    EvidenceSourceAdapter,
    SourceSearchResult,
)
from .scope_service import EntityKind, ScopeResolution

NATIVE_SOURCE_VERSION = "native-evidence.v1"
NATIVE_QUERY_VERSION = "native-evidence-query.v1"


@dataclass(frozen=True, slots=True)
class SourceFreshnessPolicy:
    source_system: str
    policy_version: str
    maximum_age: timedelta | None

    def classify(self, observed: datetime | None, *, now: datetime) -> FreshnessState:
        if observed is None or self.maximum_age is None:
            return FreshnessState.UNKNOWN
        observed = _datetime(observed)
        if observed is None:
            return FreshnessState.UNKNOWN
        return (
            FreshnessState.STALE
            if now - observed > self.maximum_age
            else FreshnessState.FRESH
        )


def default_native_freshness_policies() -> dict[str, SourceFreshnessPolicy]:
    """Return explicit source policies using the existing sync fallback grace.

    A caller with a schedule-derived source SLO replaces ``maximum_age`` for
    that source.  The 48-hour value is the existing sync-coverage fallback,
    not an Ask Dev-wide freshness threshold.
    """
    return {
        source: SourceFreshnessPolicy(
            source_system=source,
            policy_version=f"{source}-sync-fallback.v1",
            maximum_age=STALE_FALLBACK_GRACE,
        )
        for source in (
            "work_items",
            "work_units",
            "pull_requests",
            "reviews",
            "commits",
            "ci_runs",
            "deployments",
            "incidents",
        )
    }


@dataclass(frozen=True, slots=True)
class _SourceSpec:
    source_system: str
    supported_direct_kinds: frozenset[EntityKind]
    search_sql: str
    expand_sql: str


_COMMON_KINDS = frozenset({EntityKind.ORGANIZATION, EntityKind.REPOSITORY})


def _search_predicate(columns: tuple[str, ...]) -> str:
    return " OR ".join(
        f"positionCaseInsensitiveUTF8(ifNull(toString({column}), ''), {{query:String}}) > 0"
        for column in columns
    )


_SPECS: dict[str, _SourceSpec] = {
    "work_units": _SourceSpec(
        "work_units",
        _COMMON_KINDS | {EntityKind.WORK_UNIT},
        f"""
        SELECT work_unit_id AS entity_id,
               ifNull(work_unit_name, concat('Work unit ', work_unit_id)) AS display_label,
               concat('Evidence quality: ', evidence_quality_band,
                      '. Categorization status: ', categorization_status,
                      '. Effort metric: ', effort_metric) AS excerpt,
               ifNull(provider, 'native') AS provenance,
               computed_at AS observed_at, computed_at AS last_synced,
               ifNull(toString(repo_id), '') AS repository_id,
               '' AS source_url, 0 AS deleted, evidence_quality AS confidence
        FROM work_unit_investments FINAL
        WHERE org_id = {{org_id:String}}
          AND computed_at >= {{start:DateTime64(3, 'UTC')}}
          AND computed_at < {{end:DateTime64(3, 'UTC')}}
          AND ({_search_predicate(("work_unit_id", "work_unit_name", "categorization_status", "effort_metric"))})
          AND (empty({{repository_ids:Array(String)}}) OR toString(repo_id) IN {{repository_ids:Array(String)}})
          AND ({{entity_id:String}} = '' OR work_unit_id = {{entity_id:String}})
        ORDER BY computed_at DESC, work_unit_id
        LIMIT {{limit:UInt32}}
        """,
        """
        SELECT work_unit_id AS entity_id,
               ifNull(work_unit_name, concat('Work unit ', work_unit_id)) AS display_label,
               concat('Evidence quality: ', evidence_quality_band,
                      '. Categorization status: ', categorization_status,
                      '. Effort metric: ', effort_metric) AS excerpt,
               ifNull(provider, 'native') AS provenance,
               computed_at AS observed_at, computed_at AS last_synced,
               ifNull(toString(repo_id), '') AS repository_id,
               '' AS source_url, 0 AS deleted, evidence_quality AS confidence
        FROM work_unit_investments FINAL
        WHERE org_id = {org_id:String} AND work_unit_id = {entity_id:String}
          AND (empty({repository_ids:Array(String)}) OR toString(repo_id) IN {repository_ids:Array(String)})
          AND ({scope_entity_id:String} = '' OR work_unit_id = {scope_entity_id:String})
        ORDER BY computed_at DESC LIMIT 1
        """,
    ),
    "work_items": _SourceSpec(
        "work_items",
        _COMMON_KINDS | {EntityKind.PROJECT, EntityKind.ISSUE},
        f"""
        SELECT work_item_id AS entity_id, title AS display_label,
               concat('Status: ', status, '. ', ifNull(description, '')) AS excerpt,
               provider AS provenance, updated_at AS observed_at,
               last_synced, toString(repo_id) AS repository_id,
               url AS source_url, 0 AS deleted, 1.0 AS confidence
        FROM work_items FINAL
        WHERE org_id = {{org_id:String}}
          AND updated_at >= {{start:DateTime64(3, 'UTC')}}
          AND updated_at < {{end:DateTime64(3, 'UTC')}}
          AND ({_search_predicate(("work_item_id", "title", "description", "status"))})
          AND (empty({{repository_ids:Array(String)}}) OR toString(repo_id) IN {{repository_ids:Array(String)}})
          AND ({{entity_id:String}} = '' OR work_item_id = {{entity_id:String}}
               OR project_id = {{entity_id:String}} OR project_key = {{entity_id:String}})
        ORDER BY updated_at DESC, work_item_id
        LIMIT {{limit:UInt32}}
        """,
        """
        SELECT work_item_id AS entity_id, title AS display_label,
               concat('Status: ', status, '. ', ifNull(description, '')) AS excerpt,
               provider AS provenance, updated_at AS observed_at,
               last_synced, toString(repo_id) AS repository_id,
               url AS source_url, 0 AS deleted, 1.0 AS confidence
        FROM work_items FINAL
        WHERE org_id = {org_id:String} AND work_item_id = {entity_id:String}
          AND (empty({repository_ids:Array(String)}) OR toString(repo_id) IN {repository_ids:Array(String)})
          AND ({scope_entity_id:String} = '' OR work_item_id = {scope_entity_id:String}
               OR project_id = {scope_entity_id:String} OR project_key = {scope_entity_id:String})
        ORDER BY last_synced DESC LIMIT 1
        """,
    ),
    "pull_requests": _SourceSpec(
        "pull_requests",
        _COMMON_KINDS | {EntityKind.PULL_REQUEST},
        f"""
        SELECT concat(toString(repo_id), '#pr', toString(number)) AS entity_id,
               ifNull(title, concat('Pull request #', toString(number))) AS display_label,
               concat('State: ', ifNull(state, 'unknown'), '. ', ifNull(body, '')) AS excerpt,
               'native' AS provenance, coalesce(merged_at, closed_at, created_at) AS observed_at,
               last_synced, toString(repo_id) AS repository_id,
               '' AS source_url, 0 AS deleted, 1.0 AS confidence
        FROM git_pull_requests FINAL
        WHERE org_id = {{org_id:String}}
          AND created_at >= {{start:DateTime64(3, 'UTC')}}
          AND created_at < {{end:DateTime64(3, 'UTC')}}
          AND ({_search_predicate(("title", "body", "state", "number"))})
          AND (empty({{repository_ids:Array(String)}}) OR toString(repo_id) IN {{repository_ids:Array(String)}})
          AND ({{entity_id:String}} = '' OR concat(toString(repo_id), '#pr', toString(number)) = {{entity_id:String}})
        ORDER BY observed_at DESC, entity_id
        LIMIT {{limit:UInt32}}
        """,
        """
        SELECT concat(toString(repo_id), '#pr', toString(number)) AS entity_id,
               ifNull(title, concat('Pull request #', toString(number))) AS display_label,
               concat('State: ', ifNull(state, 'unknown'), '. ', ifNull(body, '')) AS excerpt,
               'native' AS provenance, coalesce(merged_at, closed_at, created_at) AS observed_at,
               last_synced, toString(repo_id) AS repository_id,
               '' AS source_url, 0 AS deleted, 1.0 AS confidence
        FROM git_pull_requests FINAL
        WHERE org_id = {org_id:String}
          AND concat(toString(repo_id), '#pr', toString(number)) = {entity_id:String}
          AND (empty({repository_ids:Array(String)}) OR toString(repo_id) IN {repository_ids:Array(String)})
          AND ({scope_entity_id:String} = '' OR concat(toString(repo_id), '#pr', toString(number)) = {scope_entity_id:String})
        ORDER BY last_synced DESC LIMIT 1
        """,
    ),
    "reviews": _SourceSpec(
        "reviews",
        _COMMON_KINDS | {EntityKind.PULL_REQUEST},
        f"""
        SELECT concat(toString(repo_id), '#pr', toString(number), '#review', review_id) AS entity_id,
               concat('Review by ', reviewer) AS display_label,
               concat('Review state: ', state) AS excerpt,
               'native' AS provenance, submitted_at AS observed_at,
               last_synced, toString(repo_id) AS repository_id,
               '' AS source_url, 0 AS deleted, 1.0 AS confidence
        FROM git_pull_request_reviews FINAL
        WHERE org_id = {{org_id:String}}
          AND submitted_at >= {{start:DateTime64(3, 'UTC')}}
          AND submitted_at < {{end:DateTime64(3, 'UTC')}}
          AND ({_search_predicate(("reviewer", "state", "review_id"))})
          AND (empty({{repository_ids:Array(String)}}) OR toString(repo_id) IN {{repository_ids:Array(String)}})
          AND ({{entity_id:String}} = '' OR startsWith(entity_id, concat({{entity_id:String}}, '#review')))
        ORDER BY submitted_at DESC, entity_id
        LIMIT {{limit:UInt32}}
        """,
        """
        SELECT concat(toString(repo_id), '#pr', toString(number), '#review', review_id) AS entity_id,
               concat('Review by ', reviewer) AS display_label,
               concat('Review state: ', state) AS excerpt,
               'native' AS provenance, submitted_at AS observed_at,
               last_synced, toString(repo_id) AS repository_id,
               '' AS source_url, 0 AS deleted, 1.0 AS confidence
        FROM git_pull_request_reviews FINAL
        WHERE org_id = {org_id:String}
          AND concat(toString(repo_id), '#pr', toString(number), '#review', review_id) = {entity_id:String}
          AND (empty({repository_ids:Array(String)}) OR toString(repo_id) IN {repository_ids:Array(String)})
          AND ({scope_entity_id:String} = '' OR startsWith(entity_id, concat({scope_entity_id:String}, '#review')))
        ORDER BY last_synced DESC LIMIT 1
        """,
    ),
    "commits": _SourceSpec(
        "commits",
        _COMMON_KINDS,
        f"""
        SELECT concat(toString(repo_id), '@', hash) AS entity_id,
               concat('Commit ', substring(hash, 1, 12)) AS display_label,
               ifNull(message, '') AS excerpt, 'native' AS provenance,
               committer_when AS observed_at, last_synced,
               toString(repo_id) AS repository_id, '' AS source_url,
               0 AS deleted, 1.0 AS confidence
        FROM git_commits FINAL
        WHERE org_id = {{org_id:String}}
          AND committer_when >= {{start:DateTime64(3, 'UTC')}}
          AND committer_when < {{end:DateTime64(3, 'UTC')}}
          AND ({_search_predicate(("hash", "message"))})
          AND (empty({{repository_ids:Array(String)}}) OR toString(repo_id) IN {{repository_ids:Array(String)}})
        ORDER BY committer_when DESC, entity_id
        LIMIT {{limit:UInt32}}
        """,
        """
        SELECT concat(toString(repo_id), '@', hash) AS entity_id,
               concat('Commit ', substring(hash, 1, 12)) AS display_label,
               ifNull(message, '') AS excerpt, 'native' AS provenance,
               committer_when AS observed_at, last_synced,
               toString(repo_id) AS repository_id, '' AS source_url,
               0 AS deleted, 1.0 AS confidence
        FROM git_commits FINAL
        WHERE org_id = {org_id:String}
          AND concat(toString(repo_id), '@', hash) = {entity_id:String}
          AND (empty({repository_ids:Array(String)}) OR toString(repo_id) IN {repository_ids:Array(String)})
        ORDER BY last_synced DESC LIMIT 1
        """,
    ),
    "ci_runs": _SourceSpec(
        "ci_runs",
        _COMMON_KINDS,
        f"""
        SELECT concat(toString(repo_id), '#ci', run_id) AS entity_id,
               concat('CI run ', run_id) AS display_label,
               concat('Status: ', ifNull(status, 'unknown')) AS excerpt,
               'native' AS provenance, coalesce(finished_at, started_at) AS observed_at,
               last_synced, toString(repo_id) AS repository_id,
               '' AS source_url, 0 AS deleted, 1.0 AS confidence
        FROM ci_pipeline_runs FINAL
        WHERE org_id = {{org_id:String}}
          AND started_at >= {{start:DateTime64(3, 'UTC')}}
          AND started_at < {{end:DateTime64(3, 'UTC')}}
          AND ({_search_predicate(("run_id", "status"))})
          AND (empty({{repository_ids:Array(String)}}) OR toString(repo_id) IN {{repository_ids:Array(String)}})
        ORDER BY observed_at DESC, entity_id
        LIMIT {{limit:UInt32}}
        """,
        """
        SELECT concat(toString(repo_id), '#ci', run_id) AS entity_id,
               concat('CI run ', run_id) AS display_label,
               concat('Status: ', ifNull(status, 'unknown')) AS excerpt,
               'native' AS provenance, coalesce(finished_at, started_at) AS observed_at,
               last_synced, toString(repo_id) AS repository_id,
               '' AS source_url, 0 AS deleted, 1.0 AS confidence
        FROM ci_pipeline_runs FINAL
        WHERE org_id = {org_id:String}
          AND concat(toString(repo_id), '#ci', run_id) = {entity_id:String}
          AND (empty({repository_ids:Array(String)}) OR toString(repo_id) IN {repository_ids:Array(String)})
        ORDER BY last_synced DESC LIMIT 1
        """,
    ),
    "deployments": _SourceSpec(
        "deployments",
        _COMMON_KINDS | {EntityKind.PULL_REQUEST},
        f"""
        SELECT concat(toString(repo_id), '#deployment', deployment_id) AS entity_id,
               concat('Deployment ', deployment_id) AS display_label,
               concat('Status: ', ifNull(status, 'unknown'), '. Environment: ', ifNull(environment, 'unknown')) AS excerpt,
               'native' AS provenance, coalesce(deployed_at, finished_at, started_at, last_synced) AS observed_at,
               last_synced, toString(repo_id) AS repository_id,
               '' AS source_url, 0 AS deleted, 1.0 AS confidence
        FROM deployments FINAL
        WHERE org_id = {{org_id:String}}
          AND observed_at >= {{start:DateTime64(3, 'UTC')}}
          AND observed_at < {{end:DateTime64(3, 'UTC')}}
          AND ({_search_predicate(("deployment_id", "status", "environment", "release_ref"))})
          AND (empty({{repository_ids:Array(String)}}) OR toString(repo_id) IN {{repository_ids:Array(String)}})
          AND ({{pr_number:UInt32}} = 0 OR pull_request_number = {{pr_number:UInt32}})
        ORDER BY observed_at DESC, entity_id
        LIMIT {{limit:UInt32}}
        """,
        """
        SELECT concat(toString(repo_id), '#deployment', deployment_id) AS entity_id,
               concat('Deployment ', deployment_id) AS display_label,
               concat('Status: ', ifNull(status, 'unknown'), '. Environment: ', ifNull(environment, 'unknown')) AS excerpt,
               'native' AS provenance, coalesce(deployed_at, finished_at, started_at, last_synced) AS observed_at,
               last_synced, toString(repo_id) AS repository_id,
               '' AS source_url, 0 AS deleted, 1.0 AS confidence
        FROM deployments FINAL
        WHERE org_id = {org_id:String}
          AND concat(toString(repo_id), '#deployment', deployment_id) = {entity_id:String}
          AND (empty({repository_ids:Array(String)}) OR toString(repo_id) IN {repository_ids:Array(String)})
          AND ({scope_pr_number:UInt32} = 0 OR pull_request_number = {scope_pr_number:UInt32})
        ORDER BY last_synced DESC LIMIT 1
        """,
    ),
    "incidents": _SourceSpec(
        "incidents",
        _COMMON_KINDS,
        f"""
        SELECT i.id AS entity_id, i.title AS display_label,
               concat('Status: ', ifNull(i.normalized_status, 'unknown'), '. ', ifNull(i.description, '')) AS excerpt,
               ifNull(i.relationship_provenance, 'native') AS provenance,
               coalesce(i.source_event_at, i.observed_at) AS observed_at,
               i.last_synced AS last_synced, ifNull(toString(e.repo_id), '') AS repository_id,
               ifNull(i.source_url, '') AS source_url, i.is_deleted AS deleted,
               ifNull(i.relationship_confidence, 1.0) AS confidence
        FROM operational_incidents AS i FINAL
        LEFT JOIN work_graph_deployment_incident_edges AS e FINAL
          ON e.org_id = toUUIDOrZero(i.org_id) AND e.incident_id = i.id
        WHERE i.org_id = {{org_id:String}}
          AND observed_at >= {{start:DateTime64(3, 'UTC')}}
          AND observed_at < {{end:DateTime64(3, 'UTC')}}
          AND ({_search_predicate(("i.id", "i.title", "i.description", "i.normalized_status"))})
          AND (empty({{repository_ids:Array(String)}}) OR repository_id IN {{repository_ids:Array(String)}})
        ORDER BY observed_at DESC, entity_id
        LIMIT {{limit:UInt32}}
        """,
        """
        SELECT i.id AS entity_id, i.title AS display_label,
               concat('Status: ', ifNull(i.normalized_status, 'unknown'), '. ', ifNull(i.description, '')) AS excerpt,
               ifNull(i.relationship_provenance, 'native') AS provenance,
               coalesce(i.source_event_at, i.observed_at) AS observed_at,
               i.last_synced AS last_synced, ifNull(toString(e.repo_id), '') AS repository_id,
               ifNull(i.source_url, '') AS source_url, i.is_deleted AS deleted,
               ifNull(i.relationship_confidence, 1.0) AS confidence
        FROM operational_incidents AS i FINAL
        LEFT JOIN work_graph_deployment_incident_edges AS e FINAL
          ON e.org_id = toUUIDOrZero(i.org_id) AND e.incident_id = i.id
        WHERE i.org_id = {org_id:String} AND i.id = {entity_id:String}
          AND (empty({repository_ids:Array(String)}) OR repository_id IN {repository_ids:Array(String)})
        ORDER BY i.last_synced DESC LIMIT 1
        """,
    ),
}


class ClickHouseEvidenceSource(EvidenceSourceAdapter):
    def __init__(
        self,
        client: Any,
        source_system: str,
        *,
        policy: SourceFreshnessPolicy,
        now: datetime | None = None,
    ) -> None:
        if source_system not in _SPECS:
            raise ValueError(f"Unsupported native evidence source: {source_system}")
        if policy.source_system != source_system:
            raise ValueError("Freshness policy must match evidence source")
        self._client = client
        self.source_system = source_system
        self._spec = _SPECS[source_system]
        self._policy = policy
        self._now = now

    async def search(
        self,
        *,
        org_id: str,
        scope: ScopeResolution,
        query: str,
        limit: int,
    ) -> SourceSearchResult:
        direct = scope.entities[0] if len(scope.entities) == 1 else None
        kind = direct.kind if direct else EntityKind.REPOSITORY
        if kind not in self._spec.supported_direct_kinds:
            return SourceSearchResult(
                self.source_system, EvidenceAvailability.NO_MATCHES
            )
        repository_ids = sorted(
            {
                value
                for entity in scope.entities
                for value in (
                    entity.canonical_id
                    if entity.kind is EntityKind.REPOSITORY
                    else None,
                    entity.repository_id,
                )
                if value
            }
        )
        entity_id = ""
        if direct and direct.kind in {
            EntityKind.PROJECT,
            EntityKind.WORK_UNIT,
            EntityKind.ISSUE,
            EntityKind.PULL_REQUEST,
        }:
            entity_id = direct.canonical_id
        pr_number = _pr_number(entity_id) if kind is EntityKind.PULL_REQUEST else 0
        rows = await query_dicts(
            self._client,
            self._spec.search_sql,
            {
                "org_id": org_id,
                "query": query,
                "start": scope.time_range.utc_start,
                "end": scope.time_range.utc_end,
                "repository_ids": repository_ids,
                "entity_id": entity_id,
                "pr_number": pr_number,
                "limit": min(limit, 100),
            },
        )
        records = tuple(self._record(row) for row in rows)
        state = (
            EvidenceAvailability.AVAILABLE
            if records
            else EvidenceAvailability.NO_MATCHES
        )
        watermark = max((record.observed_at for record in records), default=None)
        return SourceSearchResult(
            self.source_system,
            state,
            records,
            watermark.isoformat() if watermark else None,
        )

    async def expand(
        self,
        *,
        org_id: str,
        scope: ScopeResolution,
        evidence: DevEvidenceRef,
    ) -> EvidenceRecord | None:
        direct = scope.entities[0] if len(scope.entities) == 1 else None
        scope_entity_id = ""
        if direct and direct.kind in {
            EntityKind.PROJECT,
            EntityKind.WORK_UNIT,
            EntityKind.ISSUE,
            EntityKind.PULL_REQUEST,
        }:
            scope_entity_id = direct.canonical_id
        rows = await query_dicts(
            self._client,
            self._spec.expand_sql,
            {
                "org_id": org_id,
                "entity_id": evidence.entity_id,
                "repository_ids": list(evidence.repository_ids),
                "scope_entity_id": scope_entity_id,
                "scope_pr_number": _pr_number(scope_entity_id),
            },
        )
        return self._record(rows[0]) if rows else None

    def _record(self, row: Mapping[str, Any]) -> EvidenceRecord:
        observed = _datetime(row.get("observed_at")) or datetime.now(UTC)
        last_synced = _datetime(row.get("last_synced"))
        freshness = self._policy.classify(
            last_synced, now=self._now or datetime.now(UTC)
        )
        repository_id = str(row.get("repository_id") or "")
        source_url = str(row.get("source_url") or "") or None
        return EvidenceRecord(
            source_system=self.source_system,
            source_version=f"{NATIVE_SOURCE_VERSION}:{self._policy.policy_version}",
            entity_type=_entity_type(self.source_system),
            entity_id=str(row.get("entity_id") or ""),
            display_label=str(row.get("display_label") or "Evidence"),
            raw_excerpt=str(row.get("excerpt") or "") or None,
            observed_at=observed,
            freshness=freshness,
            provenance=str(row.get("provenance") or "native"),
            confidence=max(0.0, min(1.0, float(row.get("confidence") or 0.0))),
            repository_ids=(repository_id,) if repository_id else (),
            source_url=source_url,
            # External provider hosts must be supplied by an authorized provider
            # metadata adapter. Native rows alone do not authorize a hostname.
            authorized_link_hosts=(),
            stale=freshness is FreshnessState.STALE,
            deleted=bool(row.get("deleted")),
        )


def native_evidence_adapters(client: Any) -> tuple[ClickHouseEvidenceSource, ...]:
    policies = default_native_freshness_policies()
    return tuple(
        ClickHouseEvidenceSource(client, source, policy=policies[source])
        for source in _SPECS
    )


def _datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
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


def _pr_number(entity_id: str) -> int:
    marker = "#pr"
    if marker not in entity_id:
        return 0
    try:
        return int(entity_id.rsplit(marker, 1)[1])
    except ValueError:
        return 0


def _entity_type(source_system: str) -> str:
    return {
        "work_items": "issue",
        "work_units": "work_unit",
        "pull_requests": "pull_request",
        "reviews": "review",
        "commits": "commit",
        "ci_runs": "ci_run",
        "deployments": "deployment",
        "incidents": "incident",
    }[source_system]
