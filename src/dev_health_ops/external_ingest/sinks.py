"""External-ingest sink-write layer (CHAOS-2698).

Given a schema-validated, kind-normalized ``NormalizedBatch`` (CHAOS-2697's
worker output), stamp ``org_id``/``source_id`` provenance and write each of
the 9 v1 record kinds through the **existing** ClickHouse sink methods,
preserving current append/ReplacingMergeTree-dedup semantics. See
``ops/docs/architecture/external-ingest-sink-writes.md`` for design
decisions D1-D8 this module implements.

Two client lifecycles per invocation (D6):
  - one async ``ClickHouseStore`` for repository/pull_request/review/commit/
    team/identity (git-family + org-scoped kinds);
  - one sync ``ClickHouseMetricsSink`` for the work-item family
    (work_item/work_item_transition/work_item_dependency), run off the event
    loop via ``asyncio.to_thread`` per kind so one kind's ClickHouse error
    doesn't block the others (partial-batch resilience).

Identity resolution (D4): work-item assignee/reporter/actor strings go
through ``resolve_identity()`` (mirrors native connector behavior so
cross-provider identity rollups stay consistent regardless of ingestion
path); git-family author/reviewer strings are passed through **raw** exactly
as native sync stores them.

``identities``/``teams``'s ``updated_at`` is the customer-supplied RMT
version column, passed through verbatim except for the CC24 future-timestamp
clamp: values more than ``UPDATED_AT_CLAMP_SKEW`` in the future are replaced
with server ``now()`` and recorded as a ``SinkWriteResult.warnings`` entry
(not a rejection) so a buggy/malicious ``updatedAt=2100-01-01`` cannot
permanently pin an RMT row against all future corrections. Every other
timestamp in this module (``last_synced``, git-family ``last_synced``) is
always server ``now()`` — a receive-time marker, not payload content.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, cast

from dev_health_ops.metrics.identity import resolve_identity
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.sinks.factory import create_sink
from dev_health_ops.models.work_items import WorkItemDependency
from dev_health_ops.storage import create_store

from .ids import derive_repo_uuid, derive_work_item_id
from .types import AffectedScope, NormalizedBatch, SinkWriteError, SinkWriteResult

logger = logging.getLogger(__name__)

__all__ = ["write_batch"]

# CC24: customer `updatedAt` more than this far in the future is clamped to
# server now() (identities/teams RMT-version-column poisoning defense).
UPDATED_AT_CLAMP_SKEW = timedelta(minutes=5)

_GIT_SYSTEMS = {"github", "gitlab"}

# WorkItemV1.type -> derive_work_item_id's work_item_type disambiguator.
_WORK_ITEM_TYPE_FOR_ID = {"pr": "pr", "merge_request": "merge_request"}


def _getter(record: Any):
    if isinstance(record, dict):
        return record.get

    def _get(key: str, default: Any = None) -> Any:
        return getattr(record, key, default)

    return _get


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _original_index(batch: NormalizedBatch, kind: str, position: int) -> int:
    indices = batch.record_index_by_kind.get(kind)
    if indices and position < len(indices):
        return indices[position]
    return position


def _track_scope_timestamp(scope: AffectedScope, value: Any) -> None:
    dt = _coerce_datetime(value)
    if dt is None:
        return
    if scope.min_timestamp is None or dt < scope.min_timestamp:
        scope.min_timestamp = dt
    if scope.max_timestamp is None or dt > scope.max_timestamp:
        scope.max_timestamp = dt


def _track_operational_scope(scope: AffectedScope, records: list[Any]) -> None:
    for record in records:
        _track_scope_timestamp(scope, record.source_version_at)
        if record.entity_family == "operational_service":
            scope.service_ids.add(record.id)
        if record.entity_family == "operational_incident":
            scope.incident_ids.add(record.id)


def _resolve_customer_identity(system: str, raw: str | None) -> str | None:
    """D4: resolve a work-item-family raw assignee/reporter/actor string.

    Customer payloads supply one opaque string per person (unlike native
    connectors' structured email/username/account_id/display_name), so this
    is a best-effort heuristic: treat it as ``email`` when it looks like one
    (``resolve_identity`` prioritizes email), else as ``username`` +
    ``display_name`` so alias-map / provider-qualified-username resolution
    still applies.
    """
    if not raw:
        return None
    fields: dict[str, str] = {"username": raw, "display_name": raw}
    if "@" in raw:
        fields["email"] = raw
    return resolve_identity(system, fields)


def _clamp_updated_at(
    value: Any, *, now: datetime | None = None
) -> tuple[datetime, bool]:
    now = now or datetime.now(timezone.utc)
    dt = _coerce_datetime(value) or now
    if dt > now + UPDATED_AT_CLAMP_SKEW:
        return now, True
    return dt, False


# system values for which CC6 requires a git-family record's repo identifier
# to equal the batch's source.instance (custom included -- master-spec CC6:
# "For github/gitlab/custom, every git-family record's repositoryExternalId
# MUST equal source.instance").
_INSTANCE_SCOPED_SYSTEMS = {"github", "gitlab", "custom"}


def _check_instance_scope(
    *,
    kind: str,
    system: str,
    repo_identifier: str,
    batch: NormalizedBatch,
    index: int,
    warnings: list[SinkWriteError],
) -> None:
    """CC6 says kind x system / instance-match enforcement is the worker's
    (CHAOS-2697) job and this layer "may assert-but-not-reject" -- this is
    that assertion. A record whose repo identifier disagrees with the
    batch's source.instance still gets written (D2/D3 IDs are derived from
    the record's own string, not silently coerced to source_instance), but
    is flagged: it derives a DIFFERENT repo UUID / work_item_id namespace
    than a native-sync write for the "real" repo would, which forks the
    identity-continuity guarantee the whole sink layer exists to protect.
    """
    if system not in _INSTANCE_SCOPED_SYSTEMS:
        return
    if repo_identifier == batch.source_instance:
        return
    warnings.append(
        SinkWriteError(
            record_index=index,
            kind=kind,
            external_id=repo_identifier,
            code="record_outside_source_instance",
            message=(
                f"{kind}.v1 repository identifier {repo_identifier!r} does not "
                f"match this batch's source.instance {batch.source_instance!r} "
                "(master-spec CC6) -- record was still written using its own "
                "identifier, which derives a different repo UUID/work_item_id "
                "namespace than a native-sync write for the intended repo."
            ),
        )
    )


def _check_provider_scope(
    *,
    kind: str,
    record_provider: str | None,
    batch: NormalizedBatch,
    index: int,
    warnings: list[SinkWriteError],
) -> None:
    """A per-record ``provider`` field is exactly as spoofable as any other
    payload content — round-3 codex adversarial review: trusting it for ID/
    namespace derivation would let a github-authenticated batch (one
    ``source_id``/``source_system`` CHAOS-2696 actually registered) write
    rows claiming ``provider="linear"``, landing in the ``linear:``
    work_item_id namespace and inheriting Linear-only trust (native_team_key)
    despite never having been authenticated as a Linear source. Callers must
    use ``batch.source_system`` — never this field — for derivation/storage;
    this only records the disagreement as a diagnostic warning.
    """
    if not record_provider or record_provider == batch.source_system:
        return
    warnings.append(
        SinkWriteError(
            record_index=index,
            kind=kind,
            external_id=None,
            code="record_provider_mismatch",
            message=(
                f"{kind}.v1 provider {record_provider!r} does not match this "
                f"batch's source.system {batch.source_system!r} -- the record "
                f"was written under source.system's namespace "
                f"({batch.source_system!r}), not the claimed provider, to "
                "prevent cross-namespace pollution."
            ),
        )
    )


# ---------------------------------------------------------------------------
# Row builders — one per record kind. Each returns the shape the target sink
# method expects; this function is the single translation point between the
# wire-schema field names (api/external_ingest/schemas.py) and each sink's
# row contract (brief-2698-sinks.md: "this layer's write_batch() should be
# the single translation point").
# ---------------------------------------------------------------------------


def _build_repo_object(
    record: dict[str, Any], batch: NormalizedBatch
) -> SimpleNamespace:
    get = record.get
    system = str(get("source_system") or batch.source_system)
    external_id = str(get("external_id") or "")
    repo_id = derive_repo_uuid(system, batch.source_instance, external_id)
    return SimpleNamespace(
        id=repo_id,
        repo=external_id,
        ref=get("default_ref"),
        created_at=None,
        settings=get("settings") or {},
        tags=get("tags") or [],
        provider=system,
        source_id=batch.source_id,
    )


def _build_commit_row(record: dict[str, Any], batch: NormalizedBatch) -> dict[str, Any]:
    get = record.get
    repo_full_name = str(get("repository_external_id") or "")
    author_when = _coerce_datetime(get("author_when"))
    return {
        "repo_id": derive_repo_uuid(
            batch.source_system, batch.source_instance, repo_full_name
        ),
        "hash": get("hash"),
        "message": get("message"),
        "author_name": get("author_name"),
        "author_email": get("author_email"),
        "author_when": author_when,
        "committer_name": get("committer_name"),
        "committer_email": get("committer_email"),
        # git_commits.committer_when is non-nullable — CommitV1.committerWhen
        # is optional on the wire, so fall back to author_when (matches git's
        # own default: an unamended commit's committer date == author date).
        "committer_when": _coerce_datetime(get("committer_when")) or author_when,
        "parents": get("parents") if get("parents") is not None else 1,
        "source_id": batch.source_id,
    }


def _build_pr_row(record: dict[str, Any], batch: NormalizedBatch) -> dict[str, Any]:
    get = record.get
    repo_full_name = str(get("repository_external_id") or "")
    return {
        "repo_id": derive_repo_uuid(
            batch.source_system, batch.source_instance, repo_full_name
        ),
        "number": get("number"),
        "title": get("title"),
        "body": get("body"),
        "state": get("state"),
        "author_name": get("author_name"),
        "author_email": get("author_email"),
        "created_at": _coerce_datetime(get("created_at")),
        "merged_at": _coerce_datetime(get("merged_at")),
        "closed_at": _coerce_datetime(get("closed_at")),
        "head_branch": get("head_branch"),
        "base_branch": get("base_branch"),
        "additions": get("additions"),
        "deletions": get("deletions"),
        "changed_files": get("changed_files"),
        "first_review_at": _coerce_datetime(get("first_review_at")),
        "first_comment_at": _coerce_datetime(get("first_comment_at")),
        "changes_requested_count": get("changes_requested_count") or 0,
        "reviews_count": get("reviews_count") or 0,
        "comments_count": get("comments_count") or 0,
        "source_id": batch.source_id,
    }


def _build_review_row(record: dict[str, Any], batch: NormalizedBatch) -> dict[str, Any]:
    get = record.get
    repo_full_name = str(get("repository_external_id") or "")
    return {
        "repo_id": derive_repo_uuid(
            batch.source_system, batch.source_instance, repo_full_name
        ),
        "number": get("pull_request_number"),
        "review_id": get("review_id"),
        "reviewer": get("reviewer"),
        "state": get("state"),
        "submitted_at": _coerce_datetime(get("submitted_at")),
        "source_id": batch.source_id,
    }


def _coerce_provider_identities(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value or {})


def _build_identity_row(
    record: dict[str, Any],
    batch: NormalizedBatch,
    *,
    index: int,
    warnings: list[SinkWriteError],
) -> dict[str, Any]:
    get = record.get
    canonical_id = str(get("canonical_id") or "")
    updated_at, clamped = _clamp_updated_at(get("updated_at"))
    if clamped:
        warnings.append(
            SinkWriteError(
                record_index=index,
                kind="identity",
                external_id=canonical_id,
                code="updated_at_clamped",
                message=(
                    "identity.v1 updatedAt more than "
                    f"{int(UPDATED_AT_CLAMP_SKEW.total_seconds() // 60)} minutes in the "
                    "future; clamped to server now()"
                ),
            )
        )
    return {
        "canonical_id": canonical_id,
        "org_id": batch.org_id,
        "identity_uuid": get("identity_uuid"),
        "display_name": get("display_name"),
        "email": get("email"),
        "provider_identities": _coerce_provider_identities(get("provider_identities")),
        "team_ids": get("team_ids") or [],
        "is_active": int(bool(get("is_active", True))),
        "updated_at": updated_at,
        "source_id": batch.source_id,
    }


def _build_team_row(
    record: dict[str, Any],
    batch: NormalizedBatch,
    *,
    index: int,
    warnings: list[SinkWriteError],
) -> dict[str, Any]:
    get = record.get
    team_id = str(get("id") or "")
    updated_at, clamped = _clamp_updated_at(get("updated_at"))
    if clamped:
        warnings.append(
            SinkWriteError(
                record_index=index,
                kind="team",
                external_id=team_id,
                code="updated_at_clamped",
                message=(
                    "team.v1 updatedAt more than "
                    f"{int(UPDATED_AT_CLAMP_SKEW.total_seconds() // 60)} minutes in the "
                    "future; clamped to server now()"
                ),
            )
        )
    return {
        "id": team_id,
        "team_uuid": get("team_uuid"),
        "name": get("name"),
        "description": get("description"),
        "members": get("members") or [],
        "project_keys": get("project_keys") or [],
        "repo_patterns": get("repo_patterns") or [],
        "is_active": int(bool(get("is_active", True))),
        "updated_at": updated_at,
        "org_id": batch.org_id,
        "provider": get("provider") or batch.source_system,
        "native_team_key": get("native_team_key"),
        "parent_team_id": get("parent_team_id"),
        "source_id": batch.source_id,
    }


def _project_scope(
    system: str, get: Any, repository_external_id: str | None
) -> tuple[str | None, str | None, str | None]:
    """Returns (project_key, project_id, project_name) per D-table convention."""
    if system == "jira":
        return get("project_key"), None, None
    if system in _GIT_SYSTEMS:
        return None, repository_external_id, None
    if system == "linear":
        project_id = get("project_id") or get("project_name") or get("native_team_key")
        return None, project_id, get("project_name")
    # custom
    return get("project_key"), None, None


def _build_work_item_row(
    record: Any,
    batch: NormalizedBatch,
    *,
    index: int,
    warnings: list[SinkWriteError],
) -> dict[str, Any]:
    get = _getter(record)
    # Trust boundary (round-3 codex adversarial review): `system` MUST be
    # batch.source_system, never the customer-supplied per-record
    # `provider` field. A batch is authenticated/registered for exactly one
    # system (CHAOS-2696) — trusting a spoofed `provider` here would let a
    # github-scoped batch write a row claiming `provider="linear"`,
    # landing in the `linear:` work_item_id namespace (polluting/colliding
    # with real Linear pushes) and inheriting Linear-only trust
    # (native_team_key) despite never being authenticated as a Linear
    # source. The record's own provider, if present and different, only
    # produces the diagnostic warning below; it is never used for
    # derivation, storage, or namespace selection.
    _check_provider_scope(
        kind="work_item",
        record_provider=get("provider"),
        batch=batch,
        index=index,
        warnings=warnings,
    )
    system = batch.source_system
    repository_external_id = get("repository_external_id")
    if repository_external_id:
        _check_instance_scope(
            kind="work_item",
            system=system,
            repo_identifier=str(repository_external_id),
            batch=batch,
            index=index,
            warnings=warnings,
        )
    instance = repository_external_id or (
        batch.source_instance if system in _GIT_SYSTEMS else None
    )
    raw_type = str(get("type") or "unknown")
    work_item_type = _WORK_ITEM_TYPE_FOR_ID.get(raw_type, "issue")
    external_key = str(get("external_key"))
    work_item_id = derive_work_item_id(system, instance, external_key, work_item_type)
    repo_id = (
        derive_repo_uuid(system, batch.source_instance, instance)
        if system in _GIT_SYSTEMS and instance
        else None
    )
    project_key, project_id, project_name = _project_scope(
        system, get, repository_external_id
    )

    return {
        "repo_id": repo_id,
        "work_item_id": work_item_id,
        "provider": system,
        "title": str(get("title") or ""),
        "type": raw_type,
        "status": str(get("status") or "unknown"),
        "status_raw": get("status_raw"),
        "project_key": project_key,
        "project_id": project_id,
        # Team-attribution precedence (docs/architecture/team-attribution.md
        # §0, AGENTS.md) treats any non-empty work_items.native_team_key as a
        # top-precedence NATIVE fact -- native sync only ever populates it
        # for Linear (WorkItem.native_team_key docstring: "None for
        # GitHub/GitLab ... and Jira"). `system` is now always
        # batch.source_system (see trust-boundary note above), so this
        # check is no longer independently spoofable via a forged
        # per-record `provider`.
        "native_team_key": get("native_team_key") if system == "linear" else None,
        "project_name": project_name,
        "assignees": [
            identity
            for identity in (
                _resolve_customer_identity(system, a) for a in (get("assignees") or [])
            )
            if identity
        ],
        "reporter": _resolve_customer_identity(system, get("reporter")),
        "created_at": (created_at := _coerce_datetime(get("created_at"))),
        # work_items.updated_at is non-nullable — WorkItemV1.updatedAt is
        # optional on the wire, so fall back to created_at when absent
        # (mirrors WorkItem's dataclass default of never being None).
        "updated_at": _coerce_datetime(get("updated_at")) or created_at,
        "started_at": _coerce_datetime(get("started_at")),
        "completed_at": _coerce_datetime(get("completed_at")),
        "closed_at": _coerce_datetime(get("closed_at")),
        "labels": get("labels") or [],
        "story_points": get("story_points"),
        "sprint_id": get("sprint_id"),
        "sprint_name": get("sprint_name"),
        "parent_id": get("parent_id"),
        "epic_id": get("epic_id"),
        "url": get("url"),
        "org_id": batch.org_id,
        "source_id": batch.source_id,
    }


def _build_transition_row(
    record: Any,
    batch: NormalizedBatch,
    *,
    index: int,
    warnings: list[SinkWriteError],
) -> dict[str, Any]:
    get = _getter(record)
    # Trust boundary — see _build_work_item_row: system is always
    # batch.source_system, never the spoofable per-record `provider`.
    _check_provider_scope(
        kind="work_item_transition",
        record_provider=get("provider"),
        batch=batch,
        index=index,
        warnings=warnings,
    )
    system = batch.source_system
    instance = batch.source_instance if system in _GIT_SYSTEMS else None
    external_key = str(get("external_key"))
    work_item_type = get("work_item_type")
    work_item_id = derive_work_item_id(system, instance, external_key, work_item_type)
    repo_id = (
        derive_repo_uuid(
            system, batch.source_instance, instance or batch.source_instance
        )
        if system in _GIT_SYSTEMS
        else None
    )
    return {
        "repo_id": repo_id,
        "work_item_id": work_item_id,
        "occurred_at": _coerce_datetime(get("occurred_at")),
        "from_status": get("from_status"),
        "to_status": get("to_status"),
        "from_status_raw": get("from_status_raw"),
        "to_status_raw": get("to_status_raw"),
        "actor": _resolve_customer_identity(system, get("actor")),
        "org_id": batch.org_id,
        "source_id": batch.source_id,
    }


def _build_dependency(record: Any, batch: NormalizedBatch) -> WorkItemDependency:
    get = _getter(record)
    system = batch.source_system
    instance = batch.source_instance if system in _GIT_SYSTEMS else None
    source_key = str(get("source_external_key"))
    target_key = str(get("target_external_key"))
    source_work_item_id = derive_work_item_id(
        system, instance, source_key, get("source_work_item_type")
    )
    target_work_item_id = derive_work_item_id(
        system, instance, target_key, get("target_work_item_type")
    )
    return WorkItemDependency(
        source_work_item_id=source_work_item_id,
        target_work_item_id=target_work_item_id,
        relationship_type=str(get("relationship_type")),
        relationship_type_raw=str(get("relationship_type_raw") or ""),
        last_synced=datetime.now(timezone.utc),
        org_id=batch.org_id,
        source_id=batch.source_id,
    )


# ---------------------------------------------------------------------------
# write_batch
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _maybe_store(
    clickhouse_dsn: str, org_id: str, needed: bool
) -> AsyncIterator[Any]:
    """Only open a ClickHouseStore connection when a git-family/team/identity
    kind is actually present — avoids a wasted connection for pure work-item
    (jira/linear-only) batches.

    Yields ``None`` when not needed; callers only dereference the yielded
    value inside the same ``if batch.<kind>:`` guards that determined
    ``needed`` in the first place, so ``None`` is never actually touched —
    typed as plain ``Any`` (not ``Any | None``) so mypy doesn't demand a
    redundant None-check at every call site.
    """
    if not needed:
        yield None
        return
    store = create_store(clickhouse_dsn, "clickhouse")
    store.org_id = org_id
    async with store:
        yield store


def _open_metrics_sink(clickhouse_dsn: str, org_id: str) -> ClickHouseMetricsSink:
    """``create_sink()`` is typed to return the abstract ``BaseMetricsSink``;
    narrow to the concrete class this module actually needs (org_id
    attribute, work-item-family write methods with the loose row-dict
    signatures ``WorkGraphMixin`` implements, not the ABC's stricter typed
    ``Sequence[WorkItem]`` declarations). ``create_sink`` always returns
    ``ClickHouseMetricsSink`` for a clickhouse DSN — a type-checker-only
    ``cast``, not a runtime ``isinstance`` check, deliberately: unit tests
    patch ``create_sink`` with a duck-typed fake, which is not (and should
    not need to be) a ``ClickHouseMetricsSink`` subclass."""
    sink = cast(ClickHouseMetricsSink, create_sink(clickhouse_dsn))
    sink.org_id = org_id
    return sink


async def write_batch(
    batch: NormalizedBatch, *, clickhouse_dsn: str
) -> SinkWriteResult:
    errors: list[SinkWriteError] = []
    warnings: list[SinkWriteError] = []
    counts: dict[str, int] = {}
    scope = AffectedScope(
        org_id=batch.org_id,
        source_systems={batch.source_system},
        source_instances={batch.source_instance},
    )

    needs_async_store = bool(
        batch.repositories
        or batch.commits
        or batch.pull_requests
        or batch.reviews
        or batch.teams
        or batch.identities
        or batch.operational_services
        or batch.operational_incidents
        or batch.operational_alerts
        or batch.incident_timeline_events
        or batch.incident_notes
        or batch.incident_responders
        or batch.escalation_policies
        or batch.on_call_schedules
        or batch.on_call_assignments
        or batch.operational_teams
        or batch.operational_users
        or batch.service_repository_mappings
    )

    async with _maybe_store(clickhouse_dsn, batch.org_id, needs_async_store) as store:
        if batch.repositories:
            try:
                for i, r in enumerate(batch.repositories):
                    _check_instance_scope(
                        kind="repository",
                        system=str(r.get("source_system") or batch.source_system),
                        repo_identifier=str(r.get("external_id") or ""),
                        batch=batch,
                        index=_original_index(batch, "repository", i),
                        warnings=warnings,
                    )
                repo_objs = [_build_repo_object(r, batch) for r in batch.repositories]
                for repo_obj in repo_objs:
                    await store.insert_repo(repo_obj)
                    scope.repo_ids.add(repo_obj.id)
                counts["repository"] = len(batch.repositories)
                scope.record_kinds.add("repository")
            except Exception as exc:  # noqa: BLE001 - one failed kind must not sink the batch
                logger.exception("write_batch: repository sink write failed")
                errors.append(
                    SinkWriteError(
                        record_index=-1,
                        kind="repository",
                        external_id=None,
                        code="clickhouse_insert_failed",
                        message=str(exc),
                    )
                )

        if batch.commits:
            try:
                for i, r in enumerate(batch.commits):
                    _check_instance_scope(
                        kind="commit",
                        system=batch.source_system,
                        repo_identifier=str(r.get("repository_external_id") or ""),
                        batch=batch,
                        index=_original_index(batch, "commit", i),
                        warnings=warnings,
                    )
                rows = [_build_commit_row(r, batch) for r in batch.commits]
                await store.insert_git_commit_data(rows)
                counts["commit"] = len(batch.commits)
                scope.record_kinds.add("commit")
                for row in rows:
                    scope.repo_ids.add(row["repo_id"])
                    _track_scope_timestamp(scope, row.get("author_when"))
            except Exception as exc:  # noqa: BLE001
                logger.exception("write_batch: commit sink write failed")
                errors.append(
                    SinkWriteError(
                        record_index=-1,
                        kind="commit",
                        external_id=None,
                        code="clickhouse_insert_failed",
                        message=str(exc),
                    )
                )

        if batch.pull_requests:
            try:
                for i, r in enumerate(batch.pull_requests):
                    _check_instance_scope(
                        kind="pull_request",
                        system=batch.source_system,
                        repo_identifier=str(r.get("repository_external_id") or ""),
                        batch=batch,
                        index=_original_index(batch, "pull_request", i),
                        warnings=warnings,
                    )
                rows = [_build_pr_row(r, batch) for r in batch.pull_requests]
                await store.insert_git_pull_requests(rows)
                counts["pull_request"] = len(batch.pull_requests)
                scope.record_kinds.add("pull_request")
                for row in rows:
                    scope.repo_ids.add(row["repo_id"])
                    _track_scope_timestamp(scope, row.get("created_at"))
            except Exception as exc:  # noqa: BLE001
                logger.exception("write_batch: pull_request sink write failed")
                errors.append(
                    SinkWriteError(
                        record_index=-1,
                        kind="pull_request",
                        external_id=None,
                        code="clickhouse_insert_failed",
                        message=str(exc),
                    )
                )

        if batch.reviews:
            try:
                for i, r in enumerate(batch.reviews):
                    _check_instance_scope(
                        kind="review",
                        system=batch.source_system,
                        repo_identifier=str(r.get("repository_external_id") or ""),
                        batch=batch,
                        index=_original_index(batch, "review", i),
                        warnings=warnings,
                    )
                rows = [_build_review_row(r, batch) for r in batch.reviews]
                await store.insert_git_pull_request_reviews(rows)
                counts["review"] = len(batch.reviews)
                scope.record_kinds.add("review")
                for row in rows:
                    scope.repo_ids.add(row["repo_id"])
                    _track_scope_timestamp(scope, row.get("submitted_at"))
            except Exception as exc:  # noqa: BLE001
                logger.exception("write_batch: review sink write failed")
                errors.append(
                    SinkWriteError(
                        record_index=-1,
                        kind="review",
                        external_id=None,
                        code="clickhouse_insert_failed",
                        message=str(exc),
                    )
                )

        if batch.teams:
            try:
                rows = [
                    _build_team_row(
                        r,
                        batch,
                        index=_original_index(batch, "team", i),
                        warnings=warnings,
                    )
                    for i, r in enumerate(batch.teams)
                ]
                await store.insert_teams(rows)
                counts["team"] = len(batch.teams)
                scope.record_kinds.add("team")
                for row in rows:
                    scope.team_ids.add(row["id"])
            except Exception as exc:  # noqa: BLE001
                logger.exception("write_batch: team sink write failed")
                errors.append(
                    SinkWriteError(
                        record_index=-1,
                        kind="team",
                        external_id=None,
                        code="clickhouse_insert_failed",
                        message=str(exc),
                    )
                )

        if batch.identities:
            try:
                rows = [
                    _build_identity_row(
                        r,
                        batch,
                        index=_original_index(batch, "identity", i),
                        warnings=warnings,
                    )
                    for i, r in enumerate(batch.identities)
                ]
                await store.insert_identities(rows)
                counts["identity"] = len(batch.identities)
                scope.record_kinds.add("identity")
            except Exception as exc:  # noqa: BLE001
                logger.exception("write_batch: identity sink write failed")
                errors.append(
                    SinkWriteError(
                        record_index=-1,
                        kind="identity",
                        external_id=None,
                        code="clickhouse_insert_failed",
                        message=str(exc),
                    )
                )

        operational_writes = (
            (
                "operational_services",
                "operational_service",
                "insert_operational_services",
            ),
            (
                "operational_incidents",
                "operational_incident",
                "insert_operational_incidents",
            ),
            ("operational_alerts", "operational_alert", "insert_operational_alerts"),
            (
                "incident_timeline_events",
                "incident_timeline_event",
                "insert_operational_incident_timeline_events",
            ),
            ("incident_notes", "incident_note", "insert_operational_incident_notes"),
            (
                "incident_responders",
                "incident_responder",
                "insert_operational_incident_responders",
            ),
            (
                "escalation_policies",
                "escalation_policy",
                "insert_operational_escalation_policies",
            ),
            (
                "on_call_schedules",
                "on_call_schedule",
                "insert_operational_on_call_schedules",
            ),
            (
                "on_call_assignments",
                "on_call_assignment",
                "insert_operational_on_call_assignments",
            ),
            ("operational_teams", "operational_team", "insert_operational_teams"),
            ("operational_users", "operational_user", "insert_operational_users"),
            (
                "service_repository_mappings",
                "service_repository_mapping",
                "insert_operational_service_repository_mappings",
            ),
        )
        for attribute, kind, writer_name in operational_writes:
            records = getattr(batch, attribute)
            if not records:
                continue
            try:
                await getattr(store, writer_name)(records)
                counts[kind] = len(records)
                scope.record_kinds.add(kind)
                _track_operational_scope(scope, records)
            except Exception as exc:  # noqa: BLE001
                logger.exception("write_batch: %s sink write failed", kind)
                errors.append(
                    SinkWriteError(
                        record_index=-1,
                        kind=kind,
                        external_id=None,
                        code="clickhouse_insert_failed",
                        message=str(exc),
                    )
                )

    if batch.work_items:
        try:
            rows = [
                _build_work_item_row(
                    r,
                    batch,
                    index=_original_index(batch, "work_item", i),
                    warnings=warnings,
                )
                for i, r in enumerate(batch.work_items)
            ]
            sink = _open_metrics_sink(clickhouse_dsn, batch.org_id)
            try:
                await asyncio.to_thread(sink.write_work_items, rows)
            finally:
                sink.close()
            counts["work_item"] = len(batch.work_items)
            scope.record_kinds.add("work_item")
            for row in rows:
                if row["repo_id"] is not None:
                    scope.repo_ids.add(row["repo_id"])
                scope.work_item_ids.add(row["work_item_id"])
                _track_scope_timestamp(
                    scope, row.get("updated_at") or row.get("created_at")
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception("write_batch: work_item sink write failed")
            errors.append(
                SinkWriteError(
                    record_index=-1,
                    kind="work_item",
                    external_id=None,
                    code="clickhouse_insert_failed",
                    message=str(exc),
                )
            )

    if batch.work_item_transitions:
        try:
            rows = [
                _build_transition_row(
                    r,
                    batch,
                    index=_original_index(batch, "work_item_transition", i),
                    warnings=warnings,
                )
                for i, r in enumerate(batch.work_item_transitions)
            ]
            sink = _open_metrics_sink(clickhouse_dsn, batch.org_id)
            try:
                await asyncio.to_thread(sink.write_work_item_transitions, rows)
            finally:
                sink.close()
            counts["work_item_transition"] = len(batch.work_item_transitions)
            scope.record_kinds.add("work_item_transition")
            for row in rows:
                if row["repo_id"] is not None:
                    scope.repo_ids.add(row["repo_id"])
                scope.work_item_ids.add(row["work_item_id"])
                _track_scope_timestamp(scope, row.get("occurred_at"))
        except Exception as exc:  # noqa: BLE001
            logger.exception("write_batch: work_item_transition sink write failed")
            errors.append(
                SinkWriteError(
                    record_index=-1,
                    kind="work_item_transition",
                    external_id=None,
                    code="clickhouse_insert_failed",
                    message=str(exc),
                )
            )

    if batch.work_item_dependencies:
        try:
            deps = [_build_dependency(r, batch) for r in batch.work_item_dependencies]
            sink = _open_metrics_sink(clickhouse_dsn, batch.org_id)
            try:
                await asyncio.to_thread(sink.write_work_item_dependencies, deps)
            finally:
                sink.close()
            counts["work_item_dependency"] = len(batch.work_item_dependencies)
            scope.record_kinds.add("work_item_dependency")
            for dep in deps:
                scope.work_item_ids.add(dep.source_work_item_id)
                scope.work_item_ids.add(dep.target_work_item_id)
        except Exception as exc:  # noqa: BLE001
            logger.exception("write_batch: work_item_dependency sink write failed")
            errors.append(
                SinkWriteError(
                    record_index=-1,
                    kind="work_item_dependency",
                    external_id=None,
                    code="clickhouse_insert_failed",
                    message=str(exc),
                )
            )

    return SinkWriteResult(
        ingestion_id=batch.ingestion_id,
        org_id=batch.org_id,
        counts_written=counts,
        errors=errors,
        warnings=warnings,
        affected_scope=scope,
    )
