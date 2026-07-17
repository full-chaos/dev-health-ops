"""Bounded recomputation planner + Valkey debounce (CHAOS-2699).

Given the affected scope accumulated while processing an accepted
customer-push batch (org, source system/instance, repo ids, team ids,
record kinds, occurred-at window), decide which existing metric Celery
tasks to enqueue -- capped fan-out/window, never a full-org recompute by
default (master-spec CC21).

``RecomputeScope`` is internal to this module (decouples from CHAOS-2697/2698
per the synthesizer reconciliation on brief-2699-recompute.md); the public
seam other sub-issues call is :func:`schedule_or_coalesce`, which takes
plain primitives, not a shared dataclass.

Design decisions D1-D15 are recorded in
``docs/architecture/external-ingest-bounded-recompute.md``.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Any

from celery import chain

from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

# D7: record-kind -> job-category routing, mirroring _GIT_TARGETS/
# _WORK_ITEM_TARGETS (workers/task_utils.py) but for the 9 v1 customer-push
# record kinds.
_GIT_KINDS = frozenset({"pull_request.v1", "review.v1", "commit.v1"})
_WORK_ITEM_KINDS = frozenset(
    {"work_item.v1", "work_item_transition.v1", "work_item_dependency.v1"}
)
_TEAM_KINDS = frozenset({"identity.v1", "team.v1"})
_OPERATIONAL_KINDS = frozenset(
    {
        "operational_service.v1",
        "operational_incident.v1",
        "operational_alert.v1",
        "incident_timeline_event.v1",
        "incident_note.v1",
        "incident_responder.v1",
        "escalation_policy.v1",
        "on_call_schedule.v1",
        "on_call_assignment.v1",
        "operational_team.v1",
        "operational_user.v1",
        "service_repository_mapping.v1",
    }
)
_REPO_ONLY_KINDS = frozenset({"repository.v1"})
_RECOMPUTE_TRIGGER_KINDS = _GIT_KINDS | _WORK_ITEM_KINDS

# D6: the DORA/complexity tasks (deferred-v1 kinds) and the checkpoint-
# gated / unscoped partitioned daily-metrics dispatch path (checkpoint-skip
# swallows same-day data; the latter has no org_id filter at all) are
# deliberately never referenced anywhere below this point in the module --
# negative-space contract, asserted by
# tests/test_external_ingest_recompute_dispatch.py via a source-text grep,
# so this comment itself must not name any of those four disqualified
# task identifiers.
_RUN_DAILY_METRICS_TASK = "dev_health_ops.workers.tasks.run_daily_metrics"
_RUN_WORK_GRAPH_BUILD_TASK = "dev_health_ops.workers.tasks.run_work_graph_build"
_DISPATCH_INVESTMENT_MATERIALIZE_TASK = (
    "dev_health_ops.workers.tasks.dispatch_investment_materialize_partitioned"
)

_DEFAULT_DEBOUNCE_SECONDS = 45
_DEFAULT_MAX_BACKFILL_DAYS = 14
_DEFAULT_MAX_FANOUT_REPOS = 25

_PENDING_KEY_PREFIX = "external-ingest:recompute:pending"
_GUARD_KEY_PREFIX = "external-ingest:recompute:scheduled"
# Pending-blob TTL outlives the scheduled flush by a wide margin so a slow
# broker/worker doesn't let the blob expire before the flush task reads it
# (Risk 4 in the brief: only Valkey connection *errors* are covered by the
# D3 synchronous fallback, not silent key eviction -- a generous TTL is the
# cheap mitigation for the eviction case).
_PENDING_BLOB_TTL_FLOOR_SECONDS = 300
# WATCH/MULTI optimistic-lock retry budget for the pending-blob merge
# (adversarial-review finding: concurrent schedule_or_coalesce() calls for
# the same debounce key must not silently overwrite each other's scope).
_MAX_COALESCE_RETRIES = 5


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def _debounce_seconds() -> int:
    return _env_int(
        "EXTERNAL_INGEST_RECOMPUTE_DEBOUNCE_SECONDS", _DEFAULT_DEBOUNCE_SECONDS
    )


def _max_backfill_days() -> int:
    return max(
        1,
        _env_int(
            "EXTERNAL_INGEST_RECOMPUTE_MAX_BACKFILL_DAYS", _DEFAULT_MAX_BACKFILL_DAYS
        ),
    )


def _max_fanout_repos() -> int:
    return max(
        1,
        _env_int(
            "EXTERNAL_INGEST_RECOMPUTE_MAX_FANOUT_REPOS", _DEFAULT_MAX_FANOUT_REPOS
        ),
    )


def _get_redis_client() -> Any:
    """Plain non-blocking Valkey client (D3) -- same shape as
    ``api/product_telemetry/streams.py:get_redis_client()``. Deliberately
    NOT ``get_consumer_redis_client()`` (``api/_stream_consumer.py``),
    which is reserved for blocking ``XREADGROUP`` reads only."""
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return None
    try:
        import valkey as redis

        return redis.from_url(redis_url, decode_responses=True)
    except Exception:
        logger.warning("Valkey unavailable for external-ingest recompute debounce")
        return None


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _pending_key(org_id: str, source_system: str, source_instance: str) -> str:
    return f"{_PENDING_KEY_PREFIX}:{org_id}:{source_system}:{source_instance}"


def _guard_key(org_id: str, source_system: str, source_instance: str) -> str:
    return f"{_GUARD_KEY_PREFIX}:{org_id}:{source_system}:{source_instance}"


@dataclass(frozen=True)
class RecomputeScope:
    """Affected scope for one bounded-recompute decision.

    Internal to this module (synthesizer reconciliation on
    brief-2699-recompute.md: "public seam is primitives, not a shared
    dataclass" -- decouples from CHAOS-2697/2698). Built by
    :func:`schedule_or_coalesce` from the primitives its caller passes
    (optionally merged across a debounce window's multiple calls).
    """

    org_id: str
    source_system: str
    source_instance: str
    repo_ids: frozenset[str] = field(default_factory=frozenset)
    team_ids: frozenset[str] = field(default_factory=frozenset)
    record_kinds: frozenset[str] = field(default_factory=frozenset)
    ingestion_ids: frozenset[str] = field(default_factory=frozenset)
    window_start: datetime | None = None
    window_end: datetime | None = None


@dataclass(frozen=True)
class RecomputePlan:
    """Pure output of :func:`plan_recompute` -- a bounded, capped plan.

    ``trigger=False`` means nothing to dispatch (record kinds are
    ``repository.v1``-only or empty -- D7 ``_REPO_ONLY_KINDS`` row).
    """

    org_id: str
    trigger: bool
    dispatch_daily: bool
    repo_ids: tuple[str, ...]
    team_ids: tuple[str, ...]
    day: str | None
    backfill_days: int | None
    from_date: str | None
    to_date: str | None
    capped_days: bool
    capped_repos: bool
    fallback_org_wide_daily: bool
    skip_investment_no_scope: bool


def plan_recompute(scope: RecomputeScope) -> RecomputePlan:
    """Pure function: scope -> bounded plan. No I/O, no Celery calls."""
    has_git = bool(scope.record_kinds & _GIT_KINDS)
    has_work_items = bool(scope.record_kinds & _WORK_ITEM_KINDS)
    has_team = bool(scope.record_kinds & _TEAM_KINDS)
    has_operational = bool(scope.record_kinds & _OPERATIONAL_KINDS)

    if not (has_git or has_work_items or has_team or has_operational):
        return RecomputePlan(
            org_id=scope.org_id,
            trigger=False,
            dispatch_daily=False,
            repo_ids=(),
            team_ids=(),
            day=None,
            backfill_days=None,
            from_date=None,
            to_date=None,
            capped_days=False,
            capped_repos=False,
            fallback_org_wide_daily=False,
            skip_investment_no_scope=False,
        )

    max_backfill_days = _max_backfill_days()
    max_fanout = _max_fanout_repos()

    window_end = scope.window_end or scope.window_start or datetime.now(timezone.utc)
    window_start = scope.window_start or window_end
    requested_days = max(1, (window_end.date() - window_start.date()).days + 1)
    capped_days = requested_days > max_backfill_days
    backfill_days = min(requested_days, max_backfill_days)
    clamped_start_date = window_end.date() - timedelta(days=backfill_days - 1)

    day = window_end.date().isoformat()
    from_date = datetime.combine(
        clamped_start_date, time.min, tzinfo=timezone.utc
    ).isoformat()
    to_date = window_end.isoformat()

    sorted_repo_ids = sorted(scope.repo_ids)
    capped_repos = len(sorted_repo_ids) > max_fanout
    repo_ids = tuple(sorted_repo_ids[:max_fanout])
    team_ids = tuple(sorted(scope.team_ids))

    dispatch_daily = has_git or has_work_items
    # D8: org-wide day-bounded fallback ONLY for work-item kinds with an
    # empty repo scope (Jira-native work items may carry no repo linkage).
    # Git kinds always carry a repo (pull_request/review/commit); a
    # git-only batch with empty repo_ids is structurally impossible but is
    # handled defensively here by simply not dispatching daily/work-graph
    # at all rather than falling back org-wide for a kind category that
    # was never meant to trigger the fallback.
    fallback_org_wide_daily = dispatch_daily and not repo_ids and has_work_items

    # D4 hard invariant: never call dispatch_investment_materialize_partitioned
    # with both repo_ids and team_ids empty.
    skip_investment_no_scope = not repo_ids and not team_ids

    return RecomputePlan(
        org_id=scope.org_id,
        trigger=True,
        dispatch_daily=dispatch_daily,
        repo_ids=repo_ids,
        team_ids=team_ids,
        day=day,
        backfill_days=backfill_days,
        from_date=from_date,
        to_date=to_date,
        capped_days=capped_days,
        capped_repos=capped_repos,
        fallback_org_wide_daily=fallback_org_wide_daily,
        skip_investment_no_scope=skip_investment_no_scope,
    )


@dataclass(frozen=True)
class RecomputeJobRecord:
    task: str
    task_id: str | None
    queue: str
    repo_id: str | None = None


@dataclass(frozen=True)
class RecomputeDispatchResult:
    status: str  # not_applicable | dispatched | skipped_no_scope | failed
    jobs: tuple[RecomputeJobRecord, ...]
    capped_days: bool
    capped_repos: bool
    error: str | None = None


def _daily_metrics_kwargs(
    plan: RecomputePlan, *, repo_id: str | None
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"org_id": plan.org_id}
    if plan.day is not None:
        kwargs["day"] = plan.day
    if plan.backfill_days is not None:
        kwargs["backfill_days"] = plan.backfill_days
    if repo_id is not None:
        kwargs["repo_id"] = repo_id
    return kwargs


def _work_graph_build_kwargs(plan: RecomputePlan, *, repo_id: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"org_id": plan.org_id, "repo_id": repo_id}
    if plan.from_date is not None:
        kwargs["from_date"] = plan.from_date
    if plan.to_date is not None:
        kwargs["to_date"] = plan.to_date
    return kwargs


def _investment_kwargs(plan: RecomputePlan) -> dict[str, Any]:
    # D4/D5: never both repo_ids and team_ids empty -- caller only invokes
    # this when skip_investment_no_scope is False. force=False (same-day
    # investment freshness best-effort, never forces a full rematerialize).
    kwargs: dict[str, Any] = {"org_id": plan.org_id, "force": False}
    if plan.repo_ids:
        kwargs["repo_ids"] = list(plan.repo_ids)
    if plan.team_ids:
        kwargs["team_ids"] = list(plan.team_ids)
    if plan.from_date is not None:
        kwargs["from_date"] = plan.from_date
    if plan.to_date is not None:
        kwargs["to_date"] = plan.to_date
    return kwargs


def dispatch_recompute(plan: RecomputePlan) -> RecomputeDispatchResult:
    """Impure: builds and fires Celery signatures per D5/D6.

    Never raises (D13) -- catches and returns ``status="failed"`` so a
    recompute-dispatch problem can never fail (or retry-loop) the ingestion
    worker task that calls into this module.
    """
    if not plan.trigger:
        return RecomputeDispatchResult(
            status="not_applicable", jobs=(), capped_days=False, capped_repos=False
        )

    try:
        jobs: list[RecomputeJobRecord] = []

        if plan.dispatch_daily:
            if plan.repo_ids:
                # D5: run_daily_metrics/run_work_graph_build only accept a
                # single repo_id -- fan out N independent chains, one per
                # repo, via celery.chain (immutable links so a parent's
                # return dict is never injected as the next task's arg).
                for repo_id in plan.repo_ids:
                    daily_sig = celery_app.signature(
                        _RUN_DAILY_METRICS_TASK,
                        kwargs=_daily_metrics_kwargs(plan, repo_id=repo_id),
                        queue="metrics",
                        immutable=True,
                    )
                    build_sig = celery_app.signature(
                        _RUN_WORK_GRAPH_BUILD_TASK,
                        kwargs=_work_graph_build_kwargs(plan, repo_id=repo_id),
                        queue="metrics",
                        immutable=True,
                    )
                    async_result = chain(daily_sig, build_sig).apply_async()
                    daily_id = (
                        async_result.parent.id
                        if async_result.parent is not None
                        else None
                    )
                    jobs.append(
                        RecomputeJobRecord(
                            task=_RUN_DAILY_METRICS_TASK,
                            task_id=daily_id,
                            queue="metrics",
                            repo_id=repo_id,
                        )
                    )
                    jobs.append(
                        RecomputeJobRecord(
                            task=_RUN_WORK_GRAPH_BUILD_TASK,
                            task_id=async_result.id,
                            queue="metrics",
                            repo_id=repo_id,
                        )
                    )
            elif plan.fallback_org_wide_daily:
                # D8: day-bounded, all-repos fallback for repo-less
                # work-item batches. run_work_graph_build is deliberately
                # NOT dispatched here (repo_id=None there means "all
                # repos, 30-day trailing window", ignoring the tighter
                # batch window -- not useful in this fallback).
                async_result = celery_app.send_task(
                    _RUN_DAILY_METRICS_TASK,
                    kwargs=_daily_metrics_kwargs(plan, repo_id=None),
                    queue="metrics",
                )
                jobs.append(
                    RecomputeJobRecord(
                        task=_RUN_DAILY_METRICS_TASK,
                        task_id=async_result.id,
                        queue="metrics",
                        repo_id=None,
                    )
                )

        if not plan.skip_investment_no_scope:
            # D5: dispatch_investment_materialize_partitioned accepts
            # repo_ids/team_ids lists directly -- called ONCE per flush
            # with the full capped list, never per repo.
            async_result = celery_app.send_task(
                _DISPATCH_INVESTMENT_MATERIALIZE_TASK,
                kwargs=_investment_kwargs(plan),
                queue="default",
            )
            jobs.append(
                RecomputeJobRecord(
                    task=_DISPATCH_INVESTMENT_MATERIALIZE_TASK,
                    task_id=async_result.id,
                    queue="default",
                    repo_id=None,
                )
            )

        status = "dispatched" if jobs else "skipped_no_scope"
        return RecomputeDispatchResult(
            status=status,
            jobs=tuple(jobs),
            capped_days=plan.capped_days,
            capped_repos=plan.capped_repos,
        )
    except Exception as exc:
        # D13: recompute dispatch failures must never fail ingestion.
        logger.exception(
            "external_ingest.recompute.dispatch_failed org_id=%s", plan.org_id
        )
        return RecomputeDispatchResult(
            status="failed",
            jobs=(),
            capped_days=plan.capped_days,
            capped_repos=plan.capped_repos,
            error=str(exc),
        )


def dispatch_and_persist_scope(
    *,
    org_id: str,
    source_system: str,
    source_instance: str,
    ingestion_ids: list[str],
    repo_ids: list[str],
    team_ids: list[str],
    record_kinds: list[str],
    window_start: datetime | None,
    window_end: datetime | None,
) -> RecomputeDispatchResult:
    """Plan + dispatch + persist for an already-coalesced scope.

    Shared by ``workers/external_ingest_recompute.py``'s debounced flush
    task and by :func:`schedule_or_coalesce`'s Valkey-unavailable
    synchronous fallback (D3) -- both end up with the same primitives, just
    via a different trigger path. Persistence failures are logged, never
    raised (mirrors D13: a status-write hiccup must not surface as a
    recompute-dispatch failure to the caller, who has already durably
    dispatched -- or skipped -- the Celery jobs by this point).
    """
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.external_ingest.recompute_status import (
        record_recompute_dispatch,
    )

    scope = RecomputeScope(
        org_id=org_id,
        source_system=source_system,
        source_instance=source_instance,
        repo_ids=frozenset(repo_ids),
        team_ids=frozenset(team_ids),
        record_kinds=frozenset(record_kinds),
        ingestion_ids=frozenset(ingestion_ids),
        window_start=window_start,
        window_end=window_end,
    )
    plan = plan_recompute(scope)
    result = dispatch_recompute(plan)

    if scope.ingestion_ids:
        try:
            with get_postgres_session_sync() as session:
                record_recompute_dispatch(
                    session,
                    org_id=org_id,
                    ingestion_ids=sorted(scope.ingestion_ids),
                    scope=scope,
                    result=result,
                )
        except Exception:
            logger.exception(
                "external_ingest.recompute.persist_failed org_id=%s "
                "source_system=%s source_instance=%s",
                org_id,
                source_system,
                source_instance,
            )
    return result


def _merge_pending_blob(
    existing: dict[str, Any] | None, new_call: dict[str, Any]
) -> dict[str, Any]:
    if existing is None:
        return new_call
    starts = [
        d
        for d in (
            _parse_iso(existing.get("window_start")),
            _parse_iso(new_call["window_start"]),
        )
        if d is not None
    ]
    ends = [
        d
        for d in (
            _parse_iso(existing.get("window_end")),
            _parse_iso(new_call["window_end"]),
        )
        if d is not None
    ]
    return {
        "org_id": new_call["org_id"],
        "source_system": new_call["source_system"],
        "source_instance": new_call["source_instance"],
        "repo_ids": sorted(
            set(existing.get("repo_ids", [])) | set(new_call["repo_ids"])
        ),
        "team_ids": sorted(
            set(existing.get("team_ids", [])) | set(new_call["team_ids"])
        ),
        "record_kinds": sorted(
            set(existing.get("record_kinds", [])) | set(new_call["record_kinds"])
        ),
        "ingestion_ids": sorted(
            set(existing.get("ingestion_ids", [])) | set(new_call["ingestion_ids"])
        ),
        "window_start": min(starts).isoformat() if starts else None,
        "window_end": max(ends).isoformat() if ends else None,
    }


def schedule_or_coalesce(
    *,
    org_id: str,
    source_system: str,
    source_instance: str,
    ingestion_id: str,
    repo_ids: set[str] | frozenset[str],
    team_ids: set[str] | frozenset[str],
    window_start: datetime | None,
    window_end: datetime | None,
    record_kinds: set[str] | frozenset[str],
    debounce_seconds: int | None = None,
) -> None:
    """Called once per finished batch by the external-ingest worker (D3/D10).

    Debounce key grain is ``(org_id, source_system, source_instance)`` --
    D10: different source instances debounce independently since their
    repo/team scopes are disjoint. Writes/merges the pending scope blob
    into Valkey and, iff a SETNX guard key is newly acquired, schedules
    ``flush_external_ingest_recompute.apply_async(countdown=debounce_seconds)``.

    If Valkey is unavailable (no ``REDIS_URL``, connection error, or any
    other exception talking to it), degrades to an IMMEDIATE synchronous
    dispatch+persist for just this one batch's scope (D3) -- recompute
    triggering is best-effort but never silently dropped.
    """
    seconds = debounce_seconds if debounce_seconds is not None else _debounce_seconds()
    new_call = {
        "org_id": org_id,
        "source_system": source_system,
        "source_instance": source_instance,
        "repo_ids": sorted(repo_ids),
        "team_ids": sorted(team_ids),
        "record_kinds": sorted(record_kinds),
        "ingestion_ids": [ingestion_id],
        "window_start": window_start.isoformat() if window_start is not None else None,
        "window_end": window_end.isoformat() if window_end is not None else None,
    }

    def _synchronous_fallback() -> None:
        dispatch_and_persist_scope(
            org_id=org_id,
            source_system=source_system,
            source_instance=source_instance,
            ingestion_ids=[ingestion_id],
            repo_ids=sorted(repo_ids),
            team_ids=sorted(team_ids),
            record_kinds=sorted(record_kinds),
            window_start=window_start,
            window_end=window_end,
        )

    client = _get_redis_client()
    if client is None:
        logger.warning(
            "external_ingest.recompute.valkey_unavailable_sync_fallback "
            "org_id=%s source_system=%s source_instance=%s",
            org_id,
            source_system,
            source_instance,
        )
        _synchronous_fallback()
        return

    pending_key = _pending_key(org_id, source_system, source_instance)
    guard_key = _guard_key(org_id, source_system, source_instance)
    blob_ttl = max(seconds * 4, _PENDING_BLOB_TTL_FLOOR_SECONDS)
    try:
        # Adversarial-review finding: a plain GET -> merge -> SET is not
        # atomic, so two truly concurrent callers for the same debounce key
        # can race and one's widened blob (and possibly its ingestion_id)
        # is silently overwritten. WATCH/MULTI turns the whole
        # read-merge-write-and-guard-acquire into a single optimistic
        # transaction: if the watched key changes between GET and EXEC,
        # Valkey aborts the transaction and we retry with a fresh read.
        from valkey.exceptions import WatchError

        acquired = None
        for _ in range(_MAX_COALESCE_RETRIES):
            pipe = client.pipeline(transaction=True)
            try:
                pipe.watch(pending_key)
                existing_raw = pipe.get(pending_key)
                merged = _merge_pending_blob(
                    json.loads(existing_raw) if existing_raw else None, new_call
                )
                pipe.multi()
                pipe.set(pending_key, json.dumps(merged), ex=blob_ttl)
                pipe.set(guard_key, "1", nx=True, ex=seconds)
                results = pipe.execute()
                acquired = bool(results[-1])
                break
            except WatchError:
                continue
        else:
            raise RuntimeError(
                "schedule_or_coalesce: exceeded WATCH/MULTI retry budget "
                f"for org_id={org_id} source_system={source_system} "
                f"source_instance={source_instance}"
            )

        if acquired:
            from dev_health_ops.workers.external_ingest_recompute import (
                flush_external_ingest_recompute,
            )

            flush_external_ingest_recompute.apply_async(
                kwargs={
                    "org_id": org_id,
                    "source_system": source_system,
                    "source_instance": source_instance,
                },
                countdown=seconds,
            )
    except Exception:
        logger.exception(
            "external_ingest.recompute.valkey_error_sync_fallback "
            "org_id=%s source_system=%s source_instance=%s",
            org_id,
            source_system,
            source_instance,
        )
        _synchronous_fallback()


__all__ = [
    "RecomputeDispatchResult",
    "RecomputeJobRecord",
    "RecomputePlan",
    "RecomputeScope",
    "dispatch_and_persist_scope",
    "dispatch_recompute",
    "plan_recompute",
    "schedule_or_coalesce",
]
