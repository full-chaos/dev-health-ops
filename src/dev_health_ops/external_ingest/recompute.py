"""Bounded recomputation planner (CHAOS-2699).

Given the affected scope accumulated while processing an accepted
customer-push batch (org, source system/instance, repo ids, team ids,
record kinds, occurred-at window), decide whether the bounded scope needs
recomputing -- capped fan-out/window, never a full-org recompute by
default (master-spec CC21). The hand-off is native, not Celery: a
"dispatched" plan is persisted as a pending recompute scope on the
batch's own row (``recompute_status.record_recompute_dispatch``), which is
exactly the row shape the existing native external-ingest recompute drain
(``internal/externalrecompute``, CHAOS-5296) already polls for the Go
stream runner's own rows. Routing through that SAME poll -- rather than a
second compute path -- is what lets this module drop Celery entirely.

``RecomputeScope`` is internal to this module (decouples from CHAOS-2697/2698
per the synthesizer reconciliation on brief-2699-recompute.md); the public
seam other sub-issues call is :func:`schedule_or_coalesce`, which takes
plain primitives, not a shared dataclass.

CHAOS-5702 deletes the Valkey debounce/coalesce path this module used to
attempt before dispatching: it fed a Celery flush task retired under
CHAOS-3093, so past that point every call fell straight through to the
per-batch synchronous dispatch below regardless of Valkey's health.
:func:`schedule_or_coalesce` now has exactly the one live path, with no
Valkey dependency at all.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone

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

_DEFAULT_MAX_BACKFILL_DAYS = 14
_DEFAULT_MAX_FANOUT_REPOS = 25


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


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


@dataclass(frozen=True)
class RecomputeScope:
    """Affected scope for one bounded-recompute decision.

    Internal to this module (synthesizer reconciliation on
    brief-2699-recompute.md: "public seam is primitives, not a shared
    dataclass" -- decouples from CHAOS-2697/2698). Built by
    :func:`schedule_or_coalesce` from the primitives its caller passes for
    one batch.
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


def dispatch_recompute(plan: RecomputePlan) -> RecomputeDispatchResult:
    """Classify a bounded plan into a dispatch outcome (D5/D6).

    No Celery, no native call of its own: a ``"dispatched"`` outcome is
    handed off by the caller (:func:`dispatch_and_persist_scope`, via
    ``recompute_status.record_recompute_dispatch``) to the existing native
    external-ingest recompute drain (``internal/externalrecompute``,
    CHAOS-5296) -- the SAME poll that already turns the Go stream runner's
    rows into ``metrics.daily_dispatch``/``investment.materialize`` native
    handoffs, rather than a second compute path re-derived here.

    Never raises (D13) -- catches and returns ``status="failed"`` so a
    recompute-dispatch problem can never fail (or retry-loop) the ingestion
    worker task that calls into this module.
    """
    if not plan.trigger:
        return RecomputeDispatchResult(
            status="not_applicable", jobs=(), capped_days=False, capped_repos=False
        )

    try:
        # Mirrors the old fan-out's own "did anything actually get built"
        # check (D5/D8): daily fires for an explicit repo scope OR the
        # repo-less work-item fallback; investment fires whenever D4's hard
        # invariant (never both repo_ids and team_ids empty) is satisfied.
        daily_will_dispatch = plan.dispatch_daily and (
            bool(plan.repo_ids) or plan.fallback_org_wide_daily
        )
        investment_will_dispatch = not plan.skip_investment_no_scope
        if not daily_will_dispatch and not investment_will_dispatch:
            return RecomputeDispatchResult(
                status="skipped_no_scope",
                jobs=(),
                capped_days=plan.capped_days,
                capped_repos=plan.capped_repos,
            )
        return RecomputeDispatchResult(
            status="dispatched",
            jobs=(),
            capped_days=plan.capped_days,
            capped_repos=plan.capped_repos,
        )
    except Exception as exc:
        # D13: recompute dispatch failures must never fail ingestion.
        logger.error(
            "external_ingest.recompute.dispatch_failed org_id=%s reason=%s",
            plan.org_id,
            exc,
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
    """Plan + dispatch + persist for one batch's scope.

    Called by :func:`schedule_or_coalesce`. Persistence failures are logged,
    never raised (mirrors D13: a status-write hiccup must not surface as a
    recompute-dispatch failure to the caller, who has already durably
    handed the scope to the native external-ingest recompute drain -- or
    skipped it -- by this point).
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
) -> None:
    """Called once per finished batch by the external-ingest worker.

    Plans, dispatches, and persists this one batch's scope immediately
    (this used to be only the Valkey-unavailable fallback of a debounce/
    coalesce dance; CHAOS-5702 deleted that dance -- it fed a Celery flush
    task retired under CHAOS-3093, so it never dispatched anything of its
    own -- leaving this the only path, with no Valkey dependency).
    """
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
