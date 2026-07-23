"""Sync run dispatch + unit worker + finalize contract (CHAOS-2512).

FROZEN CONTRACT — the three entrypoints of the fan-out execution model. Wave 2
(CHAOS-2512) implements the bodies and wraps each with the Celery ``@app.task``
decorator. They take IDs ONLY (no credentials, no DTOs) in their payloads.

Pipeline:
    plan_sync_run (CHAOS-2511)        -> persists SyncRun + units (status=planned)
    dispatch_sync_run(run_id)         -> DispatchGuard.authorize_run, then routes
                                         + queues each unit independently
    run_sync_unit(unit_id)            -> SyncTaskBootstrap.load + ProviderRuntime,
                                         executes ONE dataset, persists unit status,
                                         updates watermark ONLY if mode==incremental
                                         and the unit succeeded
    finalize_sync_run(run_id)         -> aggregates unit statuses; materializes
                                         post-sync metrics via the
                                         SyncRunPostDispatch/outbox ledger

Idempotency and Durability rules:
  * dispatch_sync_run is redispatchable: it only queues units still in
    planned/stale-dispatching state.
  * finalize_sync_run is a no-op until all units are terminal, and a no-op if
    the run's post-sync outbox row already exists. Each terminal unit enqueues
    finalize. Finalize itself enforces once-only via the unique
    (sync_run_id, kind) constraint on SyncRunPostDispatch.
  * Metrics are never dispatched from individual units. Post-sync durability
    flows through the sync_dispatch_outbox table and the reconciler relay,
    rather than only the SyncRunPostDispatch ledger. The post_sync kind is
    relayed guarded at-least-once: the reconciler terminally marks it only
    after scheduling succeeds, and a failure releases the claim with bounded
    backoff. CHAOS-2596 made supported downstream readers generation-safe, so
    a duplicate compute generation cannot inflate their results.
Observability (CHAOS-2519):
  Every structured log line emitted by the three tasks carries the full unit
  context: sync_run_id, unit_id, source_id, dataset_key, provider, cost_class.
  On failure, an error_category is classified and stored in the unit's result
  JSON so operators can distinguish provider-wide outages from source-specific
  or dataset-specific failures without querying raw exception text.
"""

from __future__ import annotations

import logging
import math
import os
import threading
import uuid
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, TypedDict

from billiard.exceptions import SoftTimeLimitExceeded
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, SessionTransactionOrigin

from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.models import (
    BackfillJob,
    JobRun,
    JobRunStatus,
    ProviderRateLimitObservation,
    SyncComputeCheckpoint,
    SyncComputeCheckpointStatus,
    SyncComputeType,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunPostDispatch,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.providers.usage import PROVIDER_USAGE_OBSERVATION_KEY
from dev_health_ops.sync.budget import estimate_provider_budget
from dev_health_ops.sync.budget_guard import BudgetGuard
from dev_health_ops.sync.canonical_incident_gate import (
    FEATURE_DISABLED_ERROR_CATEGORY,
    CanonicalIncidentFeatureDisabledError,
    require_canonical_incident_feature_for_update_sync,
    sync_dataset_requires_canonical_incident_feature,
    sync_run_requires_canonical_incident_feature,
)
from dev_health_ops.sync.datasets import DatasetKey
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISPATCH,
    OUTBOX_KIND_FINALIZE,
    OUTBOX_KIND_POST_SYNC,
    OUTBOX_STATUS_DISPATCHED,
    OUTBOX_STATUS_PENDING,
    upsert_outbox_wakeup,
)
from dev_health_ops.sync.dispatch_policy import route
from dev_health_ops.sync.error_sanitize import sanitize_error_text
from dev_health_ops.sync.feature_denial import (
    FeatureDisabledRunTransition,
    terminalize_feature_disabled_run,
)
from dev_health_ops.sync.guard import DispatchGuard
from dev_health_ops.sync.trigger_routing import (
    stamp_sync_run_canonical_config,
)
from dev_health_ops.sync.watermarks import set_watermark
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.job_contracts import ProviderUnitPayload
from dev_health_ops.workers.job_outbox import enqueue_worker_job
from dev_health_ops.workers.job_routes import (
    RIVER_CANARY_ROUTE,
    WorkerJobRouteError,
    resolve_worker_job_route,
)
from dev_health_ops.workers.post_sync_dispatch import build_post_sync_dispatch_payload
from dev_health_ops.workers.provider_unit_route import ProviderUnitRouteSwitches
from dev_health_ops.workers.queues import _cost_class_queues_enabled
from dev_health_ops.workers.rate_limit_defer import (
    RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS,
    plan_rate_limit_deferral,
)
from dev_health_ops.workers.sync_bootstrap import (
    ProviderRuntimeCache,
    SyncTaskBootstrap,
    SyncTaskContext,
)
from dev_health_ops.workers.task_utils import _GIT_TARGETS, _WORK_ITEM_TARGETS

logger = logging.getLogger(__name__)
_runtime_cache = ProviderRuntimeCache()
_TERMINAL_RUN_STATUSES = {
    SyncRunStatus.SUCCESS.value,
    SyncRunStatus.PARTIAL_FAILED.value,
    SyncRunStatus.FAILED.value,
}
_WORK_ITEM_RESULT_OBSERVATION_FIELDS = (
    "linear_page_count",
    "linear_batch_count",
)
_LINEAR_BACKFILL_WORK_ITEM_DATASETS = frozenset(
    {
        DatasetKey.WORK_ITEMS.value,
        DatasetKey.WORK_ITEM_LABELS.value,
        DatasetKey.WORK_ITEM_PROJECTS.value,
        DatasetKey.WORK_ITEM_HISTORY.value,
        DatasetKey.WORK_ITEM_COMMENTS.value,
    }
)
_LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES = frozenset(
    {
        "work_items",
        "work_item_transitions",
        "work_item_dependencies",
        "work_item_reopen_events",
        "work_item_interactions",
        "sprints",
        "ai_attribution",
        "work_item_metrics_daily",
        "estimate_coverage_metrics_daily",
        "work_item_user_metrics_daily",
        "work_item_cycle_times",
        "work_item_state_durations_daily",
        "work_item_team_attributions",
        "issue_type_metrics_daily",
        "investment_metrics_daily",
        "investment_classifications_daily",
    }
)
# CHAOS-2710 retry idempotency matrix. A Linear backfill unit's retry re-writes the
# COMPLETE window for every surface below; each is provably idempotent under a
# same-natural-key rewrite with a newer version (computed_at/last_synced):
#   work_items / transitions / reopen_events / interactions / sprints
#                                 -> RMT + reader-side FINAL or semantic-row dedupe (Phase 2)
#   work_item_dependencies        -> RMT(last_synced); loader reads FINAL, and the work-graph
#                                 builder's raw read only feeds work_graph_edges, itself an RMT
#                                 keyed on a deterministic edge_id hash so duplicate dependency
#                                 rows collapse to one persisted edge (no global FINAL needed)
#   ai_attribution                -> RMT(computed_at) + FINAL+ROW_NUMBER resolved view
#   work_item_metrics_daily       -> RMT(computed_at) (migration 055) + FINAL/argMax readers
#   estimate_coverage_metrics_daily -> RMT(computed_at) (migration 063) + argMax readers
#   work_item_user_metrics_daily  -> RMT(computed_at) (migration 055) + FINAL readers
#   work_item_cycle_times         -> RMT(computed_at) + argMax/FINAL readers
#   work_item_state_durations_daily -> argMax(duration, computed_at) readers over the key
#   work_item_team_attributions   -> latest-snapshot (max computed_at) + FINAL resolver
#   issue_type_metrics_daily      -> only read via SELECT DISTINCT (no aggregation)
#   investment_metrics_daily      -> argMax(col, computed_at) over the natural key in every
#                                 reader, incl. the analytics templates (compiler dedup CTE)
#   investment_classifications_daily -> no production reader (deterministic rule-based rows)
#   manual_attribution_fallbacks  -> RMT(updated_at) + FINAL reader (registry entry; this
#                                 job does not write it, but it is a proven-safe surface)
# Retry stays disabled for any unit whose write set is NOT a subset of this set.
_CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES = frozenset(
    {
        "work_items",
        "work_item_transitions",
        "work_item_dependencies",
        "work_item_reopen_events",
        "work_item_interactions",
        "sprints",
        "ai_attribution",
        "work_item_metrics_daily",
        "estimate_coverage_metrics_daily",
        "work_item_user_metrics_daily",
        "work_item_cycle_times",
        "work_item_state_durations_daily",
        "work_item_team_attributions",
        "issue_type_metrics_daily",
        "investment_metrics_daily",
        "investment_classifications_daily",
        "manual_attribution_fallbacks",
    }
)


class _PendingUnitCounts(TypedDict):
    dispatchable: int
    in_flight: int
    next_deferred_at: datetime | None


# ---------------------------------------------------------------------------
# Error categorisation (CHAOS-2519)
# ---------------------------------------------------------------------------

_PROVIDER_ERROR_PATTERNS: list[tuple[str, str]] = [
    # (substring_lower, category)
    ("rate limit", "rate_limit"),
    ("ratelimit", "rate_limit"),
    ("429", "rate_limit"),
    ("timeout", "timeout"),
    ("timed out", "timeout"),
    ("connection", "network"),
    ("network", "network"),
    ("ssl", "network"),
    ("certificate", "network"),
    ("401", "auth"),
    ("403", "auth"),
    ("unauthorized", "auth"),
    ("forbidden", "auth"),
    ("not found", "not_found"),
    ("404", "not_found"),
    ("500", "provider_error"),
    ("502", "provider_error"),
    ("503", "provider_error"),
    ("server error", "provider_error"),
]


def _classify_error(exc: BaseException) -> str:
    """Return a coarse error category string from an exception.

    Categories: rate_limit, timeout, network, auth, not_found,
    provider_error, adapter_error.
    """
    if isinstance(exc, CanonicalIncidentFeatureDisabledError):
        return FEATURE_DISABLED_ERROR_CATEGORY
    msg = str(exc).lower()
    for pattern, category in _PROVIDER_ERROR_PATTERNS:
        if pattern in msg:
            return category
    return "adapter_error"


def _budget_estimate_audit(
    ctx: SyncTaskContext, log_ctx: dict[str, Any]
) -> list[dict[str, Any]] | None:
    try:
        estimates = estimate_provider_budget(ctx)
    except Exception as exc:
        logger.warning(
            "run_sync_unit.budget_estimate_failed",
            extra={**log_ctx, "error": str(exc)},
        )
        return None
    if not estimates:
        return None
    return [estimate.to_dict() for estimate in estimates]


def _attach_budget_observation(
    result: dict[str, Any], budget_audit: list[dict[str, Any]] | None
) -> dict[str, Any]:
    if budget_audit is None:
        return result
    result_payload = dict(result)
    raw_observations = result_payload.get("observations")
    observations = (
        dict(raw_observations) if isinstance(raw_observations, Mapping) else {}
    )
    observations["budget_estimate"] = budget_audit
    result_payload["observations"] = observations
    return result_payload


def _comparison_budget_key(bucket: Mapping[str, Any], *, route_family: str) -> str:
    """Mirror ``BudgetGuard._budget_key``'s format (``sync/budget_guard.py``)
    so a calibration warning's ``budget_key`` correlates 1:1 with the
    ``budget_key`` BudgetGuard's own admission logs emit for the same bucket.
    Duplicated rather than imported to avoid reaching into BudgetGuard's
    private helpers from the calibration join (CHAOS-2759).
    """
    return ":".join(
        (
            str(bucket.get("provider", "")),
            str(bucket.get("org_id", "")),
            str(bucket.get("host", "")),
            str(bucket.get("credential_fingerprint", "")),
            str(bucket.get("dimension", "")),
            route_family,
        )
    )


# Dimensions whose estimated_units approximate a literal request/page count
# (see docs/providers/rate-limit-policy.md's dimension table: rest_core is
# "Standard REST request budget", search is "Search/JQL request budget"), so a
# raw actual_requests comparison is meaningful. graphql_cost (query-cost /
# complexity points), contents_blob (high-variance blob/tree expansion), and
# secondary_abuse_risk (a flat risk-flag reservation, not a request count) are
# NOT -- comparing a request COUNT against one of those would invent a
# conversion the estimator never made (CHAOS-2759 adversarial review finding).
_REQUEST_COUNT_COMPARABLE_DIMENSIONS = frozenset({"rest_core", "search"})


def _shell_bucket(template: Mapping[str, Any], *, dimension: str) -> dict[str, Any]:
    """Build a bucket for a route_family/dimension with no estimate of its
    own, borrowing the provider/org_id/host/credential_fingerprint of a
    sibling estimate from the SAME unit (they share one ctx, so those fields
    are constant across every estimate a unit's audit produces) and swapping
    in the actual observation's own dimension.
    """

    return {**dict(template), "dimension": dimension}


def _join_budget_estimates_with_actuals(
    budget_audit: list[dict[str, Any]],
    provider_usage: list[Any],
) -> list[dict[str, Any]]:
    """Pure join of admit-time-adjacent budget estimates to CHAOS-2754's
    normalized actuals, per ``(route_family, dimension)`` (CHAOS-2759).

    Read-only: never mutates ``budget_audit`` / ``provider_usage`` and never
    re-estimates or touches ``SYNC_BUDGET_*`` consumption -- estimator outputs
    and budget enforcement live entirely in ``sync/budget.py`` /
    ``sync/budget_guard.py`` and are untouched by this function
    (``test_estimates_never_mutated_by_comparison``).

    Reports both numbers RAW: ``estimated_units`` are abstract reservation
    units (docs/ops/deployment-guide.md), never converted against
    ``actual_requests``. A route_family/dimension with an estimate but no
    drained actuals this run (e.g. code datasets, LaunchDarkly) produces no
    row -- never a fabricated 100% over-estimation. The reverse -- actual
    traffic with NO matching estimate at all, including the shared recorder's
    ``unclassified`` fallback -- is the highest-value calibration signal (real
    provider calls against zero admitted budget) and IS surfaced, with
    ``estimated_units: 0`` and ``unbudgeted_actual: True``.

    ``underestimated``/``ratio`` are only computed when the comparison is
    unit-comparable (``underestimation_assessable``): either the dimension
    denominates in something request-count-like
    (``_REQUEST_COUNT_COMPARABLE_DIMENSIONS``), or there is no estimate at all
    (a zero baseline is never a unit-conversion problem, unlike comparing a
    request count to a nonzero abstract quantity of e.g. GraphQL cost
    points). Otherwise both are left unassessed (``ratio: None``,
    ``underestimated: False``) with a ``underestimation_assessable_reason``
    explaining why, and no warning is logged for that row.

    When the CHAOS-2754 recorder's 50-key overflow marker is present, dropped
    operations could belong to any route_family (the recorder never learns
    which family a dropped operation would have joined), so every row for this
    unit is marked ``incomplete`` and no over-estimation conclusion should be
    drawn from a row where ``actual_requests <= estimated_units``.
    Underestimation remains a valid signal even when incomplete: a capped
    (undercounted) actual that already exceeds the estimate only understates
    the true overage.
    """

    incomplete = any(
        isinstance(entry, Mapping) and "dropped_operation_count" in entry
        for entry in provider_usage
    )

    actual_requests_by_key: dict[tuple[str, str], int] = defaultdict(int)
    for entry in provider_usage:
        if not isinstance(entry, Mapping) or "dropped_operation_count" in entry:
            continue
        route_family = entry.get("route_family")
        dimension = entry.get("dimension")
        if not route_family or not dimension:
            continue
        actual_requests_by_key[(route_family, dimension)] += int(
            entry.get("request_count") or 0
        )

    if not actual_requests_by_key:
        return []

    estimated_units_by_key: dict[tuple[str, str], int] = defaultdict(int)
    bucket_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    for estimate in budget_audit:
        if not isinstance(estimate, Mapping):
            continue
        route_family = estimate.get("route_family")
        bucket = estimate.get("bucket")
        if not isinstance(bucket, Mapping):
            continue
        dimension = bucket.get("dimension")
        if not route_family or not dimension:
            continue
        key = (route_family, dimension)
        estimated_units_by_key[key] += int(estimate.get("estimated_units") or 0)
        bucket_by_key.setdefault(key, bucket)

    if not bucket_by_key:
        # No usable bucket to attribute even an unbudgeted row to.
        return []
    bucket_template = next(iter(bucket_by_key.values()))

    comparisons: list[dict[str, Any]] = []
    for key in sorted(actual_requests_by_key):
        route_family, dimension = key
        actual_requests = actual_requests_by_key[key]
        unbudgeted_actual = key not in estimated_units_by_key
        estimated_units = 0 if unbudgeted_actual else estimated_units_by_key[key]
        bucket = bucket_by_key.get(key)
        if bucket is None:
            bucket = _shell_bucket(bucket_template, dimension=dimension)

        assessable = (
            unbudgeted_actual or dimension in _REQUEST_COUNT_COMPARABLE_DIMENSIONS
        )
        if assessable:
            ratio = actual_requests / estimated_units if estimated_units else None
            underestimated = actual_requests > estimated_units
            assessable_reason = None
        else:
            ratio = None
            underestimated = False
            assessable_reason = (
                f"{dimension!r} is an abstract reservation unit, not a 1:1 "
                "request count -- comparing actual_requests against it would "
                "invent a conversion the estimator never made"
            )

        comparisons.append(
            {
                "route_family": route_family,
                "dimension": dimension,
                "estimated_units": estimated_units,
                "actual_requests": actual_requests,
                "ratio": ratio,
                "underestimated": underestimated,
                "underestimation_assessable": assessable,
                "underestimation_assessable_reason": assessable_reason,
                "unbudgeted_actual": unbudgeted_actual,
                "incomplete": incomplete,
                "bucket": dict(bucket),
                "budget_key": _comparison_budget_key(bucket, route_family=route_family),
            }
        )
    return comparisons


def _attach_budget_comparison(
    result: dict[str, Any],
    budget_audit: list[dict[str, Any]] | None,
    *,
    log_ctx: dict[str, Any],
    computed_at: datetime,
) -> dict[str, Any]:
    """Attach ``observations.budget_comparison`` and log underestimation
    (CHAOS-2759). OBSERVE-ONLY: does not change ``result`` when there is
    nothing to compare, and never mutates ``budget_audit`` or the estimator
    inputs it was built from.

    Compares against the RUN-TIME budget audit computed just before this unit
    dispatched its dataset fetch, not the estimate BudgetGuard admitted at
    plan/dispatch time -- env-flag-dependent estimators (e.g. Jira's
    ``JIRA_FETCH_WORKLOGS`` / ``ATLASSIAN_GQL_ENABLED`` gating) can in
    principle disagree between admission and execution. ``computed_at`` is
    recorded precisely so this drift is inspectable; persisting the
    admit-time estimate onto the unit for a drift-free comparison is a
    deliberately deferred follow-up (open decision), not added speculatively.
    """

    if not budget_audit:
        return result
    observations = result.get("observations")
    if not isinstance(observations, Mapping):
        return result
    provider_usage = observations.get(PROVIDER_USAGE_OBSERVATION_KEY)
    if not isinstance(provider_usage, list) or not provider_usage:
        return result

    comparisons = _join_budget_estimates_with_actuals(budget_audit, provider_usage)
    if not comparisons:
        return result

    result_payload = dict(result)
    new_observations = dict(observations)
    new_observations["budget_comparison"] = comparisons
    new_observations["budget_comparison_computed_at"] = computed_at.isoformat()
    result_payload["observations"] = new_observations

    for comparison in comparisons:
        # underestimated is only ever True for an underestimation_assessable
        # row (see _join_budget_estimates_with_actuals), so this already
        # excludes abstract-unit dimensions -- no warning is invented from a
        # request-count vs. reservation-unit mismatch.
        if not comparison["underestimated"]:
            continue
        reason = (
            "unbudgeted_actual" if comparison["unbudgeted_actual"] else "underestimated"
        )
        logger.warning(
            "run_sync_unit.budget_underestimated",
            extra={
                **log_ctx,
                "bucket": comparison["bucket"],
                "budget_key": comparison["budget_key"],
                "estimated_units": comparison["estimated_units"],
                "actual_requests": comparison["actual_requests"],
                "route_family": comparison["route_family"],
                "dimension": comparison["dimension"],
                "ratio": comparison["ratio"],
                "incomplete": comparison["incomplete"],
                "reason": reason,
            },
        )
    return result_payload


def _promote_result_observation_fields(result: dict[str, Any]) -> dict[str, Any]:
    observations = result.get("observations")
    if not isinstance(observations, Mapping):
        return result
    for field_name in _WORK_ITEM_RESULT_OBSERVATION_FIELDS:
        if field_name in observations:
            result[field_name] = observations[field_name]
    return result


def _merge_partial_observations_into_result(
    result: dict[str, Any], exc: BaseException
) -> None:
    """Merge usage observations captured before a mid-sync raise into a
    failure/deferral unit result (CHAOS-2754).

    Additive only: it nests the actuals under ``observations`` and promotes the
    linear page/batch counts to the top level (admin-API contract), leaving
    ``error_category`` / ``next_retry_at`` and every other top-level field the
    admin router reads untouched.
    """

    from dev_health_ops.metrics.job_work_items import (
        read_work_item_partial_observations,
    )

    observations = read_work_item_partial_observations(exc)
    if not observations:
        return
    existing = result.get("observations")
    merged = dict(existing) if isinstance(existing, Mapping) else {}
    merged.update(observations)
    result["observations"] = merged
    _promote_result_observation_fields(result)


_AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION = "ambiguous_dimension"

# Allow-listed, normalized rate-limit reason categories (CHAOS-2758 review):
# the observation store must never persist raw exception text -- legacy
# no-signal raise sites build messages from provider response bodies, which
# can embed header/body-shaped diagnostic content. Every provider client's
# ``RateLimitSignal.reason`` value is already drawn from this vocabulary
# (`primary`/`secondary` for GitHub/GitLab/LaunchDarkly/Jira,
# `complexity` for Linear); `permission` is reserved for a future
# classification site. Anything else -- including no signal at all --
# normalizes to `unknown`.
_RATE_LIMIT_REASON_CATEGORIES = frozenset(
    {"primary", "secondary", "permission", "complexity", "unknown"}
)
_UNKNOWN_RATE_LIMIT_REASON = "unknown"


def _route_family_and_attribution(
    budget_audit: list[dict[str, Any]] | None, dimension: str | None
) -> tuple[str | None, str | None]:
    """Resolve the route family a rate-limit observation attributes to.

    Returns ``(route_family, attribution)``. ``attribution`` is ``None`` when
    ``route_family`` was confidently resolved, or
    :data:`_AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION` when it could not be -- in
    which case ``route_family`` is always ``None`` too. Never guesses: a
    future cooldown-gating consumer (CHAOS-2760) falls back to
    provider+integration+dimension gating whenever attribution is set,
    documented in ``docs/providers/rate-limit-policy.md``.

    A unit's budget estimate can carry multiple (route_family, dimension)
    pairs, and **dimension alone does not disambiguate them**: e.g. Linear's
    ``work-items`` estimator (``providers/linear/budget.py``) emits ``teams``,
    ``issues``, ``cycles``, ``comments``, ``attachments``, and ``history`` --
    all under ``graphql_cost`` -- and Jira's issue-comment datasets
    (``providers/jira/budget.py``) can emit both ``jira_issue_enrichment`` and
    ``jira_comments`` under ``rest_core``. Picking the first match in that
    case would be a guess, not an attribution.

    The rate-limit signal's ``dimension`` (populated by the provider client at
    the classification site, i.e. which kind of call actually hit the limit)
    narrows the candidate estimates when present. If the surviving candidate
    set names exactly one **distinct** route family, that family is the
    confident answer (this also covers the common single-estimate unit, e.g.
    GitHub's ``commits`` dataset, and estimates that share one family across
    multiple dimensions, e.g. GitHub's ``commit_stats``). Any other outcome --
    no budget audit, no dimension match, or more than one distinct family --
    is ambiguous and is NOT guessed at.
    """
    if not budget_audit:
        return None, _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION

    candidates = budget_audit
    if dimension is not None:
        dimension_matches = [
            entry
            for entry in budget_audit
            if isinstance(entry, Mapping)
            and isinstance(entry.get("bucket"), Mapping)
            and entry["bucket"].get("dimension") == dimension
        ]
        if not dimension_matches:
            # The signal names a dimension the unit never budgeted (e.g. a
            # secondary-abuse signal against a REST-only unit). Falling back
            # to the full audit would confidently attribute a family whose
            # dimension CONTRADICTS the signal — CHAOS-2760 must use the
            # provider+integration+dimension fallback instead.
            return None, _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION
        candidates = dimension_matches

    route_families = {
        str(entry.get("route_family"))
        for entry in candidates
        if isinstance(entry, Mapping) and entry.get("route_family")
    }
    if len(route_families) == 1:
        return next(iter(route_families)), None
    return None, _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION


def _normalized_rate_limit_reason(exc: BaseException) -> str:
    """Return an allow-listed rate-limit reason category, never raw text.

    ``signal.reason`` is already a short normalized category at every
    provider classification site (see :data:`_RATE_LIMIT_REASON_CATEGORIES`),
    but this defensively re-validates it rather than trusting it blindly, and
    NEVER falls back to ``str(exc)``: legacy no-signal raise sites build their
    message from the raw provider response body, which can embed
    header/body-shaped diagnostic content this store must not retain for its
    14-day (default) retention window.
    """
    signal = getattr(exc, "signal", None)
    if signal is not None and signal.reason:
        candidate = str(signal.reason).strip().lower()
        if candidate in _RATE_LIMIT_REASON_CATEGORIES:
            return candidate
    return _UNKNOWN_RATE_LIMIT_REASON


def _sanitize_retry_after_seconds(value: float | None) -> float | None:
    """Validate a provider-supplied retry-after value before persisting it
    (CHAOS-2760 review finding). A malformed value here -- a provider bug, a
    header-parsing edge case, or a literal inf/NaN -- would otherwise flow
    straight into the durable observation store, where the cooldown-gating
    consumer's ``timedelta(seconds=...)`` arithmetic raises on a non-finite
    value; the reader has its own fail-open guard for that
    (``sync/budget_guard.py``), but a corrupt value should never be written
    in the first place. Never NaN/inf, never negative, and capped at the
    same wall-clock budget (``RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS``) the
    deferral planner enforces -- a provider asking for a longer wait than
    the run would ever honor is not worth persisting verbatim either.
    """
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or numeric < 0:
        return None
    return min(numeric, float(RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS))


def _build_rate_limit_observation(
    *,
    unit: SyncRunUnit,
    provider: str,
    exc: BaseException,
    budget_audit: list[dict[str, Any]] | None,
    observed_at: datetime,
) -> ProviderRateLimitObservation:
    """Build the durable observation row for a rate-limit deferral (CHAOS-2758).

    Only normalized fields are persisted -- never raw provider headers or
    exception text (leak / bloat risk). ``integration_id``/``sync_run_id``/
    ``sync_run_unit_id`` come from the unit row already loaded in this task
    (never re-resolved from mutable ``Integration`` state); ``route_family``/
    ``dimension`` come from the budget estimate ALREADY computed for this
    unit at dispatch time -- never re-estimated (estimators require
    credential decryption). The ``RateLimitSignal`` (CHAOS-2753) may be absent
    on legacy-connector exceptions raised without one; every field degrades
    gracefully to the unit/exception context in that case.
    """
    signal = getattr(exc, "signal", None)
    dimension = (
        signal.dimension.value
        if signal is not None and signal.dimension is not None
        else None
    )
    retry_after_seconds = getattr(exc, "retry_after_seconds", None)
    if retry_after_seconds is None and signal is not None:
        retry_after_seconds = signal.retry_after_seconds
    retry_after_seconds = _sanitize_retry_after_seconds(retry_after_seconds)
    route_family, route_family_attribution = _route_family_and_attribution(
        budget_audit, dimension
    )
    return ProviderRateLimitObservation(
        org_id=str(unit.org_id),
        provider=(
            signal.provider if signal is not None and signal.provider else provider
        ),
        host=signal.host if signal is not None else None,
        integration_id=unit.integration_id,
        sync_run_id=unit.sync_run_id,
        sync_run_unit_id=unit.id,
        route_family=route_family,
        route_family_attribution=route_family_attribution,
        dimension=dimension,
        retry_after_seconds=retry_after_seconds,
        reset_at=signal.reset_at if signal is not None else None,
        reason=_normalized_rate_limit_reason(exc),
        request_id=signal.request_id if signal is not None else None,
        observed_at=observed_at,
    )


@celery_app.task(queue="sync", name="dev_health_ops.workers.tasks.dispatch_sync_run")
def dispatch_sync_run(sync_run_id: str) -> dict[str, Any]:
    """Authorize, route, and queue all pending units of a planned run.

    Idempotent / redispatchable. Implemented in CHAOS-2512.
    """

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.workers.reference_discovery import (
        ensure_reference_discovery_wakeup,
        reference_discovery_succeeded,
    )

    river_queued = 0
    with get_postgres_session_sync() as session:
        # The provider-unit outbox row and DISPATCHING claim must share one
        # explicit transaction. A process death therefore commits both or
        # neither, closing the producer kill window without serializing
        # credentials, callables, or route configuration into River.
        transaction = session.get_transaction()
        if (
            transaction is not None
            and transaction.origin is SessionTransactionOrigin.AUTOBEGIN
        ):
            # A task-owned fresh session begins implicitly when it first reads
            # its run. Persist that read/fixture transaction rather than
            # rolling it back: rollback can erase a just-planned run before
            # dispatch sees it. The explicit producer transaction below then
            # fences the unit claim and outbox row together.
            session.commit()
        if not session.in_transaction():
            session.begin()
        run_uuid = uuid.UUID(str(sync_run_id))
        run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
        if run is None:
            logger.warning(
                "dispatch_sync_run.missing",
                extra={"sync_run_id": sync_run_id},
            )
            return {"status": "missing", "sync_run_id": sync_run_id}
        requires_canonical_feature = sync_run_requires_canonical_incident_feature(
            session, run
        )
        if requires_canonical_feature:
            try:
                require_canonical_incident_feature_for_update_sync(session, run.org_id)
            except CanonicalIncidentFeatureDisabledError as exc:
                transition = terminalize_feature_disabled_run(session, run, exc)
                if transition.run_terminal:
                    _terminalize_feature_disabled_graph(
                        session,
                        run,
                        exc,
                    )
                else:
                    _arm_feature_disabled_finalize(
                        session,
                        run,
                        datetime.now(timezone.utc),
                    )
                session.flush()
                session.commit()
                logger.warning(
                    "dispatch_sync_run.feature_disabled",
                    extra={
                        "sync_run_id": str(run.id),
                        "org_id": str(run.org_id),
                        "error_category": FEATURE_DISABLED_ERROR_CATEGORY,
                        "running_units": transition.running_units,
                    },
                )
                return {
                    "status": FEATURE_DISABLED_ERROR_CATEGORY,
                    "sync_run_id": sync_run_id,
                    "dispatched": 0,
                    "failed_units": transition.failed_units,
                }
        if not reference_discovery_succeeded(session, run_uuid):
            now = datetime.now(timezone.utc)
            ensure_reference_discovery_wakeup(session, run_uuid, now=now)
            session.flush()
            logger.info(
                "dispatch_sync_run.blocked_on_reference_discovery",
                extra={"sync_run_id": sync_run_id},
            )
            return {
                "status": "blocked_on_reference_discovery",
                "sync_run_id": sync_run_id,
            }

        decision = DispatchGuard.authorize_run(session, sync_run_id)

        # --- Total-cap hard-deny: whole run is over the org unit ceiling ---
        if not decision.allowed:
            error = decision.reason or "sync dispatch denied"
            if _run_has_dispatching_or_running_units(session, run_uuid):
                failed_planned = _fail_planned_units(session, run_uuid, error)
                failed_stale_dispatching = _fail_stale_dispatching_units(
                    session, run_uuid, error
                )
                session.flush()
                logger.warning(
                    "dispatch_sync_run.denied_with_active_units",
                    extra={
                        "sync_run_id": sync_run_id,
                        "reason": error,
                        "failed_planned_units": failed_planned,
                        "failed_stale_dispatching_units": failed_stale_dispatching,
                    },
                )
                _enqueue_denied_active_finalize(sync_run_id)
                return {
                    "status": "denied_active",
                    "reason": error,
                    "failed_planned_units": failed_planned,
                    "failed_stale_dispatching_units": failed_stale_dispatching,
                }
            else:
                # No unit is DISPATCHING/RUNNING, so every remaining
                # non-terminal unit (PLANNED / RETRYING) can never legally
                # dispatch again — the guard re-denies every redispatch.
                # Fail them NOW: leaving them stranded under a terminal run
                # is invisible to the reconciler (it skips terminal runs)
                # and pollutes coverage as permanent requested-but-uncovered
                # windows.
                completed_at = datetime.now(timezone.utc)
                failed_planned = _fail_planned_units(session, run_uuid, error)
                run.status = SyncRunStatus.FAILED.value
                run.completed_at = completed_at
                run.error = error
                run.failed_units = int(run.failed_units or 0) + failed_planned
                run.result = {"capped_unit_ids": list(decision.capped_unit_ids)}
                sync_observers_for_terminal_sync_run(session, run)
                session.flush()
                logger.warning(
                    "dispatch_sync_run.denied",
                    extra={
                        "sync_run_id": sync_run_id,
                        "reason": run.error,
                        "failed_planned_units": failed_planned,
                    },
                )
                return {
                    "status": "denied",
                    "reason": run.error,
                    "failed_planned_units": failed_planned,
                }

        if not decision.allowed:
            logger.warning(
                "dispatch_sync_run.continuing_after_denial_for_active_units",
                extra={
                    "sync_run_id": sync_run_id,
                    "reason": decision.reason or "sync dispatch denied",
                },
            )

        # --- Concurrency partial-cap: defer overflow units, proceed with rest ---
        capped_ids: frozenset[str] = frozenset()
        if decision.concurrency_capped and decision.capped_unit_ids:
            capped_ids = frozenset(decision.capped_unit_ids)
            logger.info(
                "dispatch_sync_run.concurrency_capped",
                extra={
                    "sync_run_id": sync_run_id,
                    "capped_count": len(capped_ids),
                    "reason": decision.reason,
                },
            )

        BudgetGuard.observe_run(session, sync_run_id, capped_unit_ids=capped_ids)
        budget_result = BudgetGuard.enforce_run(
            session, sync_run_id, capped_unit_ids=capped_ids
        )
        capped_ids = frozenset((*capped_ids, *budget_result.deferred_unit_ids))

        # CHAOS-2760 TOCTOU closure (review finding): enforce_run's cooldown
        # snapshot can go stale by the time we reach the claim below —
        # budget admission does real DB work (re-estimating every active
        # unit in the bucket) in between, during which a sibling unit's 429
        # can commit a brand-new observation this pass never saw. Re-check
        # once more, right here, as the LAST read before the atomic claim —
        # reusing the estimates enforce_run already computed, no
        # re-estimation / credential decryption. reconfirm_cooldowns fully
        # defers/terminalizes any match it catches (same write path
        # enforce_run's own cooldown loop uses) — a bare exclusion here
        # would leave the unit PLANNED with no deferral-budget bookkeeping
        # and livelock the run on a bare ~60s redispatch countdown (review
        # finding, round 2).
        reconfirm_result = BudgetGuard.reconfirm_cooldowns(
            session,
            sync_run_id,
            units=budget_result.candidate_units,
            estimates_by_unit=budget_result.estimates_by_unit,
            already_excluded_ids=capped_ids,
            jitter_seconds=budget_result.jitter_seconds,
        )
        capped_ids = frozenset((*capped_ids, *reconfirm_result.excluded_unit_ids))
        next_deferred_at = budget_result.next_deferred_at
        if reconfirm_result.next_deferred_at is not None and (
            next_deferred_at is None
            or reconfirm_result.next_deferred_at < next_deferred_at
        ):
            next_deferred_at = reconfirm_result.next_deferred_at

        # The durable route row, not a process-local capability flag, owns the
        # transport decision. This FOR SHARE lock remains held through the
        # unit claim and outbox commit, serializing an operator FOR UPDATE
        # transition with every producer, including a non-canary batch during
        # a paused or drifted control-plane state.
        provider_unit_route = resolve_worker_job_route(session, "sync.provider_unit")
        provider_unit_routes = (
            ProviderUnitRouteSwitches.from_environment()
            if provider_unit_route == RIVER_CANARY_ROUTE
            else None
        )
        units = _claim_units(session, run_uuid, capped_ids=capped_ids)
        signatures = []
        for unit in units:
            in_canary_scope = ProviderUnitRouteSwitches.is_canary_scope(
                str(unit.provider), str(unit.dataset_key)
            )
            if in_canary_scope and provider_unit_route == RIVER_CANARY_ROUTE:
                if (
                    provider_unit_routes is None
                    or not provider_unit_routes.routes_to_river(
                        str(unit.provider), str(unit.dataset_key)
                    )
                ):
                    # A checked-in canary route without its complete runtime
                    # capability is an ownership fault, never a reason to
                    # silently publish legacy Celery work.
                    raise WorkerJobRouteError(
                        "sync provider canary capability is unavailable"
                    )
                enqueue_worker_job(
                    session,
                    ProviderUnitPayload(unit_id=str(unit.id)),
                    correlation_id=f"sync-run:{run.id}",
                    idempotency_key=f"sync.provider_unit:{unit.id}",
                    domain_id=str(unit.id),
                    organization_id=str(unit.org_id),
                )
                river_queued += 1
                continue
            dispatch_route = route(
                org_id=str(unit.org_id),
                provider=str(unit.provider),
                cost_class=str(unit.cost_class),
                cost_class_queues_enabled=_cost_class_queues_enabled(),
            )
            signatures.append(
                getattr(run_sync_unit, "s")(str(unit.id)).set(
                    queue=dispatch_route.queue
                )
            )

        if signatures or river_queued:
            now = datetime.now(timezone.utc)
            run.status = SyncRunStatus.DISPATCHING.value
            run.started_at = run.started_at or now
            session.flush()
        else:
            session.flush()

    if signatures:
        logger.info(
            "dispatch_sync_run.dispatched",
            extra={
                "sync_run_id": sync_run_id,
                "queued_units": len(signatures) + river_queued,
                "celery_units": len(signatures),
                "river_units": river_queued,
            },
        )
        try:
            # Unit terminal writes materialize the durable finalize wakeup.
            # Do not use a Celery chord/result backend as a coordinator: a
            # retry or partial publish remains recoverable through the same
            # SyncDispatchOutbox materializer that covers worker loss.
            for signature in signatures:
                getattr(signature, "apply_async")()
            if next_deferred_at is not None:
                _schedule_redispatch(sync_run_id, available_at=next_deferred_at)
            elif capped_ids:
                _schedule_redispatch(sync_run_id)
        except Exception as exc:
            logger.exception(
                "dispatch_sync_run.publish_failed",
                extra={"sync_run_id": sync_run_id, "error": str(exc)},
            )
            raise
        return {
            "status": "dispatched",
            "queued_units": len(signatures) + river_queued,
        }

    if river_queued:
        if next_deferred_at is not None:
            _schedule_redispatch(sync_run_id, available_at=next_deferred_at)
        elif capped_ids:
            _schedule_redispatch(sync_run_id)
        logger.info(
            "dispatch_sync_run.dispatched",
            extra={
                "sync_run_id": sync_run_id,
                "queued_units": river_queued,
                "celery_units": 0,
                "river_units": river_queued,
            },
        )
        return {"status": "dispatched", "queued_units": river_queued}

    # Fix 2: no units were claimable this pass.  Distinguish two cases:
    #   a) Deferred work remains (PLANNED units exist, not all terminal) →
    #      schedule a countdown redispatch so they drain when slots free up.
    #   b) No deferred work (zero-unit run, or every unit already terminal) →
    #      call finalize directly; redispatching would loop forever.
    with get_postgres_session_sync() as session:
        run_uuid_check = uuid.UUID(str(sync_run_id))
        pending_counts = _pending_unit_counts(session, run_uuid_check)
    next_deferred_at = pending_counts["next_deferred_at"]
    if pending_counts["dispatchable"] > 0:
        try:
            _schedule_redispatch(sync_run_id)
        except Exception as exc:
            logger.exception(
                "dispatch_sync_run.redispatch_publish_failed",
                extra={"sync_run_id": sync_run_id, "error": str(exc)},
            )
            raise
        logger.info(
            "dispatch_sync_run.noop",
            extra={
                "sync_run_id": sync_run_id,
                "queued_units": 0,
                "pending_units": pending_counts["dispatchable"],
            },
        )
        return {"status": "noop", "queued_units": 0}
    if pending_counts["in_flight"] > 0:
        logger.info(
            "dispatch_sync_run.waiting_inflight",
            extra={
                "sync_run_id": sync_run_id,
                "queued_units": 0,
                "in_flight_units": pending_counts["in_flight"],
            },
        )
        return {
            "status": "waiting_inflight",
            "queued_units": 0,
            "in_flight_units": pending_counts["in_flight"],
        }
    if next_deferred_at is not None:
        try:
            _schedule_redispatch(sync_run_id, available_at=next_deferred_at)
        except Exception as exc:
            logger.exception(
                "dispatch_sync_run.deferred_redispatch_publish_failed",
                extra={"sync_run_id": sync_run_id, "error": str(exc)},
            )
            raise
        logger.info(
            "dispatch_sync_run.deferred",
            extra={
                "sync_run_id": sync_run_id,
                "queued_units": 0,
                "next_deferred_at": next_deferred_at.isoformat(),
            },
        )
        return {
            "status": "deferred",
            "queued_units": 0,
            "next_deferred_at": next_deferred_at.isoformat(),
        }
    # No pending work — finalize (idempotent; handles zero-unit and already-finalized).
    logger.info(
        "dispatch_sync_run.noop_finalize",
        extra={"sync_run_id": sync_run_id, "queued_units": 0},
    )
    finalize_sync_run(sync_run_id)
    return {"status": "noop", "queued_units": 0}


@celery_app.task(
    bind=True,
    max_retries=0,
    queue="sync",
    name="dev_health_ops.workers.tasks.run_sync_unit",
)
def run_sync_unit(self, unit_id: str) -> dict[str, Any]:
    """Execute exactly one (source, dataset, window) unit.

    Loads context via SyncTaskBootstrap, runs the provider dataset adapter
    (CHAOS-2513), persists status/attempts/duration/result, and updates the
    watermark only when mode=="incremental" and the unit succeeded. Never
    dispatches metrics. Implemented in CHAOS-2512.
    """

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.processors.dataset_adapters import run_dataset_unit

    sync_run_id: str | None = None
    should_finalize = False
    started_at = datetime.now(timezone.utc)
    lease_owner: str | None = None
    deadline: datetime = started_at + timedelta(seconds=_max_unit_lifetime_seconds())
    terminal_txn_started = False
    heartbeat_stop: threading.Event | None = None
    heartbeat_thread: threading.Thread | None = None
    # Unit context fields for structured logging — populated once ctx is loaded.
    _log_ctx: dict[str, Any] = {"unit_id": unit_id}
    unit: SyncRunUnit | None = None
    ctx: SyncTaskContext | None = None
    budget_audit: list[dict[str, Any]] | None = None
    try:
        with get_postgres_session_sync() as session:
            unit = _load_unit(session, unit_id)
            sync_run_id = str(unit.sync_run_id)
            run = (
                session.query(SyncRun)
                .filter(SyncRun.id == unit.sync_run_id)
                .one_or_none()
            )
            if unit.status in {
                SyncRunUnitStatus.SUCCESS.value,
                SyncRunUnitStatus.FAILED.value,
            } or (run is not None and run.status in _TERMINAL_RUN_STATUSES):
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "terminal",
                }
            lease_owner = str(uuid.uuid4())
            deadline = started_at + timedelta(seconds=_max_unit_lifetime_seconds())
            lease_expires_at = min(
                started_at + timedelta(seconds=_running_lease_seconds()), deadline
            )
            claim_result: Any = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == unit.id,
                    SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
                    SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                )
                .values(
                    status=SyncRunUnitStatus.RUNNING.value,
                    attempts=SyncRunUnit.attempts + 1,
                    error=None,
                    lease_owner=lease_owner,
                    lease_expires_at=lease_expires_at,
                    last_heartbeat_at=started_at,
                    updated_at=started_at,
                )
                .execution_options(synchronize_session=False)
            )
            if int(claim_result.rowcount or 0) == 0:
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "not_dispatchable",
                }
            session.flush()

        heartbeat_stop, heartbeat_thread = _start_unit_heartbeat(
            unit_id, lease_owner, deadline
        )

        with get_postgres_session_sync() as session:
            ctx = SyncTaskBootstrap.load(session, unit_id)
            sync_run_id = ctx.sync_run_id
            unit = _load_unit(session, unit_id)
            _log_ctx = {
                "sync_run_id": ctx.sync_run_id,
                "unit_id": unit_id,
                "source_id": str(unit.source_id),
                "dataset_key": ctx.dataset_key,
                "provider": ctx.provider,
                "cost_class": ctx.cost_class,
            }
            now = datetime.now(timezone.utc)
            live_lease_refresh: Any = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == uuid.UUID(str(unit_id)),
                    SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnit.lease_owner == lease_owner,
                    SyncRunUnit.lease_expires_at.is_not(None),
                    SyncRunUnit.lease_expires_at > now,
                    SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                )
                .values(
                    lease_expires_at=min(
                        now + timedelta(seconds=_running_lease_seconds()), deadline
                    ),
                    last_heartbeat_at=now,
                )
                .execution_options(synchronize_session=False)
            )
            if int(live_lease_refresh.rowcount or 0) == 0:
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                }
        budget_audit = _budget_estimate_audit(ctx, _log_ctx)
        budget_audit_computed_at = datetime.now(timezone.utc)
        if sync_dataset_requires_canonical_incident_feature(
            str(ctx.provider),
            str(ctx.dataset_key),
        ):
            with get_postgres_session_sync() as session:
                require_canonical_incident_feature_for_update_sync(session, ctx.org_id)
        started_extra = dict(_log_ctx)
        if budget_audit is not None:
            started_extra["budget_estimate"] = budget_audit
        logger.info("run_sync_unit.started", extra=started_extra)

        runtime = _runtime_cache.get(ctx)
        if not _sync_unit_lease_is_owned_and_live(unit_id, lease_owner):
            logger.info(
                "run_sync_unit.lease_lost_before_dataset",
                extra={**_log_ctx},
            )
            return {
                "status": "skipped",
                "unit_id": unit_id,
                "reason": "lease_lost",
            }
        if sync_dataset_requires_canonical_incident_feature(
            str(ctx.provider),
            str(ctx.dataset_key),
        ):
            with get_postgres_session_sync() as session:
                require_canonical_incident_feature_for_update_sync(session, ctx.org_id)
        from dev_health_ops.metrics.job_work_items import (
            WorkItemsSyncLeaseLost,
            work_items_sync_lease_check,
        )

        try:
            with work_items_sync_lease_check(
                lambda _surface: _sync_unit_lease_is_owned_and_live(
                    unit_id, lease_owner
                )
            ):
                result = run_dataset_unit(ctx, runtime)
        except WorkItemsSyncLeaseLost as exc:
            logger.warning(
                "run_sync_unit.lease_lost_before_sink_write",
                extra={**_log_ctx, "surface": exc.surface},
            )
            return {
                "status": "skipped",
                "unit_id": unit_id,
                "reason": "lease_lost",
                "surface": exc.surface,
            }
        result_payload = _promote_result_observation_fields(
            _attach_budget_comparison(
                _attach_budget_observation(dict(result or {}), budget_audit),
                budget_audit,
                log_ctx=_log_ctx,
                computed_at=budget_audit_computed_at,
            )
        )
        watermark_at: datetime | None = None
        if ctx.mode in {
            SyncRunMode.INCREMENTAL.value,
            SyncRunMode.FULL_RESYNC.value,
        }:
            raw_watermark = result_payload.get("watermark_at")
            if raw_watermark is None:
                watermark_at = ctx.window_end
            elif isinstance(raw_watermark, str):
                watermark_at = datetime.fromisoformat(raw_watermark)
                if watermark_at.tzinfo is None:
                    watermark_at = watermark_at.replace(tzinfo=timezone.utc)
            else:
                raise ValueError(
                    "sync unit returned a non-string watermark_at: "
                    f"unit_id={unit_id} watermark_at={raw_watermark!r}"
                )
            if watermark_at is None:
                raise ValueError(
                    "sync unit cannot stamp watermark without before_at: "
                    f"unit_id={unit_id} mode={ctx.mode}"
                )

        completed_at = datetime.now(timezone.utc)
        duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
        terminal_txn_started = True
        with get_postgres_session_sync() as session:
            terminal_result: Any = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == uuid.UUID(str(unit_id)),
                    SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnit.lease_owner == lease_owner,
                    SyncRunUnit.lease_expires_at.is_not(None),
                    SyncRunUnit.lease_expires_at > completed_at,
                    SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                )
                .values(
                    status=SyncRunUnitStatus.SUCCESS.value,
                    duration_seconds=duration_seconds,
                    result=result_payload,
                    error=None,
                    # Review finding (round 3): SUCCESS ends any rate-limit
                    # episode this unit was in -- clear the shared deferral
                    # bookkeeping so a stale first_seen_at from an EARLIER,
                    # resolved episode can never be misread as still-ongoing
                    # by the cooldown gate's wall-clock-exhaustion check
                    # (sync/budget_guard.py _rate_limit_deferral_exhausted).
                    rate_limit_deferrals=0,
                    rate_limit_first_seen_at=None,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_heartbeat_at=completed_at,
                    updated_at=completed_at,
                )
                .execution_options(synchronize_session=False)
            )
            if int(terminal_result.rowcount or 0) == 0:
                logger.warning(
                    "run_sync_unit.success_stamp_noop",
                    extra={**_log_ctx},
                )
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                }
            upsert_outbox_wakeup(
                session,
                sync_run_id=ctx.sync_run_id,
                kind=OUTBOX_KIND_FINALIZE,
                available_at=completed_at,
                now=completed_at,
            )
            if watermark_at is not None:
                for watermark_dataset_key in _watermark_dataset_keys(ctx):
                    set_watermark(
                        session,
                        ctx.org_id,
                        ctx.source_external_id,
                        watermark_dataset_key,
                        watermark_at,
                    )
            session.flush()
            should_finalize = True
        logger.info(
            "run_sync_unit.success",
            extra={**_log_ctx, "duration_seconds": duration_seconds},
        )
        return {
            "status": "success",
            "unit_id": unit_id,
            "duration_seconds": duration_seconds,
        }
    except RateLimitException as exc:
        if terminal_txn_started:
            raise
        if unit is None:
            terminal_txn_started = True
            failure_result, should_finalize = _stamp_sync_unit_failed(
                unit_id=unit_id,
                sync_run_id=sync_run_id,
                lease_owner=lease_owner,
                started_at=started_at,
                exc=exc,
                log_ctx=_log_ctx,
            )
            return failure_result
        if ctx is None:
            terminal_txn_started = True
            failure_result, should_finalize = _stamp_sync_unit_failed(
                unit_id=unit_id,
                sync_run_id=sync_run_id,
                lease_owner=lease_owner,
                started_at=started_at,
                exc=exc,
                log_ctx=_log_ctx,
            )
            return failure_result
        deferral = plan_rate_limit_deferral(
            retry_after_seconds=getattr(exc, "retry_after_seconds", None),
            attempts=unit.rate_limit_deferrals,
            first_seen_at=unit.rate_limit_first_seen_at.isoformat()
            if unit.rate_limit_first_seen_at
            else None,
        )
        if deferral is None:
            terminal_txn_started = True
            failure_result, should_finalize = _stamp_sync_unit_failed(
                unit_id=unit_id,
                sync_run_id=sync_run_id,
                lease_owner=lease_owner,
                started_at=started_at,
                exc=exc,
                log_ctx=_log_ctx,
            )
            return failure_result

        now = datetime.now(timezone.utc)
        not_before = datetime.fromisoformat(deferral.not_before)
        first_seen_at = datetime.fromisoformat(deferral.first_seen_at)
        deferral_result_payload: dict[str, Any] = {
            "error_category": "rate_limit",
            "retry_after_seconds": getattr(exc, "retry_after_seconds", None),
            "not_before": deferral.not_before,
            "rate_limit_deferrals": deferral.attempts,
        }
        # Persist any actuals gathered before the rate-limit raise; never
        # overwrites error_category / retry fields (CHAOS-2754).
        _merge_partial_observations_into_result(deferral_result_payload, exc)
        terminal_txn_started = True
        with get_postgres_session_sync() as session:
            deferred_result: Any = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == uuid.UUID(str(unit_id)),
                    SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnit.lease_owner == lease_owner,
                    SyncRunUnit.lease_expires_at.is_not(None),
                    SyncRunUnit.lease_expires_at > now,
                    SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                )
                .values(
                    status=SyncRunUnitStatus.RETRYING.value,
                    available_at=not_before,
                    rate_limit_deferrals=deferral.attempts,
                    rate_limit_first_seen_at=first_seen_at,
                    error=sanitize_error_text(exc),
                    result=deferral_result_payload,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_heartbeat_at=now,
                    updated_at=now,
                )
                .execution_options(synchronize_session=False)
            )
            if int(deferred_result.rowcount or 0) == 0:
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                }
            # Durable observation store (CHAOS-2758): attempted only after the
            # CAS above confirms the RETRYING stamp actually landed, so it
            # never runs for a deferral that didn't happen. It is
            # deliberately isolated in its own SAVEPOINT rather than sharing
            # the outer transaction outright: the observation store is
            # diagnostic, not load-bearing, and a DB-level failure writing it
            # (e.g. migration 0031 not yet applied during a rolling deploy,
            # or schema drift) must never roll back the RETRYING stamp / turn
            # a recoverable rate-limit deferral into a lost unit. On success
            # the two still commit together (nested.commit() only stages the
            # savepoint into the still-open outer transaction); on failure
            # only the observation attempt rolls back and is logged.
            observation_nested = session.begin_nested()
            try:
                session.add(
                    _build_rate_limit_observation(
                        unit=unit,
                        provider=ctx.provider,
                        exc=exc,
                        budget_audit=budget_audit,
                        observed_at=now,
                    )
                )
                session.flush()
            except SQLAlchemyError as observation_exc:
                observation_nested.rollback()
                logger.warning(
                    "run_sync_unit.rate_limit_observation_persist_failed",
                    extra={**_log_ctx, "error": str(observation_exc)},
                )
            else:
                observation_nested.commit()
            if sync_run_id is not None:
                # Earlier-wins upsert (CHAOS-2647): we deliberately do NOT
                # force-set available_at=not_before here. A revived past dispatch
                # wakeup may be consumed as a no-op while all remaining units are
                # future RETRYING; the reconciler's periodic _dispatchable_run_ids
                # scan re-materializes dispatch once available_at <= now (bounded
                # delay, never stuck). Forcing not_before is unsafe: it would
                # overwrite the earlier countdown _schedule_redispatch arms for
                # capped PLANNED siblings, delaying their dispatch. The precision
                # loss is negligible versus provider rate-limit backoff windows.
                upsert_outbox_wakeup(
                    session,
                    sync_run_id=sync_run_id,
                    kind=OUTBOX_KIND_DISPATCH,
                    available_at=not_before,
                    now=now,
                )
            session.flush()
        logger.info(
            "run_sync_unit.rate_limited_deferred",
            extra={
                **_log_ctx,
                "not_before": deferral.not_before,
                "rate_limit_deferrals": deferral.attempts,
            },
        )
        return {
            "status": "rate_limited_deferred",
            "unit_id": unit_id,
            "not_before": deferral.not_before,
            "rate_limit_deferrals": deferral.attempts,
        }
    except SoftTimeLimitExceeded as exc:
        if terminal_txn_started:
            raise
        timeout_result, should_finalize = _stamp_sync_unit_soft_timeout(
            unit_id=unit_id,
            lease_owner=lease_owner,
            started_at=started_at,
            exc=exc,
            log_ctx=_log_ctx,
        )
        return timeout_result
    except Exception as exc:
        if terminal_txn_started:
            raise
        terminal_txn_started = True
        failure_result, should_finalize = _stamp_sync_unit_failed(
            unit_id=unit_id,
            sync_run_id=sync_run_id,
            lease_owner=lease_owner,
            started_at=started_at,
            exc=exc,
            log_ctx=_log_ctx,
        )
        return failure_result
    finally:
        if heartbeat_stop is not None:
            heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=2)
        if should_finalize and sync_run_id is not None:
            try:
                getattr(finalize_sync_run, "apply_async")(
                    args=(sync_run_id,), queue="sync"
                )
            except Exception:
                logger.exception(
                    "run_sync_unit.finalize_enqueue_failed",
                    extra={"sync_run_id": sync_run_id, "unit_id": unit_id},
                )


def _sync_unit_lease_is_owned_and_live(
    unit_id: str,
    lease_owner: str | None,
) -> bool:
    from dev_health_ops.db import get_postgres_session_sync

    if lease_owner is None:
        return False
    now = datetime.now(timezone.utc)
    with get_postgres_session_sync() as session:
        unit = _load_unit(session, unit_id)
        if unit.status != SyncRunUnitStatus.RUNNING.value:
            return False
        run_status = (
            session.query(SyncRun.status)
            .filter(SyncRun.id == unit.sync_run_id)
            .scalar()
        )
        if run_status in _TERMINAL_RUN_STATUSES:
            return False
        return _unit_lease_is_owned_and_live(unit, lease_owner, now)


def _expired_lease_max_retries() -> int:
    try:
        return max(0, int(os.getenv("SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES", "1")))
    except ValueError:
        return 1


def _expired_lease_retry_backoff_seconds() -> int:
    try:
        return max(
            0,
            int(os.getenv("SYNC_UNIT_EXPIRED_LEASE_RETRY_BACKOFF_SECONDS", "60")),
        )
    except ValueError:
        return 60


def _retry_surfaces_for_unit(unit: SyncRunUnit) -> frozenset[str]:
    if (
        str(unit.provider) == "linear"
        and str(unit.mode) == SyncRunMode.BACKFILL.value
        and str(unit.dataset_key) in _LINEAR_BACKFILL_WORK_ITEM_DATASETS
    ):
        return _LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES
    return frozenset()


def _sync_unit_expired_lease_retry_decision(unit: SyncRunUnit) -> dict[str, Any]:
    retry_count = int(unit.expired_lease_retry_count or 0)
    retry_surfaces = _retry_surfaces_for_unit(unit)
    base_eligible = (
        str(unit.provider) == "linear"
        and str(unit.mode) == SyncRunMode.BACKFILL.value
        and str(unit.dataset_key) in _LINEAR_BACKFILL_WORK_ITEM_DATASETS
        and bool(retry_surfaces)
        and retry_surfaces.issubset(_CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES)
    )
    max_retries = _expired_lease_max_retries()
    exhausted = base_eligible and retry_count >= max_retries
    return {
        "should_retry": base_eligible and not exhausted,
        "retry_exhausted": exhausted,
        "retry_count": retry_count,
        "next_retry_count": retry_count + 1,
        "retry_surfaces": tuple(sorted(retry_surfaces)),
        "max_retries": max_retries,
    }


def _retry_result_payload(
    *,
    error_category: str,
    retry_reason: str,
    decision: dict[str, Any],
    next_retry_at: datetime | None,
    last_lease_expired_at: datetime | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "error_category": error_category,
        "retry_count": decision["next_retry_count"],
        "retry_reason": retry_reason,
        "next_retry_at": next_retry_at.isoformat() if next_retry_at else None,
        "retry_exhausted": False,
        "retry_surfaces": list(decision["retry_surfaces"]),
    }
    if last_lease_expired_at is not None:
        payload["last_lease_expired_at"] = last_lease_expired_at.isoformat()
    return payload


def _failed_retry_result_payload(
    *,
    error_category: str,
    retry_reason: str,
    decision: dict[str, Any],
    last_lease_expired_at: datetime | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "error_category": error_category,
        "retry_count": decision["retry_count"],
        "retry_reason": retry_reason,
        "next_retry_at": None,
        "retry_exhausted": bool(decision["retry_exhausted"]),
        "retry_surfaces": list(decision["retry_surfaces"]),
    }
    if last_lease_expired_at is not None:
        payload["last_lease_expired_at"] = last_lease_expired_at.isoformat()
    return payload


def _stamp_sync_unit_soft_timeout(
    *,
    unit_id: str,
    lease_owner: str | None,
    started_at: datetime,
    exc: SoftTimeLimitExceeded,
    log_ctx: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    from dev_health_ops.db import get_postgres_session_sync

    completed_at = datetime.now(timezone.utc)
    duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
    with get_postgres_session_sync() as session:
        unit = _load_unit(session, unit_id)
        decision = _sync_unit_expired_lease_retry_decision(unit)
        if decision["should_retry"]:
            available_at = completed_at + timedelta(
                seconds=_expired_lease_retry_backoff_seconds()
            )
            result_payload = _retry_result_payload(
                error_category="soft_timeout",
                retry_reason="soft_timeout",
                decision=decision,
                next_retry_at=available_at,
                last_lease_expired_at=None,
            )
            retry_result: Any = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == uuid.UUID(str(unit_id)),
                    SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnit.lease_owner == lease_owner,
                    SyncRunUnit.lease_owner.is_not(None),
                    SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                )
                .values(
                    status=SyncRunUnitStatus.RETRYING.value,
                    available_at=available_at,
                    duration_seconds=duration_seconds,
                    error=sanitize_error_text(exc),
                    result=result_payload,
                    expired_lease_retry_count=(
                        SyncRunUnit.expired_lease_retry_count + 1
                    ),
                    last_retry_reason="soft_timeout",
                    retry_exhausted_at=None,
                    # Review finding (round 3): a soft-timeout retry is NOT a
                    # rate-limit episode -- clear any stale
                    # rate_limit_deferrals/first_seen_at carried over from an
                    # earlier, resolved rate-limit episode (same reasoning as
                    # the budget-guard deferral clear).
                    rate_limit_deferrals=0,
                    rate_limit_first_seen_at=None,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_heartbeat_at=completed_at,
                    updated_at=completed_at,
                )
                .execution_options(synchronize_session=False)
            )
            if int(retry_result.rowcount or 0) == 0:
                return (
                    {
                        "status": "skipped",
                        "unit_id": unit_id,
                        "reason": "lease_lost",
                    },
                    False,
                )
            session.flush()
            logger.warning(
                "run_sync_unit.soft_timeout_deferred",
                extra={
                    **log_ctx,
                    "duration_seconds": duration_seconds,
                    "retry_count": decision["next_retry_count"],
                    "next_retry_at": available_at.isoformat(),
                },
            )
            return (
                {
                    "status": "soft_timeout_deferred",
                    "unit_id": unit_id,
                    "error_category": "soft_timeout",
                    "retry_count": decision["next_retry_count"],
                    "next_retry_at": available_at.isoformat(),
                },
                False,
            )

        failed_payload = _failed_retry_result_payload(
            error_category="soft_timeout",
            retry_reason="soft_timeout",
            decision=decision,
            last_lease_expired_at=None,
        )
        failed_result: Any = session.execute(
            update(SyncRunUnit)
            .where(
                SyncRunUnit.id == uuid.UUID(str(unit_id)),
                SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                SyncRunUnit.lease_owner == lease_owner,
                SyncRunUnit.lease_owner.is_not(None),
                SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
            )
            .values(
                status=SyncRunUnitStatus.FAILED.value,
                available_at=None,
                duration_seconds=duration_seconds,
                error=sanitize_error_text(exc),
                result=failed_payload,
                last_retry_reason="soft_timeout",
                retry_exhausted_at=completed_at
                if failed_payload["retry_exhausted"]
                else None,
                lease_owner=None,
                lease_expires_at=None,
                last_heartbeat_at=completed_at,
                updated_at=completed_at,
            )
            .execution_options(synchronize_session=False)
        )
        if int(failed_result.rowcount or 0) == 0:
            return (
                {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                },
                False,
            )
        session.flush()
    logger.warning(
        "run_sync_unit.soft_timeout_failed",
        extra={
            **log_ctx,
            "duration_seconds": duration_seconds,
            "error_category": "soft_timeout",
            "retry_exhausted": failed_payload["retry_exhausted"],
        },
    )
    return (
        {
            "status": "failed",
            "unit_id": unit_id,
            "error": sanitize_error_text(exc),
            "error_category": "soft_timeout",
            "retry_exhausted": failed_payload["retry_exhausted"],
        },
        False,
    )


def _stamp_sync_unit_failed(
    *,
    unit_id: str,
    sync_run_id: str | None,
    lease_owner: str | None,
    started_at: datetime,
    exc: BaseException,
    log_ctx: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    from dev_health_ops.db import get_postgres_session_sync

    completed_at = datetime.now(timezone.utc)
    duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
    error_category = _classify_error(exc)
    failed_result_payload: dict[str, Any] = {"error_category": error_category}
    # Persist any actuals gathered before the raise; error_category stays intact
    # (admin-API contract) (CHAOS-2754).
    _merge_partial_observations_into_result(failed_result_payload, exc)
    with get_postgres_session_sync() as session:
        terminal_result: Any = session.execute(
            update(SyncRunUnit)
            .where(
                SyncRunUnit.id == uuid.UUID(str(unit_id)),
                SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                SyncRunUnit.lease_owner == lease_owner,
                SyncRunUnit.lease_expires_at.is_not(None),
                SyncRunUnit.lease_expires_at > completed_at,
                SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
            )
            .values(
                status=SyncRunUnitStatus.FAILED.value,
                available_at=None,
                duration_seconds=duration_seconds,
                error=sanitize_error_text(exc),
                result=failed_result_payload,
                lease_owner=None,
                lease_expires_at=None,
                last_heartbeat_at=completed_at,
                updated_at=completed_at,
            )
            .execution_options(synchronize_session=False)
        )
        if int(terminal_result.rowcount or 0) == 0:
            return (
                {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                },
                False,
            )
        if sync_run_id is not None:
            upsert_outbox_wakeup(
                session,
                sync_run_id=sync_run_id,
                kind=OUTBOX_KIND_FINALIZE,
                available_at=completed_at,
                now=completed_at,
            )
        session.flush()
    if error_category == FEATURE_DISABLED_ERROR_CATEGORY:
        logger.warning(
            "run_sync_unit.feature_disabled",
            extra={
                **log_ctx,
                "duration_seconds": duration_seconds,
                "error_category": error_category,
            },
        )
    else:
        logger.exception(
            "run_sync_unit.failed",
            extra={
                **log_ctx,
                "duration_seconds": duration_seconds,
                "error_category": error_category,
            },
        )
    return (
        {
            "status": "failed",
            "unit_id": unit_id,
            "error": sanitize_error_text(exc),
            "error_category": error_category,
        },
        True,
    )


@celery_app.task(queue="sync", name="dev_health_ops.workers.tasks.finalize_sync_run")
def finalize_sync_run(sync_run_id: str) -> dict[str, Any]:
    """Aggregate unit statuses and materialize post-sync metrics once per run.

    No-op until all units are terminal; once-only via the SyncRunPostDispatch
    ledger. The reconciler relay is the sole post-sync publisher. post_sync is
    at-least-once: a publish failure releases its guarded outbox claim for a
    bounded re-drive. Downstream readers select the newest compute generation
    per logical key, so duplicate deliveries cannot inflate supported metrics.
    """

    from dev_health_ops.db import get_postgres_session_sync

    with get_postgres_session_sync() as session:
        run_uuid = uuid.UUID(str(sync_run_id))
        run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
        if run is None:
            logger.warning(
                "finalize_sync_run.missing",
                extra={"sync_run_id": sync_run_id},
            )
            return {"status": "missing", "sync_run_id": sync_run_id}

        units = (
            session.query(SyncRunUnit)
            .filter(SyncRunUnit.sync_run_id == run_uuid)
            .order_by(SyncRunUnit.id)
            .all()
        )
        terminal_statuses = {
            SyncRunUnitStatus.SUCCESS.value,
            SyncRunUnitStatus.FAILED.value,
        }
        if any(unit.status not in terminal_statuses for unit in units):
            logger.debug(
                "finalize_sync_run.pending",
                extra={"sync_run_id": sync_run_id, "total_units": len(units)},
            )
            return {"status": "pending", "sync_run_id": sync_run_id}

        success_count = sum(
            1 for unit in units if unit.status == SyncRunUnitStatus.SUCCESS.value
        )
        failed_count = sum(
            1 for unit in units if unit.status == SyncRunUnitStatus.FAILED.value
        )
        error_category = next(
            (
                unit.result.get("error_category")
                for unit in units
                if unit.status == SyncRunUnitStatus.FAILED.value
                and isinstance(unit.result, dict)
                and unit.result.get("error_category")
            ),
            None,
        )
        total_count = len(units)
        completed_at = datetime.now(timezone.utc)
        run.completed_units = success_count
        run.failed_units = failed_count
        run.completed_at = run.completed_at or completed_at
        run.status = _aggregate_run_status(total_count, success_count, failed_count)
        result_payload: dict[str, Any] = {
            "completed_units": success_count,
            "failed_units": failed_count,
        }
        if error_category is not None:
            result_payload["error_category"] = error_category
        if error_category == FEATURE_DISABLED_ERROR_CATEGORY and run.error is None:
            run.error = next(
                (
                    unit.error
                    for unit in units
                    if unit.status == SyncRunUnitStatus.FAILED.value and unit.error
                ),
                None,
            )
        if total_count == 0:
            run.error = "No sync units planned"
            result_payload["reason"] = "no_sync_units_planned"
        run.result = result_payload
        run_success = run.status == SyncRunStatus.SUCCESS.value
        # sanitize_error_text is applied here too, not just at the original
        # write site: run.error is copied straight into
        # SyncConfiguration.last_sync_error below (another variable-to-column
        # assignment an str(exc)-focused AST guard can't see), and a row
        # written before this column was brought under sanitize_error_text's
        # discipline could still carry raw credential text (CHAOS-2766 codex
        # review finding, round 2 -- same class as
        # sync_observers_for_terminal_sync_run below). Idempotent/harmless on
        # already-sanitized text.
        run_error = (
            None
            if run_success
            else sanitize_error_text(
                run.error or "Sync run completed with failed units"
            )
        )
        stamp_sync_run_canonical_config(
            session,
            run,
            completed_at=run.completed_at,
            success=run_success,
            error=run_error,
            stats=result_payload,
        )
        sync_observers_for_terminal_sync_run(session, run)
        try:
            _checkpoint_successful_compute_inputs(
                session, units, checkpointed_at=completed_at
            )
        except SQLAlchemyError as exc:
            logger.warning(
                "finalize_sync_run.compute_checkpoint_failed",
                extra={"sync_run_id": sync_run_id, "error": str(exc)},
            )
        session.flush()

        nested = session.begin_nested()
        try:
            session.add(
                SyncRunPostDispatch(
                    org_id=str(run.org_id),
                    sync_run_id=run_uuid,
                    kind=OUTBOX_KIND_POST_SYNC,
                    dispatched_at=completed_at,
                )
            )
            session.flush()
        except IntegrityError:
            nested.rollback()
            return {"status": "already_dispatched", "sync_run_id": sync_run_id}
        else:
            upsert_outbox_wakeup(
                session,
                sync_run_id=run_uuid,
                kind=OUTBOX_KIND_POST_SYNC,
                available_at=completed_at,
                now=completed_at,
            )
            nested.commit()
        post_sync_payload = build_post_sync_dispatch_payload(session, run_uuid)
        post_sync_targets = (
            post_sync_payload.sync_targets if post_sync_payload is not None else []
        )
        session.flush()

    run_status = _aggregate_run_status(len(units), success_count, failed_count)
    logger.info(
        "finalize_sync_run.finalized",
        extra={
            "sync_run_id": sync_run_id,
            "completed_units": success_count,
            "failed_units": failed_count,
            "run_status": run_status,
        },
    )

    return {
        "status": "finalized",
        "sync_run_id": sync_run_id,
        "completed_units": success_count,
        "failed_units": failed_count,
        "post_sync_targets": post_sync_targets,
    }


def _watermark_dataset_keys(ctx: SyncTaskContext) -> list[str]:
    """Dataset keys whose watermark this unit advances on success.

    CHAOS-2721: a collapsed work-item-family unit (canonical dataset_key
    "work-items") carries the enabled family datasets as boolean
    ``family_dataset_<key>`` flags; advance each enabled dataset's watermark
    independently so per-dataset incremental identity is preserved. A plain unit
    advances only its own dataset_key.
    """
    from dev_health_ops.sync.planner import family_dataset_keys_from_flags

    family_keys = family_dataset_keys_from_flags(ctx.processor_flags)
    return family_keys or [ctx.dataset_key]


def _family_dataset_audit_metadata(unit: SyncRunUnit) -> dict[str, list[str]]:
    """CHAOS-2721: a collapsed work-item-family unit records only its canonical
    "work-items" dataset_key, which would hide that labels/projects/history/
    comments also ran. Surface the enabled family datasets in the compute
    checkpoint metadata so admin/API/debug views keep per-dataset provenance.
    """
    from dev_health_ops.sync.planner import family_dataset_keys_from_flags

    family_keys = family_dataset_keys_from_flags(unit.processor_flags)
    if not family_keys:
        return {}
    return {"family_datasets": family_keys}


def _checkpoint_successful_compute_inputs(
    session,
    units: list[SyncRunUnit],
    *,
    checkpointed_at: datetime,
) -> None:
    from dev_health_ops.sync.planner import map_datasets_to_legacy_targets

    work_graph_targets = _GIT_TARGETS | _WORK_ITEM_TARGETS
    for unit in units:
        if unit.status != SyncRunUnitStatus.SUCCESS.value:
            continue
        legacy_targets = map_datasets_to_legacy_targets(
            str(unit.provider), [str(unit.dataset_key)]
        )
        if not legacy_targets.intersection(work_graph_targets):
            continue
        checkpoint = SyncComputeCheckpoint(
            org_id=str(unit.org_id),
            sync_run_id=unit.sync_run_id,
            sync_run_unit_id=unit.id,
            source_id=unit.source_id,
            provider=str(unit.provider),
            dataset_key=str(unit.dataset_key),
            compute_type=SyncComputeType.WORK_GRAPH.value,
            status=SyncComputeCheckpointStatus.READY.value,
            window_start=unit.since_at,
            window_end=unit.before_at,
            checkpointed_at=checkpointed_at,
            checkpoint_metadata={
                "cost_class": str(unit.cost_class),
                "mode": str(unit.mode),
                "legacy_targets": sorted(legacy_targets),
                **_family_dataset_audit_metadata(unit),
            },
        )
        nested = session.begin_nested()
        try:
            session.add(checkpoint)
            session.flush()
        except IntegrityError:
            nested.rollback()
        except SQLAlchemyError as exc:
            nested.rollback()
            logger.warning(
                "finalize_sync_run.compute_checkpoint_unit_failed",
                extra={
                    "sync_run_id": str(unit.sync_run_id),
                    "unit_id": str(unit.id),
                    "compute_type": SyncComputeType.WORK_GRAPH.value,
                    "error": str(exc),
                },
            )
        else:
            nested.commit()


def _run_has_dispatching_or_running_units(session, run_uuid: uuid.UUID) -> bool:
    return (
        session.query(SyncRunUnit.id)
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status.in_(
                {
                    SyncRunUnitStatus.DISPATCHING.value,
                    SyncRunUnitStatus.RUNNING.value,
                }
            ),
        )
        .first()
        is not None
    )


def _pending_unit_counts(session, run_uuid: uuid.UUID) -> _PendingUnitCounts:
    now = datetime.now(timezone.utc)
    stale_dispatch_cutoff = now - timedelta(seconds=_stale_dispatch_seconds())
    units = (
        session.query(
            SyncRunUnit.status, SyncRunUnit.updated_at, SyncRunUnit.available_at
        )
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status.in_(
                {
                    SyncRunUnitStatus.PLANNED.value,
                    SyncRunUnitStatus.DISPATCHING.value,
                    SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnitStatus.RETRYING.value,
                }
            ),
        )
        .all()
    )
    dispatchable = 0
    in_flight = 0
    next_deferred_at: datetime | None = None
    for status, updated_at, available_at in units:
        if status == SyncRunUnitStatus.PLANNED.value:
            dispatchable += 1
        elif status == SyncRunUnitStatus.DISPATCHING.value:
            if (
                updated_at is not None
                and _as_aware(updated_at) <= stale_dispatch_cutoff
            ):
                dispatchable += 1
            else:
                in_flight += 1
        elif status == SyncRunUnitStatus.RUNNING.value:
            in_flight += 1
        elif available_at is not None:
            deferred_at = _as_aware(available_at)
            if deferred_at <= now:
                dispatchable += 1
            elif next_deferred_at is None or deferred_at < next_deferred_at:
                next_deferred_at = deferred_at
    return {
        "dispatchable": dispatchable,
        "in_flight": in_flight,
        "next_deferred_at": next_deferred_at,
    }


def sync_observers_for_terminal_sync_run(session, run: SyncRun) -> None:
    if run.status not in _TERMINAL_RUN_STATUSES:
        return
    completed_at = run.completed_at or datetime.now(timezone.utc)
    run.completed_at = completed_at
    success = run.status == SyncRunStatus.SUCCESS.value
    job_run_status = (
        JobRunStatus.SUCCESS.value if success else JobRunStatus.FAILED.value
    )
    backfill_status = "completed" if success else "failed"
    # sanitize_error_text is applied here too, not just at the original
    # write site: this function copies SyncRun.error directly into
    # BackfillJob.error_message / JobRun.error (a variable-to-column
    # assignment the str(exc)-focused AST guard can't see), and a row
    # written before this column was brought under sanitize_error_text's
    # discipline could still carry raw credential text (CHAOS-2766 codex
    # review finding, round 2). Idempotent/harmless on already-sanitized
    # text.
    error = (
        None
        if success
        else sanitize_error_text(run.error or "Sync run completed with failed units")
    )
    result_patch = {
        "sync_run_status": run.status,
        "total_units": int(run.total_units or 0),
        "completed_units": int(run.completed_units or 0),
        "failed_units": int(run.failed_units or 0),
    }
    run_result = run.result if isinstance(run.result, dict) else {}
    error_category = run_result.get("error_category")
    if error_category is not None:
        result_patch["error_category"] = error_category

    marker = f"sync_run:{run.id}"
    backfill_jobs = (
        session.query(BackfillJob)
        .filter(BackfillJob.org_id == str(run.org_id))
        .filter(BackfillJob.celery_task_id.contains(marker))
        .all()
    )
    for job in backfill_jobs:
        job.status = backfill_status
        job.total_chunks = int(run.total_units or 0)
        job.completed_chunks = int(run.completed_units or 0)
        job.failed_chunks = int(run.failed_units or 0)
        job.completed_at = completed_at
        job.error_message = error

    job_runs = (
        session.query(JobRun)
        .filter(
            JobRun.status.in_({JobRunStatus.PENDING.value, JobRunStatus.RUNNING.value})
        )
        .all()
    )
    for job_run in job_runs:
        result = job_run.result if isinstance(job_run.result, dict) else {}
        if str(result.get("sync_run_id") or "") != str(run.id):
            continue
        job_run.status = job_run_status
        job_run.completed_at = completed_at
        job_run.error = error
        job_run.result = {**result, **result_patch}


def _fail_planned_units(session, run_uuid: uuid.UUID, error: str) -> int:
    """Fail every unit of the run that is not dispatched and never will be.

    Covers PLANNED and RETRYING: on a total-cap hard deny the guard re-denies
    every future redispatch, so a deferred RETRYING unit is just as stranded
    as a PLANNED one — and a lingering RETRYING unit blocks finalize_sync_run
    (it requires all units terminal) forever.
    """
    now = datetime.now(timezone.utc)
    result = (
        session.query(SyncRunUnit)
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status.in_(
                {
                    SyncRunUnitStatus.PLANNED.value,
                    SyncRunUnitStatus.RETRYING.value,
                }
            ),
        )
        .update(
            {
                SyncRunUnit.status: SyncRunUnitStatus.FAILED.value,
                SyncRunUnit.error: error,
                SyncRunUnit.updated_at: now,
            },
            synchronize_session=False,
        )
    )
    return int(result or 0)


def _fail_stale_dispatching_units(session, run_uuid: uuid.UUID, error: str) -> int:
    now = datetime.now(timezone.utc)
    stale_dispatch_cutoff = now - timedelta(seconds=_stale_dispatch_seconds())
    # Write-time CAS (NOT load-and-mutate): the ``status == 'dispatching'`` predicate
    # is evaluated by the database at UPDATE time, so a stale row that a delayed
    # ``run_sync_unit`` concurrently claimed to RUNNING (DISPATCHING->RUNNING + live
    # lease) between our read and write is EXCLUDED -- we never overwrite a live
    # worker's claim with FAILED.  ``updated_at <= cutoff`` scopes to genuinely stale
    # rows exactly as the prior load-and-mutate did, still scoped to this run.
    result = session.execute(
        update(SyncRunUnit)
        .where(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
            SyncRunUnit.updated_at <= stale_dispatch_cutoff,
        )
        .values(
            status=SyncRunUnitStatus.FAILED.value,
            error=error,
            result={"error_category": "dispatch_denied"},
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    return int(result.rowcount or 0)


def _enqueue_denied_active_finalize(sync_run_id: str) -> None:
    try:
        getattr(finalize_sync_run, "apply_async")(args=(sync_run_id,), queue="sync")
    except Exception:
        logger.exception(
            "dispatch_sync_run.denied_active_finalize_enqueue_failed",
            extra={"sync_run_id": sync_run_id},
        )
        raise


def terminalize_feature_disabled_plan(
    session: Session,
    sync_run_id: str,
    error: CanonicalIncidentFeatureDisabledError,
) -> FeatureDisabledRunTransition:
    run_uuid = uuid.UUID(str(sync_run_id))
    run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one()
    transition = terminalize_feature_disabled_run(session, run, error)
    if not transition.run_terminal:
        raise RuntimeError(
            f"feature-disabled planned run retained nonterminal units: {sync_run_id}"
        )

    _terminalize_feature_disabled_graph(session, run, error)
    return transition


def _terminalize_feature_disabled_graph(
    session: Session,
    run: SyncRun,
    error: CanonicalIncidentFeatureDisabledError,
) -> None:
    run_uuid = run.id

    now = run.completed_at or datetime.now(timezone.utc)
    error_text = sanitize_error_text(error)
    result_payload = {"error_category": FEATURE_DISABLED_ERROR_CATEGORY}
    session.execute(
        update(SyncRunReferenceDiscovery)
        .where(
            SyncRunReferenceDiscovery.sync_run_id == run_uuid,
            SyncRunReferenceDiscovery.status.in_({"planned", "retrying", "running"}),
        )
        .values(
            status="failed",
            lease_owner=None,
            lease_expires_at=None,
            last_heartbeat_at=now,
            completed_at=now,
            error=error_text,
            result=result_payload,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    session.execute(
        update(SyncDispatchOutbox)
        .where(
            SyncDispatchOutbox.sync_run_id == run_uuid,
            SyncDispatchOutbox.status == OUTBOX_STATUS_PENDING,
        )
        .values(
            status=OUTBOX_STATUS_DISPATCHED,
            dispatched_at=now,
            last_error=FEATURE_DISABLED_ERROR_CATEGORY,
            claim_token=None,
            claim_expires_at=None,
            claim_transport=None,
            claim_route_generation=None,
            dispatched_transport=None,
            dispatched_route_generation=None,
            transport_job_id=None,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    finalize_row = (
        session.query(SyncDispatchOutbox)
        .filter(
            SyncDispatchOutbox.sync_run_id == run_uuid,
            SyncDispatchOutbox.kind == OUTBOX_KIND_FINALIZE,
        )
        .one_or_none()
    )
    if finalize_row is None:
        session.add(
            SyncDispatchOutbox(
                org_id=str(run.org_id),
                sync_run_id=run_uuid,
                kind=OUTBOX_KIND_FINALIZE,
                status=OUTBOX_STATUS_DISPATCHED,
                available_at=now,
                attempts=0,
                dispatched_at=now,
                last_error=FEATURE_DISABLED_ERROR_CATEGORY,
            )
        )
    else:
        finalize_row.status = OUTBOX_STATUS_DISPATCHED
        finalize_row.last_error = FEATURE_DISABLED_ERROR_CATEGORY
        finalize_row.dispatched_at = now
        finalize_row.claim_token = None
        finalize_row.claim_expires_at = None
        finalize_row.claim_transport = None
        finalize_row.claim_route_generation = None
        finalize_row.dispatched_transport = None
        finalize_row.dispatched_route_generation = None
        finalize_row.transport_job_id = None
        finalize_row.updated_at = now

    sync_observers_for_terminal_sync_run(session, run)
    session.flush()


def _arm_feature_disabled_finalize(
    session: Session,
    run: SyncRun,
    available_at: datetime,
) -> bool:
    nested = session.begin_nested()
    try:
        session.add(
            SyncDispatchOutbox(
                org_id=str(run.org_id),
                sync_run_id=run.id,
                kind=OUTBOX_KIND_FINALIZE,
                status=OUTBOX_STATUS_PENDING,
                available_at=available_at,
                attempts=0,
            )
        )
        session.flush()
    except IntegrityError:
        nested.rollback()
        return False
    nested.commit()
    return True


def _claim_units(
    session,
    run_uuid: uuid.UUID,
    *,
    capped_ids: frozenset[str] = frozenset(),
) -> list[SyncRunUnit]:
    """Atomically claim dispatchable units for a run.

    Fresh ``planned`` units and due ``retrying`` units are claimed with atomic
    ``UPDATE ... RETURNING`` statements so two concurrent ``dispatch_sync_run``
    calls cannot both enqueue the same unit (no double-queue / duplicate provider
    writes).  Stale ``dispatching`` units (a worker died before the unit started
    running) are reclaimed by age.

    F2: RUNNING units are NEVER reclaimed by the dispatch path — re-dispatching a
    RUNNING unit would run it a second time concurrently and cause duplicate
    provider writes.  Durable dead-worker recovery is handled instead by
    ``reconcile_sync_dispatch``, which fails a RUNNING unit once its
    ``lease_expires_at`` lapses and re-arms dispatch/finalize.  ``run_sync_unit``
    renews that lease via a heartbeat bounded by an absolute deadline
    (``SYNC_UNIT_MAX_LIFETIME_SECONDS``), so even a wedged-but-alive worker's lease
    eventually lapses and the unit is reclaimed (CHAOS-2705).

    ``capped_ids`` is the set of unit IDs that the concurrency guard deferred.
    Those units are left in PLANNED status so a later redispatch can claim them
    once slots free up.  Due RETRYING units obey the same cap exclusion.
    """
    now = datetime.now(timezone.utc)
    # Build the WHERE clause for the atomic claim, excluding capped units.
    planned_where = [
        SyncRunUnit.sync_run_id == run_uuid,
        SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value,
    ]
    if capped_ids:
        planned_where.append(
            ~SyncRunUnit.id.in_([uuid.UUID(cid) for cid in capped_ids])
        )
    claimed_ids: set[uuid.UUID] = set(
        session.execute(
            update(SyncRunUnit)
            .where(*planned_where)
            .values(status=SyncRunUnitStatus.DISPATCHING.value, updated_at=now)
            .returning(SyncRunUnit.id)
            .execution_options(synchronize_session=False)
        )
        .scalars()
        .all()
    )

    retrying_where = [
        SyncRunUnit.sync_run_id == run_uuid,
        SyncRunUnit.status == SyncRunUnitStatus.RETRYING.value,
        SyncRunUnit.available_at.is_not(None),
        SyncRunUnit.available_at <= now,
    ]
    if capped_ids:
        retrying_where.append(
            ~SyncRunUnit.id.in_([uuid.UUID(cid) for cid in capped_ids])
        )
    due_retrying: set[uuid.UUID] = set(
        session.execute(
            update(SyncRunUnit)
            .where(*retrying_where)
            .values(
                status=SyncRunUnitStatus.DISPATCHING.value,
                updated_at=now,
                available_at=None,
            )
            .returning(SyncRunUnit.id)
            .execution_options(synchronize_session=False)
        )
        .scalars()
        .all()
    )
    claimed_ids.update(due_retrying)

    # Reclaim stale DISPATCHING units only (F2: RUNNING is never reclaimed).
    # A DISPATCHING unit that is stale means the worker was enqueued but never
    # picked up (e.g. broker restart).  It is safe to re-enqueue because the
    # worker never started the provider call.
    #
    # Atomic CAS: this single UPDATE re-checks status='dispatching' AND
    # updated_at <= stale_dispatch at write time, so a row that a delayed
    # run_sync_unit concurrently claimed to RUNNING is excluded by
    # construction -- it can never be reclaimed/requeued, and no status
    # rewrite of a RUNNING row is possible.  status stays DISPATCHING; only
    # updated_at is refreshed so a later redispatch re-enqueues the unit.
    stale_dispatch = now - timedelta(seconds=_stale_dispatch_seconds())
    stale_where = [
        SyncRunUnit.sync_run_id == run_uuid,
        SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
        SyncRunUnit.updated_at <= stale_dispatch,
        ~SyncRunUnit.id.in_(claimed_ids),
    ]
    if capped_ids:
        stale_where.append(~SyncRunUnit.id.in_([uuid.UUID(cid) for cid in capped_ids]))
    stale_reclaimed: set[uuid.UUID] = set(
        session.execute(
            update(SyncRunUnit)
            .where(*stale_where)
            .values(updated_at=now)
            .returning(SyncRunUnit.id)
            .execution_options(synchronize_session=False)
        )
        .scalars()
        .all()
    )
    claimed_ids.update(stale_reclaimed)

    session.flush()
    if not claimed_ids:
        return []
    return (
        session.query(SyncRunUnit)
        .filter(SyncRunUnit.id.in_(claimed_ids))
        .order_by(SyncRunUnit.id)
        .all()
    )


def _load_unit(session, unit_id: str) -> SyncRunUnit:
    unit_uuid = uuid.UUID(str(unit_id))
    unit = session.query(SyncRunUnit).filter(SyncRunUnit.id == unit_uuid).one_or_none()
    if unit is None:
        raise ValueError(f"Sync run unit not found: {unit_id}")
    return unit


def _nonterminal_run_ids_select():
    return select(SyncRun.id).where(SyncRun.status.not_in(_TERMINAL_RUN_STATUSES))


def _aggregate_run_status(
    total_count: int, success_count: int, failed_count: int
) -> str:
    if total_count == 0:
        return SyncRunStatus.FAILED.value
    if failed_count == 0:
        return SyncRunStatus.SUCCESS.value
    if success_count == 0:
        return SyncRunStatus.FAILED.value
    return SyncRunStatus.PARTIAL_FAILED.value


def _stale_dispatch_seconds() -> int:
    try:
        return max(1, int(os.getenv("SYNC_UNIT_DISPATCH_STALE_SECONDS", "900")))
    except ValueError:
        return 900


def _as_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _running_lease_seconds() -> int:
    try:
        return max(1, int(os.getenv("SYNC_UNIT_RUNNING_LEASE_SECONDS", "300")))
    except ValueError:
        return 300


def _heartbeat_interval_seconds() -> int:
    return max(1, min(60, _running_lease_seconds() // 4))


def _max_unit_lifetime_seconds() -> int:
    """Absolute cap on how long a heartbeat may renew a unit's lease.

    Floored at 3600 (the Celery hard task_time_limit) so a misconfigured value
    cannot prematurely expire a still-progressing unit.  The heartbeat will stop
    renewing once this deadline is reached, allowing the reconciler to reclaim.
    """
    try:
        return max(3600, int(os.getenv("SYNC_UNIT_MAX_LIFETIME_SECONDS", "3720")))
    except ValueError:
        return 3720


def _unit_lease_is_owned_and_live(
    unit: SyncRunUnit,
    lease_owner: str | None,
    now: datetime,
) -> bool:
    if lease_owner is None or unit.lease_owner != lease_owner:
        return False
    if unit.lease_expires_at is None:
        return False
    return _as_aware(unit.lease_expires_at) > now


def _start_unit_heartbeat(
    unit_id: str,
    lease_owner: str | None,
    deadline: datetime,
) -> tuple[threading.Event, threading.Thread] | tuple[None, None]:
    if lease_owner is None:
        return None, None
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_heartbeat_unit_lease,
        args=(unit_id, lease_owner, stop_event, deadline),
        name=f"sync-unit-heartbeat-{unit_id}",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def _heartbeat_unit_lease(
    unit_id: str,
    lease_owner: str,
    stop_event: threading.Event,
    deadline: datetime,
) -> None:
    from dev_health_ops.db import get_postgres_session_sync

    interval = _heartbeat_interval_seconds()
    lease_seconds = _running_lease_seconds()
    while not stop_event.wait(interval):
        now = datetime.now(timezone.utc)
        if now >= deadline:
            logger.warning(
                "run_sync_unit.heartbeat_deadline_exceeded",
                extra={"unit_id": unit_id},
            )
            stop_event.set()
            break
        lease_expires_at = min(now + timedelta(seconds=lease_seconds), deadline)
        try:
            with get_postgres_session_sync() as session:
                heartbeat_result: Any = session.execute(
                    update(SyncRunUnit)
                    .where(
                        SyncRunUnit.id == uuid.UUID(str(unit_id)),
                        SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                        SyncRunUnit.lease_owner == lease_owner,
                        SyncRunUnit.lease_expires_at > now,
                        SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                    )
                    .values(
                        lease_expires_at=lease_expires_at,
                        last_heartbeat_at=now,
                    )
                    .execution_options(synchronize_session=False)
                )
                if int(heartbeat_result.rowcount or 0) == 0:
                    logger.info(
                        "run_sync_unit.heartbeat_lease_lost",
                        extra={"unit_id": unit_id},
                    )
                    stop_event.set()
        except Exception:
            logger.exception(
                "run_sync_unit.heartbeat_failed",
                extra={"unit_id": unit_id},
            )


def _schedule_redispatch(
    sync_run_id: str, *, available_at: datetime | None = None
) -> None:
    try:
        from dev_health_ops.db import get_postgres_session_sync

        countdown = int(os.getenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60"))
        now = datetime.now(timezone.utc)
        redispatch_at = available_at or now + timedelta(seconds=countdown)
        with get_postgres_session_sync() as session:
            upsert_outbox_wakeup(
                session,
                sync_run_id=sync_run_id,
                kind=OUTBOX_KIND_DISPATCH,
                available_at=redispatch_at,
                now=now,
            )
            session.execute(
                update(SyncDispatchOutbox)
                .where(
                    SyncDispatchOutbox.sync_run_id == uuid.UUID(str(sync_run_id)),
                    SyncDispatchOutbox.kind == OUTBOX_KIND_DISPATCH,
                    SyncDispatchOutbox.status == OUTBOX_STATUS_PENDING,
                    SyncDispatchOutbox.claim_token.is_(None),
                )
                .values(
                    available_at=redispatch_at,
                    updated_at=now,
                )
                .execution_options(synchronize_session=False)
            )
            session.flush()
        logger.info(
            "dispatch_sync_run.redispatch_rearmed",
            extra={
                "sync_run_id": sync_run_id,
                "countdown": countdown,
                "available_at": redispatch_at.isoformat(),
            },
        )
    except Exception:
        logger.exception(
            "dispatch_sync_run.redispatch_rearm_failed",
            extra={"sync_run_id": sync_run_id},
        )
