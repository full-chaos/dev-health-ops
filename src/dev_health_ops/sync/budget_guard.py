from __future__ import annotations

import json
import logging
import os
import random
import uuid
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import or_, text, update

from dev_health_ops.models import (
    ProviderRateLimitObservation,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.budget import BudgetEstimate, estimate_provider_budget
from dev_health_ops.workers.rate_limit_defer import (
    RATE_LIMIT_DEFAULT_COUNTDOWN_SECONDS,
    RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS,
    plan_rate_limit_deferral,
)
from dev_health_ops.workers.sync_bootstrap import SyncTaskBootstrap

logger = logging.getLogger(__name__)

# Mirrors ``workers/sync_units.py::_AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION``
# verbatim. Duplicated rather than imported: sync_units.py already imports
# BudgetGuard from this module, so the reverse import would cycle; the same
# duplicate-rather-than-reach-in pattern is already used for
# ``_comparison_budget_key`` mirroring ``_budget_key``. Pinned equal by
# ``tests/test_budget_guard_cooldown.py::test_ambiguous_attribution_constant_matches_observation_writer``.
_AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION = "ambiguous_dimension"

# Distinct from 'budget_deferred' (_defer_unit_for_budget) and 'rate_limit'
# (workers/sync_units.py's in-worker deferral) so operators can tell a
# shared-cooldown gate hit apart from either (docs/providers/rate-limit-policy.md).
_RATE_LIMIT_COOLDOWN_DEFERRED_CATEGORY = "rate_limit_cooldown_deferred"
_RATE_LIMIT_COOLDOWN_EXHAUSTED_CATEGORY = "rate_limit_cooldown_exhausted"

# Defense in depth for _rate_limit_deferral_exhausted (review finding, round
# 3): the unit's own last-recorded result.error_category must ALSO show a
# rate-limit-related cause before the wall-clock-exhaustion check can fire.
# 'rate_limit' mirrors the in-worker 429 path's category
# (workers/sync_units.py's RateLimitException handler) -- duplicated for the
# same reverse-import-cycle reason as _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION.
_RATE_LIMIT_EPISODE_ERROR_CATEGORIES = frozenset(
    {"rate_limit", _RATE_LIMIT_COOLDOWN_DEFERRED_CATEGORY}
)


@dataclass(frozen=True)
class BudgetGuardResult:
    observations: list[dict[str, Any]] = field(default_factory=list)
    deferred_unit_ids: frozenset[str] = frozenset()
    next_deferred_at: datetime | None = None
    # CHAOS-2760 TOCTOU closure: the candidate units and their (already
    # loaded, credential-decryption-free-to-reuse) estimates from THIS pass,
    # so the caller can run one more cheap cooldown re-check
    # (``reconfirm_cooldowns``) immediately before the atomic claim, without
    # re-loading estimates. See ``reconfirm_cooldowns`` docstring.
    candidate_units: tuple[SyncRunUnit, ...] = ()
    estimates_by_unit: dict[str, tuple[BudgetEstimate, ...]] = field(
        default_factory=dict
    )
    # The SAME jitter config this pass used for its own cooldown deferrals,
    # so ``reconfirm_cooldowns`` (called separately, after this returns)
    # applies byte-identical jitter rather than re-reading the env var and
    # risking drift if it changed mid-pass.
    jitter_seconds: int = 5


@dataclass(frozen=True)
class CooldownReconfirmResult:
    """Result of :meth:`BudgetGuard.reconfirm_cooldowns` -- the late,
    pre-claim re-check (CHAOS-2760 TOCTOU closure)."""

    excluded_unit_ids: frozenset[str] = frozenset()
    next_deferred_at: datetime | None = None


class BudgetGuard:
    @staticmethod
    def observe_run(
        session: Any,
        sync_run_id: str,
        *,
        capped_unit_ids: Iterable[str] = (),
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        observed_at = now or datetime.now(timezone.utc)
        ignored_unit_ids = {str(unit_id) for unit_id in capped_unit_ids}
        units = _dispatch_candidate_units(
            session,
            sync_run_id,
            ignored_unit_ids=ignored_unit_ids,
            now=observed_at,
        )
        limits = _budget_limits()
        default_limit = _env_int("SYNC_BUDGET_DRY_RUN_DEFAULT_LIMIT", 1_000_000)
        deferral_seconds = _env_int("SYNC_BUDGET_DRY_RUN_DEFERRAL_SECONDS", 60)
        consumed_by_bucket: dict[str, int] = defaultdict(int)
        observations: list[dict[str, Any]] = []

        for unit in units:
            log_ctx = {
                "sync_run_id": sync_run_id,
                "unit_id": str(unit.id),
                "source_id": str(unit.source_id),
                "dataset_key": str(unit.dataset_key),
                "provider": str(unit.provider),
                "cost_class": str(unit.cost_class),
            }
            try:
                ctx = SyncTaskBootstrap.load(session, str(unit.id))
                estimates = estimate_provider_budget(ctx)
            except Exception as exc:
                logger.warning(
                    "dispatch_sync_run.budget_guard_dry_run_failed",
                    extra={**log_ctx, "error": str(exc)},
                )
                continue

            for estimate in estimates:
                observation = _observe_estimate(
                    estimate,
                    log_ctx=log_ctx,
                    consumed_by_bucket=consumed_by_bucket,
                    limits=limits,
                    default_limit=default_limit,
                    observed_at=observed_at,
                    deferral_seconds=deferral_seconds,
                )
                observations.append(observation)
                logger.info(
                    "dispatch_sync_run.budget_guard_dry_run",
                    extra=observation,
                )

        return observations

    @staticmethod
    def enforce_run(
        session: Any,
        sync_run_id: str,
        *,
        capped_unit_ids: Iterable[str] = (),
        now: datetime | None = None,
    ) -> BudgetGuardResult:
        enforced_at = now or datetime.now(timezone.utc)
        ignored_unit_ids = {str(unit_id) for unit_id in capped_unit_ids}
        units = _dispatch_candidate_units(
            session,
            sync_run_id,
            ignored_unit_ids=ignored_unit_ids,
            now=enforced_at,
        )
        if not units:
            return BudgetGuardResult()

        limits = _enforced_budget_limits()
        default_limit = _env_int("SYNC_BUDGET_DEFAULT_LIMIT", 1_000_000)
        deferral_seconds = _env_int("SYNC_BUDGET_DEFERRAL_SECONDS", 60)
        jitter_seconds = _env_int("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", 5)
        estimates_by_unit: dict[str, tuple[BudgetEstimate, ...]] = {}
        budget_keys: set[str] = set()
        observations: list[dict[str, Any]] = []

        for unit in units:
            log_ctx = _unit_log_context(sync_run_id, unit)
            try:
                ctx = SyncTaskBootstrap.load(session, str(unit.id))
                estimates = estimate_provider_budget(ctx)
            except Exception as exc:
                logger.warning(
                    "dispatch_sync_run.budget_guard_enforce_failed",
                    extra={**log_ctx, "error": str(exc)},
                )
                estimates = ()
            estimates_by_unit[str(unit.id)] = estimates
            for estimate in estimates:
                budget_keys.add(
                    _budget_key(
                        estimate.bucket.to_dict(), route_family=estimate.route_family
                    )
                )

        _acquire_budget_advisory_locks(session, sorted(budget_keys))

        deferred_unit_ids: set[str] = set()
        next_deferred_at: datetime | None = None
        cooldown_handled_unit_ids: set[str] = set()

        # --- Shared cooldown gating (CHAOS-2760) — BEFORE budget admission,
        # so a unit gated by a known cooldown never also reserves budget
        # capacity it will not use this pass.
        cooldown_by_family, cooldown_by_dimension = _active_cooldowns(
            session,
            sync_run_id=sync_run_id,
            candidates=units,
            now=enforced_at,
        )
        for unit in units:
            estimates = estimates_by_unit[str(unit.id)]
            if not estimates:
                continue
            cooldown_expiry = None
            if cooldown_by_family or cooldown_by_dimension:
                cooldown_expiry = _matching_cooldown_expiry(
                    estimates,
                    org_id=str(unit.org_id),
                    provider=str(unit.provider),
                    integration_id=unit.integration_id,
                    cooldown_by_family=cooldown_by_family,
                    cooldown_by_dimension=cooldown_by_dimension,
                )
            log_ctx = _unit_log_context(sync_run_id, unit)
            if cooldown_expiry is not None:
                outcome = _apply_cooldown_deferral(
                    session,
                    unit,
                    cooldown_expiry=cooldown_expiry,
                    jitter_seconds=jitter_seconds,
                    now=enforced_at,
                    log_ctx=log_ctx,
                )
            elif _rate_limit_deferral_exhausted(unit, now=enforced_at):
                # Review finding: termination must not depend on a
                # currently-visible cooldown observation -- the lookback
                # window can age the causing row out of visibility at
                # roughly the SAME instant the unit's own wall-clock
                # deferral budget expires. Terminalize from the unit's own
                # persisted rate_limit_deferrals/rate_limit_first_seen_at
                # state instead of letting it dispatch and burn a worker
                # slot only to rediscover the same exhaustion in-worker.
                outcome = _terminalize_rate_limit_exhausted(
                    session, unit, now=enforced_at, log_ctx=log_ctx
                )
            else:
                continue
            if outcome is None:
                # CAS lost the race (unit moved on concurrently) — leave
                # it for the budget loop / _claim_units to sort out, same
                # as a lost _defer_unit_for_budget race.
                continue
            cooldown_handled_unit_ids.add(str(unit.id))
            available_at, terminalized = outcome
            if terminalized:
                continue
            deferred_unit_ids.add(str(unit.id))
            if next_deferred_at is None or available_at < next_deferred_at:
                next_deferred_at = available_at

        consumed_by_bucket = _active_budget_consumption(
            session,
            now=enforced_at,
            budget_keys=budget_keys,
        )

        for unit in units:
            if str(unit.id) in cooldown_handled_unit_ids:
                continue
            log_ctx = _unit_log_context(sync_run_id, unit)
            estimates = estimates_by_unit[str(unit.id)]
            if not estimates:
                continue
            unit_observations: list[dict[str, Any]] = []
            would_defer = False
            for estimate in estimates:
                observation = _observe_estimate(
                    estimate,
                    log_ctx=log_ctx,
                    consumed_by_bucket=consumed_by_bucket,
                    limits=limits,
                    default_limit=default_limit,
                    observed_at=enforced_at,
                    deferral_seconds=deferral_seconds,
                    record_consumption=False,
                )
                unit_observations.append(observation)
                if observation["decision"] == "would_defer":
                    would_defer = True

            if would_defer:
                available_at = enforced_at + timedelta(
                    seconds=deferral_seconds + random.uniform(0, float(jitter_seconds))  # noqa: S311
                )
                for observation in unit_observations:
                    observation["decision"] = "deferred"
                    observation["available_at"] = available_at.isoformat()
                deferred = _defer_unit_for_budget(
                    session,
                    unit,
                    available_at=available_at,
                    now=enforced_at,
                    observations=unit_observations,
                )
                if not deferred:
                    continue
                deferred_unit_ids.add(str(unit.id))
                if next_deferred_at is None or available_at < next_deferred_at:
                    next_deferred_at = available_at
                for observation in unit_observations:
                    logger.info(
                        "dispatch_sync_run.budget_guard_deferred",
                        extra=observation,
                    )
            else:
                for estimate in estimates:
                    budget_key = _budget_key(
                        estimate.bucket.to_dict(), route_family=estimate.route_family
                    )
                    consumed_by_bucket[budget_key] += estimate.estimated_units
                for observation in unit_observations:
                    observation["decision"] = "allowed"
                    logger.info(
                        "dispatch_sync_run.budget_guard_allowed",
                        extra=observation,
                    )
            observations.extend(unit_observations)

        return BudgetGuardResult(
            observations=observations,
            deferred_unit_ids=frozenset(deferred_unit_ids),
            next_deferred_at=next_deferred_at,
            candidate_units=tuple(units),
            estimates_by_unit=estimates_by_unit,
            jitter_seconds=jitter_seconds,
        )

    @staticmethod
    def reconfirm_cooldowns(
        session: Any,
        sync_run_id: str,
        *,
        units: Iterable[SyncRunUnit],
        estimates_by_unit: Mapping[str, tuple[BudgetEstimate, ...]],
        already_excluded_ids: frozenset[str],
        jitter_seconds: int,
        now: datetime | None = None,
    ) -> CooldownReconfirmResult:
        """Close the TOCTOU window between ``enforce_run``'s cooldown
        snapshot and the atomic claim (CHAOS-2760 review finding).

        ``enforce_run`` reads ``provider_rate_limit_observations`` once,
        early in its pass, then goes on to do real DB work of its own
        (``_active_budget_consumption`` re-estimates every active unit
        across the bucket) before returning. Under READ COMMITTED, a
        sibling unit's 429 can commit a brand-new observation row in that
        window -- one this pass's ``enforce_run`` snapshot never saw -- and
        without a second look, ``_claim_units`` would dispatch straight into
        it, defeating the whole point of the gate.

        This re-runs the SAME cheap, single indexed query
        (``_active_cooldowns``) and the SAME per-unit matching
        (``_matching_cooldown_expiry`` -- byte-identical semantics,
        including the ambiguous-dimension fallback) against the estimates
        ``enforce_run`` already computed (no re-estimation, no credential
        decryption), as the LAST read before the claim.

        A unit caught here is NOT merely excluded -- review finding (round
        2): a bare exclusion left it PLANNED with no ``RETRYING`` stamp, no
        ``available_at``, and no ``rate_limit_deferrals`` increment, which
        both breaks the "cooldown deferrals count against the shared
        rate-limit budget" binding decision AND livelocks the run (a
        PLANNED unit is "dispatchable" for ``_pending_unit_counts``, so it
        redispatches on a bare ~60s countdown forever, re-triggering this
        same exclusion indefinitely without ever accumulating enough
        deferrals to terminalize). Every match here goes through the exact
        same write path ``enforce_run``'s own cooldown loop uses
        (``_apply_cooldown_deferral`` / ``_terminalize_rate_limit_exhausted``
        for the wall-clock-exhausted-without-a-visible-observation case) --
        one deferral semantics, reused by both call sites, not a second,
        weaker one.

        Returns the unit ids to additionally exclude from this pass's claim
        (deferred AND terminalized -- terminalized units are already
        ``FAILED`` and would not match ``_claim_units``' predicate anyway,
        but including them keeps the exclusion set self-documenting) plus
        the earliest new ``available_at``, so the caller can fold it into
        ``next_deferred_at`` for the ``_schedule_redispatch`` re-arm.

        This does not achieve full serializability (a commit landing in the
        few-microsecond gap between this query and the claim's own
        ``UPDATE`` could still slip through), but it collapses the window
        from "however long budget admission takes" down to "back-to-back
        statements", consistent with how the rest of this module tolerates
        narrow races via CAS predicates rather than ``SERIALIZABLE``
        transactions.
        """
        checked_at = now or datetime.now(timezone.utc)
        candidates = [
            unit for unit in units if str(unit.id) not in already_excluded_ids
        ]
        if not candidates:
            return CooldownReconfirmResult()

        cooldown_by_family, cooldown_by_dimension = _active_cooldowns(
            session,
            sync_run_id=sync_run_id,
            candidates=candidates,
            now=checked_at,
        )

        excluded: set[str] = set()
        next_deferred_at: datetime | None = None
        for unit in candidates:
            estimates = estimates_by_unit.get(str(unit.id), ())
            if not estimates:
                continue
            cooldown_expiry = None
            if cooldown_by_family or cooldown_by_dimension:
                cooldown_expiry = _matching_cooldown_expiry(
                    estimates,
                    org_id=str(unit.org_id),
                    provider=str(unit.provider),
                    integration_id=unit.integration_id,
                    cooldown_by_family=cooldown_by_family,
                    cooldown_by_dimension=cooldown_by_dimension,
                )
            log_ctx = _unit_log_context(sync_run_id, unit)
            if cooldown_expiry is not None:
                outcome = _apply_cooldown_deferral(
                    session,
                    unit,
                    cooldown_expiry=cooldown_expiry,
                    jitter_seconds=jitter_seconds,
                    now=checked_at,
                    log_ctx=log_ctx,
                )
            elif _rate_limit_deferral_exhausted(unit, now=checked_at):
                outcome = _terminalize_rate_limit_exhausted(
                    session, unit, now=checked_at, log_ctx=log_ctx
                )
            else:
                continue
            if outcome is None:
                # CAS lost the race -- unit moved on concurrently since the
                # candidate snapshot was built; leave it for _claim_units to
                # sort out on its own terms.
                continue
            excluded.add(str(unit.id))
            available_at, terminalized = outcome
            logger.info(
                "dispatch_sync_run.rate_limit_cooldown_reconfirmed",
                extra={
                    "sync_run_id": sync_run_id,
                    "unit_id": str(unit.id),
                    "terminalized": terminalized,
                },
            )
            if not terminalized and (
                next_deferred_at is None or available_at < next_deferred_at
            ):
                next_deferred_at = available_at

        return CooldownReconfirmResult(
            excluded_unit_ids=frozenset(excluded),
            next_deferred_at=next_deferred_at,
        )


def _dispatch_candidate_units(
    session: Any,
    sync_run_id: str,
    *,
    ignored_unit_ids: set[str],
    now: datetime,
) -> list[SyncRunUnit]:
    run_uuid = uuid.UUID(str(sync_run_id))
    units = (
        session.query(SyncRunUnit)
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            or_(
                SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value,
                (
                    (SyncRunUnit.status == SyncRunUnitStatus.RETRYING.value)
                    & (SyncRunUnit.available_at.is_not(None))
                    & (SyncRunUnit.available_at <= now)
                ),
                (
                    (SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value)
                    & (SyncRunUnit.updated_at <= _stale_dispatch_cutoff(now))
                ),
            ),
        )
        .order_by(SyncRunUnit.id)
        .all()
    )
    return [unit for unit in units if str(unit.id) not in ignored_unit_ids]


def _observe_estimate(
    estimate: BudgetEstimate,
    *,
    log_ctx: dict[str, Any],
    consumed_by_bucket: dict[str, int],
    limits: Mapping[str, int],
    default_limit: int,
    observed_at: datetime,
    deferral_seconds: int,
    record_consumption: bool = True,
) -> dict[str, Any]:
    bucket = estimate.bucket.to_dict()
    budget_key = _budget_key(bucket, route_family=estimate.route_family)
    limit = _limit_for_bucket(
        bucket,
        route_family=estimate.route_family,
        limits=limits,
        default_limit=default_limit,
    )
    previous_units = consumed_by_bucket[budget_key]
    projected_units = previous_units + estimate.estimated_units
    if record_consumption:
        consumed_by_bucket[budget_key] = projected_units
    would_defer = projected_units > limit
    suggested_available_at = None
    if would_defer:
        suggested_available_at = (
            observed_at + timedelta(seconds=deferral_seconds)
        ).isoformat()

    return {
        **log_ctx,
        "decision": "would_defer" if would_defer else "would_allow",
        "bucket": bucket,
        "budget_key": budget_key,
        "estimated_units": estimate.estimated_units,
        "projected_units": projected_units,
        "budget_limit": limit,
        "confidence": estimate.confidence,
        "route_family": estimate.route_family,
        "suggested_available_at": suggested_available_at,
    }


def _unit_log_context(sync_run_id: str, unit: SyncRunUnit) -> dict[str, Any]:
    return {
        "sync_run_id": sync_run_id,
        "unit_id": str(unit.id),
        "source_id": str(unit.source_id),
        "dataset_key": str(unit.dataset_key),
        "provider": str(unit.provider),
        "cost_class": str(unit.cost_class),
    }


def _defer_unit_for_budget(
    session: Any,
    unit: SyncRunUnit,
    *,
    available_at: datetime,
    now: datetime,
    observations: list[dict[str, Any]],
) -> bool:
    stale_dispatch_cutoff = _stale_dispatch_cutoff(now)
    result: Any = session.execute(
        update(SyncRunUnit)
        .where(
            SyncRunUnit.id == unit.id,
            or_(
                SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value,
                (
                    (SyncRunUnit.status == SyncRunUnitStatus.RETRYING.value)
                    & (SyncRunUnit.available_at.is_not(None))
                    & (SyncRunUnit.available_at <= now)
                ),
                (
                    (SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value)
                    & (SyncRunUnit.updated_at <= stale_dispatch_cutoff)
                ),
            ),
        )
        .values(
            status=SyncRunUnitStatus.RETRYING.value,
            available_at=available_at,
            error="deferred by sync budget guard",
            result={
                "error_category": "budget_deferred",
                "not_before": available_at.isoformat(),
                "budget_guard": observations,
            },
            # Review finding (round 3): a budget deferral is NOT a rate-limit
            # episode -- clear any stale rate_limit_deferrals/first_seen_at
            # this unit is carrying from an EARLIER, since-resolved
            # rate-limit episode. Leaving them untouched here is exactly the
            # state-lifecycle hole that let _rate_limit_deferral_exhausted
            # (added for the cooldown gate) fire against unrelated old data.
            rate_limit_deferrals=0,
            rate_limit_first_seen_at=None,
            lease_owner=None,
            lease_expires_at=None,
            last_heartbeat_at=now,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    if int(result.rowcount or 0) > 0:
        unit.status = SyncRunUnitStatus.RETRYING.value
        unit.available_at = available_at
        unit.rate_limit_deferrals = 0
        unit.rate_limit_first_seen_at = None
        return True
    return False


def _as_aware(value: datetime) -> datetime:
    """Return a timezone-aware UTC datetime (mirrors sync_units._as_aware /
    guard._as_aware_guard). SQLite (unit tests) returns naive datetimes for
    ``DateTime(timezone=True)`` columns; Postgres returns aware ones."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _cooldown_expiry(observation: ProviderRateLimitObservation) -> datetime:
    """The moment an observation's cooldown lifts:
    ``coalesce(reset_at, observed_at + retry_after_seconds)``, falling back
    to a conservative fixed window when the signal carried neither. Never
    treated as "no cooldown" (an observation with no delay info would be
    silently ignored) nor as "cooldown forever" (over-defer) -- see
    docs/providers/rate-limit-policy.md "Cooldown gating".
    """
    if observation.reset_at is not None:
        return _as_aware(observation.reset_at)
    observed_at = _as_aware(observation.observed_at)
    if observation.retry_after_seconds is not None:
        return observed_at + timedelta(
            seconds=max(0.0, observation.retry_after_seconds)
        )
    return observed_at + timedelta(seconds=RATE_LIMIT_DEFAULT_COUNTDOWN_SECONDS)


def _cooldown_lookback_seconds() -> int:
    # Bounds the observation query to a recency window so the lookup stays
    # cheap regardless of the table's 14-day (default) retention.
    #
    # Review finding (round 2): the default must NOT equal
    # RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS exactly. A unit deferred by this
    # gate gets available_at clamped to that same wall-clock budget, so it
    # becomes due again at roughly the SAME age its causing observation's
    # observed_at has reached -- an equal lookback would age the row out of
    # visibility at EXACTLY the instant termination should kick in instead,
    # making the observation invisible right when it matters most. Padded
    # with the max configured jitter (available_at's own slop) plus a
    # generous flat skew margin (clock drift / processing latency between
    # whatever wrote the row and whatever reads it). _rate_limit_deferral_
    # exhausted() is the belt to this suspenders' braces: termination itself
    # never depends on this window either way.
    jitter_max = _env_int("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", 5)
    skew_margin = _env_int("SYNC_RATE_LIMIT_COOLDOWN_LOOKBACK_SKEW_SECONDS", 300)
    default = RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS + jitter_max + skew_margin
    return _env_int("SYNC_RATE_LIMIT_COOLDOWN_LOOKBACK_SECONDS", default)


def _active_cooldowns(
    session: Any,
    *,
    sync_run_id: str,
    candidates: Iterable[SyncRunUnit],
    now: datetime,
) -> tuple[
    dict[tuple[str, str, uuid.UUID, str], datetime],
    dict[tuple[str, str, uuid.UUID, str], datetime],
]:
    """Resolve which ``(org_id, provider, integration_id, route_family)`` /
    ambiguous-fallback ``(org_id, provider, integration_id, dimension)``
    tuples carry an ACTIVE shared cooldown right now (CHAOS-2760).

    ONE indexed query per dispatch pass -- never per unit -- over
    ``provider_rate_limit_observations``, using the ``ws-d``
    ``(provider, integration_id, route_family, observed_at)`` index. The
    match key is deliberately ``(org_id, provider, integration_id,
    route_family)``: org-scoped, and EXCLUDING credential_fingerprint/host,
    so rotating a credential can never bypass an active cooldown (the
    credentials-are-not-capacity invariant applied to gating).

    Rows with ``route_family_attribution == 'ambiguous_dimension'`` (CHAOS-2758:
    the writer could not confidently attribute one route family) carry
    ``route_family=NULL`` and are NEVER matched by family -- a NULL family is
    never treated as matching everything (over-defer) or nothing (silent
    under-defer). They instead populate the dimension-keyed fallback map, so
    a candidate unit's estimate is gated by the observation's dimension when
    its own family cannot be resolved from the ambiguous row.

    Fail-open on ANY error reading the store: a broken observation read must
    never block dispatch -- logs a warning and returns two empty maps so the
    caller proceeds exactly as if no cooldown existed.
    """
    family_cooldowns: dict[tuple[str, str, uuid.UUID, str], datetime] = {}
    dimension_cooldowns: dict[tuple[str, str, uuid.UUID, str], datetime] = {}

    org_ids: set[str] = set()
    providers: set[str] = set()
    integration_ids: set[uuid.UUID] = set()
    for unit in candidates:
        org_ids.add(str(unit.org_id))
        providers.add(str(unit.provider))
        integration_ids.add(unit.integration_id)
    if not org_ids or not providers or not integration_ids:
        return family_cooldowns, dimension_cooldowns

    lookback_cutoff = now - timedelta(seconds=_cooldown_lookback_seconds())
    try:
        rows = (
            session.query(ProviderRateLimitObservation)
            .filter(
                ProviderRateLimitObservation.org_id.in_(org_ids),
                ProviderRateLimitObservation.provider.in_(providers),
                ProviderRateLimitObservation.integration_id.in_(integration_ids),
                ProviderRateLimitObservation.observed_at >= lookback_cutoff,
            )
            .all()
        )
    except Exception as exc:
        logger.warning(
            "dispatch_sync_run.cooldown_observation_read_failed",
            extra={"sync_run_id": sync_run_id, "error": str(exc)},
        )
        return family_cooldowns, dimension_cooldowns

    for row in rows:
        # Per-row parsing is fail-open too, not just the SQL read above: a
        # single malformed row (e.g. a non-finite retry_after_seconds --
        # timedelta(seconds=inf) raises OverflowError) must not abort the
        # whole pass and block dispatch org-wide (review finding). Skip and
        # log; treat the row as "no cooldown signal" rather than crashing.
        try:
            expiry = _cooldown_expiry(row)
        except (OverflowError, ValueError, TypeError) as exc:
            logger.warning(
                "dispatch_sync_run.cooldown_observation_row_malformed",
                extra={
                    "sync_run_id": sync_run_id,
                    "observation_id": str(getattr(row, "id", None)),
                    "error": str(exc),
                },
            )
            continue
        if expiry <= now:
            continue
        key_prefix = (str(row.org_id), str(row.provider), row.integration_id)
        if row.route_family_attribution == _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION:
            if row.dimension is None:
                continue
            key = (*key_prefix, row.dimension)
            dimension_cooldowns[key] = max(expiry, dimension_cooldowns.get(key, expiry))
        elif row.route_family is not None:
            key = (*key_prefix, row.route_family)
            family_cooldowns[key] = max(expiry, family_cooldowns.get(key, expiry))

    return family_cooldowns, dimension_cooldowns


def _matching_cooldown_expiry(
    estimates: Iterable[BudgetEstimate],
    *,
    org_id: str,
    provider: str,
    integration_id: uuid.UUID,
    cooldown_by_family: Mapping[tuple[str, str, uuid.UUID, str], datetime],
    cooldown_by_dimension: Mapping[tuple[str, str, uuid.UUID, str], datetime],
) -> datetime | None:
    """Whole-unit deferral on ANY estimate match -- mirrors the existing
    would-defer-any-estimate budget semantics in ``enforce_run``: a unit
    mapping to multiple route families is held back if ANY of them is
    cooling down. When more than one matches, the unit waits for the LAST
    one to clear (max expiry), not the first.
    """
    matches: list[datetime] = []
    for estimate in estimates:
        family_key = (org_id, provider, integration_id, estimate.route_family)
        expiry = cooldown_by_family.get(family_key)
        if expiry is not None:
            matches.append(expiry)
        dimension_key = (
            org_id,
            provider,
            integration_id,
            estimate.bucket.dimension.value,
        )
        expiry = cooldown_by_dimension.get(dimension_key)
        if expiry is not None:
            matches.append(expiry)
    if not matches:
        return None
    return max(matches)


def _cooldown_claim_predicate(now: datetime) -> Any:
    stale_dispatch_cutoff = _stale_dispatch_cutoff(now)
    return or_(
        SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value,
        (
            (SyncRunUnit.status == SyncRunUnitStatus.RETRYING.value)
            & (SyncRunUnit.available_at.is_not(None))
            & (SyncRunUnit.available_at <= now)
        ),
        (
            (SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value)
            & (SyncRunUnit.updated_at <= stale_dispatch_cutoff)
        ),
    )


def _rate_limit_deferral_exhausted(unit: SyncRunUnit, *, now: datetime) -> bool:
    """True when this unit's SHARED rate-limit-deferral budget
    (``RATE_LIMIT_MAX_DEFERRALS`` / ``RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS``) is
    already spent, computed purely from the unit's OWN persisted state --
    independent of whether an observation is currently visible via
    ``_active_cooldowns`` (review finding: the lookback window can age a
    real cooldown's causing observation out of visibility at roughly the
    SAME wall-clock instant the deferral budget itself expires, since
    ``available_at`` for a cooldown-gated unit is itself derived from that
    same clamp -- termination must not depend on re-reading the store).

    A fresh unit (``rate_limit_deferrals == 0`` and
    ``rate_limit_first_seen_at is None``) is never "exhausted" -- this only
    fires for a unit with genuine prior rate-limit-deferral history (from
    EITHER this gate or the in-worker 429 path; they share the same
    columns).

    Defense in depth (review finding, round 3): ``rate_limit_deferrals`` /
    ``rate_limit_first_seen_at`` are cleared at every SUCCESS stamp and every
    non-rate-limit RETRYING stamp (budget deferral, expired-lease retry,
    soft-timeout retry) -- see the "keep or clear" audit in
    ``docs/providers/rate-limit-policy.md`` -- so a genuinely UNRELATED
    retry reason should never reach here with stale nonzero columns. This is
    a SECOND, independent check in case a clear site is ever missed: it also
    requires the unit's own MOST RECENTLY recorded ``result.error_category``
    to be rate-limit-related. A stale row surviving a missed clear would
    still show its last real cause (``budget_deferred``, ``worker_lost``,
    ``soft_timeout``, ...) and be refused here regardless.
    """
    if unit.rate_limit_deferrals <= 0 and unit.rate_limit_first_seen_at is None:
        return False
    result = unit.result
    error_category = (
        result.get("error_category") if isinstance(result, Mapping) else None
    )
    if error_category not in _RATE_LIMIT_EPISODE_ERROR_CATEGORIES:
        return False
    return (
        plan_rate_limit_deferral(
            retry_after_seconds=None,
            attempts=unit.rate_limit_deferrals,
            first_seen_at=unit.rate_limit_first_seen_at.isoformat()
            if unit.rate_limit_first_seen_at
            else None,
            now=now,
        )
        is None
    )


def _terminalize_rate_limit_exhausted(
    session: Any,
    unit: SyncRunUnit,
    *,
    now: datetime,
    log_ctx: dict[str, Any],
) -> tuple[datetime, bool] | None:
    """Terminally fail a unit whose shared rate-limit-deferral budget is
    spent (CHAOS-2742 binding decision: run-liveness beats optimism).

    Shared by :func:`_apply_cooldown_deferral` (when
    ``plan_rate_limit_deferral`` returns ``None`` for a unit matched by a
    currently-visible cooldown) and the wall-clock-exhaustion-without-a-
    visible-observation path in ``enforce_run`` / ``reconfirm_cooldowns``
    (review finding) -- one CAS, one code path, for both triggers.

    Returns ``(now, True)`` on a successful CAS transition, or ``None`` if
    the CAS lost the race (the unit moved on concurrently).
    """
    claim_predicate = _cooldown_claim_predicate(now)
    result: Any = session.execute(
        update(SyncRunUnit)
        .where(SyncRunUnit.id == unit.id, claim_predicate)
        .values(
            status=SyncRunUnitStatus.FAILED.value,
            error="rate limit cooldown deferral budget exhausted",
            result={
                "error_category": _RATE_LIMIT_COOLDOWN_EXHAUSTED_CATEGORY,
                "rate_limit_deferrals": unit.rate_limit_deferrals,
            },
            lease_owner=None,
            lease_expires_at=None,
            last_heartbeat_at=now,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    if int(result.rowcount or 0) == 0:
        return None
    unit.status = SyncRunUnitStatus.FAILED.value
    logger.warning(
        "dispatch_sync_run.rate_limit_cooldown_exhausted",
        extra={**log_ctx, "rate_limit_deferrals": unit.rate_limit_deferrals},
    )
    return now, True


def _apply_cooldown_deferral(
    session: Any,
    unit: SyncRunUnit,
    *,
    cooldown_expiry: datetime,
    jitter_seconds: int,
    now: datetime,
    log_ctx: dict[str, Any],
) -> tuple[datetime, bool] | None:
    """Defer (or, on rate-limit-deferral-budget exhaustion, terminally fail)
    a unit gated by an active shared cooldown (CHAOS-2760).

    Cooldown deferrals COUNT against the SAME
    ``rate_limit_deferrals`` / ``rate_limit_first_seen_at`` budget the
    in-worker 429 path uses (``workers/rate_limit_defer.plan_rate_limit_deferral``,
    ``RATE_LIMIT_MAX_DEFERRALS`` / ``RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS`` --
    binding CHAOS-2742 recon decision: run-liveness beats optimism, so a
    chronically rate-limited provider terminalizes here rather than holding
    the run open on repeated gate hits that never even reach the provider.

    Returns ``(available_at, terminalized)`` on a successful CAS transition,
    or ``None`` if the CAS lost the race (the unit moved on concurrently,
    e.g. another dispatcher pass claimed/reconciled it first -- the caller
    simply skips it, mirroring ``_defer_unit_for_budget``).
    """
    retry_after_seconds = max(0.0, (cooldown_expiry - now).total_seconds())
    deferral = plan_rate_limit_deferral(
        retry_after_seconds=retry_after_seconds,
        attempts=unit.rate_limit_deferrals,
        first_seen_at=unit.rate_limit_first_seen_at.isoformat()
        if unit.rate_limit_first_seen_at
        else None,
        now=now,
    )

    if deferral is None:
        return _terminalize_rate_limit_exhausted(
            session, unit, now=now, log_ctx=log_ctx
        )

    claim_predicate = _cooldown_claim_predicate(now)
    # Use plan_rate_limit_deferral's OWN not_before, not cooldown_expiry
    # directly: not_before already clamps to the remaining
    # RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS wall-clock budget (review finding --
    # a far-future reset_at must not park a unit past the point the shared
    # deferral budget says to terminalize instead). Add the SAME jitter the
    # budget-defer path uses, since not_before itself carries none -- but
    # clamp the JITTERED result too (review finding, round 2): jitter added
    # on top of an ALREADY-clamped not_before can itself push available_at
    # past the wall-clock deadline. first_seen_at is the deadline's anchor.
    not_before = datetime.fromisoformat(deferral.not_before)
    first_seen_at = datetime.fromisoformat(deferral.first_seen_at)
    wall_clock_deadline = first_seen_at + timedelta(
        seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS
    )
    available_at = min(
        not_before + timedelta(seconds=random.uniform(0, float(jitter_seconds))),  # noqa: S311
        wall_clock_deadline,
    )
    result = session.execute(
        update(SyncRunUnit)
        .where(SyncRunUnit.id == unit.id, claim_predicate)
        .values(
            status=SyncRunUnitStatus.RETRYING.value,
            available_at=available_at,
            rate_limit_deferrals=deferral.attempts,
            rate_limit_first_seen_at=first_seen_at,
            error="deferred by sync cooldown guard",
            result={
                "error_category": _RATE_LIMIT_COOLDOWN_DEFERRED_CATEGORY,
                "not_before": available_at.isoformat(),
                "rate_limit_deferrals": deferral.attempts,
            },
            lease_owner=None,
            lease_expires_at=None,
            last_heartbeat_at=now,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    if int(result.rowcount or 0) == 0:
        return None
    unit.status = SyncRunUnitStatus.RETRYING.value
    unit.available_at = available_at
    unit.rate_limit_deferrals = deferral.attempts
    unit.rate_limit_first_seen_at = first_seen_at
    logger.info(
        "dispatch_sync_run.rate_limit_cooldown_deferred",
        extra={
            **log_ctx,
            "available_at": available_at.isoformat(),
            "rate_limit_deferrals": deferral.attempts,
        },
    )
    return available_at, False


def _active_budget_consumption(
    session: Any,
    *,
    now: datetime,
    budget_keys: set[str],
) -> dict[str, int]:
    consumed_by_bucket: dict[str, int] = defaultdict(int)
    if not budget_keys:
        return consumed_by_bucket
    stale_dispatch_cutoff = _stale_dispatch_cutoff(now)
    units = (
        session.query(SyncRunUnit)
        .filter(
            or_(
                (
                    (SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value)
                    & (SyncRunUnit.updated_at > stale_dispatch_cutoff)
                ),
                (
                    (SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value)
                    & (
                        SyncRunUnit.lease_expires_at.is_(None)
                        | (SyncRunUnit.lease_expires_at > now)
                    )
                ),
            )
        )
        .order_by(SyncRunUnit.id)
        .all()
    )
    for unit in units:
        try:
            ctx = SyncTaskBootstrap.load(session, str(unit.id))
            estimates = estimate_provider_budget(ctx)
        except Exception as exc:
            logger.warning(
                "dispatch_sync_run.budget_guard_active_estimate_failed",
                extra={
                    **_unit_log_context(str(unit.sync_run_id), unit),
                    "error": str(exc),
                },
            )
            continue
        for estimate in estimates:
            budget_key = _budget_key(
                estimate.bucket.to_dict(), route_family=estimate.route_family
            )
            if budget_key in budget_keys:
                consumed_by_bucket[budget_key] += estimate.estimated_units
    return consumed_by_bucket


def _acquire_budget_advisory_locks(session: Any, budget_keys: list[str]) -> None:
    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        return
    for budget_key in budget_keys:
        session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": _advisory_lock_key(budget_key)},
        )


def _advisory_lock_key(value: str) -> int:
    import hashlib

    digest = hashlib.sha256(value.encode()).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)


def _budget_limits() -> dict[str, int]:
    raw_limits = os.getenv("SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS")
    if not raw_limits:
        return {}
    try:
        parsed = json.loads(raw_limits)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    limits: dict[str, int] = {}
    for key, value in parsed.items():
        try:
            limits[str(key)] = max(0, int(value))
        except (TypeError, ValueError):
            continue
    return limits


def _enforced_budget_limits() -> dict[str, int]:
    return _parse_budget_limits(os.getenv("SYNC_BUDGET_BUCKET_LIMITS"))


def _parse_budget_limits(raw_limits: str | None) -> dict[str, int]:
    if not raw_limits:
        return {}
    try:
        parsed = json.loads(raw_limits)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    limits: dict[str, int] = {}
    for key, value in parsed.items():
        try:
            limits[str(key)] = max(0, int(value))
        except (TypeError, ValueError):
            continue
    return limits


def _limit_for_bucket(
    bucket: Mapping[str, str],
    *,
    route_family: str,
    limits: Mapping[str, int],
    default_limit: int,
) -> int:
    provider = bucket.get("provider", "")
    org_id = bucket.get("org_id", "")
    host = bucket.get("host", "")
    credential = bucket.get("credential_fingerprint", "")
    dimension = bucket.get("dimension", "")
    candidates = (
        f"{provider}:{org_id}:{host}:{credential}:{dimension}:{route_family}",
        f"{provider}:{host}:{dimension}:{route_family}",
        f"{provider}:{dimension}:{route_family}",
        f"{dimension}:{route_family}",
        f"{provider}:{org_id}:{host}:{credential}:{dimension}",
        f"{provider}:{host}:{dimension}",
        f"{provider}:{dimension}",
        dimension,
        "*",
    )
    for key in candidates:
        if key in limits:
            return limits[key]
    return default_limit


def _budget_key(bucket: Mapping[str, str], *, route_family: str) -> str:
    return ":".join(
        (
            bucket.get("provider", ""),
            bucket.get("org_id", ""),
            bucket.get("host", ""),
            bucket.get("credential_fingerprint", ""),
            bucket.get("dimension", ""),
            route_family,
        )
    )


def _stale_dispatch_cutoff(now: datetime) -> datetime:
    return now - timedelta(seconds=_env_int("SYNC_UNIT_DISPATCH_STALE_SECONDS", 900))


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return max(0, value)
