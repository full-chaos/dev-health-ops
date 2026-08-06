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
from enum import Enum
from types import MappingProxyType
from typing import Any

from sqlalchemy import func, or_, text, update

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

# --- Budget-deferral episode (CHAOS-3412) ---------------------------------
# A unit whose estimate can never fit its bucket (a HEAVY dataset planned
# over a wide initial_sync_depth) was re-deferred forever: _defer_unit_for_
# budget re-stamps RETRYING with a fresh available_at, does NOT increment
# ``attempts``, and nothing tracked the budget episode, so no exhaustion
# predicate could ever become true. That is a CONFIGURATION error, and
# configuration errors must be visible -- these caps give the budget episode
# the same count-plus-wall-clock exit the rate-limit episode has.
_BUDGET_DEFERRED_CATEGORY = "budget_deferred"
_BUDGET_DEFERRAL_EXHAUSTED_CATEGORY = "budget_deferral_exhausted"

#: Count cap: how many consecutive budget deferrals a unit may accumulate
#: before it fails loudly (``SYNC_BUDGET_MAX_DEFERRALS``).
BUDGET_MAX_DEFERRALS_DEFAULT = 10
# --- In-cycle surplus retry (CHAOS-3465) ----------------------------------
# A pass that finishes admission with budget to spare used to let that spare
# capacity lapse: units budget-deferred moments earlier sit at
# ``available_at = now + SYNC_BUDGET_DEFERRAL_SECONDS`` and are not candidates
# again until that countdown expires, however empty the bucket became in the
# meantime. The surplus phase spends the leftover on exactly those units,
# longest-deferred first.
#
# REJECTED, deliberately: persistent cross-cycle BANKING of unused budget.
# A bank lets an idle hour accumulate capacity that a later pass spends all at
# once -- the burst-then-starve behaviour this guard exists to prevent, and one
# that would also make the bucket cap describe an average rather than a
# ceiling. Surplus here is in-cycle only: it is measured from THIS pass's
# ``consumed_by_bucket`` against THIS pass's limits, and anything unspent when
# the pass returns is simply gone.
#
#: How many not-yet-due budget-deferred units one pass may CONSIDER for
#: surplus admission (``SYNC_BUDGET_SURPLUS_MAX_CANDIDATES``). Each one costs a
#: ``SyncTaskBootstrap.load`` plus credential decryption to estimate, so the
#: work a pass takes on for units it may not admit is bounded. Set to 0 to
#: disable surplus retry entirely. Truncation is logged, never silent.
BUDGET_SURPLUS_MAX_CANDIDATES_DEFAULT = 16

#: Wall-clock cap measured from ``budget_first_deferred_at``
#: (``SYNC_BUDGET_DEFERRAL_WALL_CLOCK_SECONDS``). Deliberately longer than
#: the count cap times the default 60s deferral: a run whose budget frees up
#: (a sibling finishing, an hourly bucket rolling over) should be admitted,
#: not terminalized, so only a unit that is STILL blocked after hours of
#: real elapsed time is judged permanently oversized.
BUDGET_DEFERRAL_WALL_CLOCK_SECONDS_DEFAULT = 6 * 60 * 60  # 6 hours

# Defence in depth for _budget_deferral_exhausted, mirroring
# _RATE_LIMIT_EPISODE_ERROR_CATEGORIES: the unit's own last-recorded
# result.error_category must ALSO show a budget-deferral cause before the
# exhaustion check can fire, so a row surviving a missed clear site (whose
# last real cause was 'rate_limit', 'worker_lost', 'soft_timeout', ...) is
# refused here regardless of what the counters say.
_BUDGET_EPISODE_ERROR_CATEGORIES = frozenset({_BUDGET_DEFERRED_CATEGORY})

# --- Aggregate blocked clock (CHAOS-3412 review round 2, F2) --------------
# Both per-episode budgets are, by design, reset when the OTHER episode kind
# begins. That makes them unreachable for a unit whose blocking reason keeps
# alternating -- an oversized estimate (this ticket's configuration error)
# plus recurring sibling 429 cooldowns ping-pongs the category forever, each
# stamp clearing the other's counters, and the unit sits in RETRYING exactly
# as this ticket's acceptance forbids. ``first_blocked_at`` is the outer
# bound that no episode reset can move: set once when a unit first becomes
# blocked for ANY reason, cleared only when it actually gets dispatched or
# succeeds.
_DEFERRAL_TOTAL_EXHAUSTED_CATEGORY = "deferral_exhausted"

#: Sentinel: the verdict was refused, so THIS pass still owns the unit.
_CARRY_ON = object()

#: Aggregate wall-clock cap measured from ``first_blocked_at``
#: (``SYNC_DEFERRAL_TOTAL_WALL_CLOCK_SECONDS``). Deliberately much larger
#: than either per-episode cap: this is the backstop for "blocked for a
#: reason that keeps changing", not the primary diagnosis, so a unit with a
#: single identifiable cause still fails with that cause's specific category
#: and error text long before this fires.
DEFERRAL_TOTAL_WALL_CLOCK_SECONDS_DEFAULT = 24 * 60 * 60  # 24 hours

#: Human-readable names for the episode a unit was LAST blocked by, used in
#: the aggregate-exhaustion error text so an operator is not left guessing
#: which of the two causes was most recent.
_EPISODE_KIND_BY_ERROR_CATEGORY = {
    _BUDGET_DEFERRED_CATEGORY: "sync budget admission",
    _RATE_LIMIT_COOLDOWN_DEFERRED_CATEGORY: "provider rate-limit cooldown",
    "rate_limit": "provider rate limit (in-worker 429)",
}


@dataclass(frozen=True)
class BudgetGuardResult:
    observations: list[dict[str, Any]] = field(default_factory=list)
    deferred_unit_ids: frozenset[str] = frozenset()
    next_deferred_at: datetime | None = None
    #: Units that were NOT candidates this pass (still counting down a budget
    #: deferral) but were pulled forward into it by the surplus phase
    #: (CHAOS-3465). They are claimable by ``_claim_units`` on this same pass.
    surplus_admitted_unit_ids: frozenset[str] = frozenset()
    #: Each surplus-admitted unit's ``available_at`` from BEFORE the promotion,
    #: so a later stage that must withdraw the promotion can put the unit back
    #: exactly where it was instead of leaving it due (CHAOS-3465 review). The
    #: keys are ``surplus_admitted_unit_ids``; kept as a separate mapping
    #: because the restore VALUE is what makes the withdrawal a no-op.
    surplus_prior_available_at: Mapping[str, datetime] = field(default_factory=dict)
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
        slot_headroom: Mapping[tuple[str, str, str], int] | None = None,
        now: datetime | None = None,
    ) -> BudgetGuardResult:
        """Admit or defer this pass's dispatch candidates against the budget,
        then spend whatever budget is left over retrying units an EARLIER
        deferral is still holding back (CHAOS-3465).

        ``slot_headroom`` is ``DispatchGuard``'s per-``(org_id, provider,
        cost_class)`` count of concurrency slots still free after this pass's
        own candidates. Surplus retry admits units the concurrency guard never
        saw, so without it there is no way to know whether admitting one would
        breach ``SYNC_UNIT_CONCURRENCY_PER_BUCKET``. Omitting it therefore
        DISABLES surplus retry rather than guessing -- surplus relaxes the
        budget admission and nothing else.
        """
        enforced_at = now or datetime.now(timezone.utc)
        ignored_unit_ids = {str(unit_id) for unit_id in capped_unit_ids}
        units = _dispatch_candidate_units(
            session,
            sync_run_id,
            ignored_unit_ids=ignored_unit_ids,
            now=enforced_at,
        )
        surplus_candidates = _surplus_retry_candidates(
            session,
            sync_run_id,
            ignored_unit_ids=ignored_unit_ids,
            slot_headroom=slot_headroom,
            now=enforced_at,
        )
        if not units and not surplus_candidates:
            return BudgetGuardResult()

        limits = _enforced_budget_limits()
        default_limit = _env_int("SYNC_BUDGET_DEFAULT_LIMIT", 1_000_000)
        deferral_seconds = _env_int("SYNC_BUDGET_DEFERRAL_SECONDS", 60)
        jitter_seconds = _env_int("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", 5)
        estimates_by_unit: dict[str, tuple[BudgetEstimate, ...]] = {}
        budget_keys: set[str] = set()
        observations: list[dict[str, Any]] = []

        # Surplus candidates are estimated HERE, alongside the real candidates,
        # rather than after admission: their budget keys have to join the same
        # sorted advisory-lock batch below. A second, later acquisition would
        # take locks out of order relative to a concurrent pass that already
        # holds them, which is how the sorting rule stops being a deadlock
        # defence.
        for unit in (*units, *surplus_candidates):
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
            # Surplus candidates are included so the ONE observation query per
            # pass also covers the org/provider/integration tuples only they
            # bring, and the surplus phase can gate on the same maps rather
            # than issuing a second read.
            candidates=(*units, *surplus_candidates),
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
                outcome = _resolve_cooldown_blocked_unit(
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
                # No cooldown gates this unit, so a REFUSED verdict simply
                # means "not terminalizable on this evidence" -- it falls
                # through to normal budget admission, not to a claim into an
                # active cooldown.
                outcome = _settle_or_skip(
                    _terminalize_rate_limit_exhausted(
                        session, unit, now=enforced_at, log_ctx=log_ctx
                    )
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
        # The DURABLE baseline (review round 2, R2-F1): consumption from work
        # already dispatching/running, captured BEFORE the admission loop
        # starts adding this pass's own admissions to consumed_by_bucket. Any
        # terminal verdict below is measured against this snapshot, so it is
        # a fact about the unit and the world rather than about the order the
        # candidate loop happened to visit siblings in.
        baseline_consumption: dict[str, int] = dict(consumed_by_bucket)

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
                # Exhaustion is evaluated HERE, not before admission (review
                # round 2, F1): the question is never "has this unit been
                # deferred a lot", it is "has this unit been deferred a lot
                # AND does it still not fit RIGHT NOW". A sibling finishing or
                # a bucket rolling over between passes frees capacity, and a
                # unit that would be admitted on this pass must be admitted,
                # not killed on last pass's evidence. Every observation above
                # was computed under the advisory locks this pass holds, so
                # ``would_defer`` is the current, authoritative answer.
                terminal_outcome: tuple[datetime, bool] | None = None
                unfitness = _baseline_unfitness(
                    estimates,
                    baseline_consumption=baseline_consumption,
                    limits=limits,
                    default_limit=default_limit,
                )
                if unfitness is not None and _budget_deferral_exhausted(
                    unit, now=enforced_at
                ):
                    # The episode cap may only end a unit whose misfit is
                    # real independent of this pass's optional admissions
                    # (R2-F1). A unit deferred purely because a sibling was
                    # admitted first keeps deferring -- its counter still
                    # advances, and if the contention genuinely never clears
                    # the aggregate clock below is the loud backstop.
                    terminal_outcome = _settle_or_skip(
                        _terminalize_budget_exhausted(
                            session,
                            unit,
                            now=enforced_at,
                            log_ctx=log_ctx,
                            observations=unit_observations,
                            unfitness=unfitness,
                        )
                    )
                elif _deferral_total_exhausted(unit, now=enforced_at):
                    # Checked second: a unit with one identifiable cause fails
                    # with that cause's specific category and error text; the
                    # aggregate clock is the backstop for the alternating case
                    # no single-cause cap can reach.
                    terminal_outcome = _settle_or_skip(
                        _terminalize_deferral_total_exhausted(
                            session, unit, now=enforced_at, log_ctx=log_ctx
                        )
                    )
                if terminal_outcome is not None:
                    for observation in unit_observations:
                        observation["decision"] = "exhausted"
                    observations.extend(unit_observations)
                    continue
                # Either not exhausted, or the CAS lost the race (the unit
                # moved on concurrently) — fall through and defer normally,
                # exactly as a lost _defer_unit_for_budget race does.
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

        # --- In-cycle surplus retry (CHAOS-3465) — AFTER admission, because
        # "what is left over" is only knowable once every real candidate has
        # taken its share. consumed_by_bucket now holds durable consumption
        # plus this pass's own admissions, which is exactly the baseline a
        # surplus admission must fit on top of.
        surplus_prior_available_at = _admit_surplus_retries(
            session,
            sync_run_id,
            candidates=surplus_candidates,
            estimates_by_unit=estimates_by_unit,
            consumed_by_bucket=consumed_by_bucket,
            limits=limits,
            default_limit=default_limit,
            slot_headroom=dict(slot_headroom or {}),
            cooldown_by_family=cooldown_by_family,
            cooldown_by_dimension=cooldown_by_dimension,
            observations=observations,
            now=enforced_at,
        )

        return BudgetGuardResult(
            observations=observations,
            deferred_unit_ids=frozenset(deferred_unit_ids),
            next_deferred_at=next_deferred_at,
            # Surplus-admitted units join the candidate set the caller hands to
            # reconfirm_cooldowns: they are about to be claimed on this pass,
            # so they need the same last-read-before-the-claim cooldown check
            # every other dispatched unit gets. The caller MUST also pass
            # surplus_prior_available_at, or that check will end their budget
            # episode -- see reconfirm_cooldowns.
            candidate_units=(
                *units,
                *(
                    unit
                    for unit in surplus_candidates
                    if str(unit.id) in surplus_prior_available_at
                ),
            ),
            estimates_by_unit=estimates_by_unit,
            jitter_seconds=jitter_seconds,
            surplus_admitted_unit_ids=frozenset(surplus_prior_available_at),
            surplus_prior_available_at=dict(surplus_prior_available_at),
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
        surplus_prior_available_at: Mapping[str, datetime] = MappingProxyType({}),
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
        (``_resolve_cooldown_blocked_unit``, which owns cap ordering and the
        deferral write for both call sites) -- one deferral semantics,
        reused, not a second, weaker one.

        Returns the unit ids to additionally exclude from this pass's claim
        (deferred AND terminalized -- terminalized units are already
        ``FAILED`` and would not match ``_claim_units``' predicate anyway,
        but including them keeps the exclusion set self-documenting) plus
        the earliest new ``available_at``, so the caller can fold it into
        ``next_deferred_at`` for the ``_schedule_redispatch`` re-arm.

        SURPLUS-ADMITTED UNITS ARE WITHDRAWN, NOT DEFERRED (CHAOS-3465 review,
        CRITICAL). A unit the surplus phase pulled forward is here only
        because this pass OFFERED it a slot it was not otherwise going to get.
        Running the normal cooldown deferral on it would send it through
        ``_apply_cooldown_deferral``, which -- correctly, for a unit that was
        genuinely about to dispatch -- ends the budget episode:
        ``budget_deferrals=0``, ``budget_first_deferred_at=None``, and a
        ``rate_limit_cooldown_deferred`` category. For a surplus unit that is
        a fabricated episode change: absent the surplus phase it would have
        kept counting down with its episode intact, so "the guard tried to
        help" would silently reset the CHAOS-3412 exhaustion evidence. A unit
        at 9/10 deferrals, promoted and caught each pass, would never reach
        the specific budget verdict at all.

        So the offer is simply WITHDRAWN: ``available_at`` goes back to the
        value it had before promotion and nothing else is touched -- the same
        rule the surplus phase already follows for a candidate that does not
        fit. This is the ONLY correct reading of "a surplus attempt that does
        not end in dispatch is a no-op".

        Two consequences, both deliberate. The withdrawn unit is not folded
        into ``next_deferred_at``: it is back on its ORIGINAL countdown, whose
        wakeup was armed when it was originally deferred, and re-arming from
        here would claim this pass deferred something it did not. And an
        exhaustion verdict that ``_resolve_cooldown_blocked_unit`` might have
        reached for it is not lost, only postponed to the pass where the unit
        is genuinely due -- which is exactly where it would have been reached
        had the surplus phase never looked at it.

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
            prior_available_at = surplus_prior_available_at.get(str(unit.id))
            if cooldown_expiry is not None and prior_available_at is not None:
                # Withdraw the surplus offer instead of deferring (see the
                # docstring): this unit was never going to dispatch this pass
                # without the promotion, so the promotion is undone and its
                # budget episode is left exactly as it was.
                if _withdraw_surplus_admission(
                    session,
                    unit,
                    prior_available_at=prior_available_at,
                    now=checked_at,
                    log_ctx=log_ctx,
                ):
                    excluded.add(str(unit.id))
                continue
            if cooldown_expiry is not None:
                # Same helper enforce_run uses (review round 2, R2-F2): this
                # path previously had its own copy of the ordering that also
                # omitted the aggregate check entirely, so a unit past the
                # aggregate cap could be deferred here indefinitely.
                outcome = _resolve_cooldown_blocked_unit(
                    session,
                    unit,
                    cooldown_expiry=cooldown_expiry,
                    jitter_seconds=jitter_seconds,
                    now=checked_at,
                    log_ctx=log_ctx,
                )
            elif _rate_limit_deferral_exhausted(unit, now=checked_at):
                outcome = _settle_or_skip(
                    _terminalize_rate_limit_exhausted(
                        session, unit, now=checked_at, log_ctx=log_ctx
                    )
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


# ---------------------------------------------------------------------------
# In-cycle surplus retry (CHAOS-3465)
# ---------------------------------------------------------------------------


def _budget_surplus_max_candidates() -> int:
    return _env_int(
        "SYNC_BUDGET_SURPLUS_MAX_CANDIDATES", BUDGET_SURPLUS_MAX_CANDIDATES_DEFAULT
    )


def _surplus_retry_order(unit: SyncRunUnit) -> tuple[datetime, datetime, str]:
    """Longest-deferred first.

    Ordering by ``budget_first_deferred_at`` ASCENDING is the whole point:
    surplus must make the exhaustion path RARER, not merely rearrange who
    reaches it. The unit closest to its deferral caps is the one a spare slot
    is worth most to, and serving the newest deferral first would let a
    steady trickle of fresh deferrals starve the oldest indefinitely --
    re-creating, inside one pass, the starvation the ticket is about.

    ``first_blocked_at`` breaks ties (blocked longer overall wins), then the
    id, so the order is total and reproducible. Sorted in Python rather than
    SQL because SQLite and PostgreSQL disagree on where NULLs land in an ASC
    ordering, and an ordering rule that means different things per dialect is
    not an ordering rule.
    """
    far_future = datetime.max.replace(tzinfo=timezone.utc)
    first_deferred = unit.budget_first_deferred_at
    first_blocked = unit.first_blocked_at
    return (
        _as_aware(first_deferred) if first_deferred is not None else far_future,
        _as_aware(first_blocked) if first_blocked is not None else far_future,
        str(unit.id),
    )


def _surplus_retry_candidates(
    session: Any,
    sync_run_id: str,
    *,
    ignored_unit_ids: set[str],
    slot_headroom: Mapping[tuple[str, str, str], int] | None,
    now: datetime,
) -> list[SyncRunUnit]:
    """The units a surplus may be spent on: this run's NOT-YET-DUE budget
    deferrals, longest-deferred first.

    Membership is exactly the population the ticket is about -- a unit that
    was budget-deferred, whose ``available_at`` countdown has not expired, and
    which therefore would sit out this pass no matter how empty its bucket
    became. Units already due are excluded because they are ordinary
    candidates and were handled by the admission loop.

    Three exclusions, each of them a guard surplus must not relax:

    * ``slot_headroom`` missing/empty -> no candidates at all. Without the
      concurrency guard's headroom there is no way to admit anything without
      possibly breaching the per-bucket cap, so the phase fails CLOSED.
    * FAILED units are not reachable by this query at all. A budget-exhausted
      unit stays failed: reviving it would relax the exhaustion outcome, which
      is a deliberate, operator-visible signal that a configuration is wrong
      (CHAOS-3412), not a queue state to be undone by spare capacity.
    * A unit whose own last recorded cause is NOT ``budget_deferred`` is
      skipped. A cooldown-deferred unit is waiting on the provider, not on the
      budget, and spare budget is no reason to shorten that wait.

    The category test runs in Python for the same dialect-portability reason
    as the ordering: ``result`` is a JSON column and the SQL to reach inside it
    differs per backend. A run's unit count is capped (``SYNC_RUN_MAX_UNITS``),
    so the filtered scan is bounded by construction.
    """
    if not slot_headroom:
        return []
    run_uuid = uuid.UUID(str(sync_run_id))
    deferred = [
        unit
        for unit in (
            session.query(SyncRunUnit)
            .filter(
                SyncRunUnit.sync_run_id == run_uuid,
                SyncRunUnit.status == SyncRunUnitStatus.RETRYING.value,
                SyncRunUnit.available_at.is_not(None),
                SyncRunUnit.available_at > now,
            )
            .order_by(SyncRunUnit.id)
            .all()
        )
        if str(unit.id) not in ignored_unit_ids
        and _unit_last_error_category(unit) == _BUDGET_DEFERRED_CATEGORY
    ]
    deferred.sort(key=_surplus_retry_order)
    considered = _budget_surplus_max_candidates()
    if len(deferred) > considered:
        # A silent cap reads as "surplus considered everything and nothing
        # else fitted", which is a different fact entirely.
        logger.info(
            "dispatch_sync_run.budget_surplus_candidates_truncated",
            extra={
                "sync_run_id": sync_run_id,
                "deferred_units": len(deferred),
                "considered_units": considered,
            },
        )
        deferred = deferred[:considered]
    return deferred


def _admit_surplus_retries(
    session: Any,
    sync_run_id: str,
    *,
    candidates: list[SyncRunUnit],
    estimates_by_unit: Mapping[str, tuple[BudgetEstimate, ...]],
    consumed_by_bucket: dict[str, int],
    limits: Mapping[str, int],
    default_limit: int,
    slot_headroom: dict[tuple[str, str, str], int],
    cooldown_by_family: Mapping[tuple[str, str, uuid.UUID, str], datetime],
    cooldown_by_dimension: Mapping[tuple[str, str, uuid.UUID, str], datetime],
    observations: list[dict[str, Any]],
    now: datetime,
) -> dict[str, datetime]:
    """Spend this pass's leftover budget on deferred units, in
    longest-deferred-first order.

    Returns ``{unit_id: available_at BEFORE the promotion}`` for each unit
    admitted. The prior value is returned, not merely the id, because a later
    stage may have to WITHDRAW the promotion (see
    ``reconfirm_cooldowns``), and putting the unit back exactly where it was
    is what keeps the whole surplus attempt a no-op.

    COUNTER SEMANTICS -- the decision this phase turns on:

    A surplus attempt that does NOT succeed is a complete no-op. It leaves
    ``budget_deferrals``, ``budget_first_deferred_at``, ``first_blocked_at``,
    ``available_at`` and ``result`` exactly as the deferral that put the unit
    here left them. The unit was already counted once, for that deferral;
    counting an opportunistic second look would add one deferral per pass per
    unit purely because the guard TRIED to help, dragging the exhaustion caps
    forward fastest for the units this feature exists to rescue. That is the
    "must not double-increment within a cycle" rule, resolved at its root:
    the only place the counter moves is ``_defer_unit_for_budget``, and the
    surplus phase never calls it.

    A surplus attempt that SUCCEEDS also leaves the episode columns alone. It
    writes ``available_at`` and nothing else, because the episode's end is
    already owned elsewhere and duplicating it here would create a second
    interpretation to drift: ``_claim_units`` clears ``first_blocked_at`` when
    the unit is actually claimed, and the SUCCESS stamp clears the budget
    pair when it actually completes. Rewriting ``result`` here would be worse
    than redundant -- ``_budget_deferral_exhausted``'s defence-in-depth gate
    reads ``result.error_category``, so a surplus admission that overwrote it
    would quietly disarm the exhaustion path for that unit.

    Consequence, and it is the correct one: a unit promoted but not claimed
    (it lost the claim race) is simply a due candidate next pass, and takes
    one ordinary deferral there if it no longer fits.

    WHAT SURPLUS DOES NOT RELAX. Every other guard still decides:

    * concurrency -- ``slot_headroom`` is decremented per admission, so the
      per-bucket cap binds a surplus admission exactly as it binds a candidate.
    * cooldowns -- a unit matching an active shared cooldown is skipped, left
      deferred; spare budget is not a reason to hit a limited provider.
    * total/tier unit caps -- untouched: a surplus unit is already a unit of
      this run and was counted by ``DispatchGuard.authorize_run``.
    * exhaustion -- neither relaxed nor triggered. The exhaustion predicates
      are only ever asked about a unit that would DEFER; a surplus admission
      is by construction a unit that FITS, which is the same reason the
      admission loop does not ask about a fitting candidate either (R2-F1:
      a unit that would be admitted now must be admitted, not killed).
    """
    admitted: dict[str, datetime] = {}
    for unit in candidates:
        log_ctx = _unit_log_context(sync_run_id, unit)
        estimates = estimates_by_unit.get(str(unit.id), ())
        if not estimates:
            continue

        if (cooldown_by_family or cooldown_by_dimension) and _matching_cooldown_expiry(
            estimates,
            org_id=str(unit.org_id),
            provider=str(unit.provider),
            integration_id=unit.integration_id,
            cooldown_by_family=cooldown_by_family,
            cooldown_by_dimension=cooldown_by_dimension,
        ) is not None:
            logger.info(
                "dispatch_sync_run.budget_surplus_skipped",
                extra={**log_ctx, "reason": "cooldown_active"},
            )
            continue

        slot_key = (str(unit.org_id), str(unit.provider), str(unit.cost_class))
        if slot_headroom.get(slot_key, 0) <= 0:
            logger.info(
                "dispatch_sync_run.budget_surplus_skipped",
                extra={**log_ctx, "reason": "no_concurrency_slot"},
            )
            continue

        # Fit is decided across ALL of the unit's estimates before ANY of them
        # is charged, mirroring the admission loop's whole-unit semantics: a
        # unit that fits three buckets and overflows a fourth is not admitted,
        # and must not leave three buckets charged for work that never ran.
        surplus_observations: list[dict[str, Any]] = []
        fits = True
        for estimate in estimates:
            bucket = estimate.bucket.to_dict()
            budget_key = _budget_key(bucket, route_family=estimate.route_family)
            limit = _limit_for_bucket(
                bucket,
                route_family=estimate.route_family,
                limits=limits,
                default_limit=default_limit,
            )
            projected_units = consumed_by_bucket[budget_key] + estimate.estimated_units
            if projected_units > limit:
                fits = False
            surplus_observations.append(
                {
                    **log_ctx,
                    "decision": "surplus_admitted",
                    "bucket": bucket,
                    "budget_key": budget_key,
                    "estimated_units": estimate.estimated_units,
                    "projected_units": projected_units,
                    "budget_limit": limit,
                    "confidence": estimate.confidence,
                    "route_family": estimate.route_family,
                    "budget_deferrals": int(unit.budget_deferrals or 0),
                }
            )
        if not fits:
            logger.info(
                "dispatch_sync_run.budget_surplus_skipped",
                extra={**log_ctx, "reason": "insufficient_surplus"},
            )
            continue

        # Captured BEFORE the promotion overwrites it: this is what a later
        # withdrawal restores. A candidate always has one (the selection query
        # requires ``available_at > now``), but if that ever stops holding,
        # skip rather than promote a unit we could not put back.
        prior_available_at = unit.available_at
        if prior_available_at is None:
            logger.warning(
                "dispatch_sync_run.budget_surplus_skipped",
                extra={**log_ctx, "reason": "no_prior_available_at"},
            )
            continue
        if not _admit_unit_from_surplus(session, unit, now=now, log_ctx=log_ctx):
            # CAS lost: the unit moved on concurrently. Its budget stays
            # unspent and is offered to the next candidate.
            continue

        for estimate in estimates:
            budget_key = _budget_key(
                estimate.bucket.to_dict(), route_family=estimate.route_family
            )
            consumed_by_bucket[budget_key] += estimate.estimated_units
        slot_headroom[slot_key] -= 1
        admitted[str(unit.id)] = _as_aware(prior_available_at)
        observations.extend(surplus_observations)
        for observation in surplus_observations:
            logger.info(
                "dispatch_sync_run.budget_surplus_admitted",
                extra=observation,
            )
    return admitted


def _admit_unit_from_surplus(
    session: Any,
    unit: SyncRunUnit,
    *,
    now: datetime,
    log_ctx: dict[str, Any],
) -> bool:
    """Pull a not-yet-due budget deferral forward into THIS pass.

    The whole write is ``available_at`` (plus ``updated_at``): the unit is
    already ``RETRYING``, and becoming due is the only thing that has to
    change for ``_claim_units`` to pick it up later in the same transaction.
    Status is deliberately not re-assigned -- see ``_admit_surplus_retries``
    for why the episode columns are left alone, and note that a stamp which
    DID re-assign status would owe every per-episode column a value under the
    lifecycle contract, which is precisely the semantics this must not have.

    The CAS re-asserts the exact predicate that made the unit a surplus
    candidate (retrying, and still not due), so a concurrent pass that already
    promoted or terminalized it wins and this returns ``False``. It is
    deliberately NOT claimed here as protection against a concurrent
    re-deferral: no write path in the tree can re-defer a unit that is not yet
    due -- ``_defer_unit_for_budget`` and ``_apply_cooldown_deferral`` both
    require the claim predicate (planned / due-retrying / stale-dispatching),
    and the reconciler's retry stamp targets expired-lease RUNNING units. The
    single-winner property for the dispatch that follows is owned by
    ``_claim_units``' atomic UPDATE under the bucket advisory locks, not by
    this statement.
    """
    result: Any = session.execute(
        update(SyncRunUnit)
        .where(
            SyncRunUnit.id == unit.id,
            SyncRunUnit.status == SyncRunUnitStatus.RETRYING.value,
            SyncRunUnit.available_at.is_not(None),
            SyncRunUnit.available_at > now,
        )
        .values(available_at=now, updated_at=now)
        .execution_options(synchronize_session=False)
    )
    if int(result.rowcount or 0) == 0:
        return False
    unit.available_at = now
    logger.info(
        "dispatch_sync_run.budget_surplus_pulled_forward",
        extra={
            **log_ctx,
            "budget_deferrals": int(unit.budget_deferrals or 0),
            "available_at": now.isoformat(),
        },
    )
    return True


def _withdraw_surplus_admission(
    session: Any,
    unit: SyncRunUnit,
    *,
    prior_available_at: datetime,
    now: datetime,
    log_ctx: dict[str, Any],
) -> bool:
    """Undo a surplus promotion, putting the unit back on its ORIGINAL
    countdown (CHAOS-3465 review, CRITICAL).

    The exact inverse of :func:`_admit_unit_from_surplus` and nothing more:
    ``available_at`` returns to its pre-promotion value, and every column any
    exhaustion predicate reads -- ``budget_deferrals``,
    ``budget_first_deferred_at``, ``first_blocked_at``, ``result`` -- is left
    untouched, because from the unit's point of view this pass never happened.

    That is the whole point. The alternative, routing a withdrawn unit through
    the ordinary cooldown deferral, resets the budget episode and rewrites the
    error category, so a promotion the guard offered and then took back would
    destroy the evidence CHAOS-3412's exhaustion verdict is built on.

    ``updated_at`` does move, and that is intended: the row WAS written twice,
    and an operator reading it deserves to see that rather than a row that
    silently rewound. Nothing keys off ``updated_at`` for a RETRYING unit (the
    staleness cutoff applies only to DISPATCHING).

    The CAS pins ``available_at`` to the promoted value, so if anything else
    moved the unit between the promotion and here, this leaves it alone and
    returns ``False``.
    """
    promoted_available_at = unit.available_at
    result: Any = session.execute(
        update(SyncRunUnit)
        .where(
            SyncRunUnit.id == unit.id,
            SyncRunUnit.status == SyncRunUnitStatus.RETRYING.value,
            SyncRunUnit.available_at == promoted_available_at,
        )
        .values(available_at=prior_available_at, updated_at=now)
        .execution_options(synchronize_session=False)
    )
    if int(result.rowcount or 0) == 0:
        logger.warning(
            "dispatch_sync_run.budget_surplus_withdrawal_lost_race",
            extra={**log_ctx, "prior_available_at": prior_available_at.isoformat()},
        )
        return False
    unit.available_at = prior_available_at
    logger.info(
        "dispatch_sync_run.budget_surplus_withdrawn",
        extra={
            **log_ctx,
            "reason": "cooldown_landed_after_admission",
            "restored_available_at": prior_available_at.isoformat(),
            # Proof, in the log line itself, that the episode survived the
            # round trip -- this is the value the CRITICAL finding zeroed.
            "budget_deferrals": int(unit.budget_deferrals or 0),
        },
    )
    return True


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
                "error_category": _BUDGET_DEFERRED_CATEGORY,
                "not_before": available_at.isoformat(),
                "budget_guard": observations,
            },
            # CHAOS-3412: the budget episode's own bookkeeping. Incremented
            # in SQL (not from the stale in-memory value) so concurrent
            # dispatch passes cannot both write the same count; first-seen
            # is COALESCEd so it keeps the FIRST deferral of the episode and
            # the wall-clock cap measures the whole episode, not the last lap.
            budget_deferrals=SyncRunUnit.budget_deferrals + 1,
            budget_first_deferred_at=func.coalesce(
                SyncRunUnit.budget_first_deferred_at, now
            ),
            # AGGREGATE blocked clock (F2): set-if-null, so it marks when this
            # unit FIRST went nowhere and survives every episode change after
            # that. COALESCE, not an overwrite -- an overwrite here would let
            # the alternation reset the outer bound too, which is the whole
            # defect.
            first_blocked_at=func.coalesce(SyncRunUnit.first_blocked_at, now),
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
        # Mirror the SQL-side increment/coalesce onto the in-memory row so a
        # caller reading the ORM object in the same pass sees the same state
        # the database now holds (the other columns above do this already).
        unit.budget_deferrals = int(unit.budget_deferrals or 0) + 1
        if unit.budget_first_deferred_at is None:
            unit.budget_first_deferred_at = now
        if unit.first_blocked_at is None:
            unit.first_blocked_at = now
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


# ---------------------------------------------------------------------------
# Terminalization chokepoint (CHAOS-3412 closure)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TerminalVerdict:
    """A proposal to terminally fail a unit, carrying the evidence it rests on.

    ``episode`` names the episode the verdict ASSERTS as the cause, or
    ``None`` for the aggregate backstop which asserts no single cause.
    ``evidence`` becomes the persisted ``result`` payload, so what an operator
    reads and what the decision was made on are the same object.
    ``fitness`` is required for fitness-based categories.
    """

    error_category: str
    error_text: str
    evidence: Mapping[str, Any]
    episode: str | None = None
    fitness: BudgetUnfitness | None = None


#: Episode name -> the ``result.error_category`` values that evidence it. A
#: verdict asserting an episode may only be issued for a unit whose OWN last
#: recorded cause is in that episode's set.
_EPISODE_EVIDENCE: dict[str, frozenset[str]] = {
    "rate_limit": _RATE_LIMIT_EPISODE_ERROR_CATEGORIES,
    "budget": _BUDGET_EPISODE_ERROR_CATEGORIES,
}

#: Categories whose claim is about FITNESS, and therefore may only be issued
#: with a durable-world verdict attached.
_FITNESS_BASED_CATEGORIES = frozenset({_BUDGET_DEFERRAL_EXHAUSTED_CATEGORY})

#: Phrases an error text may use only when its own evidence licenses them.
#: This is what "the text may only assert what the evidence supports" means
#: mechanically -- instances 3 and 5 below were both this failure.
_CLAIM_LICENCES: dict[str, str] = {
    "can never be admitted": "permanently_oversized",
    "alternated": "episodes_alternated",
    "kept changing": "episodes_alternated",
}


class TerminalOutcome(str, Enum):
    """What a terminalization attempt actually did.

    REFUSED and CAS_LOST were previously both ``None``, and callers read that
    single value as "lost a race, skip this unit" -- so an evidence refusal
    left a cooldown-blocked unit unstamped, ``_claim_units`` dispatched it
    despite the active cooldown, and the claim cleared ``first_blocked_at``.
    A refusal silently resetting the aggregate clock is a direct violation of
    the invariant this module exists to enforce, so the two outcomes are now
    distinct in the type, not merely in intent.
    """

    TERMINALIZED = "terminalized"
    REFUSED = "refused"
    CAS_LOST = "cas_lost"


@dataclass(frozen=True)
class TerminalDecision:
    outcome: TerminalOutcome
    at: datetime | None = None


class TerminalVerdictError(AssertionError):
    """A malformed verdict -- a defect in this module, not in the data."""


def _unit_last_error_category(unit: SyncRunUnit) -> str | None:
    result = unit.result
    if not isinstance(result, Mapping):
        return None
    category = result.get("error_category")
    return str(category) if category is not None else None


def _terminalize_unit(
    session: Any,
    unit: SyncRunUnit,
    *,
    verdict: TerminalVerdict,
    now: datetime,
    log_ctx: dict[str, Any],
) -> TerminalDecision:
    """THE single place a sync unit is terminally failed by a deferral
    exhaustion decision, and the closure of a five-instance defect class.

    THE INVARIANT
    -------------
    A unit may only be terminalized on a fact that is true of the unit and the
    DURABLE world, and its error text may only assert what its own recorded
    evidence supports.

    Three checks enforce it, and every terminal deferral stamp routes through
    them:

    (a) EPISODE-VALIDATED EVIDENCE. A verdict naming an episode is refused
        unless the unit's own last recorded cause belongs to that episode.
        Counters alone are not evidence: they outlive the episode that wrote
        them. A refusal is NOT an error -- it means "this unit's state does
        not support this claim", and the caller defers instead.
    (b) DURABLE-WORLD FITNESS. A fitness-based category must carry a
        ``BudgetUnfitness`` measured against durable consumption only, never
        against capacity taken by this pass's own optional admissions.
    (c) LICENSED CLAIMS. An error text may contain a claim phrase only when
        the evidence licenses it. Asserting "can never be admitted" about a
        unit that fits, or "the reason kept changing" about a unit with one
        cause, is a false diagnosis that costs an operator the true one.

    (b) and (c) are invariants over code this module writes, so violating them
    raises: it is a bug here, and a silent one would be indistinguishable from
    a correct verdict. (a) is a statement about run-time data and so refuses.

    THE FIVE INSTANCES THIS SUBSUMES
    --------------------------------
    Every finding across three review rounds was the same shape -- a terminal
    verdict resting on evidence weaker than the claim it made:

    1. R1-F1: exhaustion decided from deferral HISTORY before checking whether
       the unit still failed to fit, killing units on a pass where freed
       capacity would have admitted them. -> (b).
    2. R1-F2: a per-episode counter treated as evidence of "stuck", when each
       episode resets the others, so an alternating unit was never measured at
       all. -> the aggregate clock, whose verdict carries no episode and
       therefore asserts none.
    3. R1-F1 (second order): the failure text rebuilt from the PREVIOUS pass's
       persisted observation, so it named a bucket state that no longer
       existed. -> (c), plus evidence and text now coming from one object.
    4. R2-F1: fitness measured against capacity consumed by this pass's own
       earlier admissions, making a terminal verdict depend on candidate
       ORDER. -> (b).
    5. R2-F2 / R3: an episode's counters re-read WITHOUT the category guard,
       so stale capped counters from a resolved episode produced a
       wrong-category irreversible failure; and a generic explanation asserted
       over specific evidence. -> (a) and (c).

    WHY A SIXTH CANNOT SLIP IN QUIETLY
    ----------------------------------
    ``test_every_terminal_deferral_stamp_routes_through_the_chokepoint``
    derives, by AST over this module and the two worker modules, every
    ``status=FAILED`` stamp whose recorded category is a registered deferral
    exhaustion, and asserts each one is lexically inside this function. A new
    terminal path either routes through here -- and is checked -- or fails
    that test. It cannot be added quietly, which is the only property that
    makes "the class is closed" a claim rather than a hope.
    """
    _assert_verdict_wellformed(verdict)
    if verdict.episode is not None:
        licensed = _EPISODE_EVIDENCE[verdict.episode]
        last_category = _unit_last_error_category(unit)
        if last_category not in licensed:
            # (a) Refusal, not an error: the unit's own state does not
            # evidence the episode this verdict names.
            logger.warning(
                "dispatch_sync_run.terminal_verdict_refused",
                extra={
                    **log_ctx,
                    "error_category": verdict.error_category,
                    "asserted_episode": verdict.episode,
                    "unit_last_error_category": last_category,
                },
            )
            return TerminalDecision(TerminalOutcome.REFUSED)
    result: Any = session.execute(
        update(SyncRunUnit)
        .where(SyncRunUnit.id == unit.id, _cooldown_claim_predicate(now))
        .values(
            status=SyncRunUnitStatus.FAILED.value,
            error=verdict.error_text,
            result={**verdict.evidence, "error_category": verdict.error_category},
            lease_owner=None,
            lease_expires_at=None,
            last_heartbeat_at=now,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    if int(result.rowcount or 0) == 0:
        return TerminalDecision(TerminalOutcome.CAS_LOST)
    unit.status = SyncRunUnitStatus.FAILED.value
    logger.warning(
        "dispatch_sync_run.unit_terminalized",
        extra={
            **log_ctx,
            "error_category": verdict.error_category,
            "error": verdict.error_text,
            **{f"evidence_{k}": v for k, v in verdict.evidence.items()},
        },
    )
    return TerminalDecision(TerminalOutcome.TERMINALIZED, now)


def _assert_verdict_wellformed(verdict: TerminalVerdict) -> None:
    """Checks (b) and (c) -- invariants over verdicts this module builds."""
    if verdict.episode is not None and verdict.episode not in _EPISODE_EVIDENCE:
        raise TerminalVerdictError(
            f"verdict {verdict.error_category!r} names unknown episode "
            f"{verdict.episode!r}; register it in _EPISODE_EVIDENCE so the "
            "evidence check can be applied to it"
        )
    if verdict.error_category in _FITNESS_BASED_CATEGORIES and verdict.fitness is None:
        raise TerminalVerdictError(
            f"verdict {verdict.error_category!r} makes a fitness claim but "
            "carries no BudgetUnfitness measured against the durable baseline"
        )
    if not verdict.error_text.strip():
        raise TerminalVerdictError(
            f"verdict {verdict.error_category!r} has no error text; an "
            "unexplained terminal failure is the state this ticket exists to "
            "remove"
        )
    for phrase, evidence_key in _CLAIM_LICENCES.items():
        if (
            phrase in verdict.error_text
            and verdict.evidence.get(evidence_key) is not True
        ):
            raise TerminalVerdictError(
                f"verdict {verdict.error_category!r} claims {phrase!r} but its "
                f"evidence does not license it ({evidence_key}="
                f"{verdict.evidence.get(evidence_key)!r}). An error text may "
                "only assert what its own evidence supports."
            )


def _live_rate_limit_episode(unit: SyncRunUnit) -> tuple[int, datetime | None]:
    """This unit's rate-limit episode counters IF its own last recorded cause
    evidences that the episode is live -- otherwise ``(0, None)``, a FRESH
    episode.

    THE single place that decides whether persisted rate-limit counters are
    evidence (CHAOS-3412 closure, instance 5). Counters outlive the episode
    that wrote them: every production stamp that sets a non-zero
    ``rate_limit_deferrals`` co-writes a rate-limit ``error_category``, so a
    unit whose last cause is ``worker_lost`` or ``budget_deferred`` is
    carrying bookkeeping from an episode that already resolved. Reading those
    numbers as if they described the present is what let a stale capped
    counter produce an irreversible wrong-category failure.

    Every reader goes through here, so there is no second interpretation of
    the same columns to drift from this one.
    """
    if _unit_last_error_category(unit) in _RATE_LIMIT_EPISODE_ERROR_CATEGORIES:
        return int(unit.rate_limit_deferrals or 0), unit.rate_limit_first_seen_at
    return 0, None


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
    attempts, first_seen_at = _live_rate_limit_episode(unit)
    if attempts <= 0 and first_seen_at is None:
        return False
    return (
        plan_rate_limit_deferral(
            retry_after_seconds=None,
            attempts=attempts,
            first_seen_at=first_seen_at.isoformat() if first_seen_at else None,
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
) -> TerminalDecision:
    """Propose terminal failure for a spent RATE-LIMIT episode (CHAOS-2742).

    Builds the verdict; :func:`_terminalize_unit` decides. Because the verdict
    names the ``rate_limit`` episode, the chokepoint refuses it for a unit
    whose own last cause is something else -- which is what closes instance 5,
    where this function was reachable from a counter re-read that skipped the
    category guard. Returns ``None`` on refusal or a lost CAS; the caller
    defers instead.
    """
    deferrals = int(unit.rate_limit_deferrals or 0)
    return _terminalize_unit(
        session,
        unit,
        verdict=TerminalVerdict(
            error_category=_RATE_LIMIT_COOLDOWN_EXHAUSTED_CATEGORY,
            error_text="rate limit cooldown deferral budget exhausted",
            evidence={"rate_limit_deferrals": deferrals},
            episode="rate_limit",
        ),
        now=now,
        log_ctx=log_ctx,
    )


@dataclass(frozen=True)
class BudgetUnfitness:
    """Why a unit does not fit its bucket, measured against a STABLE
    baseline (review round 2, R2-F1).

    ``durable_units`` is consumption from work already DISPATCHING/RUNNING --
    it exists whether or not this dispatch pass admits anything. It
    deliberately EXCLUDES capacity taken by siblings admitted earlier in this
    same pass, because that is an artefact of candidate ordering: the same
    unit, in the same world, is fit or unfit depending on which sibling the
    loop happened to reach first. A terminal verdict may not rest on that.

    ``permanent`` is the strong case -- the unit's own estimate exceeds the
    whole bucket limit, so no amount of other work finishing can ever make
    room. Only that case may claim "can never be admitted".
    """

    budget_key: str
    estimated_units: int
    budget_limit: int
    durable_units: int
    permanent: bool


def _baseline_unfitness(
    estimates: Iterable[BudgetEstimate],
    *,
    baseline_consumption: Mapping[str, int],
    limits: Mapping[str, int],
    default_limit: int,
) -> BudgetUnfitness | None:
    """The worst way this unit fails to fit against the durable baseline, or
    ``None`` if it fits -- meaning any deferral it just took was caused by
    contention with THIS pass's own optional admissions and must never be
    grounds for terminalizing it.

    Worst = a permanent misfit ahead of a contention misfit, then the largest
    estimate, so the reported cause is the one an operator most needs.
    """
    worst: BudgetUnfitness | None = None
    for estimate in estimates:
        bucket = estimate.bucket.to_dict()
        budget_key = _budget_key(bucket, route_family=estimate.route_family)
        limit = _limit_for_bucket(
            bucket,
            route_family=estimate.route_family,
            limits=limits,
            default_limit=default_limit,
        )
        durable = int(baseline_consumption.get(budget_key, 0))
        if durable + estimate.estimated_units <= limit:
            continue
        candidate = BudgetUnfitness(
            budget_key=budget_key,
            estimated_units=estimate.estimated_units,
            budget_limit=limit,
            durable_units=durable,
            permanent=estimate.estimated_units > limit,
        )
        rank = (candidate.permanent, candidate.estimated_units)
        if worst is None or rank > (worst.permanent, worst.estimated_units):
            worst = candidate
    return worst


def _budget_deferral_exhausted(unit: SyncRunUnit, *, now: datetime) -> bool:
    """True when this unit's BUDGET-deferral episode is spent -- it has been
    deferred by the budget guard more than ``SYNC_BUDGET_MAX_DEFERRALS``
    times, or has been stuck in one continuous budget episode for longer
    than ``SYNC_BUDGET_DEFERRAL_WALL_CLOCK_SECONDS`` (CHAOS-3412).

    Structured exactly like :func:`_rate_limit_deferral_exhausted`, but it
    reads ONLY the budget columns (``budget_deferrals`` /
    ``budget_first_deferred_at``). That separation is load-bearing: the
    invariants pinned in ``tests/test_budget_guard_cooldown.py`` require a
    budget-deferred unit NOT to be terminalized off stale rate-limit
    columns, and the converse holds here too.

    A fresh unit (``budget_deferrals == 0`` and
    ``budget_first_deferred_at is None``) is never "exhausted" -- this only
    fires for a unit with genuine prior budget-deferral history.

    Defence in depth (mirroring the rate-limit predicate's round-3 finding):
    the budget pair is cleared at every SUCCESS stamp and every
    non-budget RETRYING stamp -- see the "keep or clear" contract in
    ``docs/contribute/architecture/contracts.md`` -- so an unrelated retry reason
    should never reach here with stale nonzero columns. This is a SECOND,
    independent check in case a clear site is ever missed: the unit's own
    MOST RECENTLY recorded ``result.error_category`` must also be the budget
    guard's own deferral category.

    Go-side gap this check currently covers (CHAOS-3412 landed Python-only):
    the Go sync runtime writes non-terminal stamps of its own --
    ``lease_repair.go``'s expired-lease retry and ``repository_postgres.go``'s
    release-for-retry -- and neither resets the budget pair, because Go does
    not do budget admission and did not know the pair existed. A unit that was
    budget-deferred here and then re-stamped by Go therefore comes back with
    stale nonzero columns. It is refused anyway: both Go stamps OVERWRITE
    ``result.error_category`` with their own cause (``worker_lost``,
    ``provider_unit_retryable``), so the check above rejects it. That is this
    defence-in-depth gate doing exactly the job it was added for, not a reason
    to consider the contract complete -- the Go clears are still owed, and are
    tracked with the rest of the Go parity work.

    Known, accepted consequence of that symmetry: a unit that ALTERNATES
    between a budget deferral and a rate-limit deferral resets each episode
    as the other begins, so neither episode's caps accumulate. That is the
    correct reading of the state (the reason it is being held back genuinely
    keeps changing, and neither cause is by itself permanent), and it is the
    same property the rate-limit predicate already has. The loop CHAOS-3412
    closes is the one this ticket observed and the one that is reachable by
    configuration: an estimate that can never fit, re-deferred for the same
    reason every pass.
    """
    deferrals = int(unit.budget_deferrals or 0)
    first_deferred_at = unit.budget_first_deferred_at
    if deferrals <= 0 and first_deferred_at is None:
        return False
    result = unit.result
    error_category = (
        result.get("error_category") if isinstance(result, Mapping) else None
    )
    if error_category not in _BUDGET_EPISODE_ERROR_CATEGORIES:
        return False
    if deferrals >= _budget_max_deferrals():
        return True
    if first_deferred_at is None:
        return False
    elapsed = (now - _as_aware(first_deferred_at)).total_seconds()
    return elapsed >= _budget_deferral_wall_clock_seconds()


def _deferral_total_exhausted(unit: SyncRunUnit, *, now: datetime) -> bool:
    """True when this unit has been continuously blocked -- for ANY mix of
    reasons -- for longer than ``SYNC_DEFERRAL_TOTAL_WALL_CLOCK_SECONDS``
    (CHAOS-3412 review round 2, F2).

    Reads ONLY ``first_blocked_at``. It deliberately does not look at any
    per-episode column, and does not gate on ``result.error_category``: the
    whole point is that the category KEEPS CHANGING, so cross-reading episode
    state is exactly what makes the alternating case unreachable. The pinned
    invariants in ``tests/test_budget_guard_cooldown.py`` (a budget-deferred
    unit must not be terminalized off stale rate-limit columns, and the
    converse) are unaffected, because this predicate reads neither pair.

    What replaces the error-category gate is WHERE this is evaluated: only at
    the moment the guard is about to defer the unit AGAIN, on this pass, for a
    reason it has just re-established. A unit that would be admitted now is
    never asked this question (F1), so a stale timestamp cannot terminalize a
    unit that is no longer blocked.

    A unit that has never been blocked (``first_blocked_at is None``) is never
    exhausted.
    """
    first_blocked_at = unit.first_blocked_at
    if first_blocked_at is None:
        return False
    elapsed = (now - _as_aware(first_blocked_at)).total_seconds()
    return elapsed >= _deferral_total_wall_clock_seconds()


def _deferral_total_wall_clock_seconds() -> int:
    return max(
        1,
        _env_int(
            "SYNC_DEFERRAL_TOTAL_WALL_CLOCK_SECONDS",
            DEFERRAL_TOTAL_WALL_CLOCK_SECONDS_DEFAULT,
        ),
    )


def _last_episode_kind(unit: SyncRunUnit) -> str:
    result = unit.result
    error_category = (
        result.get("error_category") if isinstance(result, Mapping) else None
    )
    return _EPISODE_KIND_BY_ERROR_CATEGORY.get(
        str(error_category), f"unknown ({error_category!r})"
    )


def _terminalize_deferral_total_exhausted(
    session: Any,
    unit: SyncRunUnit,
    *,
    now: datetime,
    log_ctx: dict[str, Any],
) -> TerminalDecision:
    """Propose terminal failure for a unit past the AGGREGATE clock, whatever
    mix of reasons kept it there (CHAOS-3412 F2).

    The verdict names NO episode: this decision deliberately asserts that no
    single cause explains the block, so it must not be validated against one.
    Its text is built from the counters it carries -- it claims alternation
    only when both are non-zero, which the chokepoint's licence check then
    enforces independently.
    """
    budget_deferrals = int(unit.budget_deferrals or 0)
    rate_limit_deferrals = int(unit.rate_limit_deferrals or 0)
    first_blocked_at = unit.first_blocked_at
    blocked_seconds = (
        int((now - _as_aware(first_blocked_at)).total_seconds())
        if first_blocked_at is not None
        else 0
    )
    alternated = budget_deferrals > 0 and rate_limit_deferrals > 0
    cause = (
        "The blocking reason alternated between sync budget admission and "
        "provider rate limiting, so no single-cause cap applied"
        if alternated
        else "It stayed blocked without any single-cause cap being reached"
    )
    error_text = (
        f"sync unit blocked for {blocked_seconds // 3600}h without ever "
        f"running; last blocked by {_last_episode_kind(unit)} "
        f"(budget deferrals: {budget_deferrals}, rate-limit deferrals: "
        f"{rate_limit_deferrals}). {cause}. Remedies: run a scoped backfill "
        "over a narrower window, raise this bucket's cap via "
        "SYNC_BUDGET_BUCKET_LIMITS, or reduce concurrent load on the provider "
        "so cooldowns stop recurring."
    )
    return _terminalize_unit(
        session,
        unit,
        verdict=TerminalVerdict(
            error_category=_DEFERRAL_TOTAL_EXHAUSTED_CATEGORY,
            error_text=error_text,
            evidence={
                "budget_deferrals": budget_deferrals,
                "rate_limit_deferrals": rate_limit_deferrals,
                "first_blocked_at": (
                    _as_aware(first_blocked_at).isoformat()
                    if first_blocked_at is not None
                    else None
                ),
                "blocked_seconds": blocked_seconds,
                "last_episode": _last_episode_kind(unit),
                "episodes_alternated": alternated,
            },
            episode=None,
        ),
        now=now,
        log_ctx=log_ctx,
    )


def _budget_max_deferrals() -> int:
    return max(1, _env_int("SYNC_BUDGET_MAX_DEFERRALS", BUDGET_MAX_DEFERRALS_DEFAULT))


def _budget_deferral_wall_clock_seconds() -> int:
    return max(
        1,
        _env_int(
            "SYNC_BUDGET_DEFERRAL_WALL_CLOCK_SECONDS",
            BUDGET_DEFERRAL_WALL_CLOCK_SECONDS_DEFAULT,
        ),
    )


def _blocking_budget_observation(
    observations: Iterable[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    """The observation from THIS pass that actually blocked the unit -- the
    one whose projected units exceeded its bucket limit.

    Takes the current pass's observations rather than reading the unit's
    persisted ``result['budget_guard']`` (review round 2, F1): now that
    exhaustion is decided after the live fit check, the explanation must come
    from the same evaluation that made the decision, not from whatever the
    previous pass happened to record. Falls back to the largest estimate when
    no entry carries an explicit would-defer decision.
    """
    entries = [entry for entry in observations if isinstance(entry, Mapping)]
    if not entries:
        return None
    blocking = [
        entry
        for entry in entries
        if entry.get("decision") in {"would_defer", "deferred", "exhausted"}
    ]
    candidates = blocking or entries
    return max(candidates, key=lambda entry: int(entry.get("estimated_units") or 0))


def _budget_exhaustion_error_text(
    unit: SyncRunUnit,
    *,
    deferrals: int,
    unfitness: BudgetUnfitness,
) -> str:
    """Operator-actionable failure text: WHAT could not fit, BY HOW MUCH,
    over WHAT window, and what an operator can actually do.

    The "can never be admitted" claim is emitted ONLY when it is literally
    true -- the estimate alone exceeds the whole bucket cap (review round 2,
    R2-F1). When the unit is instead blocked by sustained consumption from
    work already running, the text says exactly that, because telling an
    operator a unit can never run when raising nothing and waiting would let
    it run is a false diagnosis that costs them the real one.
    """
    span_days = _window_span_days(unit)
    head = (
        f"sync budget deferral exhausted after {deferrals} deferrals: dataset "
        f"'{unit.dataset_key}' estimates {unfitness.estimated_units} units "
        f"against bucket '{unfitness.budget_key}' whose cap is "
        f"{unfitness.budget_limit}, over a {span_days}-day window"
    )
    if unfitness.permanent:
        middle = ", so it can never be admitted and was re-deferred instead of running"
    else:
        middle = (
            f", and {unfitness.durable_units} units of that cap are held by sync "
            "work already running. The contention has not cleared for the whole "
            "deferral budget, so the unit was re-deferred instead of running"
        )
    return (
        f"{head}{middle}. Remedies: run a scoped backfill over a narrower "
        "window, or raise this bucket's cap via SYNC_BUDGET_BUCKET_LIMITS."
    )


def _window_span_days(unit: SyncRunUnit) -> int:
    since_at = unit.since_at
    before_at = unit.before_at
    if since_at is None or before_at is None:
        return 0
    return max(0, (_as_aware(before_at) - _as_aware(since_at)).days)


def _terminalize_budget_exhausted(
    session: Any,
    unit: SyncRunUnit,
    *,
    now: datetime,
    log_ctx: dict[str, Any],
    observations: Iterable[Mapping[str, Any]],
    unfitness: BudgetUnfitness,
) -> TerminalDecision:
    """Propose terminal failure for a spent BUDGET episode whose misfit holds
    against the durable baseline (CHAOS-3412).

    Names the ``budget`` episode, so the chokepoint refuses it for a unit
    whose own last cause is not a budget deferral; and carries ``unfitness``,
    without which the chokepoint rejects the fitness claim outright.
    """
    unit_observations = list(observations)
    deferrals = int(unit.budget_deferrals or 0)
    return _terminalize_unit(
        session,
        unit,
        verdict=TerminalVerdict(
            error_category=_BUDGET_DEFERRAL_EXHAUSTED_CATEGORY,
            error_text=_budget_exhaustion_error_text(
                unit, deferrals=deferrals, unfitness=unfitness
            ),
            evidence={
                "budget_deferrals": deferrals,
                "budget_key": unfitness.budget_key,
                "estimated_units": unfitness.estimated_units,
                "budget_limit": unfitness.budget_limit,
                "durable_units": unfitness.durable_units,
                "permanently_oversized": unfitness.permanent,
                "budget_first_deferred_at": (
                    _as_aware(unit.budget_first_deferred_at).isoformat()
                    if unit.budget_first_deferred_at is not None
                    else None
                ),
                "budget_guard": [
                    dict(observation)
                    for observation in [_blocking_budget_observation(unit_observations)]
                    if observation
                ],
            },
            episode="budget",
            fitness=unfitness,
        ),
        now=now,
        log_ctx=log_ctx,
    )


def _plan_cooldown_deferral(
    unit: SyncRunUnit, *, cooldown_expiry: datetime, now: datetime
) -> Any:
    """The rate-limit deferral plan for a cooldown-gated unit, or ``None``
    when the shared rate-limit deferral budget is spent.

    Extracted from :func:`_apply_cooldown_deferral` (review round 2, R2-F2)
    so that exactly ONE place computes it and
    :func:`_resolve_cooldown_blocked_unit` can consult the episode-specific
    verdict BEFORE the aggregate backstop, without a second copy of the
    planning call drifting from this one.

    Reads the counters through :func:`_live_rate_limit_episode` (closure,
    instance 5): this function previously re-read them raw, so stale capped
    counters from a RESOLVED episode made it return ``None`` and terminalize
    the unit under a category its own state did not evidence. With the
    normalizer, an unevidenced episode plans as a FRESH one and the unit
    defers instead -- counters restarting from this genuine block.
    """
    attempts, first_seen_at = _live_rate_limit_episode(unit)
    return plan_rate_limit_deferral(
        retry_after_seconds=max(0.0, (cooldown_expiry - now).total_seconds()),
        attempts=attempts,
        first_seen_at=first_seen_at.isoformat() if first_seen_at else None,
        now=now,
    )


def _settle_or_skip(decision: TerminalDecision) -> tuple[datetime, bool] | None:
    """For call sites where the unit is NOT held by an active cooldown, so a
    refusal and a lost race have the same safe consequence: leave the unit to
    the pass's normal handling (budget admission / the claim), which stamps it
    either way. Distinct from :func:`_settle_terminal_decision`, which the
    cooldown path needs because there a refusal must NOT fall through to a
    claim."""
    if decision.outcome is TerminalOutcome.TERMINALIZED:
        return _terminalized_at(decision), True
    return None


def _terminalized_at(decision: TerminalDecision) -> datetime:
    """The timestamp a TERMINALIZED decision must carry. Missing it means the
    chokepoint wrote a row without recording when -- fail loudly rather than
    coercing, since a silently-None stamp reads as a successful write."""
    if decision.at is None:
        raise TerminalVerdictError(
            "TERMINALIZED decision carries no timestamp; the write happened "
            "but the caller cannot report when"
        )
    return decision.at


def _settle_terminal_decision(
    decision: TerminalDecision,
) -> tuple[datetime, bool] | None | object:
    """Map a chokepoint decision onto the caller contract, keeping REFUSED
    distinct from CAS_LOST.

    Returns ``(at, True)`` when written, ``None`` on a genuine lost race (the
    unit moved on, so another pass owns it), or :data:`_CARRY_ON` when the
    verdict was refused -- meaning THIS pass still owns the unit and must
    stamp it some other way rather than leaving it for ``_claim_units``.
    """
    if decision.outcome is TerminalOutcome.TERMINALIZED:
        return _terminalized_at(decision), True
    if decision.outcome is TerminalOutcome.CAS_LOST:
        return None
    return _CARRY_ON


def _resolve_cooldown_blocked_unit(
    session: Any,
    unit: SyncRunUnit,
    *,
    cooldown_expiry: datetime,
    jitter_seconds: int,
    now: datetime,
    log_ctx: dict[str, Any],
) -> tuple[datetime, bool] | None:
    """THE decision for a unit gated by an active shared cooldown -- one
    implementation, called by BOTH ``enforce_run`` and
    ``reconfirm_cooldowns`` (review round 2, R2-F2).

    Those two paths previously each carried their own hand-kept copy of this
    ordering, and had already drifted in both directions: ``enforce_run``
    checked the aggregate cap before the episode cap, and
    ``reconfirm_cooldowns`` did not check the aggregate cap at all. Two copies
    of an ordering rule is the same disease as any other duplicated invariant.

    The ordering, which is the actual contract:

    1. EPISODE-SPECIFIC caps first, always. A unit with one identifiable cause
       must fail with THAT cause's category and error text.
    2. The AGGREGATE clock second, as the backstop for the case no
       single-cause cap can see.
    3. Otherwise defer.

    A REFUSED verdict never falls out of this function as ``None``. The unit
    is blocked by a live cooldown right now, so it must leave here stamped:
    refusal drops through to a FRESH deferral, which is what the closure
    promises for unevidenced counters and what keeps ``first_blocked_at``
    intact. Returning ``None`` would let ``_claim_units`` dispatch it straight
    into the cooldown and reset the aggregate clock on the way.
    """
    refused = False

    # 1a. Episode cap from the unit's own persisted state.
    if _rate_limit_deferral_exhausted(unit, now=now):
        settled = _settle_terminal_decision(
            _terminalize_rate_limit_exhausted(session, unit, now=now, log_ctx=log_ctx)
        )
        if settled is not _CARRY_ON:
            return settled  # type: ignore[return-value]
        refused = True

    # 1b. Episode cap as the shared planner sees it. Skipped after a refusal:
    # the planner reads the same counters the refusal just rejected.
    deferral = (
        None
        if refused
        else _plan_cooldown_deferral(unit, cooldown_expiry=cooldown_expiry, now=now)
    )
    if not refused and deferral is None:
        settled = _settle_terminal_decision(
            _terminalize_rate_limit_exhausted(session, unit, now=now, log_ctx=log_ctx)
        )
        if settled is not _CARRY_ON:
            return settled  # type: ignore[return-value]
        refused = True

    # 2. Aggregate backstop.
    if _deferral_total_exhausted(unit, now=now):
        settled = _settle_terminal_decision(
            _terminalize_deferral_total_exhausted(
                session, unit, now=now, log_ctx=log_ctx
            )
        )
        if settled is not _CARRY_ON:
            return settled  # type: ignore[return-value]

    # 3. Defer. After a refusal this is a FRESH episode: the counters that
    # were refused as evidence are not reused as a starting point either.
    if deferral is None:
        deferral = plan_rate_limit_deferral(
            retry_after_seconds=max(0.0, (cooldown_expiry - now).total_seconds()),
            attempts=0,
            first_seen_at=None,
            now=now,
        )
    if deferral is None:
        # A fresh plan is never exhausted; if that ever changes, fail loudly
        # rather than silently leaving a cooldown-blocked unit claimable.
        raise TerminalVerdictError(
            "fresh rate-limit deferral plan came back exhausted; a "
            "cooldown-blocked unit would be left unstamped"
        )
    return _apply_cooldown_deferral(
        session,
        unit,
        deferral=deferral,
        jitter_seconds=jitter_seconds,
        now=now,
        log_ctx=log_ctx,
    )


def _apply_cooldown_deferral(
    session: Any,
    unit: SyncRunUnit,
    *,
    deferral: Any,
    jitter_seconds: int,
    now: datetime,
    log_ctx: dict[str, Any],
) -> tuple[datetime, bool] | None:
    """Write the cooldown deferral stamp for a unit whose plan is already
    known to be live (CHAOS-2760). Callers reach this only through
    :func:`_resolve_cooldown_blocked_unit`, which owns the cap ordering.

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
            # CHAOS-3412 episode symmetry: a cooldown deferral is a
            # RATE-LIMIT episode, not a budget one -- clear the budget pair,
            # exactly as _defer_unit_for_budget clears this pair.
            budget_deferrals=0,
            budget_first_deferred_at=None,
            # AGGREGATE blocked clock (F2): set-if-null, so it marks when this
            # unit FIRST went nowhere and survives every episode change after
            # that. COALESCE, not an overwrite -- an overwrite here would let
            # the alternation reset the outer bound too, which is the whole
            # defect.
            first_blocked_at=func.coalesce(SyncRunUnit.first_blocked_at, now),
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
    unit.budget_deferrals = 0
    unit.budget_first_deferred_at = None
    if unit.first_blocked_at is None:
        unit.first_blocked_at = now
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
