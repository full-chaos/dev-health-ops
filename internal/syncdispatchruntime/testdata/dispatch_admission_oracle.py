#!/usr/bin/env python3
"""Live-Python oracle for dispatch_sync_run's BudgetGuard admission math.

Emits what the REAL production functions in
``dev_health_ops.sync.budget_guard`` return for a fixed table of inputs, as
stable JSON on stdout:

- ``_observe_estimate`` (which itself calls ``_budget_key`` and
  ``_limit_for_bucket``) -- the single-bucket fit-or-defer decision every
  admission and dry-run observation is built from.
- ``_baseline_unfitness`` -- which estimate (if any) is why a deferred unit
  may be terminalized, measured against a durable baseline.
- ``_cooldown_expiry`` -- when one rate-limit observation's cooldown lifts.
- ``_matching_cooldown_expiry`` -- whether a unit's estimates land in an
  active shared cooldown, and which expiry (the latest) applies.

The Go side (native_dispatch_sync_run_oracle_test.go) executes this script
and diffs its own observeEstimate/baselineUnfitness/cooldownExpiry/
matchingCooldownExpiry against it -- per AGENTS.md's live-python-oracle
mandate, this is a DIFFERENTIAL check against the actual producer, not a
hand-authored fixture that could drift from what budget_guard.py really
does.

Importing dev_health_ops.sync.budget_guard transitively reaches
dev_health_ops.workers.sync_bootstrap, which -- like sync_units.py in the
finalize_sync_run oracle -- can have real side effects on stdout
(instrumentation init). Matching that oracle's own precedent, the import
happens with stdout redirected to /dev/null, and only the final JSON is
written to the real stdout afterwards.
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any


def _iso(value: datetime) -> str:
    return value.isoformat()


def main() -> int:
    with open(os.devnull, "w") as devnull:
        with contextlib.redirect_stdout(devnull):
            from dev_health_ops.models import ProviderRateLimitObservation
            from dev_health_ops.sync.budget_guard import (
                _baseline_unfitness,
                _budget_key,
                _cooldown_expiry,
                _matching_cooldown_expiry,
                _observe_estimate,
            )
            from dev_health_ops.sync.budget_types import (
                BudgetBucketKey,
                BudgetDimension,
                BudgetEstimate,
            )

    observed_at = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    org_a = str(uuid.UUID(int=1))
    integration_a = uuid.UUID(int=2)
    integration_b = uuid.UUID(int=3)

    def bucket(
        provider="github",
        org_id=org_a,
        host="api.github.com",
        credential="fp-1",
        dimension=BudgetDimension.REST_CORE,
    ) -> BudgetBucketKey:
        return BudgetBucketKey(
            provider=provider,
            org_id=org_id,
            host=host,
            credential_fingerprint=credential,
            dimension=dimension,
        )

    def estimate(
        estimated_units,
        route_family="core",
        confidence="high",
        **bucket_kwargs,
    ) -> BudgetEstimate:
        return BudgetEstimate(
            bucket=bucket(**bucket_kwargs),
            estimated_units=estimated_units,
            confidence=confidence,
            route_family=route_family,
        )

    # ------------------------------------------------------------------
    # observe_estimate (+ budget_key, limit_for_bucket transitively)
    # ------------------------------------------------------------------
    observe_cases: list[dict[str, Any]] = []

    def add_observe_case(
        name: str,
        est: BudgetEstimate,
        *,
        consumed_by_bucket: dict[str, int],
        limits: dict[str, int],
        default_limit: int,
        deferral_seconds: int = 60,
        record_consumption: bool = True,
    ) -> None:
        # A fresh copy so record_consumption's in-place mutation of one case
        # never bleeds into the next -- a real defaultdict(int), matching
        # production (enforce_run / _active_budget_consumption both
        # construct consumed_by_bucket that way; _observe_estimate itself
        # indexes it with a plain `[budget_key]`, not `.get(...)`).
        consumed_copy = defaultdict(int, consumed_by_bucket)
        result = _observe_estimate(
            est,
            log_ctx={},
            consumed_by_bucket=consumed_copy,
            limits=limits,
            default_limit=default_limit,
            observed_at=observed_at,
            deferral_seconds=deferral_seconds,
            record_consumption=record_consumption,
        )
        observe_cases.append(
            {
                "name": name,
                "estimated_units": est.estimated_units,
                "route_family": est.route_family,
                "bucket": est.bucket.to_dict(),
                "consumed_by_bucket": consumed_by_bucket,
                "limits": limits,
                "default_limit": default_limit,
                "deferral_seconds": deferral_seconds,
                "budget_key": result["budget_key"],
                "budget_limit": result["budget_limit"],
                "projected_units": result["projected_units"],
                "decision": result["decision"],
                "suggested_available_at": result["suggested_available_at"],
                "consumed_after": consumed_copy.get(result["budget_key"], 0),
            }
        )

    default_key = _budget_key(bucket().to_dict(), route_family="core")

    add_observe_case(
        "no limits configured, under default -> would_allow",
        estimate(10),
        consumed_by_bucket={},
        limits={},
        default_limit=1_000_000,
    )
    add_observe_case(
        "no limits configured, over default -> would_defer",
        estimate(2_000_000),
        consumed_by_bucket={},
        limits={},
        default_limit=1_000_000,
    )
    add_observe_case(
        "exact projected == limit is ALLOWED (> not >=)",
        estimate(100),
        consumed_by_bucket={default_key: 0},
        limits={default_key: 100},
        default_limit=1_000_000,
    )
    add_observe_case(
        "projected one over limit -> would_defer",
        estimate(101),
        consumed_by_bucket={default_key: 0},
        limits={default_key: 100},
        default_limit=1_000_000,
    )
    add_observe_case(
        "prior consumption pushes an otherwise-fitting estimate over",
        estimate(10),
        consumed_by_bucket={default_key: 95},
        limits={default_key: 100},
        default_limit=1_000_000,
    )
    add_observe_case(
        "most-specific candidate key wins over a less specific one",
        estimate(10),
        consumed_by_bucket={},
        limits={
            default_key: 5,
            f"{'github'}:{'rest_core'}": 1_000,
        },
        default_limit=1,
    )
    add_observe_case(
        "falls through most-specific keys to dimension:route_family",
        estimate(10),
        consumed_by_bucket={},
        limits={"rest_core:core": 5},
        default_limit=1_000_000,
    )
    add_observe_case(
        "falls through to the bare dimension key",
        estimate(10),
        consumed_by_bucket={},
        limits={"rest_core": 5},
        default_limit=1_000_000,
    )
    add_observe_case(
        "falls through all the way to the wildcard",
        estimate(10),
        consumed_by_bucket={},
        limits={"*": 5},
        default_limit=1_000_000,
    )
    add_observe_case(
        "record_consumption=false leaves the caller's map untouched",
        estimate(10),
        consumed_by_bucket={default_key: 50},
        limits={default_key: 1_000},
        default_limit=1_000_000,
        record_consumption=False,
    )
    add_observe_case(
        "different dimension changes the budget key and limit lookup",
        estimate(10, dimension=BudgetDimension.GRAPHQL_COST),
        consumed_by_bucket={},
        limits={
            _budget_key(
                bucket(dimension=BudgetDimension.GRAPHQL_COST).to_dict(),
                route_family="core",
            ): 2,
        },
        default_limit=1_000_000,
    )

    # ------------------------------------------------------------------
    # baseline_unfitness
    # ------------------------------------------------------------------
    unfitness_cases: list[dict[str, Any]] = []

    def add_unfitness_case(
        name: str,
        estimates: list[BudgetEstimate],
        *,
        baseline_consumption: dict[str, int],
        limits: dict[str, int],
        default_limit: int,
    ) -> None:
        result = _baseline_unfitness(
            estimates,
            baseline_consumption=baseline_consumption,
            limits=limits,
            default_limit=default_limit,
        )
        unfitness_cases.append(
            {
                "name": name,
                "estimates": [
                    {
                        "estimated_units": e.estimated_units,
                        "route_family": e.route_family,
                        "bucket": e.bucket.to_dict(),
                    }
                    for e in estimates
                ],
                "baseline_consumption": baseline_consumption,
                "limits": limits,
                "default_limit": default_limit,
                "result": None
                if result is None
                else {
                    "budget_key": result.budget_key,
                    "estimated_units": result.estimated_units,
                    "budget_limit": result.budget_limit,
                    "durable_units": result.durable_units,
                    "permanent": result.permanent,
                },
            }
        )

    fits_key = _budget_key(bucket().to_dict(), route_family="core")
    add_unfitness_case(
        "fits the baseline -> None",
        [estimate(10)],
        baseline_consumption={fits_key: 0},
        limits={fits_key: 100},
        default_limit=1_000_000,
    )
    add_unfitness_case(
        "contention misfit: fits alone, not against durable baseline",
        [estimate(10)],
        baseline_consumption={fits_key: 95},
        limits={fits_key: 100},
        default_limit=1_000_000,
    )
    add_unfitness_case(
        "permanent misfit: the estimate alone exceeds the limit",
        [estimate(200)],
        baseline_consumption={fits_key: 0},
        limits={fits_key: 100},
        default_limit=1_000_000,
    )
    add_unfitness_case(
        "boundary: durable + estimated == limit still fits (<=, not <)",
        [estimate(100)],
        baseline_consumption={fits_key: 0},
        limits={fits_key: 100},
        default_limit=1_000_000,
    )
    permanent_key = _budget_key(
        bucket(dimension=BudgetDimension.GRAPHQL_COST).to_dict(), route_family="core"
    )
    add_unfitness_case(
        "permanent misfit outranks a larger contention misfit",
        [
            estimate(50, dimension=BudgetDimension.GRAPHQL_COST),  # permanent: 50 > 10
            estimate(
                1_000_000
            ),  # contention: huge estimate but still <= its own limit? no, force contention below
        ],
        baseline_consumption={permanent_key: 0, fits_key: 999_990},
        limits={permanent_key: 10, fits_key: 1_000_000},
        default_limit=1_000_000,
    )
    add_unfitness_case(
        "two contention misfits: the larger estimate wins",
        [
            estimate(20, dimension=BudgetDimension.SEARCH),
            estimate(50, dimension=BudgetDimension.CONTENTS_BLOB),
        ],
        baseline_consumption={
            _budget_key(
                bucket(dimension=BudgetDimension.SEARCH).to_dict(), route_family="core"
            ): 90,
            _budget_key(
                bucket(dimension=BudgetDimension.CONTENTS_BLOB).to_dict(),
                route_family="core",
            ): 90,
        },
        limits={
            _budget_key(
                bucket(dimension=BudgetDimension.SEARCH).to_dict(), route_family="core"
            ): 100,
            _budget_key(
                bucket(dimension=BudgetDimension.CONTENTS_BLOB).to_dict(),
                route_family="core",
            ): 100,
        },
        default_limit=1_000_000,
    )

    # ------------------------------------------------------------------
    # cooldown_expiry
    # ------------------------------------------------------------------
    expiry_cases: list[dict[str, Any]] = []

    def observation(
        reset_at=None, retry_after_seconds=None, observed=observed_at
    ) -> ProviderRateLimitObservation:
        return ProviderRateLimitObservation(
            org_id=org_a,
            provider="github",
            integration_id=integration_a,
            sync_run_id=uuid.uuid4(),
            sync_run_unit_id=uuid.uuid4(),
            reset_at=reset_at,
            retry_after_seconds=retry_after_seconds,
            observed_at=observed,
        )

    def add_expiry_case(name: str, obs: ProviderRateLimitObservation) -> None:
        result = _cooldown_expiry(obs)
        expiry_cases.append(
            {
                "name": name,
                "reset_at": None if obs.reset_at is None else _iso(obs.reset_at),
                "retry_after_seconds": obs.retry_after_seconds,
                "observed_at": _iso(obs.observed_at),
                "expiry": _iso(result),
            }
        )

    add_expiry_case(
        "reset_at wins over retry_after_seconds when both are set",
        observation(
            reset_at=observed_at + timedelta(hours=1),
            retry_after_seconds=30,
        ),
    )
    add_expiry_case(
        "retry_after_seconds used when reset_at is absent",
        observation(retry_after_seconds=45),
    )
    add_expiry_case(
        "negative retry_after_seconds is clamped to zero",
        observation(retry_after_seconds=-30),
    )
    add_expiry_case(
        "neither set -> the fixed default countdown",
        observation(),
    )

    # ------------------------------------------------------------------
    # matching_cooldown_expiry
    # ------------------------------------------------------------------
    matching_cases: list[dict[str, Any]] = []

    def add_matching_case(
        name: str,
        estimates: list[BudgetEstimate],
        *,
        org_id: str,
        provider: str,
        integration_id: uuid.UUID,
        family_cooldowns: dict[tuple[str, str, str, str], datetime],
        dimension_cooldowns: dict[tuple[str, str, str, str], datetime],
    ) -> None:
        by_family = {
            (key[0], key[1], uuid.UUID(key[2]), key[3]): value
            for key, value in family_cooldowns.items()
        }
        by_dimension = {
            (key[0], key[1], uuid.UUID(key[2]), key[3]): value
            for key, value in dimension_cooldowns.items()
        }
        result = _matching_cooldown_expiry(
            estimates,
            org_id=org_id,
            provider=provider,
            integration_id=integration_id,
            cooldown_by_family=by_family,
            cooldown_by_dimension=by_dimension,
        )
        matching_cases.append(
            {
                "name": name,
                "org_id": org_id,
                "provider": provider,
                "integration_id": str(integration_id),
                "family_cooldowns": {
                    "|".join(k): _iso(v) for k, v in family_cooldowns.items()
                },
                "dimension_cooldowns": {
                    "|".join(k): _iso(v) for k, v in dimension_cooldowns.items()
                },
                "estimates": [
                    {
                        "route_family": e.route_family,
                        "dimension": e.bucket.dimension.value,
                    }
                    for e in estimates
                ],
                "expiry": None if result is None else _iso(result),
            }
        )

    add_matching_case(
        "no cooldown active -> None",
        [estimate(10)],
        org_id=org_a,
        provider="github",
        integration_id=integration_a,
        family_cooldowns={},
        dimension_cooldowns={},
    )
    family_expiry = observed_at + timedelta(minutes=5)
    add_matching_case(
        "family cooldown matches -> that expiry",
        [estimate(10, route_family="core")],
        org_id=org_a,
        provider="github",
        integration_id=integration_a,
        family_cooldowns={(org_a, "github", str(integration_a), "core"): family_expiry},
        dimension_cooldowns={},
    )
    dimension_expiry = observed_at + timedelta(minutes=10)
    add_matching_case(
        "dimension cooldown matches -> that expiry",
        [estimate(10, dimension=BudgetDimension.REST_CORE)],
        org_id=org_a,
        provider="github",
        integration_id=integration_a,
        family_cooldowns={},
        dimension_cooldowns={
            (org_a, "github", str(integration_a), "rest_core"): dimension_expiry
        },
    )
    add_matching_case(
        "both match -> the LATEST expiry wins, not the first",
        [estimate(10, route_family="core", dimension=BudgetDimension.REST_CORE)],
        org_id=org_a,
        provider="github",
        integration_id=integration_a,
        family_cooldowns={(org_a, "github", str(integration_a), "core"): family_expiry},
        dimension_cooldowns={
            (org_a, "github", str(integration_a), "rest_core"): dimension_expiry
        },
    )
    add_matching_case(
        "a cooldown under a DIFFERENT integration never matches",
        [estimate(10, route_family="core")],
        org_id=org_a,
        provider="github",
        integration_id=integration_a,
        family_cooldowns={(org_a, "github", str(integration_b), "core"): family_expiry},
        dimension_cooldowns={},
    )
    add_matching_case(
        "multiple estimates: only the matching one contributes",
        [estimate(10, route_family="core"), estimate(5, route_family="graphql")],
        org_id=org_a,
        provider="github",
        integration_id=integration_a,
        family_cooldowns={
            (org_a, "github", str(integration_a), "graphql"): family_expiry
        },
        dimension_cooldowns={},
    )
    earlier_family_expiry = observed_at + timedelta(minutes=2)
    later_family_expiry = observed_at + timedelta(minutes=20)
    add_matching_case(
        "two DIFFERENT estimates each match a different family cooldown -> waits for the LATER one, not the first-seen",
        [estimate(10, route_family="core"), estimate(5, route_family="graphql")],
        org_id=org_a,
        provider="github",
        integration_id=integration_a,
        family_cooldowns={
            (org_a, "github", str(integration_a), "core"): earlier_family_expiry,
            (org_a, "github", str(integration_a), "graphql"): later_family_expiry,
        },
        dimension_cooldowns={},
    )
    add_matching_case(
        "same pair reordered (later-expiry estimate iterated FIRST) still picks the max, not the first-seen",
        [estimate(5, route_family="graphql"), estimate(10, route_family="core")],
        org_id=org_a,
        provider="github",
        integration_id=integration_a,
        family_cooldowns={
            (org_a, "github", str(integration_a), "core"): earlier_family_expiry,
            (org_a, "github", str(integration_a), "graphql"): later_family_expiry,
        },
        dimension_cooldowns={},
    )

    payload = {
        "observe_estimate": observe_cases,
        "baseline_unfitness": unfitness_cases,
        "cooldown_expiry": expiry_cases,
        "matching_cooldown_expiry": matching_cases,
    }
    json.dump(payload, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
