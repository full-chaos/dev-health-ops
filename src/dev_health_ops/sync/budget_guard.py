from __future__ import annotations

import json
import logging
import os
import uuid
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import or_

from dev_health_ops.models import SyncRunUnit, SyncRunUnitStatus
from dev_health_ops.sync.budget import BudgetEstimate, estimate_provider_budget
from dev_health_ops.workers.sync_bootstrap import SyncTaskBootstrap

logger = logging.getLogger(__name__)


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
) -> dict[str, Any]:
    bucket = estimate.bucket.to_dict()
    budget_key = _budget_key(bucket)
    limit = _limit_for_bucket(bucket, limits=limits, default_limit=default_limit)
    previous_units = consumed_by_bucket[budget_key]
    projected_units = previous_units + estimate.estimated_units
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


def _limit_for_bucket(
    bucket: Mapping[str, str], *, limits: Mapping[str, int], default_limit: int
) -> int:
    provider = bucket.get("provider", "")
    org_id = bucket.get("org_id", "")
    host = bucket.get("host", "")
    credential = bucket.get("credential_fingerprint", "")
    dimension = bucket.get("dimension", "")
    candidates = (
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


def _budget_key(bucket: Mapping[str, str]) -> str:
    return ":".join(
        (
            bucket.get("provider", ""),
            bucket.get("org_id", ""),
            bucket.get("host", ""),
            bucket.get("credential_fingerprint", ""),
            bucket.get("dimension", ""),
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
