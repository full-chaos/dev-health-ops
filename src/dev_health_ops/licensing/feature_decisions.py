from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import JsonValue
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from dev_health_ops.licensing.feature_decision_store import (
    FeatureRows,
    load_feature_rows_async,
    load_feature_rows_sync,
)
from dev_health_ops.licensing.feature_policy import (
    FeatureDecision,
    FeatureDecisionContext,
    FeatureDecisionReason,
    FeatureOverrideSnapshot,
    closed_feature_decision,
    decide_feature,
)
from dev_health_ops.licensing.types import LicenseTier

if TYPE_CHECKING:
    from dev_health_ops.models.licensing import OrgFeatureOverride


def _unique_keys(feature_keys: Sequence[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(feature_keys))


def _normalize_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _resolved_org_tier(rows: FeatureRows) -> LicenseTier:
    raw_tier = rows.org_license.tier if rows.org_license is not None else rows.org_tier
    try:
        return LicenseTier(str(raw_tier))
    except ValueError:
        return LicenseTier.COMMUNITY


def _override_snapshot(
    override: OrgFeatureOverride | None,
) -> FeatureOverrideSnapshot | None:
    if override is None:
        return None
    raw_config = override.config
    config: dict[str, JsonValue] | None = (
        {str(key): value for key, value in raw_config.items()}
        if isinstance(raw_config, dict)
        else None
    )
    return FeatureOverrideSnapshot(
        is_enabled=bool(override.is_enabled),
        expires_at=_normalize_utc(override.expires_at),
        config=config,
    )


def _decisions_from_rows(
    feature_keys: Sequence[str],
    rows: FeatureRows,
    evaluated_at: datetime,
) -> dict[str, FeatureDecision]:
    features_by_key = {str(feature.key): feature for feature in rows.features}
    overrides_by_feature_id = {
        str(override.feature_id): override for override in rows.overrides
    }
    raw_license_overrides = (
        rows.org_license.features_override if rows.org_license is not None else None
    )
    license_overrides = (
        {str(key): bool(value) for key, value in raw_license_overrides.items()}
        if isinstance(raw_license_overrides, dict)
        else {}
    )
    org_tier = _resolved_org_tier(rows)
    decisions: dict[str, FeatureDecision] = {}

    for feature_key in feature_keys:
        feature = features_by_key.get(feature_key)
        if feature is None:
            decisions[feature_key] = decide_feature(
                FeatureDecisionContext(
                    feature_key=feature_key,
                    is_registered=False,
                    is_storage_valid=True,
                    globally_enabled=False,
                    min_tier=LicenseTier.COMMUNITY,
                    org_tier=org_tier,
                    org_override=None,
                    license_override=license_overrides.get(feature_key),
                    evaluated_at=evaluated_at,
                )
            )
            continue

        try:
            min_tier = LicenseTier(str(feature.min_tier))
            is_storage_valid = True
        except ValueError:
            min_tier = LicenseTier.COMMUNITY
            is_storage_valid = False
        decisions[feature_key] = decide_feature(
            FeatureDecisionContext(
                feature_key=feature_key,
                is_registered=True,
                is_storage_valid=is_storage_valid,
                globally_enabled=bool(feature.is_enabled),
                min_tier=min_tier,
                org_tier=org_tier,
                org_override=_override_snapshot(
                    overrides_by_feature_id.get(str(feature.id))
                ),
                license_override=license_overrides.get(feature_key),
                evaluated_at=evaluated_at,
            )
        )
    return decisions


def evaluate_org_features_sync(
    session: Session,
    org_id: uuid.UUID,
    feature_keys: Sequence[str],
) -> dict[str, FeatureDecision]:
    keys = _unique_keys(feature_keys)
    try:
        rows = load_feature_rows_sync(session, org_id, keys)
    except SQLAlchemyError:
        return {
            key: closed_feature_decision(key, FeatureDecisionReason.STORAGE_ERROR)
            for key in keys
        }
    return _decisions_from_rows(keys, rows, datetime.now(UTC))


async def evaluate_org_features_async(
    session: AsyncSession,
    org_id: uuid.UUID,
    feature_keys: Sequence[str],
) -> dict[str, FeatureDecision]:
    keys = _unique_keys(feature_keys)
    try:
        rows = await load_feature_rows_async(session, org_id, keys)
    except SQLAlchemyError:
        return {
            key: closed_feature_decision(key, FeatureDecisionReason.STORAGE_ERROR)
            for key in keys
        }
    return _decisions_from_rows(keys, rows, datetime.now(UTC))


def evaluate_org_feature_sync(
    session: Session,
    org_id: uuid.UUID,
    feature_key: str,
) -> FeatureDecision:
    return evaluate_org_features_sync(session, org_id, (feature_key,))[feature_key]


async def evaluate_org_feature_async(
    session: AsyncSession,
    org_id: uuid.UUID,
    feature_key: str,
) -> FeatureDecision:
    decisions = await evaluate_org_features_async(session, org_id, (feature_key,))
    return decisions[feature_key]


def is_org_feature_enabled_sync(
    session: Session,
    org_id: uuid.UUID,
    feature_key: str,
) -> bool:
    return evaluate_org_feature_sync(session, org_id, feature_key).allowed


async def is_org_feature_enabled_async(
    session: AsyncSession,
    org_id: uuid.UUID,
    feature_key: str,
) -> bool:
    return (await evaluate_org_feature_async(session, org_id, feature_key)).allowed
