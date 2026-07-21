from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import assert_never

from pydantic import JsonValue

from dev_health_ops.licensing.registry import (
    is_explicit_purchase_feature,
    is_org_override_only_feature,
)
from dev_health_ops.licensing.types import TIER_ORDER, LicenseTier

_TIER_BOUND_OVERRIDE_FEATURES = frozenset({"customer_push_ingest"})


class FeatureDecisionReason(StrEnum):
    ENABLED_BY_ORG_OVERRIDE = "enabled_by_org_override"
    ENABLED_BY_LICENSE_OVERRIDE = "enabled_by_license_override"
    ENABLED_BY_TIER = "enabled_by_tier"
    FEATURE_NOT_REGISTERED = "feature_not_registered"
    GLOBAL_DISABLED = "global_disabled"
    INVALID_FEATURE_STATE = "invalid_feature_state"
    STORAGE_ERROR = "storage_error"
    ORG_OVERRIDE_EXPIRED = "org_override_expired"
    ORG_OVERRIDE_DISABLED = "org_override_disabled"
    ORG_OVERRIDE_REQUIRED = "org_override_required"
    LICENSE_OVERRIDE_DISABLED = "license_override_disabled"
    EXPLICIT_PURCHASE_REQUIRED = "explicit_purchase_required"
    TIER_REQUIRED = "tier_required"


@dataclass(frozen=True, slots=True)
class FeatureOverrideSnapshot:
    is_enabled: bool
    expires_at: datetime | None = None
    config: dict[str, JsonValue] | None = None


@dataclass(frozen=True, slots=True)
class FeatureDecisionContext:
    feature_key: str
    is_registered: bool
    is_storage_valid: bool
    globally_enabled: bool
    min_tier: LicenseTier
    org_tier: LicenseTier
    org_override: FeatureOverrideSnapshot | None
    license_override: bool | None
    evaluated_at: datetime


@dataclass(frozen=True, slots=True)
class FeatureDecision:
    feature_key: str
    allowed: bool
    reason: FeatureDecisionReason
    expires_at: datetime | None = None
    config: dict[str, JsonValue] | None = None

    @property
    def message(self) -> str | None:
        message: str | None
        match self.reason:
            case (
                FeatureDecisionReason.ENABLED_BY_ORG_OVERRIDE
                | FeatureDecisionReason.ENABLED_BY_LICENSE_OVERRIDE
                | FeatureDecisionReason.ENABLED_BY_TIER
            ):
                message = None
            case FeatureDecisionReason.FEATURE_NOT_REGISTERED:
                message = f"Unknown feature: {self.feature_key}"
            case FeatureDecisionReason.GLOBAL_DISABLED:
                message = "Feature is globally disabled"
            case FeatureDecisionReason.INVALID_FEATURE_STATE:
                message = "Feature configuration is invalid"
            case FeatureDecisionReason.STORAGE_ERROR:
                message = "Feature decision storage is unavailable"
            case FeatureDecisionReason.ORG_OVERRIDE_EXPIRED:
                message = "Organization feature override has expired"
            case FeatureDecisionReason.ORG_OVERRIDE_DISABLED:
                message = "Feature disabled for this organization"
            case FeatureDecisionReason.ORG_OVERRIDE_REQUIRED:
                message = "Requires an active organization feature override"
            case FeatureDecisionReason.LICENSE_OVERRIDE_DISABLED:
                message = "Feature disabled in license"
            case FeatureDecisionReason.EXPLICIT_PURCHASE_REQUIRED:
                message = "Requires an explicit organization purchase"
            case FeatureDecisionReason.TIER_REQUIRED:
                message = f"Requires {self.feature_key} minimum tier"
            case unreachable:
                assert_never(unreachable)
        return message


def closed_feature_decision(
    feature_key: str,
    reason: FeatureDecisionReason,
) -> FeatureDecision:
    return FeatureDecision(feature_key=feature_key, allowed=False, reason=reason)


def decide_feature(context: FeatureDecisionContext) -> FeatureDecision:
    if not context.is_storage_valid:
        return closed_feature_decision(
            context.feature_key,
            FeatureDecisionReason.INVALID_FEATURE_STATE,
        )
    if not context.is_registered:
        return closed_feature_decision(
            context.feature_key,
            FeatureDecisionReason.FEATURE_NOT_REGISTERED,
        )
    if not context.globally_enabled:
        return closed_feature_decision(
            context.feature_key,
            FeatureDecisionReason.GLOBAL_DISABLED,
        )

    tier_allowed = TIER_ORDER.index(context.org_tier) >= TIER_ORDER.index(
        context.min_tier
    )
    if context.org_override is not None:
        override_expired = (
            context.org_override.expires_at is not None
            and context.org_override.expires_at <= context.evaluated_at
        )
        if override_expired and is_org_override_only_feature(context.feature_key):
            return closed_feature_decision(
                context.feature_key,
                FeatureDecisionReason.ORG_OVERRIDE_EXPIRED,
            )
        if not override_expired:
            if not context.org_override.is_enabled:
                return closed_feature_decision(
                    context.feature_key,
                    FeatureDecisionReason.ORG_OVERRIDE_DISABLED,
                )
            if (
                context.feature_key in _TIER_BOUND_OVERRIDE_FEATURES
                and not tier_allowed
            ):
                return closed_feature_decision(
                    context.feature_key,
                    FeatureDecisionReason.TIER_REQUIRED,
                )
            return FeatureDecision(
                feature_key=context.feature_key,
                allowed=True,
                reason=FeatureDecisionReason.ENABLED_BY_ORG_OVERRIDE,
                expires_at=context.org_override.expires_at,
                config=context.org_override.config,
            )

    if context.license_override is not None:
        if not context.license_override:
            return closed_feature_decision(
                context.feature_key,
                FeatureDecisionReason.LICENSE_OVERRIDE_DISABLED,
            )
        if is_org_override_only_feature(context.feature_key):
            return closed_feature_decision(
                context.feature_key,
                FeatureDecisionReason.ORG_OVERRIDE_REQUIRED,
            )
        if context.feature_key in _TIER_BOUND_OVERRIDE_FEATURES and not tier_allowed:
            return closed_feature_decision(
                context.feature_key,
                FeatureDecisionReason.TIER_REQUIRED,
            )
        return FeatureDecision(
            feature_key=context.feature_key,
            allowed=True,
            reason=FeatureDecisionReason.ENABLED_BY_LICENSE_OVERRIDE,
        )

    if is_explicit_purchase_feature(context.feature_key):
        return closed_feature_decision(
            context.feature_key,
            FeatureDecisionReason.EXPLICIT_PURCHASE_REQUIRED,
        )
    if tier_allowed:
        return FeatureDecision(
            feature_key=context.feature_key,
            allowed=True,
            reason=FeatureDecisionReason.ENABLED_BY_TIER,
        )
    return closed_feature_decision(
        context.feature_key,
        FeatureDecisionReason.TIER_REQUIRED,
    )
