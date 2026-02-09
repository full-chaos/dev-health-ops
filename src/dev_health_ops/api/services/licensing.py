"""Licensing and feature gating service for Enterprise Edition."""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import TYPE_CHECKING, Any

import jwt
from jwt.exceptions import InvalidTokenError

from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.models.licensing import (
    FeatureFlag,
    OrgFeatureOverride,
    OrgLicense,
    STANDARD_FEATURES,
    TIER_LIMITS,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

LICENSE_JWT_ALGORITHM = "RS256"
LICENSE_JWT_ALGORITHM_SYMMETRIC = "HS256"


def _get_license_public_key() -> str | None:
    return os.getenv("LICENSE_PUBLIC_KEY")


def _get_license_secret_key() -> str | None:
    return os.getenv("LICENSE_SECRET_KEY")


@dataclass
class LicenseInfo:
    """Decoded license information."""

    tier: LicenseTier
    org_id: str
    licensed_users: int | None = None
    licensed_repos: int | None = None
    issued_at: datetime | None = None
    expires_at: datetime | None = None
    features: dict[str, bool] | None = None
    limits: dict[str, Any] | None = None
    customer_id: str | None = None
    is_valid: bool = True
    validation_error: str | None = None


@dataclass
class FeatureAccess:
    """Result of a feature access check."""

    allowed: bool
    reason: str | None = None
    expires_at: datetime | None = None
    config: dict[str, Any] | None = None


class LicenseService:
    """License validation and feature gating service."""

    def __init__(
        self,
        public_key: str | None = None,
        secret_key: str | None = None,
    ):
        self.public_key = public_key or _get_license_public_key()
        self.secret_key = secret_key or _get_license_secret_key()

    def validate_license_key(self, license_key: str) -> LicenseInfo:
        """Validate a license key and extract its information."""
        if not license_key:
            return LicenseInfo(
                tier=LicenseTier.COMMUNITY,
                org_id="",
                is_valid=False,
                validation_error="No license key provided",
            )

        try:
            key = self.public_key or self.secret_key
            algorithm = (
                LICENSE_JWT_ALGORITHM
                if self.public_key
                else LICENSE_JWT_ALGORITHM_SYMMETRIC
            )

            if not key:
                return LicenseInfo(
                    tier=LicenseTier.COMMUNITY,
                    org_id="",
                    is_valid=False,
                    validation_error="No license verification key configured",
                )

            payload = jwt.decode(
                license_key,
                key,
                algorithms=[algorithm],
                options={"require": ["exp", "sub", "tier"]},
            )

            expires_at = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
            issued_at = (
                datetime.fromtimestamp(payload["iat"], tz=timezone.utc)
                if "iat" in payload
                else None
            )

            return LicenseInfo(
                tier=LicenseTier(payload.get("tier", "community")),
                org_id=payload["sub"],
                licensed_users=payload.get("users"),
                licensed_repos=payload.get("repos"),
                issued_at=issued_at,
                expires_at=expires_at,
                features=payload.get("features"),
                limits=payload.get("limits"),
                customer_id=payload.get("customer_id"),
                is_valid=True,
            )
        except jwt.ExpiredSignatureError:
            return LicenseInfo(
                tier=LicenseTier.COMMUNITY,
                org_id="",
                is_valid=False,
                validation_error="License has expired",
            )
        except InvalidTokenError as e:
            return LicenseInfo(
                tier=LicenseTier.COMMUNITY,
                org_id="",
                is_valid=False,
                validation_error=f"Invalid license key: {e}",
            )

    def create_license_key(
        self,
        org_id: str,
        tier: LicenseTier,
        expires_at: datetime,
        licensed_users: int | None = None,
        licensed_repos: int | None = None,
        features: dict[str, bool] | None = None,
        limits: dict[str, Any] | None = None,
        customer_id: str | None = None,
    ) -> str | None:
        """Create a license key (for SaaS/internal use)."""
        if not self.secret_key:
            logger.error("Cannot create license: LICENSE_SECRET_KEY not configured")
            return None

        payload = {
            "sub": org_id,
            "tier": tier.value,
            "exp": expires_at,
            "iat": datetime.now(timezone.utc),
            "jti": str(uuid.uuid4()),
        }

        if licensed_users is not None:
            payload["users"] = licensed_users
        if licensed_repos is not None:
            payload["repos"] = licensed_repos
        if features:
            payload["features"] = features
        if limits:
            payload["limits"] = limits
        if customer_id:
            payload["customer_id"] = customer_id

        return jwt.encode(
            payload, self.secret_key, algorithm=LICENSE_JWT_ALGORITHM_SYMMETRIC
        )


class FeatureService:
    """Feature flag checking service with caching."""

    def __init__(self, session: "Session"):
        self.session = session
        self._feature_cache: dict[str, FeatureFlag] = {}
        self._override_cache: dict[
            tuple[uuid.UUID, str], OrgFeatureOverride | None
        ] = {}

    def _get_feature(self, feature_key: str) -> FeatureFlag | None:
        if feature_key in self._feature_cache:
            return self._feature_cache[feature_key]

        feature = (
            self.session.query(FeatureFlag)
            .filter(FeatureFlag.key == feature_key)
            .first()
        )
        self._feature_cache[feature_key] = feature
        return feature

    def _get_override(
        self, org_id: uuid.UUID, feature_id: uuid.UUID
    ) -> OrgFeatureOverride | None:
        cache_key = (org_id, str(feature_id))
        if cache_key in self._override_cache:
            return self._override_cache[cache_key]

        override = (
            self.session.query(OrgFeatureOverride)
            .filter(
                OrgFeatureOverride.org_id == org_id,
                OrgFeatureOverride.feature_id == feature_id,
            )
            .first()
        )
        self._override_cache[cache_key] = override
        return override

    def _get_org_license(self, org_id: uuid.UUID) -> OrgLicense | None:
        return (
            self.session.query(OrgLicense).filter(OrgLicense.org_id == org_id).first()
        )

    def check_feature_access(
        self, org_id: uuid.UUID, feature_key: str
    ) -> FeatureAccess:
        """Check if an organization has access to a feature."""
        feature = self._get_feature(feature_key)
        if not feature:
            return FeatureAccess(
                allowed=False,
                reason=f"Unknown feature: {feature_key}",
            )

        if not feature.is_enabled:
            return FeatureAccess(
                allowed=False,
                reason="Feature is globally disabled",
            )

        if feature.is_deprecated:
            logger.warning("Access to deprecated feature: %s", feature_key)

        override = self._get_override(org_id, feature.id)
        if override:
            now = datetime.now(timezone.utc)
            if override.expires_at and override.expires_at < now:
                pass
            else:
                if override.is_enabled:
                    return FeatureAccess(
                        allowed=True,
                        expires_at=override.expires_at,
                        config=override.config,
                    )
                else:
                    return FeatureAccess(
                        allowed=False,
                        reason="Feature disabled for this organization",
                    )

        org_license = self._get_org_license(org_id)
        org_tier = (
            LicenseTier(org_license.tier) if org_license else LicenseTier.COMMUNITY
        )

        if org_license and org_license.features_override:
            if feature_key in org_license.features_override:
                if org_license.features_override[feature_key]:
                    return FeatureAccess(allowed=True)
                else:
                    return FeatureAccess(
                        allowed=False,
                        reason="Feature disabled in license",
                    )

        feature_min_tier = LicenseTier(feature.min_tier)
        tier_order = [
            LicenseTier.COMMUNITY,
            LicenseTier.TEAM,
            LicenseTier.ENTERPRISE,
        ]

        if tier_order.index(org_tier) >= tier_order.index(feature_min_tier):
            return FeatureAccess(allowed=True)

        return FeatureAccess(
            allowed=False,
            reason=f"Requires {feature_min_tier.value} tier or higher",
        )

    def has_feature(self, org_id: uuid.UUID, feature_key: str) -> bool:
        """Simple boolean check for feature access."""
        return self.check_feature_access(org_id, feature_key).allowed

    def clear_cache(self) -> None:
        """Clear the feature and override caches."""
        self._feature_cache.clear()
        self._override_cache.clear()


class TierLimitService:
    """Tier limit checking and enforcement service."""

    def __init__(self, session: "Session"):
        self.session = session

    def _get_org_license(self, org_id: uuid.UUID) -> OrgLicense | None:
        return (
            self.session.query(OrgLicense).filter(OrgLicense.org_id == org_id).first()
        )

    def get_limit(self, org_id: uuid.UUID, limit_key: str) -> int | float | None:
        """Get a specific limit for an organization."""
        org_license = self._get_org_license(org_id)
        org_tier = (
            LicenseTier(org_license.tier) if org_license else LicenseTier.COMMUNITY
        )

        if org_license and org_license.limits_override:
            if limit_key in org_license.limits_override:
                return org_license.limits_override[limit_key]

        tier_limits = TIER_LIMITS.get(org_tier, TIER_LIMITS[LicenseTier.COMMUNITY])
        return tier_limits.get(limit_key)

    def get_all_limits(self, org_id: uuid.UUID) -> dict[str, int | float | None]:
        """Get all limits for an organization."""
        org_license = self._get_org_license(org_id)
        org_tier = (
            LicenseTier(org_license.tier) if org_license else LicenseTier.COMMUNITY
        )

        limits = dict(TIER_LIMITS.get(org_tier, TIER_LIMITS[LicenseTier.COMMUNITY]))

        if org_license and org_license.limits_override:
            limits.update(org_license.limits_override)

        return limits

    def check_limit(
        self, org_id: uuid.UUID, limit_key: str, current_value: int | float
    ) -> tuple[bool, str | None]:
        """Check if a value is within the organization's limit."""
        limit = self.get_limit(org_id, limit_key)

        if limit is None:
            return True, None

        if current_value >= limit:
            return False, f"Limit exceeded: {limit_key} ({current_value}/{limit})"

        return True, None

    def check_user_limit(
        self, org_id: uuid.UUID, current_users: int
    ) -> tuple[bool, str | None]:
        """Check if organization can add more users."""
        return self.check_limit(org_id, "max_users", current_users)

    def check_repo_limit(
        self, org_id: uuid.UUID, current_repos: int
    ) -> tuple[bool, str | None]:
        """Check if organization can add more repos."""
        return self.check_limit(org_id, "max_repos", current_repos)


@lru_cache(maxsize=1)
def get_standard_feature_keys() -> frozenset[str]:
    """Get all standard feature keys."""
    return frozenset(f[0] for f in STANDARD_FEATURES)


def seed_feature_flags(session: "Session") -> int:
    """Seed the feature_flags table with standard features."""
    existing = {f.key for f in session.query(FeatureFlag.key).all()}
    created = 0

    for key, name, category, min_tier, description in STANDARD_FEATURES:
        if key in existing:
            continue

        feature = FeatureFlag(
            key=key,
            name=name,
            category=category.value,
            min_tier=min_tier.value,
            description=description,
        )
        session.add(feature)
        created += 1

    if created > 0:
        session.commit()
        logger.info("Seeded %d feature flags", created)

    return created


async def seed_feature_flags_async(session: Any) -> int:
    """Seed the feature_flags table with standard features (async session)."""
    from sqlalchemy import select

    result = await session.execute(select(FeatureFlag.key))
    existing = {row[0] for row in result.all()}
    created = 0

    for key, name, category, min_tier, description in STANDARD_FEATURES:
        if key in existing:
            continue

        feature = FeatureFlag(
            key=key,
            name=name,
            category=category.value,
            min_tier=min_tier.value,
            description=description,
        )
        session.add(feature)
        created += 1

    if created > 0:
        await session.commit()
        logger.info("Seeded %d feature flags", created)

    return created


_license_service: LicenseService | None = None


def get_license_service() -> LicenseService:
    """Get the global license service instance."""
    global _license_service
    if _license_service is None:
        _license_service = LicenseService()
    return _license_service
