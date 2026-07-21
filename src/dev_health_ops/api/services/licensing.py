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

from dev_health_ops.licensing.feature_decisions import evaluate_org_feature_sync
from dev_health_ops.licensing.types import TIER_ORDER, LicenseTier
from dev_health_ops.models.licensing import (
    STANDARD_FEATURES,
    TIER_LIMITS_DEFAULTS,
    FeatureFlag,
    OrgFeatureOverride,
    OrgLicense,
    TierLimit,
)
from dev_health_ops.models.users import Organization

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

LICENSE_JWT_ALGORITHM = "RS256"
LICENSE_JWT_ALGORITHM_SYMMETRIC = "HS256"


def _coerce_limit_map(value: object) -> dict[str, int | float | None]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, int | float | None] = {}
    for key, raw in value.items():
        if raw is None or isinstance(raw, (int, float)):
            result[str(key)] = raw
    return result


def resolve_org_tier(
    session: Session,
    org_id: uuid.UUID,
    org_license: OrgLicense | None,
) -> LicenseTier:
    """Resolve an organization's license tier from an already-fetched OrgLicense.

    Mirrors ``licensing.gating.get_org_entitlements_from_db`` so tier limits and
    entitlements always agree (CHAOS-2256): ``OrgLicense.tier`` wins when a row
    exists; otherwise fall back to the ``Organization.tier`` column (set at org
    creation / by billing webhooks); default to COMMUNITY.
    """
    if org_license is not None:
        try:
            return LicenseTier(org_license.tier)
        except ValueError:
            logger.warning(
                "Invalid OrgLicense tier=%s for org_id=%s; defaulting to community",
                org_license.tier,
                org_id,
            )
            return LicenseTier.COMMUNITY

    org_tier = (
        session.query(Organization.tier).filter(Organization.id == org_id).scalar()
    )
    if org_tier is not None:
        try:
            return LicenseTier(org_tier)
        except ValueError:
            logger.warning(
                "Invalid Organization tier=%s for org_id=%s; defaulting to community",
                org_tier,
                org_id,
            )
    return LicenseTier.COMMUNITY


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

        payload: dict[str, object] = {
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

    def __init__(self, session: Session):
        self.session = session
        self._feature_cache: dict[str, FeatureFlag | None] = {}
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
        decision = evaluate_org_feature_sync(self.session, org_id, feature_key)
        return FeatureAccess(
            allowed=decision.allowed,
            reason=decision.message,
            expires_at=decision.expires_at,
            config=dict(decision.config) if decision.config is not None else None,
        )

    def has_feature(self, org_id: uuid.UUID, feature_key: str) -> bool:
        """Simple boolean check for feature access."""
        return self.check_feature_access(org_id, feature_key).allowed

    def clear_cache(self) -> None:
        """Clear the feature and override caches."""
        self._feature_cache.clear()
        self._override_cache.clear()


def feature_flag_state(
    session: Session,
    org_id: uuid.UUID,
    feature_key: str,
    *,
    min_tier: LicenseTier | None = None,
) -> str:
    """Return feature state: 'enabled' | 'disabled' | 'unregistered'.

    'unregistered' covers pre-migration / minimal DBs where the feature_flags
    table is absent or the requested row has not been seeded. Genuine lookup errors are NOT swallowed --
    they propagate so callers can fail CLOSED (a kill switch must survive
    degraded licensing storage rather than silently allow).
    """
    import sqlalchemy as sa

    if not sa.inspect(session.get_bind()).has_table("feature_flags"):
        return "unregistered"
    svc = FeatureService(session)
    if min_tier is not None:
        org_license = svc._get_org_license(org_id)
        org_tier = resolve_org_tier(session, org_id, org_license)
        if TIER_ORDER.index(org_tier) < TIER_ORDER.index(min_tier):
            return "disabled"
    access = svc.check_feature_access(org_id, feature_key)
    if access.allowed:
        return "enabled"
    if (access.reason or "").startswith("Unknown feature"):
        return "unregistered"
    return "disabled"


def byo_llm_flag_state(session: Session, org_id: uuid.UUID) -> str:
    """Return the byo_llm flag state: 'enabled' | 'disabled' | 'unregistered'.

    'unregistered' covers pre-migration / minimal DBs where the feature_flags
    table is absent or the byo_llm row has not been seeded; callers treat it as
    backward-compatible (ungated). Genuine lookup errors are NOT swallowed --
    they propagate so callers can fail CLOSED (a kill switch must survive
    degraded licensing storage rather than silently allow).
    """
    # byo_llm enforces a hard TEAM-tier floor that positive per-org overrides
    # must NOT bypass (matching the admin gate's tier check).
    return feature_flag_state(session, org_id, "byo_llm", min_tier=LicenseTier.TEAM)


class TierLimitService:
    """Tier limit checking and enforcement service.

    Resolution order for a given (org, limit_key):
      1. ``OrgLicense.limits_override`` — per-org JSON overrides
      2. ``tier_limits`` table — database-driven defaults per tier
      3. ``TIER_LIMITS_DEFAULTS`` — hardcoded fallback (code deploy required)
    """

    def __init__(self, session: Session):
        self.session = session

    def _get_org_license(self, org_id: uuid.UUID) -> OrgLicense | None:
        return (
            self.session.query(OrgLicense).filter(OrgLicense.org_id == org_id).first()
        )

    def _get_db_tier_limits(self, tier: str) -> dict[str, int | float | None]:
        """Read tier limits from the tier_limits table."""
        try:
            rows = self.session.query(TierLimit).filter(TierLimit.tier == tier).all()
            return {str(row.limit_key): row.typed_value for row in rows}
        except Exception:
            # Table may not exist yet (pre-migration) — fall through to
            # hardcoded defaults. Must NOT call session.rollback() here: this
            # service is invoked from async callers via run_sync, and a sync
            # rollback in that path breaks the greenlet context
            # (sqlalchemy MissingGreenlet). Pre-migration planner transaction
            # recovery is handled in the planner's own sync context.
            return {}

    def _resolve_tier_limits(
        self, org_tier: LicenseTier
    ) -> dict[str, int | float | None]:
        """Merge DB tier limits over hardcoded defaults for a tier."""
        defaults = _coerce_limit_map(
            TIER_LIMITS_DEFAULTS.get(
                org_tier, TIER_LIMITS_DEFAULTS[LicenseTier.COMMUNITY]
            )
        )
        db_limits = self._get_db_tier_limits(org_tier.value)
        if db_limits:
            defaults.update(db_limits)
        return defaults

    def get_limit(self, org_id: uuid.UUID, limit_key: str) -> int | float | None:
        """Get a specific limit for an organization."""
        org_license = self._get_org_license(org_id)
        org_tier = resolve_org_tier(self.session, org_id, org_license)

        # 1. Per-org override (highest priority)
        if org_license and org_license.limits_override:
            limits_override = _coerce_limit_map(org_license.limits_override)
            if limit_key in limits_override:
                return limits_override[limit_key]

        # 2. DB tier defaults → 3. Hardcoded fallback
        tier_limits = self._resolve_tier_limits(org_tier)
        return tier_limits.get(limit_key)

    def get_all_limits(self, org_id: uuid.UUID) -> dict[str, int | float | None]:
        """Get all limits for an organization."""
        org_license = self._get_org_license(org_id)
        org_tier = resolve_org_tier(self.session, org_id, org_license)

        limits = self._resolve_tier_limits(org_tier)

        # Per-org overrides win
        if org_license and org_license.limits_override:
            limits.update(_coerce_limit_map(org_license.limits_override))

        return limits

    def check_limit(
        self, org_id: uuid.UUID, limit_key: str, current_value: int | float
    ) -> tuple[bool, str | None]:
        """Check if a value is within the organization's limit."""
        limit = self.get_limit(org_id, limit_key)

        if limit is None:
            return True, None

        if current_value > limit:
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

    def check_backfill_limit(
        self, org_id: uuid.UUID, requested_days: int
    ) -> tuple[bool, str | None]:
        limit = self.get_limit(org_id, "backfill_days")
        if limit is None:
            return True, None
        if requested_days > limit:
            return (
                False,
                f"Backfill limit exceeded: requested {requested_days} days, limit is {limit} days",
            )
        return True, None


@lru_cache(maxsize=1)
def get_standard_feature_keys() -> frozenset[str]:
    """Get all standard feature keys."""
    return frozenset(f[0] for f in STANDARD_FEATURES)


def seed_feature_flags(session: Session) -> int:
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
