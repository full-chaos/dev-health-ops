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
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.licensing.feature_decisions import (
    evaluate_org_feature_sync,
    evaluate_org_features_async,
)
from dev_health_ops.licensing.feature_policy import FeatureDecisionReason
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
from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

LICENSE_JWT_ALGORITHM = "RS256"
LICENSE_JWT_ALGORITHM_SYMMETRIC = "HS256"

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)

#: Async tier resolution at the GraphQL edge seam (CHAOS-4697 prerequisite,
#: this module's ``resolve_org_tier_async``). Labeled by the resolved tier so
#: a silent "always community" regression (the failure mode the precedence
#: tests below guard against) is visible in metrics, not just in tests.
_TIER_RESOLVED_ASYNC_TOTAL = build_counter(
    "devhealth_org_tier_resolved_async_total",
    "Async tier resolutions at the GraphQL edge seam, by resolved tier",
    ["tier"],
    meter=_meter,
    prometheus=_prometheus,
)

#: `licensed_features` producer outcomes (CHAOS-4697 prerequisite, this
#: module's ``resolve_licensed_features_async``). "ok" covers BOTH an org
#: with features and an org with none -- that distinction lives in the
#: envelope's own claim list length, not in this label. "storage_error"
#: outcomes always pair with a raised ``LicensedFeaturesLookupError``, never
#: a silently-empty return -- see that function's docstring.
_LICENSED_FEATURES_TOTAL = build_counter(
    "devhealth_licensed_features_resolved_total",
    "licensed_features lookups at the GraphQL edge seam, by outcome",
    ["outcome"],
    meter=_meter,
    prometheus=_prometheus,
)


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


async def resolve_org_tier_async(
    session: AsyncSession,
    org_id: uuid.UUID,
) -> LicenseTier:
    """Async-usable tier resolution for seams that only hold an ``AsyncSession``
    (CHAOS-4697 prerequisite -- ``graphql/app.py``'s ``get_context`` is async,
    on ``get_postgres_session``'s async engine, but ``resolve_org_tier`` is
    sync and expects a pre-fetched ``OrgLicense``).

    Does **not** reimplement ``resolve_org_tier``'s precedence
    (``OrgLicense.tier`` wins, else ``Organization.tier``, else COMMUNITY).
    It fetches the ``OrgLicense`` row through the async session, then hands
    the actual decision to ``resolve_org_tier`` via ``AsyncSession.run_sync``
    -- the exact bridge pattern already proven in this codebase at
    ``api/admin/llm_settings.py::require_byo_llm_access`` (``_resolve``
    passed to ``session.run_sync``). Two implementations of one precedence
    rule is the failure mode this repo has already been bitten by --
    ``licensing/feature_decisions.py``'s ``_resolved_org_tier`` and
    ``licensing/gating.py``'s inline copy inside
    ``get_org_entitlements_from_db`` are two pre-existing instances of
    exactly that (not introduced by this change, and not something this
    function adds a third to -- see this lane's final report).
    """
    org_license = await session.scalar(
        select(OrgLicense).where(OrgLicense.org_id == org_id)
    )

    def _resolve(sync_session: Session) -> LicenseTier:
        return resolve_org_tier(sync_session, org_id, org_license)

    tier = await session.run_sync(_resolve)
    _TIER_RESOLVED_ASYNC_TOTAL.labels(tier=tier.value).inc()
    return tier


class LicensedFeaturesLookupError(RuntimeError):
    """Raised when a ``licensed_features`` storage lookup itself fails.

    Keeps "the org has no licensed features" (a valid, expected empty list)
    distinguishable from "the lookup could not be completed" (a storage
    failure). A caller must never see the two collapse into the same empty
    list -- see ``resolve_licensed_features_async``'s docstring.
    """


async def resolve_licensed_features_async(
    session: AsyncSession,
    org_id: uuid.UUID,
) -> list[str]:
    """The feature keys ``org_id`` currently has access to, resolved live.

    "Feature keys the org currently has access to" is the envelope contract
    (``docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md``'s
    v1 claim table; ``principal_envelope.EffectivePrincipalEnvelopeClaims
    .licensed_features``). This had **no production producer anywhere** in
    the repo before this change (CHAOS-4697 prerequisite).

    Enumerates every feature key currently registered in the live
    ``feature_flags`` table -- not the static ``STANDARD_FEATURES`` registry,
    which can drift ahead of what a given deployment has actually migrated
    and seeded -- and evaluates each through
    ``licensing.feature_decisions.evaluate_org_features_async``: the same
    tier/org-override/license-override decision logic every other feature
    gate in this codebase uses (``FeatureService``, ``feature_flag_state``),
    not reimplemented here.

    Authorization is re-checked live every call (North Star check 18): no
    caching across requests, no memoized result.

    An empty list means the org genuinely has no licensed features, OR no
    features are registered at all yet -- a pre-migration / minimal DB. That
    second case mirrors ``feature_flag_state``'s existing "unregistered"
    treatment: not an error, a legitimate state. A genuine STORAGE failure
    during evaluation is never folded into that same empty list -- it raises
    ``LicensedFeaturesLookupError`` instead, so a caller (and the signed
    envelope this feeds) cannot mistake "the lookup failed" for "the org has
    no features". This is the one branch this function refuses to fail open
    on: an empty ``licensed_features`` claim is a security-relevant
    assertion, and a caller building a trust boundary on it must be able to
    tell "verified empty" from "unknown".
    """

    def _has_feature_flags_table(sync_session: Session) -> bool:
        import sqlalchemy as sa

        return sa.inspect(sync_session.get_bind()).has_table("feature_flags")

    has_table = await session.run_sync(_has_feature_flags_table)
    if not has_table:
        _LICENSED_FEATURES_TOTAL.labels(outcome="unregistered_table").inc()
        return []

    keys = tuple(sorted((await session.scalars(select(FeatureFlag.key))).all()))
    if not keys:
        _LICENSED_FEATURES_TOTAL.labels(outcome="no_features_registered").inc()
        return []

    decisions = await evaluate_org_features_async(session, org_id, keys)

    failed_keys = sorted(
        key
        for key, decision in decisions.items()
        if decision.reason is FeatureDecisionReason.STORAGE_ERROR
    )
    if failed_keys:
        _LICENSED_FEATURES_TOTAL.labels(outcome="storage_error").inc()
        raise LicensedFeaturesLookupError(
            f"licensed_features lookup failed for org_id={org_id}: "
            f"storage error resolving {failed_keys}"
        )

    allowed = sorted(key for key, decision in decisions.items() if decision.allowed)
    _LICENSED_FEATURES_TOTAL.labels(outcome="ok").inc()
    return allowed


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
        if self.session.in_nested_transaction():
            # Let the caller-owned SAVEPOINT roll back before the fallback
            # consumes a missing-table error.
            rows = self.session.query(TierLimit).filter(TierLimit.tier == tier).all()
            return {str(row.limit_key): row.typed_value for row in rows}
        try:
            with self.session.begin_nested():
                rows = (
                    self.session.query(TierLimit).filter(TierLimit.tier == tier).all()
                )
        except SQLAlchemyError:
            # Table may not exist yet (pre-migration) — fall through to
            # hardcoded defaults without rolling back the caller's transaction.
            return {}
        return {str(row.limit_key): row.typed_value for row in rows}

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
