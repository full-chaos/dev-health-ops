"""Validation helpers for FeatureBundle.features keys.

Source of truth: STANDARD_FEATURES imported from dev_health_ops.models.licensing.
This is a static compile-time set that covers all 25 canonical feature keys.  It is
preferred over a live DB query so that validation works even when the feature_flags
table has not yet been seeded (e.g. in tests or fresh environments) and avoids the
need for an extra async session inside the startup hook.

Layer 1 — write-time (application-level):
    validate_bundle_feature_keys(features) raises ValueError for any unknown key.
    Used in the admin CLI and any future HTTP bundle-creation path.

Layer 2 — startup-time (integrity check):
    validate_bundle_keys(session) scans every FeatureBundle row in the DB and raises
    RuntimeError if any key is unknown.  Set env var ALLOW_STALE_FEATURE_BUNDLES=1
    to log a warning and continue instead of failing startup.
"""

from __future__ import annotations

import logging
import os

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class FeatureBundleIntegrityError(RuntimeError):
    """Raised when FeatureBundle rows reference feature keys not in STANDARD_FEATURES."""


def _known_feature_keys() -> frozenset[str]:
    """Return the canonical set of feature keys from STANDARD_FEATURES."""
    from dev_health_ops.models.licensing import STANDARD_FEATURES  # noqa: PLC0415

    return frozenset(key for key, *_rest in STANDARD_FEATURES)


def validate_bundle_feature_keys(features: list[str]) -> None:
    """Layer 1: validate a list of feature keys against the canonical registry.

    Args:
        features: list of feature key strings to validate.

    Raises:
        ValueError: if any key is not in STANDARD_FEATURES, naming the offending key.
    """
    known = _known_feature_keys()
    for key in features:
        if key not in known:
            raise ValueError(
                f"Unknown feature key {key!r}. Valid keys are: {sorted(known)}"
            )


async def validate_bundle_keys(session: AsyncSession) -> None:
    """Layer 2: startup-time integrity check.

    Scans every FeatureBundle row and verifies all feature keys are known.

    By default: log ERROR and raise RuntimeError for any unknown key.
    Set ALLOW_STALE_FEATURE_BUNDLES=1 to log WARNING and continue instead
    (emergency ops bypass only).

    Args:
        session: async SQLAlchemy session with access to the feature_bundles table.

    Raises:
        FeatureBundleIntegrityError: if unknown keys are found and ALLOW_STALE_FEATURE_BUNDLES != "1".
    """
    from dev_health_ops.models.billing import FeatureBundle  # noqa: PLC0415

    known = _known_feature_keys()
    allow_stale = os.getenv("ALLOW_STALE_FEATURE_BUNDLES", "0").strip() == "1"

    result = await session.execute(select(FeatureBundle.key, FeatureBundle.features))
    rows = result.all()

    violations: list[tuple[str, str]] = []  # (bundle_key, bad_feature_key)
    for bundle_key, features in rows:
        if not features:
            continue
        for fkey in features:
            if fkey not in known:
                violations.append((bundle_key, fkey))

    if not violations:
        logger.debug("validate_bundle_keys: all bundle feature keys are valid")
        return

    # Format the report
    lines = [f"  bundle={bkey!r} unknown_feature={fkey!r}" for bkey, fkey in violations]
    detail = "\n".join(lines)

    if allow_stale:
        logger.warning(
            "ALLOW_STALE_FEATURE_BUNDLES=1: ignoring %d unknown bundle feature key(s):\n%s",
            len(violations),
            detail,
        )
        return

    logger.error(
        "Startup aborted: %d unknown bundle feature key(s) found:\n%s\n"
        "Fix the feature_bundles table or set ALLOW_STALE_FEATURE_BUNDLES=1 to bypass.",
        len(violations),
        detail,
    )
    raise FeatureBundleIntegrityError(
        f"FeatureBundle integrity check failed: {len(violations)} unknown feature key(s). "
        f"Set ALLOW_STALE_FEATURE_BUNDLES=1 to bypass."
    )
