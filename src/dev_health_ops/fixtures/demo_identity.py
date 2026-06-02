"""Curated, customer-safe demo identity for synthetic fixtures (CHAOS-2037).

The synthetic fixture generator historically emitted fixture-only identifiers
that leaked onto demo and customer-facing surfaces:

* the org switcher rendered ``Fixture Org (<uuid>)``;
* churn / coverage views rendered ``acme/demo-app`` and its ``-1``/``-2``
  numeric suffixes;
* repository scopes surfaced the bare ``acme/demo-app`` slug.

This module centralizes a small curated set of believable org/repo names so the
seed produces realistic, coherent labels. Names are intentionally aligned with
the dev-health-web sample data (``web-app``, ``core-api``, ``auth-service`` …)
so the two repos stay consistent.
"""

from __future__ import annotations

# Curated organization brand used for all synthetic/demo seed data. Replaces the
# legacy ``Fixture Org (<uuid>)`` / ``Default Organization`` display names. The
# org *slug* remains derived deterministically from the org_id elsewhere; only
# the human-readable name is sourced here.
DEMO_ORG_NAME = "Meridian"

# Curated, realistic repository names. The fixture runner assigns these to the
# generated repos in order; only when the requested repo count exceeds this list
# does it fall back to the legacy numeric-suffix scheme.
DEMO_REPO_NAMES: tuple[str, ...] = (
    "meridian/web-app",
    "meridian/core-api",
    "meridian/auth-service",
    "meridian/billing-service",
    "meridian/search-service",
    "meridian/mobile-app",
    "meridian/data-pipeline",
    "meridian/infra",
    "meridian/notifications",
    "meridian/analytics",
)

# Default repo name used when no ``--repo-name`` is supplied.
DEFAULT_DEMO_REPO_NAME = DEMO_REPO_NAMES[0]


def demo_repo_name(base_name: str, index: int, repo_count: int) -> str:
    """Return a believable repo name for the repo at ``index``.

    When the runner generates multiple repos it previously suffixed the base
    name (``acme/demo-app-1``, ``acme/demo-app-2`` …). For the curated demo seed
    we instead draw distinct, realistic names from :data:`DEMO_REPO_NAMES` so
    churn/coverage surfaces never render ``base-1``/``base-2`` scaffolding.

    The curated list is only used when the caller kept the curated default base
    name; a caller that passes an explicit ``--repo-name`` keeps the legacy
    numeric-suffix behaviour (and falls back to it once the curated list is
    exhausted).
    """
    if repo_count <= 1:
        return base_name
    if base_name == DEFAULT_DEMO_REPO_NAME and index < len(DEMO_REPO_NAMES):
        return DEMO_REPO_NAMES[index]
    return f"{base_name}-{index + 1}"
