"""Canonical investment taxonomy (themes and subcategories).

This module is the single authoritative registry for investment categorization keys.
It is intentionally outside `dev_health_ops/` so it can be shared across compute-time
(`work_graph/`) and request-time (API) code paths without duplication.
"""

from __future__ import annotations

from typing import Dict, Set

__all__ = ["THEMES", "SUBCATEGORIES", "theme_of"]

THEMES: Set[str] = {
    "feature_delivery",
    "operational",
    "maintenance",
    "quality",
    "risk",
}

SUBCATEGORIES: Set[str] = {
    "feature_delivery.customer",
    "feature_delivery.roadmap",
    "feature_delivery.enablement",
    "operational.incident_response",
    "operational.on_call",
    "operational.support",
    "maintenance.refactor",
    "maintenance.upgrade",
    "maintenance.debt",
    "quality.testing",
    "quality.bugfix",
    "quality.reliability",
    "risk.security",
    "risk.compliance",
    "risk.vulnerability",
}

_SUBCATEGORY_TO_THEME: Dict[str, str] = {
    subcategory: subcategory.split(".", 1)[0] for subcategory in SUBCATEGORIES
}


def theme_of(subcategory_key: str) -> str:
    """Return the canonical theme for a subcategory key."""
    return _SUBCATEGORY_TO_THEME.get(subcategory_key, "")
