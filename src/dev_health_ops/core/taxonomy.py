"""Investment taxonomy helpers — pure business logic, no I/O.

Centralises the formatting and parsing of theme/subcategory keys so that
api/, workers/, and metrics/ code can share the same rules without importing
from each other.
"""

from __future__ import annotations

from dev_health_ops.investment_taxonomy import THEMES

__all__ = [
    "THEME_LABELS",
    "THEME_KEYS_BY_LABEL",
    "format_theme_label",
    "format_subcategory_label",
    "normalize_theme_key",
    "split_category_filters",
]

# Canonical human-readable labels for each theme key.
THEME_LABELS: dict[str, str] = {
    "feature_delivery": "Feature Delivery",
    "operational": "Operational / Support",
    "maintenance": "Maintenance / Tech Debt",
    "quality": "Quality / Reliability",
    "risk": "Risk / Security",
}

# Reverse mapping: lowercase label -> theme key.
THEME_KEYS_BY_LABEL: dict[str, str] = {
    label.lower(): key for key, label in THEME_LABELS.items()
}


def _title_case(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").strip().title()


def format_theme_label(theme_key: str) -> str:
    """Return the canonical human-readable label for a theme key.

    Falls back to title-casing the raw key if not in the registry.
    """
    key = str(theme_key or "").strip().lower()
    if key in THEME_LABELS:
        return THEME_LABELS[key]
    return _title_case(theme_key)


def format_subcategory_label(subcategory_key: str) -> str:
    """Return a human-readable label for a subcategory key.

    E.g. "feature_delivery.customer" -> "Feature Delivery · Customer".
    """
    if "." not in subcategory_key:
        return _title_case(subcategory_key)
    theme, sub = subcategory_key.split(".", 1)
    return f"{_title_case(theme)} · {_title_case(sub)}"


def normalize_theme_key(theme_key: str | None) -> str | None:
    """Normalise a theme key or label to a canonical key, or return None."""
    if theme_key is None:
        return None
    raw = str(theme_key).strip()
    if not raw:
        return None
    lowered = raw.lower()
    if lowered in THEMES:
        return lowered
    if lowered in THEME_KEYS_BY_LABEL:
        return THEME_KEYS_BY_LABEL[lowered]
    return None


def split_category_filters(
    work_category: list[str | None] | None,
) -> tuple[list[str], list[str]]:
    """Split work category filter values into (themes, subcategories).

    A value with a dot (e.g. "feature_delivery.customer") is treated as a
    subcategory; its theme prefix is also added to the themes list.
    A value without a dot is treated as a theme only.

    Args:
        work_category: List of raw category strings, possibly containing None.

    Returns:
        (themes, subcategories) — both deduplicated and ordered by first appearance.
    """
    themes: list[str] = []
    subcategories: list[str] = []
    for category in work_category or []:
        if not category:
            continue
        category_str = str(category).strip()
        if not category_str:
            continue
        if "." in category_str:
            subcategories.append(category_str)
            themes.append(category_str.split(".", 1)[0])
        else:
            themes.append(category_str)
    return list(dict.fromkeys(themes)), list(dict.fromkeys(subcategories))
