"""Shared work_unit_membership projection (CHAOS-2429/2430/2439).

This module holds the SINGLE source of truth for turning a work unit's theme /
subcategory distributions into ``work_unit_membership`` rows, so the post-sync
LLM materializer (``materialize.py``) and the daily no-LLM backfill
(``backfill.py``) emit byte-for-byte identical rows for the same persisted
distributions. The projection is pure: it never calls the categorizer / LLM.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from dev_health_ops.metrics.schemas import WorkUnitMembershipRecord
from dev_health_ops.work_graph.investment.constants import MEMBERSHIP_WEIGHT_THRESHOLD

NodeKey = tuple[str, str]


def _float_value(value: object) -> float:
    """Best-effort float coercion (bools are not numbers here)."""
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def lexical_argmax(distribution: dict[str, float]) -> str:
    """Return the key with the highest value; break ties lexically (smallest wins).

    An empty distribution returns "unknown". The lexical tie-break makes the
    dominant choice deterministic across runs regardless of dict ordering.
    """
    if not distribution:
        return "unknown"
    return min(distribution, key=lambda k: (-_float_value(distribution[k]), k))


def membership_categories(
    distribution: dict[str, float],
) -> list[tuple[str, float, int]]:
    """Return (category, weight, is_dominant) rows to emit for one distribution.

    Multi-membership: every category with weight >= MEMBERSHIP_WEIGHT_THRESHOLD
    is emitted, so a mixed unit is findable under each significant category. The
    argmax category (lexical tie-break) is ALWAYS included even when below the
    threshold and is flagged ``is_dominant=1``, so every node is findable under
    at least its dominant category. Returns at least one row whenever the
    distribution is non-empty.
    """
    if not distribution:
        return []
    dominant = lexical_argmax(distribution)
    out: list[tuple[str, float, int]] = []
    seen: set[str] = set()
    for category, raw_weight in distribution.items():
        weight = _float_value(raw_weight)
        is_dominant = 1 if category == dominant else 0
        if weight >= MEMBERSHIP_WEIGHT_THRESHOLD or is_dominant:
            out.append((category, weight, is_dominant))
            seen.add(category)
    # Defensive: ensure the dominant row is present even if it was filtered.
    if dominant not in seen:
        out.append((dominant, _float_value(distribution.get(dominant, 0.0)), 1))
    return out


def build_membership_records(
    *,
    unit_nodes: Iterable[NodeKey],
    work_unit_id: str,
    theme_distribution: dict[str, float],
    subcategory_distribution: dict[str, float],
    categorization_status: str,
    computed_at: datetime,
    org_id: str,
) -> list[WorkUnitMembershipRecord]:
    """Project one work unit's distributions into ``work_unit_membership`` rows.

    Emits one row per (node, category) for the theme distribution and one per
    (node, category) for the subcategory distribution, using
    ``membership_categories`` for the threshold + is_dominant logic. This is the
    shared seam used by BOTH the LLM materializer and the no-LLM backfill, so the
    two paths produce identical rows for identical persisted distributions.
    """
    theme_categories = membership_categories(theme_distribution)
    subcategory_categories = membership_categories(subcategory_distribution)
    records: list[WorkUnitMembershipRecord] = []
    for node_type, node_id in unit_nodes:
        for category, weight, is_dominant in theme_categories:
            records.append(
                WorkUnitMembershipRecord(
                    org_id=org_id,
                    node_type=node_type,
                    node_id=node_id,
                    work_unit_id=work_unit_id,
                    category_kind="theme",
                    category=category,
                    weight=weight,
                    is_dominant=is_dominant,
                    categorization_status=categorization_status,
                    computed_at=computed_at,
                )
            )
        for category, weight, is_dominant in subcategory_categories:
            records.append(
                WorkUnitMembershipRecord(
                    org_id=org_id,
                    node_type=node_type,
                    node_id=node_id,
                    work_unit_id=work_unit_id,
                    category_kind="subcategory",
                    category=category,
                    weight=weight,
                    is_dominant=is_dominant,
                    categorization_status=categorization_status,
                    computed_at=computed_at,
                )
            )
    return records
