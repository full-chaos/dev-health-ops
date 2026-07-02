"""Shared constants for investment materialization and validation."""

from __future__ import annotations

import os

MIN_EVIDENCE_CHARS = 300

# Maximum node count for a single investment work-unit component (CHAOS-2775).
# Connected components of ``work_graph_edges`` larger than this are
# deterministically split (see ``components.build_components``) rather than
# materialized as one giant unit. Without this cap a single densely-linked hub
# (e.g. a changelog PR) can percolate thousands of issues/PRs/commits into one
# component that dominates the Investment allocation chart. Env-overridable via
# ``INVESTMENT_MAX_COMPONENT_NODES``.
INVESTMENT_MAX_COMPONENT_NODES = 150


def resolve_max_component_nodes(value: int | None = None) -> int:
    """Resolve the max component node cap (explicit arg > env > default).

    Falls back to :data:`INVESTMENT_MAX_COMPONENT_NODES` for a missing,
    unparseable, or non-positive value.
    """
    if value is not None:
        return value if value >= 1 else INVESTMENT_MAX_COMPONENT_NODES
    raw = os.getenv("INVESTMENT_MAX_COMPONENT_NODES")
    if raw is None:
        return INVESTMENT_MAX_COMPONENT_NODES
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return INVESTMENT_MAX_COMPONENT_NODES
    return parsed if parsed >= 1 else INVESTMENT_MAX_COMPONENT_NODES


# Minimum category weight for a node to be recorded as a member of that
# theme/subcategory in work_unit_membership (CHAOS-2430). Multi-membership: a
# node is emitted once per category at/above this weight, so a mixed unit (e.g.
# 45% feature / 40% maintenance) is findable under either. The argmax category
# of each kind is always emitted even if below this threshold (is_dominant=1).
MEMBERSHIP_WEIGHT_THRESHOLD = 0.2
