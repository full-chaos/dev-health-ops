"""Rule-based operational recommendations engine.

This package provides the foundational registry and schema for deterministic,
rule-based recommendations derived from persisted ClickHouse metrics.

Sub-modules
-----------
schema      — Frozen dataclasses: RuleDef, EvidenceRef, Recommendation + type aliases.
thresholds  — Named numeric constants for the 5 canonical rules (with PRD provenance).
registry    — Canonical RuleDef list keyed by rule id; get_rule(), all_rules().

Out of scope here
-----------------
engine, rule logic, sinks, and GraphQL are implemented in sibling sub-issues
(CHAOS-1622, CHAOS-1623, CHAOS-1624).
"""

from dev_health_ops.recommendations.registry import all_rules, get_rule
from dev_health_ops.recommendations.schema import (
    EvidenceRef,
    Recommendation,
    RuleDef,
    Severity,
    Theme,
    WindowUnit,
)

__all__ = [
    "EvidenceRef",
    "Recommendation",
    "RuleDef",
    "Severity",
    "Theme",
    "WindowUnit",
    "all_rules",
    "get_rule",
]
