"""Frozen dataclasses and type aliases for the recommendations engine.

All public types are frozen and (where possible) use __slots__ for memory
efficiency and accidental-mutation protection.

Canonical rule IDs
------------------
"saturation" | "review-concentration" | "thrash" | "sustainability-risk" | "compounding-risk"
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

Theme = Literal[
    "feature-delivery",
    "operational-support",
    "maintenance-tech-debt",
    "quality-reliability",
    "risk-security",
]

Severity = Literal["warning", "critical"]

WindowUnit = Literal["day", "week", "cycle"]


# ---------------------------------------------------------------------------
# Core dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    """Pointer to a stored metric row that supports a recommendation.

    All fields reference persisted ClickHouse data so every recommendation
    is fully traceable to stored evidence.

    Attributes
    ----------
    team_id:
        Team the metric row belongs to.
    metric_table:
        ClickHouse table name (e.g. ``"work_item_metrics_daily"``).
    window_start:
        Inclusive start of the metric window (UTC date).
    window_end:
        Exclusive end of the metric window (UTC date).
    field:
        Column name within *metric_table* that triggered the rule.
    value:
        Observed value of *field* at evaluation time.
    """

    team_id: str
    metric_table: str
    window_start: date
    window_end: date
    field: str
    value: float


@dataclass(frozen=True, slots=True)
class RuleDef:
    """Static definition of a single recommendation rule.

    Instances live in the registry (``registry.py``); they are never created
    at runtime by rule logic.

    Attributes
    ----------
    id:
        Stable machine identifier (e.g. ``"saturation"``).  Must be unique
        across all rules — the registry guard enforces this at import.
    title:
        Short human label shown in UI / reports.
    description:
        One-sentence description of the signal this rule detects.
    success_criterion:
        Measurable condition that, when met, means the recommendation is
        resolved (shown to the operator).
    severity:
        ``"warning"`` for early signals; ``"critical"`` for acute risk.
    theme:
        Investment theme this rule primarily surfaces.
    """

    id: str
    title: str
    description: str
    success_criterion: str
    severity: Severity
    theme: Theme


@dataclass(frozen=True, slots=True)
class Recommendation:
    """A single fired recommendation for a team in a time window.

    Produced by the evaluation engine (CHAOS-1622) and persisted via the
    ClickHouse sink.  Immutable — never mutated after construction.

    Attributes
    ----------
    rule_id:
        Matches ``RuleDef.id``; use ``get_rule(rule_id)`` to resolve the
        full definition.
    team_id:
        Team this recommendation is for.
    org_id:
        Organisation this recommendation is for.
    computed_at:
        UTC datetime the recommendation was produced.  Acts as the
        append-only "triggered at" timestamp.
    window_start:
        Inclusive start of the metric window evaluated (UTC date).
    window_end:
        Exclusive end of the metric window evaluated (UTC date).
    severity:
        Severity at evaluation time (may differ from ``RuleDef.severity``
        if rules support dynamic severity in future; kept here for
        persistence completeness).
    title:
        Human-readable title (copied from ``RuleDef.title`` at compute
        time so the record is self-contained).
    rationale:
        One-sentence explanation of *why* this rule fired (references
        observed metric values).
    success_criterion:
        Copied from ``RuleDef.success_criterion`` at compute time.
    evidence:
        Tuple of evidence pointers to the stored metric rows that caused
        this recommendation to fire.  May be empty only when a rule cannot
        identify specific rows (strongly discouraged).
    """

    rule_id: str
    team_id: str
    org_id: str
    computed_at: datetime
    window_start: date
    window_end: date
    severity: Severity
    title: str
    rationale: str
    success_criterion: str
    evidence: tuple[EvidenceRef, ...]
