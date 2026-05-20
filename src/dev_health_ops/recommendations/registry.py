"""Canonical registry of all recommendation rule definitions.

The registry is code, not configuration.  Rules are defined as RuleDef
instances in _RULES below.  Two public accessors are provided:

    get_rule(id: str) -> RuleDef   — raises KeyError for unknown ids
    all_rules() -> tuple[RuleDef, ...]  — ordered tuple of all rules

Duplicate-ID guard
------------------
A module-level assertion fires at import time if any two rules share an id,
making registry corruption fail loudly instead of silently losing a rule.

Adding a rule
-------------
1.  Add a RuleDef entry to _RULES.
2.  Add the corresponding thresholds to thresholds.py.
3.  Implement the rule function under recommendations/rules/ (CHAOS-1623).
4.  Add a test asserting the rule id is present and the registry is valid.
"""

from __future__ import annotations

from dev_health_ops.recommendations.schema import RuleDef

# ---------------------------------------------------------------------------
# Canonical rule definitions
# (order is stable; do not reorder — ids are the public contract)
# ---------------------------------------------------------------------------

_RULES: tuple[RuleDef, ...] = (
    RuleDef(
        id="saturation",
        title="Team Saturation",
        description=(
            "Rising WIP combined with flat throughput signals the team is "
            "taking on more work than it can complete."
        ),
        success_criterion=(
            "WIP trend turns negative OR throughput trend turns positive "
            "within 2 cycles."
        ),
        severity="warning",
        theme="operational-support",
    ),
    RuleDef(
        id="review-concentration",
        title="Review Concentration Risk",
        description=(
            "High review latency paired with a concentrated reviewer set "
            "creates a delivery bottleneck and bus-factor risk."
        ),
        success_criterion=(
            "Reviewer Gini drops below threshold OR review latency p75 drops "
            "below threshold within 2 cycles."
        ),
        severity="warning",
        theme="risk-security",
    ),
    RuleDef(
        id="thrash",
        title="Thrash Detected",
        description=(
            "High rework churn alongside low delivery output suggests repeated "
            "work on the same code paths without forward progress."
        ),
        success_criterion=(
            "Churn ratio drops below threshold OR throughput trend turns "
            "positive within 2 cycles."
        ),
        severity="warning",
        theme="quality-reliability",
    ),
    RuleDef(
        id="sustainability-risk",
        title="Sustainability Risk",
        description=(
            "Elevated after-hours activity combined with rising cycle time "
            "suggests delivery is being sustained by time debt."
        ),
        success_criterion=(
            "After-hours ratio drops below threshold AND cycle time trend "
            "stabilises within 2 cycles."
        ),
        severity="critical",
        theme="operational-support",
    ),
    RuleDef(
        id="compounding-risk",
        title="Compounding Code Risk",
        description=(
            "Complexity is rising in the files that already receive the most "
            "change pressure, compounding architectural risk."
        ),
        success_criterion=(
            "Complexity delta in hotspot files drops below threshold within "
            "2 cycles."
        ),
        severity="critical",
        theme="maintenance-tech-debt",
    ),
)

# ---------------------------------------------------------------------------
# Duplicate-ID guard — fires at import time, not at call time
# ---------------------------------------------------------------------------

_ids: list[str] = [rule.id for rule in _RULES]
_seen: set[str] = set()
for _rule_id in _ids:
    if _rule_id in _seen:
        raise ValueError(
            f"Duplicate rule id {_rule_id!r} detected in recommendations registry. "
            "Each rule id must be unique."
        )
    _seen.add(_rule_id)
del _ids, _seen, _rule_id  # clean up module namespace

# ---------------------------------------------------------------------------
# Public accessors
# ---------------------------------------------------------------------------

_INDEX: dict[str, RuleDef] = {rule.id: rule for rule in _RULES}


def get_rule(rule_id: str) -> RuleDef:
    """Return the RuleDef for *rule_id*.

    Raises
    ------
    KeyError
        If *rule_id* is not a known canonical rule id.
    """
    try:
        return _INDEX[rule_id]
    except KeyError:
        known = ", ".join(repr(r) for r in _INDEX)
        raise KeyError(
            f"Unknown rule id {rule_id!r}. Known ids: {known}"
        ) from None


def all_rules() -> tuple[RuleDef, ...]:
    """Return all registered rules in stable definition order."""
    return _RULES
