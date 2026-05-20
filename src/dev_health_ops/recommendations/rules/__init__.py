"""Canonical recommendation rules — auto-registered from this package.

Each rule is a pure function::

    evaluate_<rule>(snapshot: MetricsSnapshot, now: datetime) -> Recommendation | None

Import RULE_EVALUATORS to get the full id → callable mapping that the
RuleEngine iterates over.

NOTE: imports of schema / thresholds will resolve once CHAOS-1621 (rec-foundation)
and CHAOS-1622 (rec-engine-core) land. Until then this module uses
forward-compatible TYPE_CHECKING guards.
"""

from __future__ import annotations

from dev_health_ops.recommendations.rules.compounding_risk import (
    evaluate_compounding_risk,
)
from dev_health_ops.recommendations.rules.review_concentration import (
    evaluate_review_concentration,
)
from dev_health_ops.recommendations.rules.saturation import evaluate_saturation
from dev_health_ops.recommendations.rules.sustainability_risk import (
    evaluate_sustainability_risk,
)
from dev_health_ops.recommendations.rules.thrash import evaluate_thrash

# Canonical mapping: rule_id → evaluator callable.
# The RuleEngine iterates this dict; order is deterministic (insertion order).
RULE_EVALUATORS = {
    "saturation": evaluate_saturation,
    "review-concentration": evaluate_review_concentration,
    "thrash": evaluate_thrash,
    "sustainability-risk": evaluate_sustainability_risk,
    "compounding-risk": evaluate_compounding_risk,
}

__all__ = [
    "RULE_EVALUATORS",
    "evaluate_compounding_risk",
    "evaluate_review_concentration",
    "evaluate_saturation",
    "evaluate_sustainability_risk",
    "evaluate_thrash",
]
