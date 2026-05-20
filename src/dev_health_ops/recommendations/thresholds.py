"""Named numeric constants for the 5 canonical recommendation rules.

Every constant carries a provenance comment quoting the PRD signal table
(docs/product/dev-health-product-market-simplification.md lines 176-180).

These values are intentionally grouped by rule so rule implementations can
import exactly what they need:

    from dev_health_ops.recommendations.thresholds import (
        WIP_RISING_SLOPE_THRESHOLD,
        THROUGHPUT_FLAT_DELTA_THRESHOLD,
    )

Anti-config contract
--------------------
Thresholds live here as named Python constants — NOT in YAML, a database,
or environment variables.  Per-team overrides are explicitly forbidden.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Rule: saturation
# PRD line 176: "Rising WIP + flat throughput →
#                The team is saturating. Reduce active work before adding scope."
# ---------------------------------------------------------------------------

# Minimum daily WIP increase (items/day) averaged over the evaluation window
# that qualifies as "rising".
WIP_RISING_SLOPE_THRESHOLD: float = 0.1

# Maximum items/cycle throughput delta that still qualifies as "flat".
# A delta at or below this value (including negative) signals stagnation.
THROUGHPUT_FLAT_DELTA_THRESHOLD: float = 0.0

# ---------------------------------------------------------------------------
# Rule: review-concentration
# PRD line 177: "High review latency + concentrated reviewers →
#                Review dependency risk. Add reviewers or rotate ownership."
# ---------------------------------------------------------------------------

# p75 review latency (hours) at or above which latency is considered "high".
REVIEW_LATENCY_P75_HOURS: float = 24.0

# Reviewer Gini coefficient at or above which ownership is "concentrated".
# 0.0 = perfectly equal, 1.0 = single reviewer owns everything.
REVIEWER_GINI_THRESHOLD: float = 0.6

# ---------------------------------------------------------------------------
# Rule: thrash
# PRD line 178: "High churn + low delivery →
#                Thrash likely. Inspect hotspots and rework loops."
# ---------------------------------------------------------------------------

# Rework churn ratio (rework LOC / total LOC) at or above which churn is
# considered "high".  Computed by quality.compute_rework_churn_ratio().
CHURN_RATIO_THRESHOLD: float = 0.3

# Items/cycle throughput delta at or below which delivery is "low".
THROUGHPUT_LOW_DELTA_THRESHOLD: float = 0.0

# ---------------------------------------------------------------------------
# Rule: sustainability-risk
# PRD line 179: "High after-hours + rising cycle time →
#                Sustainability risk. Delivery may be propped up by time debt."
# ---------------------------------------------------------------------------

# Fraction of work activity (commits, reviews, etc.) occurring outside
# core business hours that qualifies as "high".
AFTER_HOURS_RATIO_THRESHOLD: float = 0.2

# Minimum hourly increase per day in cycle time trend that qualifies as
# "rising cycle time".
CYCLE_TIME_RISING_SLOPE_THRESHOLD: float = 0.1

# ---------------------------------------------------------------------------
# Rule: compounding-risk
# PRD line 180: "Complexity rising in hotspots →
#                Code risk is compounding where change pressure is highest."
# ---------------------------------------------------------------------------

# Normalised complexity increase (0.0–1.0) in hotspot files required to
# qualify as "rising".
COMPLEXITY_DELTA_THRESHOLD: float = 0.2

# Fraction of hotspot files (by churn volume) that must show rising
# complexity for the rule to fire.
HOTSPOT_CHURN_OVERLAP_THRESHOLD: float = 0.4
