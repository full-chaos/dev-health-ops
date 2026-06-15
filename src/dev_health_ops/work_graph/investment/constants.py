"""Shared constants for investment materialization and validation."""

MIN_EVIDENCE_CHARS = 300

# Minimum category weight for a node to be recorded as a member of that
# theme/subcategory in work_unit_membership (CHAOS-2430). Multi-membership: a
# node is emitted once per category at/above this weight, so a mixed unit (e.g.
# 45% feature / 40% maintenance) is findable under either. The argmax category
# of each kind is always emitted even if below this threshold (is_dominant=1).
MEMBERSHIP_WEIGHT_THRESHOLD = 0.2
