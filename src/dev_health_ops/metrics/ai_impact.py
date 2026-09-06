from __future__ import annotations

from enum import StrEnum

# CHAOS-5234/CHAOS-3092: compute_ai_impact_metrics_daily and every private
# helper that fed it (_PRFact, _Agg, _to_utc, _avg, _ratio, _safe_bucket,
# _is_test_path, _attribution_index, _reviews_by_pr, _test_changes_by_pr,
# _first_review_at_by_pr, _followup_commits_by_pr, _aggregate,
# _component_delta, TeamResolver, NON_AI_BUCKET, UNKNOWN_BUCKET) are DELETED
# -- chris's standing rule (CHAOS-5233): once a family's Go executor is on
# main, its Python compute is deleted, never kept alive just to give a rot
# guard something to compare against. AIImpactExecutor (native Go,
# CHAOS-4280) is now the only computer of ai_impact_metrics_daily.
# codegraph_explore + rg confirmed compute_ai_impact_metrics_daily's only
# real callers, once job_daily.py's own reference was removed, were its Go
# bit-exact oracle rot guard (TestAIImpactMatchesLivePythonProduction +
# internal/jobs/metrics/aiimpact/testdata/python_ai_impact_oracle.py) and
# its own dedicated tests (tests/metrics/test_ai_impact.py) -- both also
# deleted in this same PR (one sink-write test from that file was preserved
# by rewriting it to construct an AIImpactMetricsDailyRecord directly
# instead of going through the compute function).
#
# AttributionBucket and AI_BUCKETS below are NOT touched -- they have real,
# separate callers: the GraphQL API resolver
# (api/graphql/resolvers/ai.py) and the opportunities detector
# (metrics/opportunities/ai_detector.py).


class AttributionBucket(StrEnum):
    """Canonical buckets for AI workflow impact rollups.

    The metrics layer slots every PR (and every reviewed artifact) into
    exactly one bucket so the per-bucket aggregates remain disjoint and
    sum to the row total. Bucket string values are also the storage
    representation in ClickHouse (``ai_impact_metrics_daily.attribution_bucket``).
    """

    AI_ASSISTED = "ai_assisted"
    AGENT_CREATED = "agent_created"
    AI_REVIEW = "ai_review"
    HUMAN = "human"
    UNKNOWN = "unknown"


#: AI-coded variants of :class:`AttributionBucket`.
AI_BUCKETS: frozenset[AttributionBucket] = frozenset(
    {
        AttributionBucket.AI_ASSISTED,
        AttributionBucket.AGENT_CREATED,
        AttributionBucket.AI_REVIEW,
    }
)
