from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

from dev_health_ops.metrics.schemas import (
    AIImpactMetricsDailyRecord,
    AIOperatingLeverageComponents,
)
from dev_health_ops.metrics.sinks.clickhouse.ai_impact import AIImpactMixin

# CHAOS-5234/CHAOS-3092: this file used to hold ~500 lines of business-logic
# tests directly against compute_ai_impact_metrics_daily (float/decomposition
# rules, revert/rework/test-gap semantics, followup-commit derivation, etc.).
# That function is DELETED (chris's standing rule, CHAOS-5233: once a
# family's Go executor is on main, its Python compute is deleted) -- its
# Go bit-exact oracle rot guard (TestAIImpactMatchesLivePythonProduction,
# internal/jobs/metrics/aiimpact/testdata/python_ai_impact_oracle.py) is
# ALSO deleted, since it was the last real caller keeping the Python
# function alive, and AIImpactExecutor (native Go, CHAOS-4280) is now the
# sole computer of ai_impact_metrics_daily -- its own Go-only tests in
# internal/jobs/metrics/aiimpact/compute_test.go remain the real coverage.
#
# ONE test survives here, rewritten: test_clickhouse_sink_writes_ai_impact_
# rows_with_dimensions tested the SINK's write_ai_impact_metrics, not the
# compute function -- it used to build its input row via
# compute_ai_impact_metrics_daily (through this file's old _rows/_pr/_attr
# helpers) purely as a convenient way to get a populated
# AIImpactMetricsDailyRecord. write_ai_impact_metrics itself still has a
# real caller (tests/api/graphql/test_go_api_dual_run_operating_review.py
# seeds data through it), so this sink-level coverage is preserved by
# constructing the record directly instead.

DAY = date(2026, 5, 18)
COMPUTED_AT = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)


def test_clickhouse_sink_writes_ai_impact_rows_with_dimensions():
    row = AIImpactMetricsDailyRecord(
        org_id="org-a",
        team_id=None,
        repo_id=uuid4(),
        work_type="pull_request",
        day=DAY,
        attribution_bucket="ai_assisted",
        prs_total=1,
        prs_merged=1,
        ai_assisted_prs=1,
        agent_created_prs=0,
        human_prs=0,
        unknown_prs=0,
        ai_assisted_pr_ratio=1.0,
        agent_created_pr_count=0,
        cycle_time_avg_hours=2.0,
        baseline_cycle_time_avg_hours=None,
        ai_cycle_time_delta_hours=None,
        reviews_per_pr=0.0,
        baseline_reviews_per_pr=None,
        ai_review_amplification=None,
        changes_requested_per_pr=0.0,
        rework_prs=0,
        rework_drag_rate=None,
        followup_commits_count=0,
        revert_prs=0,
        revert_rate=None,
        incidents_count=0,
        incident_drag_rate=None,
        test_gap_prs=0,
        test_gap_rate=None,
        leverage=AIOperatingLeverageComponents(
            prs_component=1.0,
            cycle_time_component=None,
            review_component=None,
            rework_component=None,
            test_component=None,
            incident_component=None,
        ),
        computed_at=COMPUTED_AT,
    )

    class Client:
        def __init__(self):
            self.calls = []

        def insert(self, table, matrix, column_names):
            self.calls.append((table, matrix, column_names))

    class Sink(AIImpactMixin):
        def __init__(self):
            self.client = Client()

    sink = Sink()
    sink.write_ai_impact_metrics([row])

    table, matrix, columns = sink.client.calls[0]
    assert table == "ai_impact_metrics_daily"
    assert "org_id" in columns
    assert "team_id" in columns
    assert "repo_id" in columns
    assert "work_type" in columns
    assert matrix[0][columns.index("attribution_bucket")] == "ai_assisted"
