"""CHAOS-9604 (D5988, D5998): a co-owning team holds a share of a repository's
pull-request counts, so the stored counts are Float64 and the loader must not
truncate them."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone

from dev_health_ops.metrics.loaders.ai_impact import _to_record

_COUNT_FIELDS = {
    "prs_total": 3.3,
    "prs_merged": 2.4,
    "ai_assisted_prs": 0.9,
    "agent_created_prs": 0.6,
    "human_prs": 1.2,
    "unknown_prs": 0.3,
    "agent_created_pr_count": 1.1,
    "rework_prs": 1.5,
    "followup_commits_count": 1.8,
    "revert_prs": 0.3,
    "incidents_count": 0.6,
    "test_gap_prs": 1.2,
}


def test_fractional_counts_are_kept_not_truncated() -> None:
    raw = {
        "org_id": "org",
        "team_id": "lesser",
        "repo_id": str(uuid.uuid4()),
        "day": date(2026, 8, 1),
        "computed_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
        **_COUNT_FIELDS,
    }
    record = _to_record(raw)
    for name, value in _COUNT_FIELDS.items():
        assert getattr(record, name) == value, name


def test_a_missing_count_reads_zero() -> None:
    raw = {
        "repo_id": str(uuid.uuid4()),
        "day": date(2026, 8, 1),
        "computed_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
    }
    record = _to_record(raw)
    for name in _COUNT_FIELDS:
        assert getattr(record, name) == 0, name
