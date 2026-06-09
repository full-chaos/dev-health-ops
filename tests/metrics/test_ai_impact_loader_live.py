"""Live-ClickHouse fixture-backed tests for CHAOS-2180 Wave 2 loader fixes.

Covers the three Codex-review regressions with controlled, inserted data:

* **Event-day semantics** — a PR created before the window but merged inside
  it must land its engagement row on the MERGE day (the same
  ``event_at = merged_at or created_at`` rule as
  ``compute_ai_impact_metrics_daily``), not on the out-of-window creation day.
* **Multi-tenant isolation** — rows from a foreign org sharing repo_id and
  commit_hash values must not leak into this org's overlap counts.
* **Discriminating hotspot threshold** — with one clear hotspot file among
  many low-risk files, PRs touching only low-risk files score 0 and mixed
  populations land strictly between 0 and 1 (a bare ``risk_score > 0`` cut
  saturates at ~1.0).

Run locally with:
  CLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/default \
  pytest tests/metrics/test_ai_impact_loader_live.py -v

Skips automatically when CLICKHOUSE_URI is unset so it's CI-safe.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone

import pytest

from dev_health_ops.metrics.loaders.ai_impact import AIImpactClickHouseLoader

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]

# Deterministic ids: reruns overwrite the same ReplacingMergeTree keys.
ORG = "11110000-c218-0000-0000-00000000aaaa"
FOREIGN_ORG = "22220000-c218-0000-0000-00000000bbbb"
REPO = uuid.UUID("33330000-c218-0000-0000-00000000cccc")

START_DAY = date(2026, 5, 1)
END_DAY = date(2026, 5, 31)
START = datetime(2026, 5, 1, tzinfo=timezone.utc)
END = datetime(2026, 5, 31, 23, 59, 59, tzinfo=timezone.utc)

CREATED_BEFORE_WINDOW = datetime(2026, 4, 20, 9, 0, 0)
MERGED_IN_WINDOW = datetime(2026, 5, 10, 15, 0, 0)
FIRST_REVIEW = datetime(2026, 4, 20, 13, 0, 0)  # 4h after creation
NOW = datetime(2026, 5, 31, 12, 0, 0)


def _sync_client():
    import clickhouse_connect

    return clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)


def _seed(client) -> None:
    """Insert the controlled fixture rows (idempotent via ReplacingMergeTree)."""
    # --- PRs ------------------------------------------------------------
    # PR 9001: created BEFORE the window, merged INSIDE it (event-day test);
    #          touches ONLY the hotspot file.
    # PR 9002: created+merged inside the window; touches ONLY low-risk files.
    # PR 9003: created+merged inside the window; touches ONLY low-risk files.
    client.insert(
        "git_pull_requests",
        [
            [
                str(REPO),
                9001,
                "hotspot pr",
                "open->merge",
                "merged",
                "a",
                "a@x",
                CREATED_BEFORE_WINDOW,
                MERGED_IN_WINDOW,
                MERGED_IN_WINDOW,
                "f",
                "main",
                100,
                50,
                1,
                FIRST_REVIEW,
                None,
                0,
                1,
                3,
                NOW,
                ORG,
            ],
            [
                str(REPO),
                9002,
                "cold pr",
                "",
                "merged",
                "a",
                "a@x",
                datetime(2026, 5, 5, 9, 0),
                datetime(2026, 5, 6, 9, 0),
                datetime(2026, 5, 6, 9, 0),
                "f",
                "main",
                10,
                5,
                1,
                datetime(2026, 5, 5, 11, 0),
                None,
                0,
                1,
                1,
                NOW,
                ORG,
            ],
            [
                str(REPO),
                9003,
                "cold pr 2",
                "",
                "merged",
                "a",
                "a@x",
                datetime(2026, 5, 7, 9, 0),
                datetime(2026, 5, 8, 9, 0),
                datetime(2026, 5, 8, 9, 0),
                "f",
                "main",
                10,
                5,
                1,
                datetime(2026, 5, 7, 11, 0),
                None,
                0,
                1,
                1,
                NOW,
                ORG,
            ],
        ],
        column_names=[
            "repo_id",
            "number",
            "title",
            "body",
            "state",
            "author_name",
            "author_email",
            "created_at",
            "merged_at",
            "closed_at",
            "head_branch",
            "base_branch",
            "additions",
            "deletions",
            "changed_files",
            "first_review_at",
            "first_comment_at",
            "changes_requested_count",
            "reviews_count",
            "comments_count",
            "last_synced",
            "org_id",
        ],
    )

    # --- Attributions (direct subject_id path: "<number>") ---------------
    attr_cols = [
        "record_id",
        "org_id",
        "provider",
        "subject_type",
        "subject_id",
        "repo_id",
        "kind",
        "source",
        "confidence",
        "actor",
        "evidence",
        "observed_at",
        "ingested_at",
        "computed_at",
    ]

    def _attr(number: int, org: str) -> list:
        # Deterministic record_id per (org, number) so reruns replace.
        rid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"c2180-live-{org}-{number}"))
        return [
            rid,
            org,
            "github",
            "pull_request",
            str(number),
            str(REPO),
            "ai_assisted",
            "commit_trailer",
            0.9,
            None,
            "test",
            NOW,
            NOW,
            NOW,
        ]

    client.insert(
        "ai_attribution",
        [_attr(9001, ORG), _attr(9002, ORG), _attr(9003, ORG)],
        column_names=attr_cols,
    )

    # --- PR -> commit -> files -------------------------------------------
    pc_cols = [
        "repo_id",
        "pr_number",
        "commit_hash",
        "confidence",
        "provenance",
        "evidence",
        "last_synced",
        "org_id",
    ]
    client.insert(
        "work_graph_pr_commit",
        [
            [str(REPO), 9001, "c2180hot", 1.0, "native", "test", NOW, ORG],
            [str(REPO), 9002, "c2180cold1", 1.0, "native", "test", NOW, ORG],
            [str(REPO), 9003, "c2180cold2", 1.0, "native", "test", NOW, ORG],
        ],
        column_names=pc_cols,
    )
    cs_cols = [
        "repo_id",
        "commit_hash",
        "file_path",
        "additions",
        "deletions",
        "old_file_mode",
        "new_file_mode",
        "last_synced",
        "org_id",
    ]
    client.insert(
        "git_commit_stats",
        [
            [str(REPO), "c2180hot", "src/hot.py", 100, 50, "", "", NOW, ORG],
            [str(REPO), "c2180cold1", "src/cold1.py", 10, 5, "", "", NOW, ORG],
            [str(REPO), "c2180cold2", "src/cold2.py", 10, 5, "", "", NOW, ORG],
        ],
        column_names=cs_cols,
    )

    # --- Hotspot snapshots: 1 hot file among 10 (top decile = exactly 1) --
    hs_cols = [
        "repo_id",
        "day",
        "file_path",
        "churn_loc_30d",
        "churn_commits_30d",
        "cyclomatic_total",
        "cyclomatic_avg",
        "blame_concentration",
        "risk_score",
        "computed_at",
        "org_id",
    ]
    hs_rows = [
        [
            str(REPO),
            date(2026, 5, 15),
            "src/hot.py",
            500,
            50,
            100,
            10.0,
            None,
            5.0,
            NOW,
            ORG,
        ]
    ]
    for i in range(1, 10):
        path = f"src/cold{i}.py" if i <= 2 else f"src/filler{i}.py"
        hs_rows.append(
            [str(REPO), date(2026, 5, 15), path, 10, 1, 5, 1.0, None, 0.1, NOW, ORG]
        )
    client.insert("file_hotspot_daily", hs_rows, column_names=hs_cols)

    # --- Foreign-org rows with IDENTICAL repo_id + commit hashes ----------
    # A foreign PR 9002 link pointing its commit at the HOT file: without
    # org scoping on pc/cs this would flip PR 9002 into a hotspot-toucher.
    client.insert(
        "work_graph_pr_commit",
        [[str(REPO), 9002, "c2180foreign", 1.0, "native", "test", NOW, FOREIGN_ORG]],
        column_names=pc_cols,
    )
    client.insert(
        "git_commit_stats",
        [
            [str(REPO), "c2180foreign", "src/hot.py", 1, 1, "", "", NOW, FOREIGN_ORG],
            # Foreign stats for an in-org commit hash: org filter on cs must
            # exclude this row even though pc resolves in-org.
            [str(REPO), "c2180cold1", "src/hot.py", 1, 1, "", "", NOW, FOREIGN_ORG],
        ],
        column_names=cs_cols,
    )


@pytest.fixture(scope="module")
def seeded() -> None:
    client = _sync_client()
    try:
        _seed(client)
    finally:
        client.close()


async def _client():
    from dev_health_ops.api.queries.client import get_global_client

    assert CLICKHOUSE_URI is not None
    return await get_global_client(CLICKHOUSE_URI)


async def test_engagement_daily_lands_on_merge_day(seeded) -> None:
    """PR 9001 (created 2026-04-20, merged 2026-05-10) must produce its
    engagement on the merge day — inside the window — not on the creation
    day, matching compute_ai_impact_metrics_daily's event_at rule."""
    loader = AIImpactClickHouseLoader(await _client(), org_id=ORG)
    rows = await loader.load_review_engagement(start=START, end=END, repo_id=REPO)
    ai_days = {r["day"] for r in rows if r["bucket"] == "ai_assisted"}
    assert date(2026, 5, 10) in ai_days, f"merge-day cell missing: {rows}"
    assert date(2026, 4, 20) not in ai_days, "engagement leaked to creation day"
    merge_day_row = next(
        r
        for r in rows
        if r["bucket"] == "ai_assisted" and r["day"] == date(2026, 5, 10)
    )
    # Pickup latency still measures open -> first review (4h), regardless of
    # which day the PR is bucketed under.
    assert merge_day_row["prs_with_first_review"] == 1
    assert float(merge_day_row["pickup_latency_hours"]) == pytest.approx(4.0)


async def test_hotspot_overlap_rate_is_fractional_not_saturated(seeded) -> None:
    """1 of 3 assessable AI PRs touches the single top-decile file → rate
    must be strictly between 0 and 1 (here 1/3), and the foreign-org rows
    sharing repo_id/commit_hash must not inflate the count."""
    loader = AIImpactClickHouseLoader(await _client(), org_id=ORG)
    rows = await loader.load_hotspot_overlap(
        start=START,
        end=END,
        start_day=START_DAY,
        end_day=END_DAY,
        repo_id=REPO,
    )
    by_bucket = {r["bucket"]: r for r in rows}
    ai = by_bucket["ai_assisted"]
    assert int(ai["prs_total"]) == 3
    assert int(ai["prs_touching_hotspots"]) == 1, (
        "expected exactly PR 9001 to touch the top-decile file; foreign-org "
        f"leakage or threshold saturation? row={ai}"
    )
    rate = int(ai["prs_touching_hotspots"]) / int(ai["prs_total"])
    assert 0.0 < rate < 1.0
    assert float(ai["avg_hotspot_risk_score"]) == pytest.approx(5.0)


async def test_hotspot_overlap_zero_when_only_low_risk_touched(seeded) -> None:
    """Scoped to a window where only the cold PRs exist (9001 merged on the
    10th; restrict to the 1st-9th window → only 9002/9003): rate must be a
    real computed zero."""
    loader = AIImpactClickHouseLoader(await _client(), org_id=ORG)
    rows = await loader.load_hotspot_overlap(
        start=datetime(2026, 5, 1, tzinfo=timezone.utc),
        end=datetime(2026, 5, 9, tzinfo=timezone.utc),
        start_day=START_DAY,
        end_day=END_DAY,
        repo_id=REPO,
    )
    by_bucket = {r["bucket"]: r for r in rows}
    ai = by_bucket["ai_assisted"]
    assert int(ai["prs_total"]) == 2
    assert int(ai["prs_touching_hotspots"]) == 0
