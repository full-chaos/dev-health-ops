"""Live-Python oracle for the ai_impact native port (CHAOS-4280).

Runs the PRODUCTION function -- metrics.ai_impact.compute_ai_impact_metrics_daily
-- over a pinned fixture and prints the result as JSON on the last stdout line.
The Go side builds the byte-identical fixture and compares every persisted
column, bit-exactly.

Run only through ci/check_go.sh's live-python-oracles verb, which also checks
this run's proof marker.

# What the fixture is built to catch -- and what it is NOT

Each PR below exists to make one specific parity rule observable. Every claim
here was verified by MUTATION, not by inspection; the two float rules landed
differently and the difference is recorded rather than smoothed over.

  * NEUMAIER SUMMATION -- caught by THIS oracle. cycle_time_avg_hours is
    _avg() = sum()/len() over floats, and CPython >= 3.12's sum() is
    Neumaier-COMPENSATED. PRs 1-3 give the ai_assisted bucket three cycle
    values SEARCHED FOR (not guessed) such that a naive Go `total +=` loop
    produces a different float64 mean: 19.30812752564815 vs the correct
    19.308127525648146. Verified by reverting pythonparity.Sum and watching
    this oracle redden.

    The first draft of this fixture used tidy values (1h+1us, 3h+7us,
    11h+999999us) and asserted the same thing. It was WRONG: the mutant
    survived the oracle and was caught only by the unit test. Values that
    merely look irregular do not exercise compensated summation -- the
    magnitudes have to be spread enough for the compensation term to matter.

  * DIVISION ORDER -- NOT caught by this oracle. cycle_hours is
    (merged - created).total_seconds() / 3600.0: two divisions in that order
    over an exact microsecond integer, where Go's Duration.Hours() is a
    different rounding. Swapping in Duration.Hours() leaves this oracle GREEN
    for these inputs -- the two orders happen to agree on every duration here.
    TestCycleHoursUsesPythonsDivisionOrder is the real pin for that rule, and
    it is where the mutant actually dies. Do not read a green oracle as
    evidence that the division order is correct.

computed_at is excluded from the comparison: it is now() on both sides
(standing rot-guard rule -- compare the payload, never provenance).
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, cast

from dev_health_ops.metrics.ai_impact import compute_ai_impact_metrics_daily
from dev_health_ops.metrics.schemas import (
    AIPullRequestAttributionRow,
    CommitStatRow,
    IncidentRow,
    PullRequestReviewRow,
    PullRequestRow,
)

DAY = date(2026, 9, 3)
ORG = "70d529e0-3c06-4597-8480-794fd02328b6"
REPO = uuid.UUID("d4f322ad-2102-1fbf-8425-7400573194f7")
REPO_B = uuid.UUID("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")

BASE = datetime(2026, 9, 3, 0, 0, 0, tzinfo=timezone.utc)


def pr(
    number: int,
    *,
    created_offset_us: int,
    merged_offset_us: int | None = None,
    repo: uuid.UUID = REPO,
    **extra: Any,
) -> PullRequestRow:
    row: dict[str, Any] = {
        "repo_id": repo,
        "number": number,
        "created_at": BASE + timedelta(microseconds=created_offset_us),
        "merged_at": (
            None
            if merged_offset_us is None
            else BASE + timedelta(microseconds=merged_offset_us)
        ),
        "additions": 10,
        "deletions": 5,
        "changed_files": 2,
    }
    row.update(extra)
    return cast(PullRequestRow, row)


# PRs 1-3 carry SEARCHED values (see the module docstring): their three cycle
# hours are the ones that make compensated and naive summation disagree in the
# persisted mean. Do not "tidy" them -- tidy values silently disarm the oracle.
HOUR_US = 3_600_000_000
PULL_REQUESTS: list[PullRequestRow] = [
    # 1-3: ai_assisted, three distinct non-round cycle times (Neumaier).
    pr(1, created_offset_us=0, merged_offset_us=57_078_437_271),
    pr(2, created_offset_us=0, merged_offset_us=73_634_014_885),
    pr(3, created_offset_us=0, merged_offset_us=77_815_325_121),
    # 4-5: human baseline, also three-summand-free but non-round.
    pr(4, created_offset_us=0, merged_offset_us=2 * HOUR_US + 333_333),
    pr(5, created_offset_us=0, merged_offset_us=5 * HOUR_US + 666_667),
    # 6: agent_created, unmerged -> cycle_hours None, prs_merged excludes it.
    pr(6, created_offset_us=HOUR_US),
    # 7: ai_review bucket.
    pr(7, created_offset_us=0, merged_offset_us=HOUR_US + 12_345),
    # 8: no attribution row at all -> unknown bucket AND team_resolver path.
    pr(8, created_offset_us=0, merged_offset_us=HOUR_US + 54_321),
    # 9: revert shape -- deletions > additions*2 AND deletions >= 50.
    pr(9, created_offset_us=0, merged_offset_us=HOUR_US, additions=10, deletions=60),
    # 10: near-revert negative control -- deletions >= 50 but NOT > additions*2.
    pr(10, created_offset_us=0, merged_offset_us=HOUR_US, additions=30, deletions=60),
    # 11: reviews_count present but ZERO -> Python's `or` falls back to the
    #     count derived from review rows. The truthiness rule, not the column.
    pr(11, created_offset_us=0, merged_offset_us=HOUR_US, reviews_count=0),
    # 12: reviews_count present and non-zero -> the column wins.
    pr(12, created_offset_us=0, merged_offset_us=HOUR_US, reviews_count=7),
    # 13: a second repo, so grouping by repo is exercised.
    pr(13, created_offset_us=0, merged_offset_us=HOUR_US, repo=REPO_B),
    # 14: OUT OF WINDOW (merged the next day) -> excluded entirely.
    pr(14, created_offset_us=0, merged_offset_us=25 * HOUR_US),
    # 15: created in-window, never merged -> included via created_at.
    pr(15, created_offset_us=2 * HOUR_US),
]

REVIEWS: list[PullRequestReviewRow] = cast(
    list[PullRequestReviewRow],
    [
        # PR 11 has two reviews, one CHANGES_REQUESTED -> drives the fallback.
        {
            "repo_id": REPO,
            "number": 11,
            "state": "APPROVED",
            "submitted_at": BASE + timedelta(minutes=10),
        },
        {
            "repo_id": REPO,
            "number": 11,
            "state": "changes_requested",
            "submitted_at": BASE + timedelta(minutes=20),
        },
        # Lowercase input proves .upper() is applied rather than an exact match.
        {
            "repo_id": REPO,
            "number": 12,
            "state": "CHANGES_REQUESTED",
            "submitted_at": BASE + timedelta(minutes=5),
        },
        # A review with no submitted_at must not break first_review_at.
        {"repo_id": REPO, "number": 1, "state": "APPROVED", "submitted_at": None},
    ],
)

ATTRIBUTIONS: list[AIPullRequestAttributionRow] = cast(
    list[AIPullRequestAttributionRow],
    [
        {
            "repo_id": REPO,
            "number": 1,
            "kind": "ai_assisted",
            "work_type": "pull_request",
            "team_id": None,
        },
        # Mixed case + hyphen -> _safe_bucket must strip/lower/replace.
        {
            "repo_id": REPO,
            "number": 2,
            "kind": "  AI-Assisted  ",
            "work_type": "pull_request",
            "team_id": None,
        },
        {
            "repo_id": REPO,
            "number": 3,
            "kind": "ai_assisted",
            "work_type": "pull_request",
            "team_id": None,
        },
        {
            "repo_id": REPO,
            "number": 4,
            "kind": "human",
            "work_type": "pull_request",
            "team_id": None,
        },
        {
            "repo_id": REPO,
            "number": 5,
            "kind": "human",
            "work_type": "pull_request",
            "team_id": None,
        },
        {
            "repo_id": REPO,
            "number": 6,
            "kind": "agent_created",
            "work_type": "pull_request",
            "team_id": None,
        },
        {
            "repo_id": REPO,
            "number": 7,
            "kind": "ai_review",
            "work_type": "pull_request",
            "team_id": None,
        },
        # 9/10/11/12 are human so they land in the baseline bucket.
        {
            "repo_id": REPO,
            "number": 9,
            "kind": "human",
            "work_type": "pull_request",
            "team_id": None,
        },
        {
            "repo_id": REPO,
            "number": 10,
            "kind": "human",
            "work_type": "pull_request",
            "team_id": None,
        },
        {
            "repo_id": REPO,
            "number": 11,
            "kind": "human",
            "work_type": "pull_request",
            "team_id": None,
        },
        {
            "repo_id": REPO,
            "number": 12,
            "kind": "human",
            "work_type": "pull_request",
            "team_id": None,
        },
        # An unrecognised kind -> unknown, never folded into the human baseline.
        {
            "repo_id": REPO,
            "number": 15,
            "kind": "vibes",
            "work_type": "pull_request",
            "team_id": None,
        },
        # A different work_type -> its own group.
        {
            "repo_id": REPO_B,
            "number": 13,
            "kind": "ai_assisted",
            "work_type": "issue",
            "team_id": None,
        },
    ],
)

INCIDENTS: list[IncidentRow] = cast(
    list[IncidentRow],
    [
        {"repo_id": REPO, "started_at": BASE + timedelta(hours=3)},
        {"repo_id": REPO, "started_at": BASE + timedelta(hours=4)},
        # Out of window -> must not count.
        {"repo_id": REPO, "started_at": BASE + timedelta(hours=30)},
    ],
)

# PR->commit linkage. Present (not None), so has_test_change is KNOWN for the
# PRs listed and unknown (None) for those absent -- the CHAOS-2183 distinction.
PR_COMMIT_STATS: dict[tuple[uuid.UUID, int], list[CommitStatRow]] = cast(
    dict[tuple[uuid.UUID, int], list[CommitStatRow]],
    {
        # PR 1: a test file -> has_test_change True.
        (REPO, 1): [
            {
                "file_path": "src/Tests/Thing.spec.ts",  # mixed case -> needs .lower()
                "commit_hash": "aaa",
                "committer_when": BASE + timedelta(minutes=30),
                "evidence": "native",
            }
        ],
        # PR 2: no test file -> a real gap.
        (REPO, 2): [
            {
                "file_path": "src/thing.ts",
                "commit_hash": "bbb",
                "committer_when": BASE + timedelta(minutes=30),
                "evidence": "native",
            }
        ],
        # PR 4: ONLY the squash artifact -> followup_commits must be 0, not 1.
        (REPO, 4): [
            {
                "file_path": "src/x.ts",
                "commit_hash": "ccc",
                "committer_when": BASE + timedelta(minutes=90),
                "evidence": "commit_message_squash_pr_ref",
            }
        ],
        # PR 5: the SAME hash appears twice, once ordinary and once as the merge
        # artifact. Python collects it into artifact_hashes and then POPS it, so it
        # is excluded. A single-pass `continue` would keep it and count 1.
        (REPO, 5): [
            {
                "file_path": "src/y.ts",
                "commit_hash": "ddd",
                "committer_when": BASE + timedelta(minutes=100),
                "evidence": "native",
            },
            {
                "file_path": "src/y.ts",
                "commit_hash": "ddd",
                "committer_when": BASE + timedelta(minutes=100),
                "evidence": "commit_message_pr_ref",
            },
            {
                "file_path": "src/z.ts",
                "commit_hash": "eee",
                "committer_when": BASE + timedelta(minutes=110),
                "evidence": "native",
            },
        ],
    },
)


def resolver(_repo_id, repo_name, _identity):
    # Mirrors job_daily's lambda: only reached when the attribution row has no
    # team_id, which after the loader's `or None` normalisation is every row.
    return ("team-" + (repo_name or "none"), None)


def main() -> None:
    rows = compute_ai_impact_metrics_daily(
        day=DAY,
        org_id=ORG,
        pull_request_rows=PULL_REQUESTS,
        pull_request_review_rows=REVIEWS,
        ai_attribution_rows=ATTRIBUTIONS,
        incident_rows=INCIDENTS,
        commit_stat_rows=(),
        computed_at=datetime(2026, 9, 4, 0, 0, 0, tzinfo=timezone.utc),
        team_resolver=resolver,
        repo_names_by_id={REPO: "acme/alpha", REPO_B: "acme/beta"},
        pr_commit_stats=PR_COMMIT_STATS,
    )
    print(
        json.dumps(
            [
                {
                    "org_id": r.org_id,
                    "team_id": r.team_id,
                    "repo_id": str(r.repo_id),
                    "work_type": r.work_type,
                    "day": r.day.isoformat(),
                    "attribution_bucket": str(r.attribution_bucket),
                    "prs_total": r.prs_total,
                    "prs_merged": r.prs_merged,
                    "ai_assisted_prs": r.ai_assisted_prs,
                    "agent_created_prs": r.agent_created_prs,
                    "human_prs": r.human_prs,
                    "unknown_prs": r.unknown_prs,
                    "ai_assisted_pr_ratio": r.ai_assisted_pr_ratio,
                    "agent_created_pr_count": r.agent_created_pr_count,
                    "cycle_time_avg_hours": r.cycle_time_avg_hours,
                    "baseline_cycle_time_avg_hours": r.baseline_cycle_time_avg_hours,
                    "ai_cycle_time_delta_hours": r.ai_cycle_time_delta_hours,
                    "reviews_per_pr": r.reviews_per_pr,
                    "baseline_reviews_per_pr": r.baseline_reviews_per_pr,
                    "ai_review_amplification": r.ai_review_amplification,
                    "changes_requested_per_pr": r.changes_requested_per_pr,
                    "rework_prs": r.rework_prs,
                    "rework_drag_rate": r.rework_drag_rate,
                    "followup_commits_count": r.followup_commits_count,
                    "revert_prs": r.revert_prs,
                    "revert_rate": r.revert_rate,
                    "incidents_count": r.incidents_count,
                    "incident_drag_rate": r.incident_drag_rate,
                    "test_gap_prs": r.test_gap_prs,
                    "test_gap_rate": r.test_gap_rate,
                    "leverage_prs_component": r.leverage.prs_component,
                    "leverage_cycle_time_component": r.leverage.cycle_time_component,
                    "leverage_review_component": r.leverage.review_component,
                    "leverage_rework_component": r.leverage.rework_component,
                    "leverage_test_component": r.leverage.test_component,
                    "leverage_incident_component": r.leverage.incident_component,
                }
                for r in rows
            ]
        )
    )


if __name__ == "__main__":
    main()
