"""Golden generator for the repo_user_commit family (CHAOS-4275).

Frozen output of the REAL Python compute path -- compute_daily_metrics
(compute.py) plus the four repo-level kernels it takes as plain float/int
maps (compute_rework_churn_ratio, compute_single_owner_file_ratio from
quality.py; compute_bus_factor, compute_code_ownership_gini from
knowledge.py) -- against a small synthetic dataset covering: multiple
authors, a large commit, a merged PR with a first review and a changes-
requested round, a second merged PR with no review facts, an unmerged PR
(created-only), a review from a non-author, a file touched by more than one
commit (rework), a single-owner file, and one MTTR-eligible bug work item.

internal/jobs/metrics/daily/repouser's Go port has no IdentityResolver and
no team_resolver/repo_team_resolver (see that package's doc comment for why),
so this generator deliberately passes none either -- the frozen output is
therefore Python's OWN no-resolver fallback path, which is the exact
behaviour the Go port reproduces. A generator that passed real resolvers
would freeze a golden the Go port could never match, which would make the
rot guard permanently red for a reason that has nothing to do with a real
divergence.

Regenerate with `python tests/fixtures/generate_repo_user_commit_python_golden.py`.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from dev_health_ops.metrics.compute import compute_daily_metrics
from dev_health_ops.metrics.knowledge import (
    compute_bus_factor,
    compute_code_ownership_gini,
)
from dev_health_ops.metrics.quality import (
    compute_rework_churn_ratio,
    compute_single_owner_file_ratio,
)
from dev_health_ops.metrics.schemas import (
    CommitStatRow,
    PullRequestReviewRow,
    PullRequestRow,
)

OUTPUT = Path(__file__).with_name("repo_user_commit_python_golden.json")

REPO_A = UUID("00000000-0000-4000-8000-00000000000a")
REPO_B = UUID("00000000-0000-4000-8000-00000000000b")
DAY = date(2026, 7, 20)
COMPUTED_AT = datetime(2026, 7, 21, tzinfo=timezone.utc)


def _dt(hour: int, minute: int = 0, day: date = DAY) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc)


def _commit_stat_rows() -> list[CommitStatRow]:
    return [
        # alice: two commits touching shared.py (rework file, repo A) plus
        # her own file. One commit is "large" (total_loc > 300).
        {
            "repo_id": REPO_A,
            "commit_hash": "c1",
            "author_email": "alice@example.com",
            "author_name": "Alice",
            "committer_when": _dt(9),
            "file_path": "shared.py",
            "additions": 200,
            "deletions": 150,
        },
        {
            "repo_id": REPO_A,
            "commit_hash": "c2",
            "author_email": "alice@example.com",
            "author_name": "Alice",
            "committer_when": _dt(10),
            "file_path": "alice_only.py",
            "additions": 10,
            "deletions": 5,
        },
        # bob: one commit also touching shared.py (makes it a rework file),
        # plus his own single-owner file.
        {
            "repo_id": REPO_A,
            "commit_hash": "c3",
            "author_email": "bob@example.com",
            "author_name": "Bob",
            "committer_when": _dt(11),
            "file_path": "shared.py",
            "additions": 5,
            "deletions": 5,
        },
        {
            "repo_id": REPO_A,
            "commit_hash": "c3",
            "author_email": "bob@example.com",
            "author_name": "Bob",
            "committer_when": _dt(11),
            "file_path": "bob_only.py",
            "additions": 3,
            "deletions": 1,
        },
        # repo B: single small commit by carol, no PRs -- exercises a repo
        # with commits but no merged PRs (change_failure_rate divide-by-1
        # branch, empty PR percentile branches).
        {
            "repo_id": REPO_B,
            "commit_hash": "d1",
            "author_email": "carol@example.com",
            "author_name": "Carol",
            "committer_when": _dt(14),
            "file_path": "b_only.py",
            "additions": 4,
            "deletions": 2,
        },
    ]


def _pull_request_rows() -> list[PullRequestRow]:
    # NOTE: no "title" key anywhere below, deliberately. PullRequestRow
    # (schemas.py) has no "title" field at all, and loaders/clickhouse.py's
    # real pr_query never SELECTs git_pull_requests.title either -- so
    # compute.py's `pr.get("title")` revert-PR detection is DEAD in
    # production today; `pr.get("title")` always returns None, giving ""
    # after `.strip().lower()`, which never matches "revert". This generator
    # freezes what Python ACTUALLY produces, not what it could produce with
    # a title wired in -- see internal/jobs/metrics/daily/repouser's
    # PullRequestRow.Title doc comment for the Go-side mirror of this gap.
    return [
        # PR 1 (repo A, alice): merged in-window, has a first review, one
        # changes-requested round (rework), 400 LOC (large PR, threshold 1000
        # is NOT met here -- keep it a normal-size merged PR).
        {
            "repo_id": REPO_A,
            "number": 1,
            "author_email": "alice@example.com",
            "author_name": "Alice",
            "created_at": _dt(8),
            "merged_at": _dt(13),
            "first_review_at": _dt(9, 30),
            "first_comment_at": _dt(9, 0),
            "changes_requested_count": 1,
            "reviews_count": 2,
            "comments_count": 3,
            "additions": 250,
            "deletions": 150,
            "changed_files": 4,
        },
        # PR 2 (repo A, bob): merged in-window, a title that WOULD be a
        # revert if titles were wired in (they are not -- see above), no
        # review facts at all -- exercises the "no first review"
        # percentile-None branches.
        {
            "repo_id": REPO_A,
            "number": 2,
            "author_email": "bob@example.com",
            "author_name": "Bob",
            "created_at": _dt(10),
            "merged_at": _dt(12),
            "first_review_at": None,
            "first_comment_at": None,
            "changes_requested_count": 0,
            "reviews_count": 0,
            "comments_count": 0,
            "additions": 20,
            "deletions": 5,
            "changed_files": 1,
        },
        # PR 3 (repo A, alice): created in-window but NOT merged -- only
        # counts toward prs_authored, exercises the created-only branch.
        {
            "repo_id": REPO_A,
            "number": 3,
            "author_email": "alice@example.com",
            "author_name": "Alice",
            "created_at": _dt(15),
            "merged_at": None,
        },
    ]


def _pull_request_review_rows() -> list[PullRequestReviewRow]:
    return [
        # bob reviews alice's PR 1 (review participation + reviews_received).
        {
            "repo_id": REPO_A,
            "number": 1,
            "reviewer": "bob@example.com",
            "submitted_at": _dt(9, 30),
            "state": "APPROVED",
        },
    ]


def _bug_items() -> list[dict[str, Any]]:
    return [
        {
            "repo_id": REPO_A,
            "started_at": _dt(6, day=DAY - timedelta(days=2)),
            "completed_at": _dt(10),
        },
    ]


def _mttr_by_repo() -> dict[UUID, float]:
    start = DAY
    end = DAY + timedelta(days=1)
    hours_by_repo: dict[UUID, list[float]] = {}
    for item in _bug_items():
        completed = item["completed_at"]
        if not (
            datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
            <= completed
            < datetime.combine(end, datetime.min.time(), tzinfo=timezone.utc)
        ):
            continue
        hours = (completed - item["started_at"]).total_seconds() / 3600.0
        hours_by_repo.setdefault(item["repo_id"], []).append(hours)
    return {
        repo_id: sum(hours) / len(hours) for repo_id, hours in hours_by_repo.items()
    }


def _serialize_record(record: Any) -> dict[str, Any]:
    fields = {}
    for name, value in vars(record).items():
        if isinstance(value, UUID):
            fields[name] = str(value)
        elif isinstance(value, (date, datetime)):
            fields[name] = value.isoformat()
        else:
            fields[name] = value
    return fields


def render() -> str:
    commit_rows = _commit_stat_rows()
    pr_rows = _pull_request_rows()
    review_rows = _pull_request_review_rows()

    active_repos = [REPO_A, REPO_B]
    rework_by_repo = {
        repo_id: compute_rework_churn_ratio(
            repo_id=str(repo_id), window_stats=commit_rows
        )
        for repo_id in active_repos
    }
    single_owner_by_repo = {
        repo_id: compute_single_owner_file_ratio(
            repo_id=str(repo_id), window_stats=commit_rows
        )
        for repo_id in active_repos
    }
    bus_factor_by_repo = {
        repo_id: compute_bus_factor(repo_id=str(repo_id), window_stats=commit_rows)
        for repo_id in active_repos
    }
    gini_by_repo = {
        repo_id: compute_code_ownership_gini(
            repo_id=str(repo_id), window_stats=commit_rows
        )
        for repo_id in active_repos
    }
    mttr_by_repo = _mttr_by_repo()

    result = compute_daily_metrics(
        day=DAY,
        commit_stat_rows=commit_rows,
        pull_request_rows=pr_rows,
        pull_request_review_rows=review_rows,
        computed_at=COMPUTED_AT,
        mttr_by_repo=mttr_by_repo,
        rework_churn_ratio_by_repo=rework_by_repo,
        single_owner_file_ratio_by_repo=single_owner_by_repo,
        bus_factor_by_repo=bus_factor_by_repo,
        code_ownership_gini_by_repo=gini_by_repo,
    )

    value = {
        "schema_version": 1,
        "repo_metrics": [_serialize_record(r) for r in result.repo_metrics],
        "user_metrics": [_serialize_record(r) for r in result.user_metrics],
        "commit_metrics": [_serialize_record(r) for r in result.commit_metrics],
        "kernels": {
            "rework_churn_ratio_by_repo": {
                str(k): v for k, v in rework_by_repo.items()
            },
            "single_owner_file_ratio_by_repo": {
                str(k): v for k, v in single_owner_by_repo.items()
            },
            "bus_factor_by_repo": {str(k): v for k, v in bus_factor_by_repo.items()},
            "code_ownership_gini_by_repo": {str(k): v for k, v in gini_by_repo.items()},
            "mttr_by_repo": {str(k): v for k, v in mttr_by_repo.items()},
        },
    }
    return (
        json.dumps(value, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n"
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--stdout",
        action="store_true",
        help=(
            "Render to stdout instead of writing the checked-in file. The live "
            "rot guard (internal/jobs/metrics/daily/repouser) uses this to "
            "compare what TODAY's production Python produces against the "
            "frozen file, so a drift is reported as a diff rather than a bare "
            "exit code."
        ),
    )
    args = parser.parse_args()
    rendered = render()
    if args.stdout:
        print(rendered, end="")
        return 0
    if args.check:
        return 0 if OUTPUT.read_text() == rendered else 1
    OUTPUT.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
