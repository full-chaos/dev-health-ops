"""Golden generator for the file_hotspots + file_risk_hotspots families
(CHAOS-4277).

Frozen output of the REAL Python compute path -- compute_file_hotspots and
compute_file_risk_hotspots (src/dev_health_ops/metrics/hotspots.py) -- against
a small synthetic dataset covering:

  - a file touched by two different authors across two different days within
    the 30-day window (churn accumulates, contributors=2, commits_count=2)
  - a single-author, single-commit file
  - the AGGREGATE_STATS_MARKER ("__AGGREGATE__") backfill sentinel row, which
    must be skipped entirely from BOTH families (CHAOS-2376 round-4)
  - a row with no file_path at all (also skipped)
  - a second repository's rows, which must never leak into repo A's output
    (both compute functions filter on repo_id internally)
  - a file with NO churn in the window but a static complexity snapshot
    (idle_complex.py) -- proves compute_file_risk_hotspots unions churned
    files with complexity-only files rather than intersecting them
  - a blame_map entry present for one file and absent for another, exercising
    both the concentration-present and concentration-None branches

Regenerate with `python tests/fixtures/generate_file_hotspots_python_golden.py`.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from dev_health_ops.metrics.hotspots import (
    compute_file_hotspots,
    compute_file_risk_hotspots,
)
from dev_health_ops.metrics.schemas import CommitStatRow, FileComplexitySnapshot

OUTPUT = Path(__file__).with_name("file_hotspots_python_golden.json")

REPO_A = UUID("00000000-0000-4000-8000-0000000000fa")
REPO_B = UUID("00000000-0000-4000-8000-0000000000fb")
DAY = date(2026, 7, 20)
COMPUTED_AT = datetime(2026, 7, 21, tzinfo=timezone.utc)


def _dt(day: date, hour: int = 12) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=timezone.utc)


def _window_stats() -> list[CommitStatRow]:
    return [
        # shared.py: alice on day D-25, bob on day D -- two authors, two
        # commits, churn = (40+10) + (5+5) = 60.
        {
            "repo_id": REPO_A,
            "commit_hash": "c1",
            "author_email": "alice@example.com",
            "author_name": "Alice",
            "committer_when": _dt(date(2026, 6, 25)),
            "file_path": "shared.py",
            "additions": 40,
            "deletions": 10,
        },
        {
            "repo_id": REPO_A,
            "commit_hash": "c2",
            "author_email": "bob@example.com",
            "author_name": "Bob",
            "committer_when": _dt(DAY),
            "file_path": "shared.py",
            "additions": 5,
            "deletions": 5,
        },
        # solo.py: alice only, one commit, churn = 3+1 = 4.
        {
            "repo_id": REPO_A,
            "commit_hash": "c3",
            "author_email": "alice@example.com",
            "author_name": "Alice",
            "committer_when": _dt(DAY),
            "file_path": "solo.py",
            "additions": 3,
            "deletions": 1,
        },
        # The aggregate-backfill sentinel -- must be skipped from both
        # families entirely, not merely excluded from ranking.
        {
            "repo_id": REPO_A,
            "commit_hash": "c4",
            "author_email": "backfill@example.com",
            "author_name": "Backfill Bot",
            "committer_when": _dt(DAY),
            "file_path": "__AGGREGATE__",
            "additions": 9999,
            "deletions": 9999,
        },
        # No file_path at all -- also skipped.
        {
            "repo_id": REPO_A,
            "commit_hash": "c5",
            "author_email": "carol@example.com",
            "author_name": "Carol",
            "committer_when": _dt(DAY),
            "file_path": None,
            "additions": 1,
            "deletions": 1,
        },
        # Whitespace-only author_email (codex round 1, finding 5): Python's
        # `(author_email or author_name or "unknown").strip()` selects on
        # RAW truthiness -- a whitespace-only string is truthy in Python, so
        # it wins the `or` chain and is stripped to "" AFTER selection,
        # rather than being treated as absent and falling through to
        # author_name. This makes wendy_c6 (email=" ") and wendy_c7
        # (email="Wendy") two DIFFERENT contributor keys ("" and "Wendy"),
        # not one -- a subtle but real production behavior this port must
        # reproduce exactly, not "fix".
        {
            "repo_id": REPO_A,
            "commit_hash": "c6",
            "author_email": " ",
            "author_name": "Wendy",
            "committer_when": _dt(DAY),
            "file_path": "ws_email.py",
            "additions": 5,
            "deletions": 0,
        },
        {
            "repo_id": REPO_A,
            "commit_hash": "c7",
            "author_email": "Wendy",
            "author_name": "Wendy",
            "committer_when": _dt(DAY),
            "file_path": "ws_email.py",
            "additions": 5,
            "deletions": 0,
        },
        # A different repository entirely -- must never leak into REPO_A's
        # output (and REPO_A's rows must never leak into REPO_B's).
        {
            "repo_id": REPO_B,
            "commit_hash": "b1",
            "author_email": "dave@example.com",
            "author_name": "Dave",
            "committer_when": _dt(DAY),
            "file_path": "other_repo.py",
            "additions": 500,
            "deletions": 500,
        },
    ]


def _complexity_map() -> dict[str, FileComplexitySnapshot]:
    return {
        # shared.py has both churn and a complexity snapshot.
        "shared.py": FileComplexitySnapshot(
            repo_id=REPO_A,
            as_of_day=DAY,
            ref="",
            file_path="shared.py",
            language="python",
            loc=120,
            functions_count=8,
            cyclomatic_total=24,
            cyclomatic_avg=3.0,
            high_complexity_functions=1,
            very_high_complexity_functions=0,
            computed_at=COMPUTED_AT,
        ),
        # idle_complex.py has a complexity snapshot but NO churn in the
        # window -- proves the union (not intersection) with churn_map.
        "idle_complex.py": FileComplexitySnapshot(
            repo_id=REPO_A,
            as_of_day=DAY,
            ref="",
            file_path="idle_complex.py",
            language="python",
            loc=400,
            functions_count=20,
            cyclomatic_total=80,
            cyclomatic_avg=4.0,
            high_complexity_functions=3,
            very_high_complexity_functions=1,
            computed_at=COMPUTED_AT,
        ),
    }


def _blame_map() -> dict[str, float]:
    # Only shared.py has blame data; solo.py and idle_complex.py fall through
    # to blame_concentration=None.
    return {"shared.py": 0.75}


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
    window_stats = _window_stats()
    complexity_map = _complexity_map()
    blame_map = _blame_map()

    file_metrics_a = compute_file_hotspots(
        repo_id=REPO_A, day=DAY, window_stats=window_stats, computed_at=COMPUTED_AT
    )
    file_metrics_b = compute_file_hotspots(
        repo_id=REPO_B, day=DAY, window_stats=window_stats, computed_at=COMPUTED_AT
    )
    risk_hotspots_a = compute_file_risk_hotspots(
        repo_id=REPO_A,
        day=DAY,
        window_stats=window_stats,
        complexity_map=complexity_map,
        blame_map=blame_map,
        computed_at=COMPUTED_AT,
    )
    # REPO_B has no complexity snapshots and no blame data of its own --
    # exercises complexity_map={}/blame_map=None (falsy) at once.
    risk_hotspots_b = compute_file_risk_hotspots(
        repo_id=REPO_B,
        day=DAY,
        window_stats=window_stats,
        complexity_map={},
        blame_map=None,
        computed_at=COMPUTED_AT,
    )

    value = {
        "schema_version": 1,
        "file_metrics_repo_a": [_serialize_record(r) for r in file_metrics_a],
        "file_metrics_repo_b": [_serialize_record(r) for r in file_metrics_b],
        "risk_hotspots_repo_a": [_serialize_record(r) for r in risk_hotspots_a],
        "risk_hotspots_repo_b": [_serialize_record(r) for r in risk_hotspots_b],
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
            "Render to stdout instead of writing the checked-in file. The "
            "live rot guard (internal/jobs/metrics/daily/filehotspots) uses "
            "this to compare what TODAY's production Python produces "
            "against the frozen file."
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
