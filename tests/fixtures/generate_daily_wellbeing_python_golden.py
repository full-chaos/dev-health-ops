"""Generate/verify the frozen team_wellbeing Python golden (CHAOS-4276).

Mirrors tests/fixtures/generate_remaining_metrics_python_golden.py's shape
for one family: compute_team_wellbeing_metrics_daily
(src/dev_health_ops/metrics/compute_wellbeing.py:39) is the production
Python this repo is porting to Go
(internal/jobs/metrics/numerical/wellbeing.go's ComputeTeamWellbeing). This
generator is the single source both the frozen golden and the live rot guard
(internal/jobs/metrics/numerical/wellbeing_golden_rot_guard_test.go) render
from, so the two can never independently drift out of sync with each other --
only the frozen file can drift from a CHANGED production Python, which the
rot guard exists to catch.

job_daily.py calls compute_team_wellbeing_metrics_daily WITHOUT an
identity_resolver (it defaults to None), so every case here also omits it --
reproducing an identity-alias-resolving path here would generate a golden
for behaviour production never actually exercises.
"""

from __future__ import annotations

import argparse
import json
import uuid
from collections.abc import Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, cast

from dev_health_ops.metrics.compute_wellbeing import (
    compute_team_wellbeing_metrics_daily,
)
from dev_health_ops.metrics.schemas import CommitStatRow
from dev_health_ops.providers.teams import (
    RepoPatternTeamResolver,
    TeamResolver,
    build_repo_pattern_resolver,
)

OUTPUT = Path(__file__).with_name("daily_wellbeing_python_golden.json")

REPO_A = uuid.UUID("00000000-0000-4000-8000-00000000000a")
REPO_B = uuid.UUID("00000000-0000-4000-8000-00000000000b")
REPO_UNMAPPED = uuid.UUID("00000000-0000-4000-8000-00000000000c")
DAY = date(2026, 8, 24)  # Monday
COMPUTED_AT = datetime(2026, 8, 25, tzinfo=timezone.utc)


def _row(
    *,
    repo_id: uuid.UUID,
    commit_hash: str,
    author_email: str | None,
    author_name: str | None,
    committer_when: datetime,
) -> dict[str, Any]:
    return {
        "repo_id": repo_id,
        "commit_hash": commit_hash,
        "author_email": author_email,
        "author_name": author_name,
        "committer_when": committer_when,
        "file_path": "irrelevant.py",
        "additions": 1,
        "deletions": 0,
    }


def _case(
    *,
    label: str,
    day: date,
    rows: list[dict[str, Any]],
    team_resolver: TeamResolver | None,
    repo_team_resolver: RepoPatternTeamResolver | None,
    repo_names_by_id: dict[uuid.UUID, str],
    business_timezone: str = "UTC",
    business_hours_start: int = 9,
    business_hours_end: int = 17,
) -> dict[str, Any]:
    records = compute_team_wellbeing_metrics_daily(
        day=day,
        commit_stat_rows=cast(Sequence[CommitStatRow], rows),
        team_resolver=team_resolver,
        computed_at=COMPUTED_AT,
        repo_team_resolver=repo_team_resolver,
        repo_names_by_id=repo_names_by_id,
        business_timezone=business_timezone,
        business_hours_start=business_hours_start,
        business_hours_end=business_hours_end,
    )
    return {
        "label": label,
        "day": day.isoformat(),
        "rows": [
            {
                "repo_id": str(row["repo_id"]),
                "commit_hash": row["commit_hash"],
                "author_email": row["author_email"],
                "author_name": row["author_name"],
                "committer_when": row["committer_when"]
                .isoformat()
                .replace("+00:00", "Z"),
            }
            for row in rows
        ],
        "repo_names_by_id": {str(k): v for k, v in repo_names_by_id.items()},
        "business_timezone": business_timezone,
        "business_hours_start": business_hours_start,
        "business_hours_end": business_hours_end,
        "expected": [
            {
                "team_id": r.team_id,
                "team_name": r.team_name,
                "commits_count": r.commits_count,
                "after_hours_commits_count": r.after_hours_commits_count,
                "weekend_commits_count": r.weekend_commits_count,
                "after_hours_commit_ratio": r.after_hours_commit_ratio,
                "weekend_commit_ratio": r.weekend_commit_ratio,
            }
            for r in records
        ],
    }


def _team_wellbeing() -> list[dict[str, Any]]:
    teams_data = [
        {
            "id": "team-repo",
            "name": "Repo Team",
            "members": ["member@example.com"],
            "repo_patterns": ["org/service-a"],
        },
        {
            "id": "team-wild",
            "name": "Wildcard Team",
            "members": [],
            "repo_patterns": ["org/wild-*"],
        },
        {
            "id": "team-member",
            "name": "Member Team",
            "members": ["dev@example.com", "Display Name"],
            "repo_patterns": [],
        },
    ]
    repo_team_resolver = build_repo_pattern_resolver(teams_data)
    team_resolver = TeamResolver(
        member_to_team={
            "dev@example.com": ("team-member", "Member Team"),
            "display name": ("team-member", "Member Team"),
        }
    )
    repo_names_by_id = {
        REPO_A: "org/service-a",
        REPO_B: "org/wild-service",
    }

    cases: list[dict[str, Any]] = []

    # 1. Repo-pattern resolution wins over membership even when the author is
    #    also a team member elsewhere.
    cases.append(
        _case(
            label="repo_pattern_wins_over_membership",
            day=DAY,
            rows=[
                _row(
                    repo_id=REPO_A,
                    commit_hash="c1",
                    author_email="dev@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
                )
            ],
            team_resolver=team_resolver,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
    )

    # 2. Wildcard repo pattern match.
    cases.append(
        _case(
            label="wildcard_repo_pattern",
            day=DAY,
            rows=[
                _row(
                    repo_id=REPO_B,
                    commit_hash="c2",
                    author_email="nobody@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
                )
            ],
            team_resolver=team_resolver,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
    )

    # 3. No repo pattern match -> membership fallback by email.
    cases.append(
        _case(
            label="membership_fallback_email",
            day=DAY,
            rows=[
                _row(
                    repo_id=REPO_UNMAPPED,
                    commit_hash="c3",
                    author_email="dev@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
                )
            ],
            team_resolver=team_resolver,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
    )

    # 4. No repo pattern match, no email -> membership fallback by display name.
    cases.append(
        _case(
            label="membership_fallback_display_name",
            day=DAY,
            rows=[
                _row(
                    repo_id=REPO_UNMAPPED,
                    commit_hash="c4",
                    author_email=None,
                    author_name="Display Name",
                    committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
                )
            ],
            team_resolver=team_resolver,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
    )

    # 5. No match anywhere -> unassigned bucket.
    cases.append(
        _case(
            label="unassigned_bucket",
            day=DAY,
            rows=[
                _row(
                    repo_id=REPO_UNMAPPED,
                    commit_hash="c5",
                    author_email="ghost@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
                )
            ],
            team_resolver=team_resolver,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
    )

    # 6. Weekend and after-hours are mutually exclusive; multiple commits
    #    aggregate into one team bucket with correct ratios.
    cases.append(
        _case(
            label="weekend_and_after_hours_mix",
            day=date(2026, 8, 22),  # Saturday
            rows=[
                _row(
                    repo_id=REPO_UNMAPPED,
                    commit_hash="c6",
                    author_email="ghost@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 22, 2, 0, tzinfo=timezone.utc),
                ),
                _row(
                    repo_id=REPO_UNMAPPED,
                    commit_hash="c7",
                    author_email="ghost@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 22, 23, 0, tzinfo=timezone.utc),
                ),
                _row(
                    repo_id=REPO_UNMAPPED,
                    commit_hash="c8",
                    author_email="ghost@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc),
                ),
            ],
            team_resolver=team_resolver,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
    )

    # 7. Weekday after-hours (before start / at-or-after end).
    cases.append(
        _case(
            label="weekday_after_hours",
            day=DAY,
            rows=[
                _row(
                    repo_id=REPO_UNMAPPED,
                    commit_hash="c9",
                    author_email="ghost@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 7, 0, tzinfo=timezone.utc),
                ),
                _row(
                    repo_id=REPO_UNMAPPED,
                    commit_hash="c10",
                    author_email="ghost@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 18, 0, tzinfo=timezone.utc),
                ),
                _row(
                    repo_id=REPO_UNMAPPED,
                    commit_hash="c11",
                    author_email="ghost@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
                ),
            ],
            team_resolver=team_resolver,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
    )

    # 8. Business timezone conversion shifts the local calendar day.
    cases.append(
        _case(
            label="business_timezone_conversion",
            day=DAY,
            rows=[
                # 2026-08-24 03:00 UTC == 2026-08-23 23:00 America/New_York
                # (EDT, UTC-4) -- Sunday locally.
                _row(
                    repo_id=REPO_UNMAPPED,
                    commit_hash="c12",
                    author_email="ghost@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 3, 0, tzinfo=timezone.utc),
                )
            ],
            team_resolver=team_resolver,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
            business_timezone="America/New_York",
        )
    )

    # 9. Duplicate (repo_id, commit_hash) rows (per-file join product) collapse
    #    to one commit -- the in-memory dedup this function performs.
    cases.append(
        _case(
            label="duplicate_commit_hash_dedup",
            day=DAY,
            rows=[
                _row(
                    repo_id=REPO_A,
                    commit_hash="c13",
                    author_email="dev@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
                ),
                _row(
                    repo_id=REPO_A,
                    commit_hash="c13",
                    author_email="dev@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
                ),
            ],
            team_resolver=team_resolver,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
    )

    # 10. Multiple teams in one day sort by team_id in the output.
    cases.append(
        _case(
            label="multiple_teams_sorted",
            day=DAY,
            rows=[
                _row(
                    repo_id=REPO_B,
                    commit_hash="c14",
                    author_email="nobody@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
                ),
                _row(
                    repo_id=REPO_A,
                    commit_hash="c15",
                    author_email="dev@example.com",
                    author_name=None,
                    committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
                ),
            ],
            team_resolver=team_resolver,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
    )

    return cases


def render() -> str:
    value = {
        "schema_version": 1,
        "team_wellbeing": _team_wellbeing(),
    }
    return json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--stdout",
        action="store_true",
        help=(
            "Render to stdout instead of writing the checked-in file. The "
            "live rot guard (internal/jobs/metrics/numerical) uses this to "
            "compare what TODAY's production Python produces against the "
            "frozen file, so a drift is reported as a diff rather than a "
            "bare exit code."
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
