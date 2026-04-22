from collections import Counter
from datetime import datetime, timedelta, timezone

from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.fixtures.runner import _build_repo_team_assignments
from dev_health_ops.models.work_items import WorkItem


def _build_work_item(
    index: int,
    completed_at: datetime,
    assignees: list[str] | None = None,
) -> WorkItem:
    started_at = completed_at - timedelta(hours=24)
    created_at = started_at - timedelta(hours=12)
    return WorkItem(
        work_item_id=f"gh:acme/demo-app#{index}",
        provider="github",
        title=f"Synthetic item {index}",
        type="task",
        status="done",
        status_raw="done",
        created_at=created_at,
        updated_at=completed_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=completed_at,
        project_id="acme/demo-app",
        project_key="acme/demo-app",
        assignees=assignees or [],
    )


def test_cycle_times_fallback_spreads_same_cell_across_multiple_teams_asymmetrically() -> None:
    teams = SyntheticDataGenerator(repo_name="acme/demo-app", seed=7).generate_teams(count=4)
    generator = SyntheticDataGenerator(
        repo_name="acme/demo-app",
        provider="github",
        seed=11,
        assigned_teams=teams,
    )
    completed_at = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)
    work_items = [_build_work_item(index=i, completed_at=completed_at) for i in range(6)]

    generate_cycle_times = getattr(generator, "generate_work_item_cycle_times")
    cycle_times = generate_cycle_times(work_items=work_items)

    counts = Counter(record.team_id for record in cycle_times)
    assert len(counts) >= 2
    assert max(counts.values()) > min(counts.values())


def test_assignee_resolution_wins_over_fallback_distribution() -> None:
    teams = SyntheticDataGenerator(repo_name="acme/demo-app", seed=21).generate_teams(count=4)
    generator = SyntheticDataGenerator(
        repo_name="acme/demo-app",
        provider="github",
        seed=42,
        assigned_teams=teams,
    )
    completed_at = datetime(2026, 4, 21, 15, 0, tzinfo=timezone.utc)
    explicit_member = str(teams[0].members[0])
    work_items = [
        _build_work_item(index=1, completed_at=completed_at, assignees=[explicit_member]),
        _build_work_item(index=2, completed_at=completed_at),
        _build_work_item(index=3, completed_at=completed_at),
        _build_work_item(index=4, completed_at=completed_at),
    ]

    generate_cycle_times = getattr(generator, "generate_work_item_cycle_times")
    cycle_times = generate_cycle_times(work_items=work_items)

    assigned_record = next(record for record in cycle_times if record.assignee == explicit_member)
    assert assigned_record.team_id == teams[0].id
    assert assigned_record.team_name == teams[0].name

    fallback_counts = Counter(
        record.team_id for record in cycle_times if record.assignee != explicit_member
    )
    assert len(fallback_counts) >= 2


def test_repo_team_assignments_default_shape_supports_top_n_other_bucket() -> None:
    teams = SyntheticDataGenerator(repo_name="acme/demo-app", seed=5).generate_teams(count=10)

    assignments = _build_repo_team_assignments(teams, repo_count=6, seed=5)

    assigned_team_ids = {team.id for repo_teams in assignments for team in repo_teams}
    assert len(assigned_team_ids) == 10
