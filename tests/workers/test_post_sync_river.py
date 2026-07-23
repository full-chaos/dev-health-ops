from __future__ import annotations

import uuid
from datetime import date

import pytest

from dev_health_ops.workers.post_sync_dispatch import PostSyncDispatchPayload
from dev_health_ops.workers.post_sync_river import build_river_post_sync_plan

_RUN_ID = uuid.UUID("00000000-0000-4000-8000-000000000051")


def _payload(
    *,
    targets: list[str],
    from_date: str | None = "2026-07-23",
    to_date: str | None = "2026-07-23",
    auto_import_teams: bool = False,
) -> PostSyncDispatchPayload:
    return PostSyncDispatchPayload(
        provider="github",
        sync_targets=targets,
        org_id="00000000-0000-4000-8000-000000000010",
        from_date=from_date,
        to_date=to_date,
        work_graph_from_date="2026-07-23T00:00:00+00:00",
        work_graph_to_date="2026-07-24T00:00:00+00:00",
        auto_import_teams=auto_import_teams,
    )


@pytest.mark.parametrize(
    ("consumer", "targets", "auto_import_teams"),
    (
        ("complexity", ["git"], False),
        ("daily", ["git"], False),
        ("workgraph", ["work-items"], False),
        ("investment", ["work-items"], False),
        ("dora", ["deployments"], False),
        ("team_autoimport", ["git"], True),
    ),
)
def test_every_consumer_has_duplicate_stable_identity(
    consumer: str,
    targets: list[str],
    auto_import_teams: bool,
) -> None:
    payload = _payload(
        targets=targets,
        auto_import_teams=auto_import_teams,
    )

    first = build_river_post_sync_plan(_RUN_ID, payload, today=date(2026, 7, 23))
    duplicate = build_river_post_sync_plan(_RUN_ID, payload, today=date(2026, 7, 23))

    first_target = next(target for target in first if target.consumer == consumer)
    duplicate_target = next(
        target for target in duplicate if target.consumer == consumer
    )
    assert first_target == duplicate_target
    assert first_target.generation == f"post-sync:{_RUN_ID}"
    assert first_target.idempotency_key.endswith(first_target.job_kind)


def test_git_sync_builds_complete_fanout_without_team_autoimport() -> None:
    plan = build_river_post_sync_plan(
        _RUN_ID,
        _payload(targets=["git"]),
        today=date(2026, 7, 23),
    )

    assert [target.consumer for target in plan] == [
        "complexity",
        "daily",
        "workgraph",
        "investment",
        "dora",
    ]
    assert len({target.domain_id for target in plan}) == len(plan)
    assert len({target.idempotency_key for target in plan}) == len(plan)


def test_historical_sync_skips_only_complexity() -> None:
    plan = build_river_post_sync_plan(
        _RUN_ID,
        _payload(
            targets=["git"],
            from_date="2026-07-01",
            to_date="2026-07-22",
        ),
        today=date(2026, 7, 23),
    )

    assert [target.consumer for target in plan] == [
        "daily",
        "workgraph",
        "investment",
        "dora",
    ]


def test_dora_only_sync_does_not_create_unrelated_metric_requests() -> None:
    plan = build_river_post_sync_plan(
        _RUN_ID,
        _payload(targets=["incidents"]),
        today=date(2026, 7, 23),
    )

    assert [target.consumer for target in plan] == ["dora"]


def test_team_autoimport_is_independent_of_metric_targets() -> None:
    plan = build_river_post_sync_plan(
        _RUN_ID,
        _payload(targets=["teams"], auto_import_teams=True),
        today=date(2026, 7, 23),
    )

    assert [target.consumer for target in plan] == ["team_autoimport"]
