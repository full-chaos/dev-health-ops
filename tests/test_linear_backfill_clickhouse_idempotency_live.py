from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from dev_health_ops.metrics.sinks.clickhouse.idempotency import (
    SPRINTS_DEDUPED,
    WORK_ITEM_INTERACTIONS_DEDUPED,
    WORK_ITEM_REOPEN_EVENTS_DEDUPED,
)

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/ci_local_validate)",
    ),
]


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    s = ClickHouseMetricsSink(CLICKHOUSE_URI)
    s.ensure_schema(force=True)
    yield s
    s.close()


def _delete_org_rows(sink: Any, org_id: str, tables: list[str]) -> None:
    for table in tables:
        sink.client.command(
            f"ALTER TABLE {table} DELETE WHERE org_id = {{org_id:String}} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )


def _rows(sink: Any, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    return sink.query_dicts(query, params)


def test_work_item_readers_collapse_duplicate_retry_rows(sink: Any) -> None:
    from dev_health_ops.metrics.compute_capacity import get_backlog_size_clickhouse
    from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
    from dev_health_ops.work_graph.investment.queries import (
        fetch_parent_titles,
        fetch_work_items,
    )

    org_id = f"test-chaos-2710-wi-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = f"linear:CHAOS-{uuid.uuid4()}"
    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=7)
    created_at = start + timedelta(hours=1)
    last_synced = start + timedelta(hours=2)
    tables = ["work_items"]

    row = [
        org_id,
        repo_id,
        work_item_id,
        "linear",
        "Retry-safe work item",
        "task",
        "todo",
        "Todo",
        "CHAOS",
        "linear-project",
        ["alice@example.com"],
        "reporter@example.com",
        created_at,
        created_at,
        last_synced,
    ]
    columns = [
        "org_id",
        "repo_id",
        "work_item_id",
        "provider",
        "title",
        "type",
        "status",
        "status_raw",
        "project_key",
        "project_id",
        "assignees",
        "reporter",
        "created_at",
        "updated_at",
        "last_synced",
    ]

    try:
        sink.client.insert("work_items", [row], column_names=columns)
        loader = ClickHouseDataLoader(sink.client, org_id=org_id)
        before_items, _ = asyncio.run(loader.load_work_items(start, end, repo_id))
        before_investment = fetch_work_items(
            sink, work_item_ids=[work_item_id], org_id=org_id
        )
        before_titles = fetch_parent_titles(
            sink, work_item_ids=[work_item_id], org_id=org_id
        )
        before_backlog = asyncio.run(
            get_backlog_size_clickhouse(sink.client, org_id=org_id)
        )

        retry_row = list(row)
        retry_row[-1] = last_synced + timedelta(minutes=5)
        sink.client.insert("work_items", [retry_row], column_names=columns)

        after_items, _ = asyncio.run(loader.load_work_items(start, end, repo_id))
        after_investment = fetch_work_items(
            sink, work_item_ids=[work_item_id], org_id=org_id
        )
        after_titles = fetch_parent_titles(
            sink, work_item_ids=[work_item_id], org_id=org_id
        )
        after_backlog = asyncio.run(
            get_backlog_size_clickhouse(sink.client, org_id=org_id)
        )

        assert [item.work_item_id for item in before_items] == [work_item_id]
        assert [item.work_item_id for item in after_items] == [work_item_id]
        assert len(before_investment) == len(after_investment) == 1
        assert before_titles == after_titles == {work_item_id: "Retry-safe work item"}
        assert before_backlog == after_backlog == 1
    finally:
        _delete_org_rows(sink, org_id, tables)


def test_transition_reader_semantic_dedupe_preserves_distinct_events(
    sink: Any,
) -> None:
    from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader

    org_id = f"test-chaos-2710-tr-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = f"linear:CHAOS-{uuid.uuid4()}"
    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=7)
    occurred_at = start + timedelta(hours=3)
    last_synced = start + timedelta(hours=4)
    columns = [
        "org_id",
        "repo_id",
        "work_item_id",
        "occurred_at",
        "provider",
        "from_status",
        "to_status",
        "from_status_raw",
        "to_status_raw",
        "actor",
        "last_synced",
    ]
    rows = [
        [
            org_id,
            repo_id,
            work_item_id,
            occurred_at,
            "linear",
            "todo",
            "in_progress",
            "Todo",
            "Started",
            "alice@example.com",
            last_synced,
        ],
        [
            org_id,
            repo_id,
            work_item_id,
            occurred_at,
            "linear",
            "in_progress",
            "done",
            "Started",
            "Done",
            "bob@example.com",
            last_synced,
        ],
    ]

    try:
        for row in rows:
            sink.client.insert("work_item_transitions", [row], column_names=columns)
        loader = ClickHouseDataLoader(sink.client, org_id=org_id)
        _, before = asyncio.run(loader.load_work_items(start, end, repo_id))

        retry_rows = [list(row) for row in rows]
        for row in retry_rows:
            row[-1] = last_synced + timedelta(minutes=5)
        for row in retry_rows:
            sink.client.insert("work_item_transitions", [row], column_names=columns)
        _, after = asyncio.run(loader.load_work_items(start, end, repo_id))

        before_pairs = {(row.from_status, row.to_status, row.actor) for row in before}
        after_pairs = {(row.from_status, row.to_status, row.actor) for row in after}
        assert before_pairs == after_pairs
        assert after_pairs == {
            ("todo", "in_progress", "alice@example.com"),
            ("in_progress", "done", "bob@example.com"),
        }
    finally:
        _delete_org_rows(sink, org_id, ["work_item_transitions"])


def test_interaction_semantic_dedupe_preserves_same_timestamp_distinct_actors(
    sink: Any,
) -> None:
    org_id = f"test-chaos-2710-int-{uuid.uuid4()}"
    work_item_id = f"linear:CHAOS-{uuid.uuid4()}"
    occurred_at = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)
    last_synced = occurred_at + timedelta(minutes=1)
    columns = [
        "org_id",
        "work_item_id",
        "provider",
        "interaction_type",
        "occurred_at",
        "actor",
        "body_length",
        "last_synced",
    ]
    rows = [
        [
            org_id,
            work_item_id,
            "linear",
            "comment",
            occurred_at,
            "alice@example.com",
            24,
            last_synced,
        ],
        [
            org_id,
            work_item_id,
            "linear",
            "comment",
            occurred_at,
            "bob@example.com",
            48,
            last_synced,
        ],
    ]

    try:
        for row in rows:
            sink.client.insert("work_item_interactions", [row], column_names=columns)
        before = _rows(
            sink,
            f"""
            SELECT actor, body_length
            FROM {WORK_ITEM_INTERACTIONS_DEDUPED}
            WHERE org_id = %(org_id)s AND work_item_id = %(work_item_id)s
            ORDER BY actor
            """,
            {"org_id": org_id, "work_item_id": work_item_id},
        )

        retry_rows = [list(row) for row in rows]
        for row in retry_rows:
            row[-1] = last_synced + timedelta(minutes=5)
        for row in retry_rows:
            sink.client.insert("work_item_interactions", [row], column_names=columns)
        after = _rows(
            sink,
            f"""
            SELECT actor, body_length
            FROM {WORK_ITEM_INTERACTIONS_DEDUPED}
            WHERE org_id = %(org_id)s AND work_item_id = %(work_item_id)s
            ORDER BY actor
            """,
            {"org_id": org_id, "work_item_id": work_item_id},
        )

        assert before == after
        assert [(row["actor"], row["body_length"]) for row in after] == [
            ("alice@example.com", 24),
            ("bob@example.com", 48),
        ]
    finally:
        _delete_org_rows(sink, org_id, ["work_item_interactions"])


def test_reopen_event_semantic_dedupe_keeps_retry_count_stable(sink: Any) -> None:
    org_id = f"test-chaos-2710-reopen-{uuid.uuid4()}"
    work_item_id = f"linear:CHAOS-{uuid.uuid4()}"
    occurred_at = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)
    last_synced = occurred_at + timedelta(minutes=1)
    columns = [
        "org_id",
        "work_item_id",
        "occurred_at",
        "from_status",
        "to_status",
        "from_status_raw",
        "to_status_raw",
        "actor",
        "last_synced",
    ]
    rows = [
        [
            org_id,
            work_item_id,
            occurred_at,
            "done",
            "in_progress",
            "Done",
            "Started",
            "alice@example.com",
            last_synced,
        ]
    ]

    try:
        sink.client.insert("work_item_reopen_events", rows, column_names=columns)
        before = _rows(
            sink,
            f"""
            SELECT from_status, to_status, actor
            FROM {WORK_ITEM_REOPEN_EVENTS_DEDUPED}
            WHERE org_id = %(org_id)s AND work_item_id = %(work_item_id)s
            """,
            {"org_id": org_id, "work_item_id": work_item_id},
        )

        retry_rows = [list(row) for row in rows]
        retry_rows[0][-1] = last_synced + timedelta(minutes=5)
        sink.client.insert("work_item_reopen_events", retry_rows, column_names=columns)
        after = _rows(
            sink,
            f"""
            SELECT from_status, to_status, actor
            FROM {WORK_ITEM_REOPEN_EVENTS_DEDUPED}
            WHERE org_id = %(org_id)s AND work_item_id = %(work_item_id)s
            """,
            {"org_id": org_id, "work_item_id": work_item_id},
        )

        assert before == after
        assert after == [
            {
                "from_status": "done",
                "to_status": "in_progress",
                "actor": "alice@example.com",
            }
        ]
    finally:
        _delete_org_rows(sink, org_id, ["work_item_reopen_events"])


def test_sprints_final_collapses_duplicate_retry_rows(sink: Any) -> None:
    org_id = f"test-chaos-2710-sprint-{uuid.uuid4()}"
    sprint_id = f"cycle-{uuid.uuid4()}"
    started_at = datetime(2026, 6, 1, tzinfo=timezone.utc)
    ended_at = started_at + timedelta(days=14)
    last_synced = started_at + timedelta(minutes=1)
    columns = [
        "org_id",
        "provider",
        "sprint_id",
        "name",
        "state",
        "started_at",
        "ended_at",
        "completed_at",
        "last_synced",
    ]
    row = [
        org_id,
        "linear",
        sprint_id,
        "Cycle 1",
        "active",
        started_at,
        ended_at,
        None,
        last_synced,
    ]

    try:
        sink.client.insert("sprints", [row], column_names=columns)
        before = _rows(
            sink,
            f"""
            SELECT sprint_id, name
            FROM {SPRINTS_DEDUPED}
            WHERE org_id = %(org_id)s AND provider = 'linear' AND sprint_id = %(sprint_id)s
            """,
            {"org_id": org_id, "sprint_id": sprint_id},
        )

        retry_row = list(row)
        retry_row[-1] = last_synced + timedelta(minutes=5)
        sink.client.insert("sprints", [retry_row], column_names=columns)
        after = _rows(
            sink,
            f"""
            SELECT sprint_id, name
            FROM {SPRINTS_DEDUPED}
            WHERE org_id = %(org_id)s AND provider = 'linear' AND sprint_id = %(sprint_id)s
            """,
            {"org_id": org_id, "sprint_id": sprint_id},
        )

        assert before == after == [{"sprint_id": sprint_id, "name": "Cycle 1"}]
    finally:
        _delete_org_rows(sink, org_id, ["sprints"])


def test_team_attribution_latest_snapshot_retry_keeps_results_stable(
    sink: Any,
) -> None:
    from dev_health_ops.api.graphql.context import GraphQLContext
    from dev_health_ops.api.graphql.resolvers.team_attribution import (
        resolve_work_item_team_attributions,
    )
    from dev_health_ops.metrics.schemas import WorkItemTeamAttributionRecord

    assert CLICKHOUSE_URI is not None
    org_id = f"test-chaos-2710-attr-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = f"linear:CHAOS-{uuid.uuid4()}"
    computed_at = datetime(2026, 6, 1, tzinfo=timezone.utc)
    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client)

    def candidate_rows(ts: datetime) -> list[WorkItemTeamAttributionRecord]:
        return [
            WorkItemTeamAttributionRecord(
                work_item_id=work_item_id,
                provider="linear",
                source="native_team",
                is_primary=1,
                confidence="high",
                evidence="native_team_key=CHAOS",
                computed_at=ts,
                repo_id=repo_id,
                team_id="team-a",
                team_name="Team A",
                org_id=org_id,
            ),
            WorkItemTeamAttributionRecord(
                work_item_id=work_item_id,
                provider="linear",
                source="assignee_membership",
                is_primary=0,
                confidence="medium",
                evidence="assignee=alice@example.com",
                computed_at=ts,
                repo_id=repo_id,
                team_id="team-b",
                team_name="Team B",
                org_id=org_id,
            ),
        ]

    try:
        sink.write_work_item_team_attributions(candidate_rows(computed_at))
        before = asyncio.run(
            resolve_work_item_team_attributions(context, work_item_ids=[work_item_id])
        )

        sink.write_work_item_team_attributions(
            candidate_rows(computed_at + timedelta(minutes=5))
        )
        after = asyncio.run(
            resolve_work_item_team_attributions(context, work_item_ids=[work_item_id])
        )

        before_rows = [
            (row.team_id, row.source.value, row.is_primary) for row in before
        ]
        after_rows = [(row.team_id, row.source.value, row.is_primary) for row in after]
        assert before_rows == after_rows
        assert after_rows == [
            ("team-a", "native_team", True),
            ("team-b", "assignee_membership", False),
        ]
    finally:
        _delete_org_rows(sink, org_id, ["work_item_team_attributions"])


def test_scenario_9_complete_window_retry_rewrite_does_not_duplicate_surfaces(
    sink: Any,
) -> None:
    from dev_health_ops.api.graphql.context import GraphQLContext
    from dev_health_ops.api.graphql.resolvers.team_attribution import (
        resolve_work_item_team_attributions,
    )
    from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
    from dev_health_ops.metrics.schemas import WorkItemTeamAttributionRecord

    assert CLICKHOUSE_URI is not None
    org_id = f"test-chaos-2710-scenario9-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = f"linear:CHAOS-{uuid.uuid4()}"
    target_id = f"linear:CHAOS-{uuid.uuid4()}"
    sprint_id = f"cycle-{uuid.uuid4()}"
    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=7)
    created_at = start + timedelta(hours=1)
    occurred_at = start + timedelta(hours=2)
    version_at = start + timedelta(hours=3)
    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client)
    loader = ClickHouseDataLoader(sink.client, org_id=org_id)
    tables = [
        "work_item_team_attributions",
        "sprints",
        "work_item_reopen_events",
        "work_item_interactions",
        "work_item_dependencies",
        "work_item_transitions",
        "work_items",
    ]

    work_item_columns = [
        "org_id",
        "repo_id",
        "work_item_id",
        "provider",
        "title",
        "type",
        "status",
        "status_raw",
        "project_key",
        "project_id",
        "assignees",
        "reporter",
        "created_at",
        "updated_at",
        "sprint_id",
        "sprint_name",
        "last_synced",
    ]
    transition_columns = [
        "org_id",
        "repo_id",
        "work_item_id",
        "occurred_at",
        "provider",
        "from_status",
        "to_status",
        "from_status_raw",
        "to_status_raw",
        "actor",
        "last_synced",
    ]
    dependency_columns = [
        "org_id",
        "source_work_item_id",
        "target_work_item_id",
        "relationship_type",
        "relationship_type_raw",
        "last_synced",
    ]
    interaction_columns = [
        "org_id",
        "work_item_id",
        "provider",
        "interaction_type",
        "occurred_at",
        "actor",
        "body_length",
        "last_synced",
    ]
    reopen_columns = [
        "org_id",
        "work_item_id",
        "occurred_at",
        "from_status",
        "to_status",
        "from_status_raw",
        "to_status_raw",
        "actor",
        "last_synced",
    ]
    sprint_columns = [
        "org_id",
        "provider",
        "sprint_id",
        "name",
        "state",
        "started_at",
        "ended_at",
        "completed_at",
        "last_synced",
    ]

    def write_complete_window(ts: datetime) -> None:
        sink.client.insert(
            "work_items",
            [
                [
                    org_id,
                    repo_id,
                    work_item_id,
                    "linear",
                    "Scenario 9 work item",
                    "task",
                    "in_progress",
                    "Started",
                    "CHAOS",
                    "linear-project",
                    ["alice@example.com"],
                    "reporter@example.com",
                    created_at,
                    occurred_at,
                    sprint_id,
                    "Cycle 9",
                    ts,
                ]
            ],
            column_names=work_item_columns,
        )
        sink.client.insert(
            "work_item_transitions",
            [
                [
                    org_id,
                    repo_id,
                    work_item_id,
                    occurred_at,
                    "linear",
                    "todo",
                    "in_progress",
                    "Todo",
                    "Started",
                    "alice@example.com",
                    ts,
                ]
            ],
            column_names=transition_columns,
        )
        sink.client.insert(
            "work_item_dependencies",
            [[org_id, work_item_id, target_id, "relates", "related", ts]],
            column_names=dependency_columns,
        )
        sink.client.insert(
            "work_item_interactions",
            [
                [
                    org_id,
                    work_item_id,
                    "linear",
                    "comment",
                    occurred_at + timedelta(minutes=1),
                    "alice@example.com",
                    42,
                    ts,
                ]
            ],
            column_names=interaction_columns,
        )
        sink.client.insert(
            "work_item_reopen_events",
            [
                [
                    org_id,
                    work_item_id,
                    occurred_at + timedelta(minutes=2),
                    "done",
                    "in_progress",
                    "Done",
                    "Started",
                    "alice@example.com",
                    ts,
                ]
            ],
            column_names=reopen_columns,
        )
        sink.client.insert(
            "sprints",
            [
                [
                    org_id,
                    "linear",
                    sprint_id,
                    "Cycle 9",
                    "active",
                    start,
                    end,
                    None,
                    ts,
                ]
            ],
            column_names=sprint_columns,
        )
        sink.write_work_item_team_attributions(
            [
                WorkItemTeamAttributionRecord(
                    work_item_id=work_item_id,
                    provider="linear",
                    source="native_team",
                    is_primary=1,
                    confidence="high",
                    evidence="native_team_key=CHAOS",
                    computed_at=ts,
                    repo_id=repo_id,
                    team_id="team-a",
                    team_name="Team A",
                    org_id=org_id,
                ),
                WorkItemTeamAttributionRecord(
                    work_item_id=work_item_id,
                    provider="linear",
                    source="assignee_membership",
                    is_primary=0,
                    confidence="medium",
                    evidence="assignee=alice@example.com",
                    computed_at=ts,
                    repo_id=repo_id,
                    team_id="team-b",
                    team_name="Team B",
                    org_id=org_id,
                ),
            ]
        )

    def read_surface_snapshot() -> dict[str, Any]:
        items, transitions = asyncio.run(loader.load_work_items(start, end, repo_id))
        dependencies = asyncio.run(
            loader.load_work_item_dependencies(source_work_item_ids=[work_item_id])
        )
        attributions = asyncio.run(
            resolve_work_item_team_attributions(context, work_item_ids=[work_item_id])
        )
        interactions = _rows(
            sink,
            f"""
            SELECT interaction_type, actor, body_length
            FROM {WORK_ITEM_INTERACTIONS_DEDUPED}
            WHERE org_id = %(org_id)s AND work_item_id = %(work_item_id)s
            ORDER BY interaction_type, actor, body_length
            """,
            {"org_id": org_id, "work_item_id": work_item_id},
        )
        reopen_events = _rows(
            sink,
            f"""
            SELECT from_status, to_status, actor
            FROM {WORK_ITEM_REOPEN_EVENTS_DEDUPED}
            WHERE org_id = %(org_id)s AND work_item_id = %(work_item_id)s
            ORDER BY from_status, to_status, actor
            """,
            {"org_id": org_id, "work_item_id": work_item_id},
        )
        sprints = _rows(
            sink,
            f"""
            SELECT sprint_id, name, state
            FROM {SPRINTS_DEDUPED}
            WHERE org_id = %(org_id)s AND provider = 'linear' AND sprint_id = %(sprint_id)s
            ORDER BY sprint_id
            """,
            {"org_id": org_id, "sprint_id": sprint_id},
        )
        return {
            "work_items": [(item.work_item_id, item.title) for item in items],
            "transitions": [
                (row.work_item_id, row.from_status, row.to_status, row.actor)
                for row in transitions
            ],
            "dependencies": [
                (
                    row.source_work_item_id,
                    row.target_work_item_id,
                    row.relationship_type,
                )
                for row in dependencies
            ],
            "interactions": interactions,
            "reopen_events": reopen_events,
            "sprints": sprints,
            "team_attributions": [
                (row.team_id, row.source.value, row.is_primary) for row in attributions
            ],
        }

    try:
        write_complete_window(version_at)
        before = read_surface_snapshot()

        write_complete_window(version_at + timedelta(minutes=5))
        after = read_surface_snapshot()

        assert before == after
        assert after == {
            "work_items": [(work_item_id, "Scenario 9 work item")],
            "transitions": [(work_item_id, "todo", "in_progress", "alice@example.com")],
            "dependencies": [(work_item_id, target_id, "relates")],
            "interactions": [
                {
                    "interaction_type": "comment",
                    "actor": "alice@example.com",
                    "body_length": 42,
                }
            ],
            "reopen_events": [
                {
                    "from_status": "done",
                    "to_status": "in_progress",
                    "actor": "alice@example.com",
                }
            ],
            "sprints": [{"sprint_id": sprint_id, "name": "Cycle 9", "state": "active"}],
            "team_attributions": [
                ("team-a", "native_team", True),
                ("team-b", "assignee_membership", False),
            ],
        }
    finally:
        _delete_org_rows(sink, org_id, tables)


def test_soft_timeout_after_work_items_then_retry_full_window_collapses_item(
    sink: Any,
) -> None:
    from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader

    org_id = f"test-chaos-2710-partial-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = f"linear:CHAOS-{uuid.uuid4()}"
    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=7)
    created_at = start + timedelta(hours=1)
    occurred_at = start + timedelta(hours=2)
    first_synced = start + timedelta(hours=3)
    retry_synced = first_synced + timedelta(minutes=5)
    loader = ClickHouseDataLoader(sink.client, org_id=org_id)
    work_item_columns = [
        "org_id",
        "repo_id",
        "work_item_id",
        "provider",
        "title",
        "type",
        "status",
        "status_raw",
        "project_key",
        "project_id",
        "assignees",
        "reporter",
        "created_at",
        "updated_at",
        "last_synced",
    ]
    transition_columns = [
        "org_id",
        "repo_id",
        "work_item_id",
        "occurred_at",
        "provider",
        "from_status",
        "to_status",
        "from_status_raw",
        "to_status_raw",
        "actor",
        "last_synced",
    ]

    def write_work_item(ts: datetime) -> None:
        sink.client.insert(
            "work_items",
            [
                [
                    org_id,
                    repo_id,
                    work_item_id,
                    "linear",
                    "Partial retry work item",
                    "task",
                    "in_progress",
                    "Started",
                    "CHAOS",
                    "linear-project",
                    ["alice@example.com"],
                    "reporter@example.com",
                    created_at,
                    occurred_at,
                    ts,
                ]
            ],
            column_names=work_item_columns,
        )

    def write_transition(ts: datetime) -> None:
        sink.client.insert(
            "work_item_transitions",
            [
                [
                    org_id,
                    repo_id,
                    work_item_id,
                    occurred_at,
                    "linear",
                    "todo",
                    "in_progress",
                    "Todo",
                    "Started",
                    "alice@example.com",
                    ts,
                ]
            ],
            column_names=transition_columns,
        )

    try:
        write_work_item(first_synced)
        partial_items, partial_transitions = asyncio.run(
            loader.load_work_items(start, end, repo_id)
        )
        assert [item.work_item_id for item in partial_items] == [work_item_id]
        assert partial_transitions == []

        write_work_item(retry_synced)
        write_transition(retry_synced)
        retry_items, retry_transitions = asyncio.run(
            loader.load_work_items(start, end, repo_id)
        )

        assert [(item.work_item_id, item.title) for item in retry_items] == [
            (work_item_id, "Partial retry work item")
        ]
        assert [
            (row.work_item_id, row.from_status, row.to_status, row.actor)
            for row in retry_transitions
        ] == [(work_item_id, "todo", "in_progress", "alice@example.com")]
    finally:
        _delete_org_rows(sink, org_id, ["work_item_transitions", "work_items"])
