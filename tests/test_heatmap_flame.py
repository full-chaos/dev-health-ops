from datetime import datetime, timezone

from dev_health_ops.api.models.schemas import (
    FlameFrame,
    FlameTimeline,
    HeatmapAxes,
    HeatmapCell,
    HeatmapLegend,
    HeatmapResponse,
)
from dev_health_ops.api.services.flame import validate_flame_frames
from dev_health_ops.api.services.heatmap import (
    HEATMAP_METRICS,
    WEEKDAY_LABELS,
    _hour_labels,
)


def test_heatmap_schema_shape():
    response = HeatmapResponse(
        axes=HeatmapAxes(x=["00"], y=["Mon"]),
        cells=[HeatmapCell(x="00", y="Mon", value=2.0)],
        legend=HeatmapLegend(unit="hours", scale="linear"),
        evidence=[{"id": "sample"}],
    )
    assert response.axes.x == ["00"]


def test_heatmap_no_person_matrix():
    assert all(
        not (metric.x_axis == "person" and metric.y_axis == "person")
        for metric in HEATMAP_METRICS
    )


def test_heatmap_axis_label_snapshot():
    assert WEEKDAY_LABELS == ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    assert _hour_labels() == [f"{hour:02d}" for hour in range(24)]


def test_heatmap_legend_units_snapshot():
    expected = {
        "review_wait_density": "hours",
        "repo_touchpoints": "commits",
        "hotspot_risk": "hotspot score",
        "active_hours": "commits",
    }
    assert {metric.metric: metric.unit for metric in HEATMAP_METRICS} == expected


def test_flame_frames_cover_lifecycle():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)
    timeline = FlameTimeline(start=start, end=end)
    frames = [
        FlameFrame(
            id="root",
            parent_id=None,
            label="Lifecycle",
            start=start,
            end=end,
            state="active",
            category="planned",
        ),
        FlameFrame(
            id="child",
            parent_id="root",
            label="Phase",
            start=start,
            end=end,
            state="active",
            category="planned",
        ),
    ]
    assert validate_flame_frames(timeline, frames)


def test_flame_frames_gap_detection():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)
    timeline = FlameTimeline(start=start, end=end)
    frames = [
        FlameFrame(
            id="root-a",
            parent_id=None,
            label="Phase A",
            start=start,
            end=datetime(2024, 1, 1, 12, tzinfo=timezone.utc),
            state="active",
            category="planned",
        ),
        FlameFrame(
            id="root-b",
            parent_id=None,
            label="Phase B",
            start=datetime(2024, 1, 1, 13, tzinfo=timezone.utc),
            end=end,
            state="active",
            category="planned",
        ),
    ]
    assert not validate_flame_frames(timeline, frames)


# Aggregated flame graph tests


def test_aggregated_flame_empty_response_shape():
    """Verify empty aggregated flame response has valid structure."""
    from datetime import date
    from dev_health_ops.api.models.schemas import (
        AggregatedFlameNode,
        AggregatedFlameMeta,
        AggregatedFlameResponse,
    )

    response = AggregatedFlameResponse(
        mode="cycle_breakdown",
        unit="hours",
        root=AggregatedFlameNode(name="Cycle Time", value=0, children=[]),
        meta=AggregatedFlameMeta(
            window_start=date(2024, 1, 1),
            window_end=date(2024, 1, 31),
            filters={},
            notes=["No data available."],
        ),
    )
    assert response.mode == "cycle_breakdown"
    assert response.root.value == 0
    assert response.root.children == []
    assert len(response.meta.notes) == 1


def test_aggregated_flame_cycle_breakdown_structure():
    """Verify cycle breakdown tree builds correctly with categories."""
    from dev_health_ops.api.services.aggregated_flame import (
        _build_cycle_breakdown_tree,
    )

    rows = [
        {"status": "in_progress", "total_hours": 100.0, "total_items": 50},
        {"status": "review", "total_hours": 80.0, "total_items": 40},
        {"status": "waiting", "total_hours": 60.0, "total_items": 30},
        {"status": "blocked", "total_hours": 20.0, "total_items": 10},
    ]

    root = _build_cycle_breakdown_tree(rows)

    assert root.name == "Cycle Time"
    assert root.value == 260.0  # 100 + 80 + 60 + 20
    assert len(root.children) >= 1

    # Check categories exist
    category_names = {child.name for child in root.children}
    assert "Active Work" in category_names or any(
        c for c in root.children if c.value >= 100
    )

    # Children are sorted by value descending
    values = [c.value for c in root.children]
    assert values == sorted(values, reverse=True)


def test_aggregated_flame_code_hotspots_hierarchy():
    """Verify code hotspots tree builds path hierarchy correctly."""
    from dev_health_ops.api.services.aggregated_flame import (
        _build_code_hotspots_tree,
    )

    rows = [
        {"repo_id": "repo1", "file_path": "src/main.py", "total_churn": 100},
        {"repo_id": "repo1", "file_path": "src/utils/helper.py", "total_churn": 50},
        {"repo_id": "repo2", "file_path": "lib/core.py", "total_churn": 75},
    ]
    repo_names = {"repo1": "my-app", "repo2": "shared-lib"}

    root = _build_code_hotspots_tree(rows, repo_names)

    assert root.name == "Code Churn"
    assert root.value == 225  # 100 + 50 + 75
    assert len(root.children) == 2

    # Check repo nodes exist with proper names
    repo_node_names = {child.name for child in root.children}
    assert "my-app" in repo_node_names
    assert "shared-lib" in repo_node_names

    # Children are sorted by value descending
    values = [c.value for c in root.children]
    assert values == sorted(values, reverse=True)


def test_aggregated_flame_filter_propagation():
    """Verify filters are included in meta."""
    from datetime import date
    from dev_health_ops.api.models.schemas import (
        AggregatedFlameNode,
        AggregatedFlameMeta,
        AggregatedFlameResponse,
    )

    response = AggregatedFlameResponse(
        mode="code_hotspots",
        unit="loc",
        root=AggregatedFlameNode(name="Code Churn", value=100, children=[]),
        meta=AggregatedFlameMeta(
            window_start=date(2024, 1, 1),
            window_end=date(2024, 1, 31),
            filters={"repo_id": "abc-123", "team_id": "team-alpha"},
            notes=[],
        ),
    )
    assert response.meta.filters["repo_id"] == "abc-123"
    assert response.meta.filters["team_id"] == "team-alpha"


def test_aggregated_flame_throughput_tree_structure():
    """Verify throughput tree builds correctly with work types and teams/repos."""
    from dev_health_ops.api.services.aggregated_flame import _build_throughput_tree

    rows = [
        {"work_type": "Feature", "team_id": "team1", "repo_id": None, "throughput": 10},
        {"work_type": "Bug", "team_id": "team1", "repo_id": None, "throughput": 5},
        {"work_type": "Feature", "team_id": "team2", "repo_id": None, "throughput": 8},
    ]

    root = _build_throughput_tree(rows)

    assert root.name == "Work Delivered"
    assert root.value == 23  # 10 + 5 + 8
    assert len(root.children) == 2  # Feature, Bug

    feature_node = next(n for n in root.children if n.name == "Feature")
    assert feature_node.value == 18
    assert len(feature_node.children) == 2  # team1, team2


def test_aggregated_flame_milestone_approximation():
    """Verify cycle_breakdown fallback to milestones when status durations are missing."""
    from unittest.mock import patch, MagicMock
    from datetime import date
    from dev_health_ops.api.services.aggregated_flame import (
        build_aggregated_flame_response,
    )

    # Mock DB functions
    async def mock_fetch_status_durations(*args, **kwargs):
        return []  # No status data

    async def mock_fetch_cycle_milestones(*args, **kwargs):
        return [
            {"milestone": "opened_to_started", "avg_hours": 24, "total_items": 10},
            {"milestone": "started_to_merged", "avg_hours": 48, "total_items": 10},
        ]

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def mock_clickhouse_client(db_url):
        yield MagicMock()

    with (
        patch(
            "dev_health_ops.api.services.aggregated_flame.fetch_cycle_breakdown",
            side_effect=mock_fetch_status_durations,
        ),
        patch(
            "dev_health_ops.api.services.aggregated_flame.fetch_cycle_milestones",
            side_effect=mock_fetch_cycle_milestones,
        ),
        patch(
            "dev_health_ops.api.services.aggregated_flame.clickhouse_client",
            side_effect=mock_clickhouse_client,
        ),
    ):
        # Test the approximation fallback
        import anyio

        async def run_test():
            return await build_aggregated_flame_response(
                db_url="mock://",
                mode="cycle_breakdown",
                start_day=date(2024, 1, 1),
                end_day=date(2024, 1, 31),
            )

        response = anyio.run(run_test)

        assert response.meta.approximation.used is True
        assert response.meta.approximation.method == "milestones"
        assert response.root.value > 0
        assert any("approximation" in n.lower() for n in response.meta.notes)
