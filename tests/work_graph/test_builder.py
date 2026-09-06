"""Tests for work graph builder."""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.work_graph.builder import (
    BuildConfig,
    WorkGraphBuilder,
)


@pytest.fixture
def mock_ch_client():
    """Create a mock ClickHouse client."""
    client = MagicMock()
    # Mock query_df to return empty dataframe
    try:
        import pandas as pd

        client.query_df.return_value = pd.DataFrame()
    except ImportError:
        # pandas is an optional test dependency; if it's not available,
        # leave query_df unconfigured and allow tests that need it to handle this.
        pass
    return client


@pytest.fixture
def config():
    """Create a build config."""
    return BuildConfig(
        dsn="clickhouse://localhost:9000/default",
    )


class TestBuildConfig:
    """Tests for BuildConfig."""

    def test_defaults(self):
        """Default values should be set."""
        cfg = BuildConfig(
            dsn="clickhouse://localhost:9000/default",
        )
        assert cfg.from_date is None
        assert cfg.to_date is None
        assert cfg.repo_id is None
        assert cfg.heuristic_days_window == 7
        assert cfg.heuristic_confidence == 0.3

    def test_custom_values(self):
        """Custom values should be set."""
        from_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        repo_id = uuid.uuid4()
        cfg = BuildConfig(
            dsn="clickhouse://localhost:9000/default",
            from_date=from_dt,
            repo_id=repo_id,
            heuristic_days_window=14,
            heuristic_confidence=0.5,
        )
        assert cfg.from_date == from_dt
        assert cfg.repo_id == repo_id
        assert cfg.heuristic_days_window == 14
        assert cfg.heuristic_confidence == 0.5


class TestWorkGraphBuilder:
    """Tests for WorkGraphBuilder."""

    def test_init(self, config):
        """Builder should initialize with config using sink pattern."""
        # Create a fake sink that mimics ClickHouseMetricsSink
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.client = MagicMock()

        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            assert builder.config == config
            assert builder.sink == fake_sink
            builder.close()
            fake_sink.close.assert_called_once()

    def test_passes_operational_scope_to_edge_builder(self) -> None:
        from_date = datetime(2026, 7, 15, tzinfo=timezone.utc)
        to_date = datetime(2026, 7, 17, tzinfo=timezone.utc)
        repo_id = uuid.uuid4()
        config = BuildConfig(
            dsn="clickhouse://localhost:9000/default",
            org_id="org-a",
            from_date=from_date,
            to_date=to_date,
            repo_id=repo_id,
        )
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"

        with (
            patch(
                "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
            ),
            patch(
                "dev_health_ops.work_graph.builder.build_operational_incident_edges",
                return_value=[],
            ) as build_operational_edges,
        ):
            builder = WorkGraphBuilder(config)
            with patch.object(builder, "_write_edges", return_value=0):
                builder._build_operational_incident_edges()
            builder.close()

        build_operational_edges.assert_called_once_with(
            fake_sink,
            "org-a",
            builder._now,
            config.heuristic_days_window,
            config.heuristic_confidence,
            from_date,
            to_date,
            repo_id,
        )


class TestDependencyIssuePrLinks:
    def test_pr_attachment_dependency_is_not_written_as_issue_issue_edge(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.query_dicts.return_value = [
            {
                "source_work_item_id": "ghpr:full-chaos/dev-health-ops#968",
                "target_work_item_id": "linear:CHAOS-2400",
                "relationship_type": "relates_to",
                "relationship_type_raw": "linear_attachment",
                "last_synced": datetime(2026, 7, 1, tzinfo=timezone.utc),
            }
        ]

        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._build_issue_issue_edges()
            builder.close()

        assert count == 0
        fake_sink.write_work_graph_edges.assert_not_called()

    def test_regular_dependency_still_writes_issue_issue_edge(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.client = MagicMock()
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.query_dicts.return_value = [
            {
                "source_work_item_id": "linear:CHAOS-2400",
                "target_work_item_id": "linear:CHAOS-2401",
                "relationship_type": "blocks",
                "relationship_type_raw": "blocks",
                "last_synced": datetime(2026, 7, 1, tzinfo=timezone.utc),
            }
        ]

        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._build_issue_issue_edges()
            builder.close()

        assert count == 1
        fake_sink.write_work_graph_edges.assert_called_once()
        edge = fake_sink.write_work_graph_edges.call_args[0][0][0]
        assert edge.source_type == "issue"
        assert edge.source_id == "linear:CHAOS-2400"
        assert edge.target_type == "issue"
        assert edge.target_id == "linear:CHAOS-2401"
        assert edge.edge_type == "blocks"

    def test_blocker_rebuild_mixes_legacy_and_v2_without_double_swapping(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.client = MagicMock()
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_projection_runs = MagicMock()
        fake_sink.query_dicts.return_value = [
            {
                # Legacy GitHub encoded "blocked by blocker" backwards.
                "source_work_item_id": "gh:org/repo#blocked",
                "target_work_item_id": "gh:org/repo#blocker",
                "relationship_type": "blocks",
                "relationship_type_raw": "blocks",
                "relationship_semantics_version": "legacy.v1",
                "last_synced": datetime(2026, 7, 1, tzinfo=timezone.utc),
            },
            {
                # New rows already obey source --blocks--> target.
                "source_work_item_id": "gh:org/repo#blocker",
                "target_work_item_id": "gh:org/repo#blocked",
                "relationship_type": "blocks",
                "relationship_type_raw": "blocked by #blocker",
                "relationship_semantics_version": "canonical-blocks.v2",
                "last_synced": datetime(2026, 7, 1, tzinfo=timezone.utc),
            },
        ]

        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            assert builder._build_issue_issue_edges() == 1
            assert builder._build_issue_issue_edges() == 1
            builder.close()

        for call in fake_sink.write_work_graph_edges.call_args_list:
            edges = call.args[0]
            assert len(edges) == 1
            assert edges[0].source_id == "gh:org/repo#blocker"
            assert edges[0].target_id == "gh:org/repo#blocked"
            assert edges[0].edge_type == "blocks"
        assert fake_sink.client.command.call_count == 4
        first_edge_delete = fake_sink.client.command.call_args_list[1]
        assert first_edge_delete.kwargs["parameters"]["org_id"] == "org-a"
        assert len(first_edge_delete.kwargs["parameters"]["edge_ids"]) == 6
        assert fake_sink.write_work_graph_projection_runs.call_count == 2

    def test_blocker_projection_fails_before_write_when_cleanup_is_unavailable(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.client = object()
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_projection_runs = MagicMock()
        fake_sink.query_dicts.return_value = [
            {
                "source_work_item_id": "jira:BLOCK-1",
                "target_work_item_id": "jira:DONE-1",
                "relationship_type": "blocks",
                "relationship_type_raw": "blocks",
                "relationship_semantics_version": "canonical-blocks.v2",
                "last_synced": datetime(2026, 7, 1, tzinfo=timezone.utc),
            }
        ]
        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")

        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            with pytest.raises(RuntimeError, match="cleanup is unavailable"):
                builder._build_issue_issue_edges()
            builder.close()

        fake_sink.write_work_graph_edges.assert_not_called()
        fake_sink.write_work_graph_projection_runs.assert_not_called()

    def test_blocker_projection_fails_before_cleanup_when_marker_sink_is_unavailable(
        self,
    ):
        fake_sink = MagicMock(
            spec=["backend_type", "client", "query_dicts", "ensure_schema", "close"]
        )
        fake_sink.backend_type = "clickhouse"
        fake_sink.client = MagicMock()
        fake_sink.query_dicts.return_value = [
            {
                "source_work_item_id": "jira:BLOCK-1",
                "target_work_item_id": "jira:DONE-1",
                "relationship_type": "blocks",
                "relationship_type_raw": "blocks",
                "relationship_semantics_version": "canonical-blocks.v2",
                "last_synced": datetime(2026, 7, 1, tzinfo=timezone.utc),
            }
        ]
        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")

        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            with pytest.raises(RuntimeError, match="watermark sink is unavailable"):
                builder._build_issue_issue_edges()
            builder.close()

        fake_sink.client.command.assert_not_called()

    def test_zero_dependency_rebuild_removes_stale_blockers_before_publishing(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.client = MagicMock()
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_projection_runs = MagicMock()
        fake_sink.query_dicts.side_effect = [
            [],
            [{"edge_id": "stale-blocker-edge"}],
        ]
        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")

        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            assert builder._build_issue_issue_edges() == 0
            builder.close()

        fake_sink.write_work_graph_edges.assert_not_called()
        marker_delete, delete = fake_sink.client.command.call_args_list
        assert "work_graph_projection_runs" in marker_delete.args[0]
        assert delete.kwargs["parameters"] == {
            "org_id": "org-a",
            "edge_ids": ["stale-blocker-edge"],
        }
        marker = fake_sink.write_work_graph_projection_runs.call_args.args[0][0]
        assert marker.row_count == 0
        assert marker.rule_version == "canonical-blocks.v2"

    def test_blocker_projection_persists_through_real_clickhouse_sink_contract(self):
        from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

        sink = object.__new__(ClickHouseMetricsSink)
        sink.client = MagicMock()
        builder = object.__new__(WorkGraphBuilder)
        builder.config = BuildConfig(
            dsn="clickhouse://localhost:9000/default", org_id="org-a"
        )
        builder.sink = sink
        builder._now = datetime(2026, 7, 28, 12, tzinfo=timezone.utc)

        builder._publish_blocker_projection([], [])

        insert = sink.client.insert.call_args
        assert insert.args[0] == "work_graph_projection_runs"
        assert insert.args[1][0][0:4] == [
            "org-a",
            "issue_blockers",
            None,
            "canonical-blocks.v2",
        ]
        assert insert.args[1][0][5] == 0

    def test_stale_pr_dependency_issue_edge_cleanup_is_scoped(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.client = MagicMock()

        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            builder._delete_stale_pr_dependency_issue_edges()
            builder.close()

        sql = fake_sink.client.command.call_args.args[0]
        params = fake_sink.client.command.call_args.kwargs["parameters"]
        assert "ALTER TABLE work_graph_edges DELETE WHERE" in sql
        assert "source_type = 'issue'" in sql
        assert "target_type = 'issue'" in sql
        assert "evidence = 'linear_attachment'" in sql
        assert "github_comment_linear_url" not in sql
        assert "startsWith(source_id, 'ghpr:')" in sql
        assert "startsWith(source_id, 'gitlab:')" in sql
        assert "startsWith(target_id, 'linear:')" in sql
        assert "org_id = {org_id:String}" in sql
        assert params == {"org_id": "org-a"}

    def test_stale_pr_dependency_cleanup_skips_unscoped_builds(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts = MagicMock()

        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            builder._delete_stale_pr_dependency_issue_edges()
            builder.close()

        fake_sink.query_dicts.assert_not_called()
        fake_sink.client.command.assert_not_called()


class TestHeuristicMatching:
    """Tests for heuristic issue->PR matching with binary search optimization."""

    def test_heuristic_finds_closest_pr_in_window(self):
        """Heuristic should find the closest PR within time window."""
        repo_id = uuid.uuid4()
        base_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts = MagicMock()

        wi_rows = [
            {
                "repo_id": repo_id,
                "work_item_id": "jira:TEST-1",
                "updated_at": base_time,
            },
        ]
        pr_rows = [
            {
                "repo_id": repo_id,
                "number": 1,
                "created_at": base_time - timedelta(days=10),
            },
            {
                "repo_id": repo_id,
                "number": 2,
                "created_at": base_time - timedelta(days=2),
            },
            {
                "repo_id": repo_id,
                "number": 3,
                "created_at": base_time + timedelta(days=1),
            },
            {
                "repo_id": repo_id,
                "number": 4,
                "created_at": base_time + timedelta(days=10),
            },
        ]

        def mock_query(query, params):
            if "work_items" in query:
                return wi_rows
            if "git_pull_requests" in query:
                return pr_rows
            return []

        fake_sink.query_dicts.side_effect = mock_query
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_issue_pr = MagicMock()

        config = BuildConfig(
            dsn="clickhouse://localhost:9000/default", heuristic_days_window=7
        )

        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._build_heuristic_issue_pr_edges(set())
            builder.close()

        assert count == 1
        written_edges = fake_sink.write_work_graph_edges.call_args[0][0]
        assert len(written_edges) == 1
        assert written_edges[0].confidence == 0.3

    def test_heuristic_excludes_prs_outside_window(self):
        """PRs outside time window should not be matched."""
        repo_id = uuid.uuid4()
        base_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"

        wi_rows = [
            {
                "repo_id": repo_id,
                "work_item_id": "jira:TEST-1",
                "updated_at": base_time,
            },
        ]
        pr_rows = [
            {
                "repo_id": repo_id,
                "number": 1,
                "created_at": base_time - timedelta(days=30),
            },
            {
                "repo_id": repo_id,
                "number": 2,
                "created_at": base_time + timedelta(days=30),
            },
        ]

        def mock_query(query, params):
            if "work_items" in query:
                return wi_rows
            if "git_pull_requests" in query:
                return pr_rows
            return []

        fake_sink.query_dicts.side_effect = mock_query
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_issue_pr = MagicMock()

        config = BuildConfig(
            dsn="clickhouse://localhost:9000/default", heuristic_days_window=7
        )

        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._build_heuristic_issue_pr_edges(set())
            builder.close()

        assert count == 0

    def test_heuristic_skips_explicit_links(self):
        """Already-linked work items should be skipped."""
        repo_id = uuid.uuid4()
        base_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"

        wi_rows = [
            {
                "repo_id": repo_id,
                "work_item_id": "jira:TEST-1",
                "updated_at": base_time,
            },
        ]
        pr_rows = [
            {"repo_id": repo_id, "number": 1, "created_at": base_time},
        ]

        def mock_query(query, params):
            if "work_items" in query:
                return wi_rows
            if "git_pull_requests" in query:
                return pr_rows
            return []

        fake_sink.query_dicts.side_effect = mock_query
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_issue_pr = MagicMock()

        config = BuildConfig(
            dsn="clickhouse://localhost:9000/default", heuristic_days_window=7
        )
        explicit_links = {("jira:TEST-1", 1)}

        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._build_heuristic_issue_pr_edges(explicit_links)
            builder.close()

        assert count == 0


class TestWorkGraphBuilderIntegration:
    """Integration tests for WorkGraphBuilder.

    These tests are skipped by default and require a real ClickHouse instance.
    Run with: pytest -m integration
    """

    @pytest.mark.skip(reason="Requires ClickHouse instance")
    def test_full_build(self):
        """Build complete work graph."""
        pass

    @pytest.mark.skip(reason="Requires ClickHouse instance")
    def test_incremental_build(self):
        """Incremental build with from_date parameter."""
        pass


class TestFlagGuardsEdges:
    """CHAOS-2630 Phase C1: flag -> issue GUARDS edges from real text references."""

    def _run(self, flag_rows, wi_rows, *, org_id="org-1"):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"

        def mock_query(query, params):
            if "feature_flag" in query:
                return flag_rows
            if "work_items" in query:
                return wi_rows
            return []

        fake_sink.query_dicts = MagicMock(side_effect=mock_query)
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_feature_flag_links = MagicMock()

        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id=org_id)
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._build_flag_guards_edges()
            builder.close()
        return count, fake_sink

    @pytest.mark.parametrize(
        "flag_provider,work_item_id",
        [
            ("launchdarkly", "jira:ABC-1"),
            ("launchdarkly", "gh:org/repo#5"),
            ("gitlab", "gl:grp/proj#9"),
            ("gitlab", "linear:TEAM-3"),
        ],
    )
    def test_known_flag_in_issue_text_emits_guards_edge(
        self, flag_provider, work_item_id
    ):
        flag_rows = [
            {
                "flag_key": "checkout-v2",
                "provider": flag_provider,
                "project_key": "web",
            },
        ]
        wi_rows = [
            {
                "work_item_id": work_item_id,
                "title": "Roll out checkout-v2",
                "description": "Gate the new flow behind the flag",
            },
        ]
        count, sink = self._run(flag_rows, wi_rows)

        assert count == 1
        edges = sink.write_work_graph_edges.call_args[0][0]
        assert len(edges) == 1
        edge = edges[0]
        assert edge.edge_type == "guards"
        assert edge.source_type == "feature_flag"
        assert edge.target_type == "issue"
        assert edge.target_id == work_item_id
        assert edge.provenance == "explicit_text"
        assert edge.confidence == 0.6
        assert edge.provider == flag_provider
        assert edge.evidence == "flagref:checkout-v2"

        links = sink.write_feature_flag_links.call_args[0][0]
        assert len(links) == 1
        link = links[0]
        assert link.flag_key == "checkout-v2"
        assert link.target_type == "issue"
        assert link.target_id == work_item_id
        assert link.link_source == "explicit_text"
        assert link.link_type == "tracks"
        assert link.evidence_type == "issue_text"
        assert link.confidence == 0.6
        assert link.provider == flag_provider
        assert link.org_id == "org-1"

    def test_unknown_flag_key_emits_nothing(self):
        flag_rows = [
            {
                "flag_key": "checkout-v2",
                "provider": "launchdarkly",
                "project_key": "web",
            },
        ]
        wi_rows = [
            {
                "work_item_id": "jira:ABC-1",
                "title": "Toggle mystery-flag",
                "description": "not a registered flag",
            },
        ]
        count, sink = self._run(flag_rows, wi_rows)
        assert count == 0
        sink.write_work_graph_edges.assert_not_called()
        sink.write_feature_flag_links.assert_not_called()

    def test_empty_registry_skips_emission(self):
        count, sink = self._run(
            [],
            [{"work_item_id": "jira:ABC-1", "title": "x", "description": "y"}],
        )
        assert count == 0
        sink.write_work_graph_edges.assert_not_called()
        sink.write_feature_flag_links.assert_not_called()

    def test_substring_reference_does_not_emit(self):
        flag_rows = [
            {
                "flag_key": "search",
                "provider": "launchdarkly",
                "project_key": "web",
            },
        ]
        wi_rows = [
            {
                "work_item_id": "jira:ABC-1",
                "title": "we are searching the logs",
                "description": "",
            },
        ]
        count, sink = self._run(flag_rows, wi_rows)
        assert count == 0
        sink.write_work_graph_edges.assert_not_called()
