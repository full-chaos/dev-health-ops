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
    def test_linear_attachment_derives_issue_pr_fast_path_link(self):
        repo_id = uuid.uuid4()
        synced_at = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)

        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.write_work_graph_issue_pr = MagicMock()

        def mock_query(query, params):
            if "work_item_dependencies" in query:
                return [
                    {
                        "org_id": "org-a",
                        "source_work_item_id": "ghpr:full-chaos/dev-health-ops#968",
                        "target_work_item_id": "linear:CHAOS-2400",
                        "relationship_type_raw": "linear_attachment",
                        "last_synced": synced_at,
                    }
                ]
            if "FROM repos" in query:
                return [
                    {
                        "org_id": "org-a",
                        "id": repo_id,
                        "repo": "full-chaos/dev-health-ops",
                    }
                ]
            if "git_pull_requests" in query:
                return [
                    {
                        "org_id": "org-a",
                        "repo_id": repo_id,
                        "number": 968,
                        "created_at": synced_at,
                    }
                ]
            if "work_items" in query:
                return [{"org_id": "org-a", "work_item_id": "linear:CHAOS-2400"}]
            return []

        fake_sink.query_dicts.side_effect = mock_query

        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_issue_pr_links_from_dependencies()
            builder.close()

        assert count == 1
        records = fake_sink.write_work_graph_issue_pr.call_args[0][0]
        assert len(records) == 1
        record = records[0]
        assert record.repo_id == repo_id
        assert record.work_item_id == "linear:CHAOS-2400"
        assert record.pr_number == 968
        assert record.provenance == "native"
        assert record.confidence == 1.0
        assert record.evidence == "linear_attachment"
        assert record.org_id == "org-a"

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

    def test_generic_pr_shaped_dependency_does_not_derive_issue_pr_link(self):
        repo_id = uuid.uuid4()
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.write_work_graph_issue_pr = MagicMock()

        def mock_query(query, params):
            if "work_item_dependencies" in query:
                return [
                    {
                        "org_id": "org-a",
                        "source_work_item_id": "ghpr:full-chaos/dev-health-ops#10",
                        "target_work_item_id": "gh:full-chaos/dev-health-ops#123",
                        "relationship_type_raw": "blocks",
                        "last_synced": datetime(2026, 7, 1, tzinfo=timezone.utc),
                    }
                ]
            if "FROM repos" in query:
                return [
                    {
                        "org_id": "org-a",
                        "id": repo_id,
                        "repo": "full-chaos/dev-health-ops",
                    }
                ]
            if "git_pull_requests" in query:
                return [{"org_id": "org-a", "repo_id": repo_id, "number": 10}]
            if "work_items" in query:
                return [
                    {
                        "org_id": "org-a",
                        "work_item_id": "gh:full-chaos/dev-health-ops#123",
                    }
                ]
            return []

        fake_sink.query_dicts.side_effect = mock_query

        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_issue_pr_links_from_dependencies()
            builder.close()

        assert count == 0
        fake_sink.write_work_graph_issue_pr.assert_not_called()

    def test_dependency_derivation_skips_unscoped_builds(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts = MagicMock()
        fake_sink.write_work_graph_issue_pr = MagicMock()

        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_issue_pr_links_from_dependencies()
            builder._delete_stale_pr_dependency_issue_edges()
            builder.close()

        assert count == 0
        fake_sink.query_dicts.assert_not_called()
        fake_sink.write_work_graph_issue_pr.assert_not_called()
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


class TestDerivePRCommitLinks:
    """Tests for live PR->commit derivation from commit messages.

    These prove the seam that wires ``work_graph_pr_commit`` onto the live path:
    the builder must parse already-synced ``git_commits`` for PR refs and persist
    ``WorkGraphPRCommit`` rows via ``sink.write_work_graph_pr_commit`` -- previously
    only fixtures wrote that table, so real orgs saw no commits under PRs.
    """

    def _build_sink(self, pr_rows, commit_rows):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"

        def mock_query(query, params):
            if "git_pull_requests" in query:
                return pr_rows
            if "git_commits" in query:
                return commit_rows
            return []

        fake_sink.query_dicts.side_effect = mock_query
        fake_sink.write_work_graph_pr_commit = MagicMock()
        return fake_sink

    def test_derives_links_from_merge_keyword_commits(self):
        """GitHub/GitLab merge-keyword commit messages referencing known PRs yield
        links; the ambiguous squash ``(#N)`` form does NOT."""
        repo_id = uuid.uuid4()
        base_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        pr_rows = [
            {"repo_id": repo_id, "number": 42},
            {"repo_id": repo_id, "number": 7},
            {"repo_id": repo_id, "number": 45},
        ]
        commit_rows = [
            {
                "repo_id": repo_id,
                "hash": "aaa111",
                "message": "Merge pull request #42 from feature/x",
                "author_when": base_time,
            },
            {
                "repo_id": repo_id,
                "hash": "ggg777",
                "message": "See merge request grp/proj!45",
                "author_when": base_time,
            },
            {
                "repo_id": repo_id,
                "hash": "bbb222",
                # A bare/plain issue mention is ambiguous with an issue ref and
                # carries no squash suffix -> must be ignored, even though PR #7
                # exists in this repo.
                "message": "Add retry logic for #7",
                "author_when": base_time,
            },
        ]
        fake_sink = self._build_sink(pr_rows, commit_rows)

        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_pr_commit_links()
            builder.close()

        assert count == 2
        records = fake_sink.write_work_graph_pr_commit.call_args[0][0]
        assert len(records) == 2
        by_commit = {r.commit_hash: r for r in records}
        # Only the two unambiguous merge-keyword commits are linked.
        assert by_commit["aaa111"].pr_number == 42
        assert by_commit["aaa111"].repo_id == repo_id
        assert by_commit["ggg777"].pr_number == 45
        assert "bbb222" not in by_commit
        for record in records:
            assert record.provenance == "explicit_text"
            assert record.confidence == 0.9
            assert record.evidence == "commit_message_pr_ref"
            assert record.org_id == ""

    def test_org_id_scoped_onto_records(self):
        """The configured org_id is stamped onto every derived record."""
        repo_id = uuid.uuid4()
        pr_rows = [{"repo_id": repo_id, "number": 99}]
        commit_rows = [
            {
                "repo_id": repo_id,
                "hash": "ccc333",
                "message": "Merge pull request #99 from fix/flake",
                "author_when": datetime(2024, 1, 1, tzinfo=timezone.utc),
            },
        ]
        fake_sink = self._build_sink(pr_rows, commit_rows)

        config = BuildConfig(
            dsn="clickhouse://localhost:9000/default", org_id="org-abc"
        )
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_pr_commit_links()
            builder.close()

        assert count == 1
        record = fake_sink.write_work_graph_pr_commit.call_args[0][0][0]
        assert record.org_id == "org-abc"
        assert record.commit_hash == "ccc333"
        assert record.pr_number == 99

    def test_ignores_refs_to_unknown_prs(self):
        """A '#N' that is not a real PR number must not produce a link."""
        repo_id = uuid.uuid4()
        pr_rows = [{"repo_id": repo_id, "number": 5}]
        commit_rows = [
            {
                "repo_id": repo_id,
                "hash": "ddd444",
                # #500 is not a known PR; should be ignored entirely.
                "message": "Closes issue #500, unrelated to any PR",
                "author_when": datetime(2024, 1, 1, tzinfo=timezone.utc),
            },
        ]
        fake_sink = self._build_sink(pr_rows, commit_rows)

        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_pr_commit_links()
            builder.close()

        assert count == 0
        fake_sink.write_work_graph_pr_commit.assert_not_called()

    def test_plain_issue_ref_colliding_with_pr_number_is_not_linked(self):
        """A plain issue mention (``Fixes #N``) must NOT be linked to PR #N.

        Regression guard for the false-positive corruption flagged in review:
        an ordinary issue reference whose number happens to equal a real PR
        number in the same repo must never become a persisted PR->commit edge.
        """
        repo_id = uuid.uuid4()
        # PR #7 exists in this repo...
        pr_rows = [{"repo_id": repo_id, "number": 7}]
        commit_rows = [
            {
                "repo_id": repo_id,
                "hash": "fff666",
                # ...but this commit merely *closes issue* #7; it is unrelated
                # to PR #7 and must not be promoted into a PR->commit link.
                "message": "Fixes #7 in the parser",
                "author_when": datetime(2024, 1, 1, tzinfo=timezone.utc),
            },
        ]
        fake_sink = self._build_sink(pr_rows, commit_rows)

        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_pr_commit_links()
            builder.close()

        assert count == 0
        fake_sink.write_work_graph_pr_commit.assert_not_called()

    def test_squash_paren_ref_to_known_pr_is_linked_with_distinct_evidence(self):
        """A squash ``(#N)`` matching a KNOWN PR links with lower-confidence,
        distinctly-tagged evidence (CHAOS-2435 interim recovery).

        GitHub's *squash and merge* writes ``"<subject> (#N)"`` with no explicit
        merge keyword, so the strict :func:`extract_pr_refs` discards it -- which
        zeroed PR->commit edges for squash-merge orgs. The interim approach
        promotes the squash suffix when ``N`` is a real PR in the same
        (org, repo), but tags it ``provenance=heuristic`` / ``confidence=0.6`` /
        ``evidence='commit_message_squash_pr_ref'`` so downstream consumers can
        weight or filter the (inherently more ambiguous) link.
        """
        repo_id = uuid.uuid4()
        pr_rows = [{"repo_id": repo_id, "number": 42}]
        commit_rows = [
            {
                "repo_id": repo_id,
                "hash": "f00d42",
                "message": "Fix parser edge case (#42)",
                "author_when": datetime(2024, 1, 1, tzinfo=timezone.utc),
            },
        ]
        fake_sink = self._build_sink(pr_rows, commit_rows)

        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_pr_commit_links()
            builder.close()

        assert count == 1
        record = fake_sink.write_work_graph_pr_commit.call_args[0][0][0]
        assert record.commit_hash == "f00d42"
        assert record.pr_number == 42
        assert record.repo_id == repo_id
        assert record.provenance == "heuristic"
        assert record.confidence == 0.6
        assert record.evidence == "commit_message_squash_pr_ref"

    def test_squash_paren_ref_to_unknown_pr_is_not_linked(self):
        """A squash ``(#N)`` whose N is NOT a known PR must NOT link.

        The known-PR corroboration is the only guard against attaching a
        hand-authored parenthetical issue ref to an unrelated PR. If #99 is not
        a real PR in this (org, repo), no link is written.
        """
        repo_id = uuid.uuid4()
        pr_rows = [{"repo_id": repo_id, "number": 42}]
        commit_rows = [
            {
                "repo_id": repo_id,
                "hash": "f00d99",
                "message": "Fix parser edge case (#99)",
                "author_when": datetime(2024, 1, 1, tzinfo=timezone.utc),
            },
        ]
        fake_sink = self._build_sink(pr_rows, commit_rows)

        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_pr_commit_links()
            builder.close()

        assert count == 0
        fake_sink.write_work_graph_pr_commit.assert_not_called()

    def test_squash_paren_ref_does_not_link_across_orgs(self):
        """A squash ``(#N)`` in org A must NEVER link to org B's PR #N.

        CHAOS-2189 mirror: ``repo_id`` can collide across tenants. Org A owns
        PR #5 in this repo; org B does not. A squash-merge commit that belongs
        to **org B** referencing ``(#5)`` must not be corroborated against org
        A's PR #5 -- known PRs are keyed by (org_id, repo_id) and a commit is
        only matched against PRs in its own org. Only org A's own commit links.
        """
        repo_id = uuid.uuid4()  # shared across both tenants
        base_time = datetime(2024, 6, 15, tzinfo=timezone.utc)
        # Only org A owns PR #5 in this repo.
        pr_rows = [{"org_id": "org-a", "repo_id": repo_id, "number": 5}]
        commit_rows = [
            {
                "org_id": "org-a",
                "repo_id": repo_id,
                "hash": "aaa555",
                "message": "Add caching layer (#5)",
                "author_when": base_time,
            },
            {
                "org_id": "org-b",
                "repo_id": repo_id,
                "hash": "bbb555",
                # org B has no PR #5 -> this squash ref must NOT cross tenants.
                "message": "Unrelated change (#5)",
                "author_when": base_time,
            },
        ]
        fake_sink = self._build_sink(pr_rows, commit_rows)

        # Unscoped build: both orgs' rows are visible, so the per-org keying is
        # what prevents the cross-tenant link (not a SQL org filter).
        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_pr_commit_links()
            builder.close()

        assert count == 1
        records = fake_sink.write_work_graph_pr_commit.call_args[0][0]
        by_commit = {r.commit_hash: r for r in records}
        # Only org A's own commit links to org A's PR #5.
        assert "aaa555" in by_commit
        assert by_commit["aaa555"].pr_number == 5
        # org B's colliding squash ref is never promoted to org A's PR.
        assert "bbb555" not in by_commit

    def test_revert_commit_is_not_linked_to_reverted_pr(self):
        """A revert of a merge commit must NOT produce a PR->commit link.

        CHAOS-2375 round-3: ``Revert "Merge pull request #42 ..."`` quotes the
        reverted PR's merge subject but is a later undo commit, not a commit
        contained by PR #42. Persisting the link would attribute the revert's
        changes back to the original PR and skew downstream metrics. PR #42
        exists in this repo, so only the revert guard prevents the bad link.
        """
        repo_id = uuid.uuid4()
        pr_rows = [{"repo_id": repo_id, "number": 42}]
        commit_rows = [
            {
                "repo_id": repo_id,
                "hash": "rev042",
                "message": (
                    'Revert "Merge pull request #42 from team/x"\n\n'
                    "This reverts commit 0123abcd."
                ),
                "author_when": datetime(2024, 1, 1, tzinfo=timezone.utc),
            },
        ]
        fake_sink = self._build_sink(pr_rows, commit_rows)

        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_pr_commit_links()
            builder.close()

        assert count == 0
        fake_sink.write_work_graph_pr_commit.assert_not_called()

    def test_pr_number_does_not_collide_across_repos(self):
        """PR #1 in repo A and PR #1 in repo B are distinct.

        Guards id-uniqueness: a commit in repo A merging PR #1 must link only to
        repo A's PR #1, never repo B's. The derivation keys known PRs by repo_id,
        so repo B's PR #1 commit (which references no real PR in repo B) is not
        mis-linked. work_graph_pr_commit rows carry repo_id, keeping the
        (org, repo, pr_number, commit_hash) identity unique.
        """
        repo_a = uuid.uuid4()
        repo_b = uuid.uuid4()
        base_time = datetime(2024, 6, 15, tzinfo=timezone.utc)
        # Both repos have a PR #1.
        pr_rows = [
            {"repo_id": repo_a, "number": 1},
            {"repo_id": repo_b, "number": 1},
        ]
        commit_rows = [
            {
                "repo_id": repo_a,
                "hash": "a0001",
                "message": "Merge pull request #1 from team/a-feature",
                "author_when": base_time,
            },
            {
                "repo_id": repo_b,
                "hash": "b0001",
                "message": "Merge pull request #1 from team/b-feature",
                "author_when": base_time,
            },
        ]
        fake_sink = self._build_sink(pr_rows, commit_rows)

        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._derive_pr_commit_links()
            builder.close()

        assert count == 2
        records = fake_sink.write_work_graph_pr_commit.call_args[0][0]
        by_commit = {r.commit_hash: r for r in records}
        # Each commit links to its OWN repo's PR #1, never the other repo's.
        assert by_commit["a0001"].repo_id == repo_a
        assert by_commit["a0001"].pr_number == 1
        assert by_commit["b0001"].repo_id == repo_b
        assert by_commit["b0001"].pr_number == 1
        # The (repo_id, pr_number, commit_hash) identities are distinct.
        identities = {(r.repo_id, r.pr_number, r.commit_hash) for r in records}
        assert len(identities) == 2

    def test_build_invokes_derivation_before_fast_path(self):
        """build() must derive PR->commit links so the fast path is non-empty."""
        repo_id = uuid.uuid4()
        pr_rows = [{"repo_id": repo_id, "number": 3}]
        commit_rows = [
            {
                "repo_id": repo_id,
                "hash": "eee555",
                "message": "Merge pull request #3 from team/ship-it",
                "author_when": datetime(2024, 1, 1, tzinfo=timezone.utc),
            },
        ]
        fake_sink = self._build_sink(pr_rows, commit_rows)
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_issue_pr = MagicMock()

        config = BuildConfig(dsn="clickhouse://localhost:9000/default")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            builder.build()
            builder.close()

        # The seam: build() persisted PR->commit links via the sink.
        fake_sink.write_work_graph_pr_commit.assert_called_once()
        records = fake_sink.write_work_graph_pr_commit.call_args[0][0]
        assert records[0].pr_number == 3
        assert records[0].commit_hash == "eee555"


class TestFastPathTenantIsolation:
    """Tenant-isolation guards for ``_build_pr_commit_edges_from_fast_path``.

    The fast path materializes PR->commit edges by joining
    ``work_graph_pr_commit`` to ``git_commits`` on (repo_id, commit_hash).
    Because ``repo_id``/``commit_hash`` values can collide across tenants
    (documented in metrics/loaders/ai_impact.py), the commit side MUST be
    scoped to the same org as the PR-commit row -- otherwise an org-scoped
    build could pick up another tenant's ``git_commits`` row and stamp a
    cross-tenant edge into ``work_graph_edges``.
    """

    def test_join_predicate_scopes_commits_by_org(self):
        """The fast-path SQL must equate ``c.org_id`` with ``p.org_id``.

        Without this the join only matches on (repo_id, commit_hash) and a
        colliding commit from another org would satisfy the join.
        """
        captured: dict[str, str] = {}

        def mock_query(query, params):
            if "work_graph_pr_commit AS p" in query:
                captured["query"] = query
            return []

        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts.side_effect = mock_query

        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            builder._build_pr_commit_edges_from_fast_path()
            builder.close()

        sql = captured["query"]
        normalized = " ".join(sql.split())
        # Both join sides must be org-scoped together.
        assert "toString(p.org_id) = toString(c.org_id)" in normalized
        assert "work_graph_pr_commit AS p FINAL" in normalized
        assert "git_commits AS c FINAL" in normalized
        # The selected org is still pinned via the WHERE filter.
        assert "p.org_id = 'org-a'" in normalized

    def test_issue_pr_fast_path_uses_final_and_scopes_prs_by_org(self):
        captured: dict[str, str] = {}

        def mock_query(query, params):
            if "work_graph_issue_pr AS p" in query:
                captured["query"] = query
            return []

        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts.side_effect = mock_query

        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            builder._build_issue_pr_edges_from_fast_path()
            builder.close()

        sql = captured["query"]
        normalized = " ".join(sql.split())
        assert "work_graph_issue_pr AS p FINAL" in normalized
        assert "git_pull_requests AS pr FINAL" in normalized
        assert "toString(p.org_id) = toString(pr.org_id)" in normalized
        assert "p.org_id = 'org-a'" in normalized

    def test_cross_tenant_commit_collision_is_excluded(self):
        """Two orgs share repo_id+commit_hash; org A's build must not use B's commit.

        The fake sink simulates the JOIN honoring whatever predicates the
        builder emits: org A has a PR-commit fast-path row but NO matching
        ``git_commits`` row of its own; org B owns the colliding commit. With
        the org-equality predicate present, the join yields zero rows for
        org A, so no edge is created. (Drop ``c.org_id = p.org_id`` from the
        builder and this test fails -- the join would match org B's commit.)
        """
        shared_repo = uuid.uuid4()
        shared_hash = "deadbeef"
        base_time = datetime(2024, 6, 15, tzinfo=timezone.utc)

        # work_graph_pr_commit: org A has a fast-path link row.
        pr_commit_rows = [
            {
                "repo_id": shared_repo,
                "org_id": "org-a",
                "pr_number": 1,
                "commit_hash": shared_hash,
            },
        ]
        # git_commits: ONLY org B owns the colliding commit row.
        git_commits = [
            {
                "repo_id": shared_repo,
                "org_id": "org-b",
                "hash": shared_hash,
                "author_when": base_time,
            },
        ]

        def mock_query(query, params):
            if "work_graph_pr_commit AS p" not in query:
                return []
            normalized = " ".join(query.split())
            # Simulate the INNER JOIN + WHERE org filter the builder emits.
            org_equijoin = "toString(p.org_id) = toString(c.org_id)" in normalized
            results = []
            for p in pr_commit_rows:
                # WHERE p.org_id = '<selected>'
                if f"p.org_id = '{p['org_id']}'" not in normalized:
                    continue
                for c in git_commits:
                    if str(p["repo_id"]) != str(c["repo_id"]):
                        continue
                    if p["commit_hash"] != c["hash"]:
                        continue
                    # The org-equality predicate, when present, excludes the
                    # cross-tenant commit.
                    if org_equijoin and str(p["org_id"]) != str(c["org_id"]):
                        continue
                    results.append(
                        {
                            "repo_id": p["repo_id"],
                            "pr_number": p["pr_number"],
                            "commit_hash": p["commit_hash"],
                            "confidence": 0.9,
                            "provenance": "explicit_text",
                            "evidence": "commit_message_pr_ref",
                            "last_synced": base_time,
                            "author_when": c["author_when"],
                        }
                    )
            return results

        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts.side_effect = mock_query
        fake_sink.write_work_graph_edges = MagicMock()

        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._build_pr_commit_edges_from_fast_path()
            builder.close()

        # org A has no commit of its own -> the colliding org B commit must be
        # excluded -> zero edges, no cross-tenant contamination.
        assert count == 0
        fake_sink.write_work_graph_edges.assert_not_called()

    def test_same_tenant_commit_still_links(self):
        """The org-equality predicate must not break the legitimate same-org join."""
        repo = uuid.uuid4()
        commit_hash = "cafef00d"
        base_time = datetime(2024, 6, 15, tzinfo=timezone.utc)

        pr_commit_rows = [
            {
                "repo_id": repo,
                "org_id": "org-a",
                "pr_number": 7,
                "commit_hash": commit_hash,
            },
        ]
        git_commits = [
            {
                "repo_id": repo,
                "org_id": "org-a",
                "hash": commit_hash,
                "author_when": base_time,
            },
        ]

        def mock_query(query, params):
            if "work_graph_pr_commit AS p" not in query:
                return []
            normalized = " ".join(query.split())
            org_equijoin = "toString(p.org_id) = toString(c.org_id)" in normalized
            results = []
            for p in pr_commit_rows:
                if f"p.org_id = '{p['org_id']}'" not in normalized:
                    continue
                for c in git_commits:
                    if str(p["repo_id"]) != str(c["repo_id"]):
                        continue
                    if p["commit_hash"] != c["hash"]:
                        continue
                    if org_equijoin and str(p["org_id"]) != str(c["org_id"]):
                        continue
                    results.append(
                        {
                            "repo_id": p["repo_id"],
                            "pr_number": p["pr_number"],
                            "commit_hash": p["commit_hash"],
                            "confidence": 0.9,
                            "provenance": "explicit_text",
                            "evidence": "commit_message_pr_ref",
                            "last_synced": base_time,
                            "author_when": c["author_when"],
                        }
                    )
            return results

        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts.side_effect = mock_query
        fake_sink.write_work_graph_edges = MagicMock(return_value=1)

        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._build_pr_commit_edges_from_fast_path()
            builder.close()

        assert count == 1
        fake_sink.write_work_graph_edges.assert_called_once()
        edge_records = fake_sink.write_work_graph_edges.call_args[0][0]
        assert len(edge_records) == 1
        assert edge_records[0].repo_id == repo
        assert edge_records[0].org_id == "org-a"


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


class TestDependencyEdgeConfidence:
    """CHAOS-4752/CHAOS-4758 — associative dependency edges rank BELOW delivery.

    Every ``work_item_dependencies``-derived edge used to be written at 1.0, the
    same tier as a PR's ``implements`` edge. The CHAOS-2775 oversized-component
    split only drops edges strictly below a component's max confidence, so a
    graph of all-1.0 edges left it nothing to drop and it fell through to
    ``_remove_hubs``, which DELETES nodes from every output work unit.
    """

    @staticmethod
    def _build_one(relationship_type: str, *, raw: str | None = None):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.client = MagicMock()
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_projection_runs = MagicMock()
        fake_sink.query_dicts.return_value = [
            {
                "source_work_item_id": "linear:CHAOS-2400",
                "target_work_item_id": "linear:CHAOS-2401",
                "relationship_type": relationship_type,
                "relationship_type_raw": raw or relationship_type,
                "last_synced": datetime(2026, 7, 1, tzinfo=timezone.utc),
            }
        ]
        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            builder._build_issue_issue_edges()
            builder.close()
        fake_sink.write_work_graph_edges.assert_called_once()
        return fake_sink.write_work_graph_edges.call_args[0][0][0]

    @pytest.mark.parametrize(
        "relationship_type",
        ["relates", "is_related_to", "blocks", "is_blocked_by", "duplicates"],
    )
    def test_associative_dependency_edges_rank_below_delivery(self, relationship_type):
        edge = self._build_one(relationship_type)
        # Literals, not the module constants: this exact file must run against
        # origin/main to show the red, and the property the split depends on is
        # "strictly below the delivery tier", not any particular value.
        assert edge.confidence == pytest.approx(0.9)
        assert edge.confidence < 1.0

    @pytest.mark.parametrize("relationship_type", ["parent", "child"])
    def test_hierarchy_dependency_edges_keep_the_delivery_tier(self, relationship_type):
        # A sub-issue hierarchy is structural containment, not an associative
        # link: grouping a parent with its children is the intended behaviour.
        edge = self._build_one(relationship_type)
        assert edge.confidence == pytest.approx(1.0)

    def test_split_drops_associative_edges_instead_of_deleting_nodes(self):
        """End-to-end: builder output -> build_components, no node destroyed.

        A hub issue ``relates``-linked to nine issues, each implemented by its
        own PR: 19 nodes against a cap of 5. With every edge at 1.0 the split
        has nothing to drop and deletes the hub -- and with it the hub issue's
        membership of any work unit. With the associative tier the split drops
        the nine ``relates`` edges instead and every node survives.
        """
        from dev_health_ops.work_graph.investment.components import (
            ComponentBuildStats,
            build_components,
        )

        spokes = [f"linear:CHAOS-{4300 + i}" for i in range(9)]
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.client = MagicMock()
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_projection_runs = MagicMock()
        fake_sink.query_dicts.return_value = [
            {
                "source_work_item_id": "linear:CHAOS-HUB",
                "target_work_item_id": spoke,
                "relationship_type": "relates",
                "relationship_type_raw": "relates",
                "last_synced": datetime(2026, 7, 1, tzinfo=timezone.utc),
            }
            for spoke in spokes
        ]
        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id="org-a")
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            builder._build_issue_issue_edges()
            builder.close()

        written = fake_sink.write_work_graph_edges.call_args[0][0]
        edges: list[dict[str, object]] = [
            {
                "edge_id": edge.edge_id,
                "source_type": "issue",
                "source_id": edge.source_id,
                "target_type": "issue",
                "target_id": edge.target_id,
                "edge_type": edge.edge_type,
                "confidence": edge.confidence,
            }
            for edge in written
        ]
        # One PR implementing each spoke, at the delivery tier -- this is what
        # keeps the component's max confidence at 1.0 in the real graph.
        edges += [
            {
                "edge_id": f"pr-edge-{i}",
                "source_type": "pr",
                "source_id": f"repo#pr{i}",
                "target_type": "issue",
                "target_id": spoke,
                "edge_type": "implements",
                "confidence": 1.0,
            }
            for i, spoke in enumerate(spokes)
        ]

        stats = ComponentBuildStats()
        components = build_components(edges, max_component_nodes=5, stats=stats)

        assert stats.oversized_components == 1
        assert stats.dropped_nodes == 0
        # The split binary-searches the SMALLEST droppable prefix, so the exact
        # count is a property of the graph, not a constant -- what matters is
        # that the edge phase did the work and the node phase never ran.
        assert 0 < stats.dropped_edges <= len(spokes)
        grouped = {node for nodes, _ in components for node in nodes}
        assert ("issue", "linear:CHAOS-HUB") in grouped
        assert len(grouped) == 1 + len(spokes) * 2
        assert max(len(nodes) for nodes, _ in components) <= 5


class TestDependencyConfidenceConstants:
    """The invariant the CHAOS-2775 split relies on, pinned at the source."""

    def test_associative_tier_is_strictly_below_the_delivery_tier(self):
        from dev_health_ops.work_graph.builder import (
            ASSOCIATIVE_DEPENDENCY_CONFIDENCE,
            ASSOCIATIVE_DEPENDENCY_EDGE_TYPES,
            DEPENDENCY_EDGE_DEFAULT_CONFIDENCE,
            dependency_edge_confidence,
        )
        from dev_health_ops.work_graph.models import EdgeType

        assert ASSOCIATIVE_DEPENDENCY_CONFIDENCE < DEPENDENCY_EDGE_DEFAULT_CONFIDENCE
        for edge_type in ASSOCIATIVE_DEPENDENCY_EDGE_TYPES:
            assert dependency_edge_confidence(edge_type) == pytest.approx(
                ASSOCIATIVE_DEPENDENCY_CONFIDENCE
            )
        for edge_type in (EdgeType.PARENT_OF, EdgeType.CHILD_OF, EdgeType.IMPLEMENTS):
            assert dependency_edge_confidence(edge_type) == pytest.approx(
                DEPENDENCY_EDGE_DEFAULT_CONFIDENCE
            )


class TestDependencyConfidenceBackfill:
    """CHAOS-4752 — the stored-row half of the confidence policy."""

    @staticmethod
    def _builder(fake_sink, *, org_id: str):
        config = BuildConfig(dsn="clickhouse://localhost:9000/default", org_id=org_id)
        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            return WorkGraphBuilder(config)

    def test_refuses_without_an_org_scope(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        builder = self._builder(fake_sink, org_id="")
        with pytest.raises(RuntimeError, match="org scope"):
            builder.backfill_dependency_edge_confidence()
        fake_sink.client.command.assert_not_called()

    def test_applies_a_bounded_monotone_mutation(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts.return_value = [
            {"edge_type": "blocks", "rows": 632},
            {"edge_type": "relates", "rows": 2750},
        ]
        builder = self._builder(fake_sink, org_id="org-a")

        stats = builder.backfill_dependency_edge_confidence()

        assert stats["outcome"] == "applied"
        assert stats["rows"] == 3382
        assert stats["rows_by_edge_type"] == {"blocks": 632, "relates": 2750}
        fake_sink.client.command.assert_called_once()
        sql = fake_sink.client.command.call_args[0][0]
        parameters = fake_sink.client.command.call_args.kwargs["parameters"]
        assert "ALTER TABLE work_graph_edges" in sql
        assert "UPDATE confidence = {target:Float32}" in sql
        # Monotone: only rows ABOVE the target are touched, so a second run is
        # a no-op and the value is never raised.
        assert "confidence > {target:Float32}" in sql
        # Org-scoped, and restricted to the rows the builder itself writes.
        assert "org_id = {org_id:String}" in sql
        assert "provenance = 'native'" in sql
        assert "source_type = 'issue'" in sql
        assert "target_type = 'issue'" in sql
        assert "mutations_sync=2" in sql
        assert parameters["org_id"] == "org-a"
        assert parameters["target"] == 0.9
        assert set(parameters["edge_types"]) == {
            "relates",
            "is_related_to",
            "blocks",
            "is_blocked_by",
            "duplicates",
            "is_duplicate_of",
        }

    def test_is_idempotent_when_nothing_is_left_to_lower(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts.return_value = []
        builder = self._builder(fake_sink, org_id="org-a")

        stats = builder.backfill_dependency_edge_confidence()

        assert stats["outcome"] == "noop"
        assert stats["rows"] == 0
        fake_sink.client.command.assert_not_called()

    def test_dry_run_reports_without_mutating(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts.return_value = [{"edge_type": "relates", "rows": 7}]
        builder = self._builder(fake_sink, org_id="org-a")

        stats = builder.backfill_dependency_edge_confidence(dry_run=True)

        assert stats["outcome"] == "dry_run"
        assert stats["rows"] == 7
        fake_sink.client.command.assert_not_called()

    def test_counts_rows_and_runs_in_telemetry(self):
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts.return_value = [{"edge_type": "relates", "rows": 3}]
        builder = self._builder(fake_sink, org_id="org-a")

        with patch(
            "dev_health_ops.work_graph.builder."
            "record_work_graph_dependency_confidence_backfill"
        ) as record:
            builder.backfill_dependency_edge_confidence()

        record.assert_called_once_with(
            rows_by_edge_type={"relates": 3}, outcome="applied"
        )
