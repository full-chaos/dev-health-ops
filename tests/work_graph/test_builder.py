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
                # Squash form is ambiguous with an issue ref -> must be ignored,
                # even though PR #7 exists in this repo.
                "message": "Add retry logic (#7)",
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

    def test_squash_paren_ref_colliding_with_pr_number_is_not_linked(self):
        """A parenthetical ``(#N)`` issue ref must NOT be linked to PR #N.

        Primary CHAOS-2375 round-2 corruption case: "Fix parser edge case (#42)"
        is a hand-authored issue reference, but the squash convention produces the
        identical "<subject> (#42)" shape. With PR #42 present in the same repo,
        the old ``SQUASH_PR_PATTERN`` would have attached this unrelated commit to
        PR #42. The fix drops the squash form, so no link is written.
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

        assert count == 0
        fake_sink.write_work_graph_pr_commit.assert_not_called()

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
        # The selected org is still pinned via the WHERE filter.
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
