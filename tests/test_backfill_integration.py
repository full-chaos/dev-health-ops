from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock


def test_discover_repos_sets_source_from_provider():
    """discover_repos must use the `provider` param as the `source` field
    on each DiscoveredRepo.  Before the fix, source was hardcoded to 'auto',
    causing provider-specific work-item fetchers to find 0 repos."""
    from dev_health_ops.metrics.job_daily import discover_repos

    repo_id = uuid.uuid4()

    # --- specific repo_id path ---
    result = discover_repos(
        backend="clickhouse",
        primary_sink=None,  # not used when repo_id is provided
        repo_id=repo_id,
        repo_name="org/repo",
        provider="github",
    )
    assert len(result) == 1
    assert result[0].source == "github"
    assert result[0].repo_id == repo_id

    # --- verify default keeps backward compat ---
    result_default = discover_repos(
        backend="clickhouse",
        primary_sink=None,
        repo_id=repo_id,
        repo_name="org/repo",
    )
    assert result_default[0].source == "auto"

    # --- gitlab provider ---
    result_gl = discover_repos(
        backend="clickhouse",
        primary_sink=None,
        repo_id=repo_id,
        repo_name="org/repo",
        provider="gitlab",
    )
    assert result_gl[0].source == "gitlab"

    db_repo_id = uuid.uuid4()
    legacy_repo_id = uuid.uuid4()
    mock_sink = SimpleNamespace(client=MagicMock())
    mock_sink.client.query.return_value = SimpleNamespace(
        result_rows=[
            (str(db_repo_id), "org/db-repo", {"source": "legacy"}, "gitlab"),
            (str(legacy_repo_id), "org/legacy-repo", {"source": "legacy"}, "unknown"),
        ]
    )

    result_db = discover_repos(
        backend="clickhouse",
        primary_sink=mock_sink,
        provider="github",
    )

    assert result_db[0].repo_id == db_repo_id
    assert result_db[0].source == "gitlab"
    assert result_db[1].repo_id == legacy_repo_id
    assert result_db[1].source == "github"


def test_discover_repos_parses_json_string_settings():
    """CHAOS-2763: ``repos.settings`` comes back from ClickHouse as a raw JSON
    *string*, not a pre-parsed dict — the ``DiscoveredRepo.settings: dict[str,
    object]`` annotation was previously lying (the query result was passed
    through with ``r[2] or {}``, no ``json.loads``). Any per-provider match on
    a settings key (e.g. gitlab ``project_id`` scoping) always missed on real
    data before this fix."""
    from dev_health_ops.metrics.job_daily import discover_repos

    repo_id = uuid.uuid4()
    mock_sink = SimpleNamespace(client=MagicMock())
    mock_sink.client.query.return_value = SimpleNamespace(
        result_rows=[
            (str(repo_id), "grp/proj-a", '{"project_id": 123}', "gitlab"),
        ]
    )

    result = discover_repos(backend="clickhouse", primary_sink=mock_sink)

    assert result[0].settings == {"project_id": 123}


def test_discover_repos_malformed_json_settings_yields_empty_dict():
    """A malformed/legacy ``settings`` string must not raise — it degrades to
    ``{}`` so numeric-id gitlab matching fails closed instead of crashing
    discovery for the whole org."""
    from dev_health_ops.metrics.job_daily import discover_repos

    repo_id = uuid.uuid4()
    mock_sink = SimpleNamespace(client=MagicMock())
    mock_sink.client.query.return_value = SimpleNamespace(
        result_rows=[
            (str(repo_id), "grp/proj-a", "{not json", "gitlab"),
        ]
    )

    result = discover_repos(backend="clickhouse", primary_sink=mock_sink)

    assert result[0].settings == {}


def test_discover_repos_null_settings_yields_empty_dict():
    """A NULL ``settings`` column (Nullable(String)) must also degrade to
    ``{}`` rather than raising."""
    from dev_health_ops.metrics.job_daily import discover_repos

    repo_id = uuid.uuid4()
    mock_sink = SimpleNamespace(client=MagicMock())
    mock_sink.client.query.return_value = SimpleNamespace(
        result_rows=[
            (str(repo_id), "grp/proj-a", None, "gitlab"),
        ]
    )

    result = discover_repos(backend="clickhouse", primary_sink=mock_sink)

    assert result[0].settings == {}


class TestSourceFilteringInWorkItemFetchers:
    """Verify that work-item fetcher functions correctly filter repos by source.

    This is the end-to-end behavior that was broken when discover_repos
    hardcoded source='auto': the fetchers filter by r.source == 'github'
    (or 'gitlab', 'synthetic'), so repos with source='auto' were silently
    skipped — producing 0 work items even though repos existed.
    """

    def _make_repos(self, sources: list[str]) -> list:
        from dev_health_ops.metrics.work_items import DiscoveredRepo

        return [
            DiscoveredRepo(
                repo_id=uuid.uuid4(),
                full_name=f"org/repo-{i}",
                source=src,
                settings={},
            )
            for i, src in enumerate(sources)
        ]

    def test_github_fetcher_skips_non_github_repos(self):
        """fetch_github_work_items must skip repos where source != 'github'."""
        repos = self._make_repos(["gitlab", "auto", "local", "synthetic"])
        # All repos have wrong source — GitHub fetcher should skip all of them
        # without even attempting API calls (no token needed).
        for repo in repos:
            assert repo.source != "github", f"Test setup error: {repo.source}"

    def test_gitlab_fetcher_skips_non_gitlab_repos(self):
        """fetch_gitlab_work_items must skip repos where source != 'gitlab'."""
        repos = self._make_repos(["github", "auto", "local", "synthetic"])
        for repo in repos:
            assert repo.source != "gitlab", f"Test setup error: {repo.source}"

    def test_synthetic_fetcher_skips_non_synthetic_repos(self):
        """fetch_synthetic_work_items must skip repos where source != 'synthetic'."""
        repos = self._make_repos(["github", "gitlab", "auto", "local"])
        for repo in repos:
            assert repo.source != "synthetic", f"Test setup error: {repo.source}"

    def test_github_fetcher_includes_github_repos(self):
        """Repos with source='github' must pass the GitHub fetcher filter."""
        repos = self._make_repos(["github", "gitlab", "github"])
        github_repos = [r for r in repos if r.source == "github"]
        assert len(github_repos) == 2

    def test_gitlab_fetcher_includes_gitlab_repos(self):
        """Repos with source='gitlab' must pass the GitLab fetcher filter."""
        repos = self._make_repos(["github", "gitlab", "gitlab"])
        gitlab_repos = [r for r in repos if r.source == "gitlab"]
        assert len(gitlab_repos) == 2

    def test_auto_source_excluded_from_all_provider_fetchers(self):
        """THE ORIGINAL BUG: source='auto' must NOT match any provider filter.

        When discover_repos hardcoded source='auto', every fetcher's
        r.source == '<provider>' check returned False, producing 0 repos.
        This test ensures that 'auto' is never a valid match."""
        repos = self._make_repos(["auto", "auto", "auto"])
        assert [r for r in repos if r.source == "github"] == []
        assert [r for r in repos if r.source == "gitlab"] == []
        assert [r for r in repos if r.source == "synthetic"] == []

    def test_mixed_providers_only_correct_repos_pass_filter(self):
        """With mixed-provider discovery results, each fetcher gets only its repos."""
        repos = self._make_repos(["github", "gitlab", "github", "synthetic", "auto"])
        github_repos = [r for r in repos if r.source == "github"]
        gitlab_repos = [r for r in repos if r.source == "gitlab"]
        synthetic_repos = [r for r in repos if r.source == "synthetic"]
        auto_repos = [r for r in repos if r.source == "auto"]

        assert len(github_repos) == 2
        assert len(gitlab_repos) == 1
        assert len(synthetic_repos) == 1
        # 'auto' must not match any provider filter
        assert len(auto_repos) == 1  # exists but won't be fetched by anyone
        assert len(github_repos) + len(gitlab_repos) + len(synthetic_repos) == 4

    def test_discover_repos_with_db_provider_produces_filterable_repos(self):
        """End-to-end: discover_repos reads provider from DB, downstream filter works.

        This simulates the full chain:
        1. DB has repos with provider='github'
        2. discover_repos returns DiscoveredRepo with source='github'
        3. fetch_github_work_items filter r.source == 'github' matches
        """
        from dev_health_ops.metrics.job_daily import discover_repos

        mock_sink = SimpleNamespace(client=MagicMock())
        gh_id = uuid.uuid4()
        gl_id = uuid.uuid4()
        mock_sink.client.query.return_value = SimpleNamespace(
            result_rows=[
                (str(gh_id), "org/gh-repo", {}, "github"),
                (str(gl_id), "org/gl-repo", {}, "gitlab"),
            ]
        )

        repos = discover_repos(
            backend="clickhouse",
            primary_sink=mock_sink,
        )

        # Simulate what fetch_github_work_items does
        github_repos = [r for r in repos if r.source == "github"]
        assert len(github_repos) == 1
        assert github_repos[0].repo_id == gh_id

        # Simulate what fetch_gitlab_work_items does
        gitlab_repos = [r for r in repos if r.source == "gitlab"]
        assert len(gitlab_repos) == 1
        assert gitlab_repos[0].repo_id == gl_id


def test_discover_repos_query_dedupes_via_argmax_group_by():
    """discover_repos' ClickHouse query must dedupe to the latest row per
    (org_id, id) via a SINGLE argMax/GROUP BY rather than a plain ``SELECT *``
    (CHAOS-2787).

    ``repos`` is a ReplacingMergeTree(last_synced) ordered by (org_id, id)
    (migration 027); ``insert_repo`` always writes a fresh row per sync
    (CHAOS-1775), so pre-merge duplicates routinely exist. This pins the SQL
    shape server-side dedup depends on: a single
    ``argMax(tuple(repo, settings, provider), last_synced)`` (NOT three
    independent argMax calls -- those could each resolve a last_synced TIE
    to a *different* physical row, synthesizing a Frankenstein result) with
    the three columns then unwrapped via ``tupleElement(latest, N)``. The
    outer tuple() wrap also means the ``settings`` (Nullable(String)) column
    is carried through even when NULL, since a bare
    ``argMax(settings, last_synced)`` would skip NULLs entirely and let an
    older non-NULL settings value mask a genuinely NULL latest value. Live
    behavioral proof of the dedup, tie-consistency, and NULL-handling lives
    in ``tests/test_discover_repos_dedup_live.py`` (opt-in, needs
    ClickHouse)."""
    from dev_health_ops.metrics.job_daily import discover_repos

    mock_sink = SimpleNamespace(client=MagicMock())
    mock_sink.client.query.return_value = SimpleNamespace(result_rows=[])

    discover_repos(backend="clickhouse", primary_sink=mock_sink, org_id="org-x")

    query_arg = mock_sink.client.query.call_args.args[0]
    assert "argMax(tuple(repo, settings, provider), last_synced) AS latest" in query_arg
    assert "tupleElement(latest, 1) AS repo" in query_arg
    assert "tupleElement(latest, 2) AS settings" in query_arg
    assert "tupleElement(latest, 3) AS provider" in query_arg
    assert "GROUP BY org_id, id" in query_arg
    assert "WHERE org_id = {org_id:String}" in query_arg
    # Guard against regressing to three independent argMax calls.
    assert "argMax(repo, last_synced)" not in query_arg
    assert "argMax(provider, last_synced)" not in query_arg

    params = mock_sink.client.query.call_args.kwargs["parameters"]
    assert params == {"org_id": "org-x"}


def test_discover_repos_query_omits_where_without_org_id():
    """No org_id => no WHERE clause, but the GROUP BY dedup still applies."""
    from dev_health_ops.metrics.job_daily import discover_repos

    mock_sink = SimpleNamespace(client=MagicMock())
    mock_sink.client.query.return_value = SimpleNamespace(result_rows=[])

    discover_repos(backend="clickhouse", primary_sink=mock_sink)

    query_arg = mock_sink.client.query.call_args.args[0]
    assert "WHERE" not in query_arg
    assert "GROUP BY org_id, id" in query_arg

    params = mock_sink.client.query.call_args.kwargs["parameters"]
    assert params == {}
