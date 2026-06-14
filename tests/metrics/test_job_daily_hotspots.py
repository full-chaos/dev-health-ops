"""CHAOS-2376: file_hotspot_daily live compute + git_blame default backfill.

These seams are proven here without a live ClickHouse / GitHub / GitLab:

(a) ``job_daily`` loads the latest complexity snapshot per file and feeds it
    into ``compute_file_risk_hotspots`` so ``file_hotspot_daily`` is written on
    the live daily path (not only by fixtures).
(b) ``job_daily`` loads per-file ownership concentration from ``git_blame`` and
    threads it into ``blame_concentration`` so the Ownership-risk dimension is
    non-NULL for real orgs (not only fixtures).
(c) the default ``backfill_missing and sync_git`` onboarding backfills in the
    GitHub / GitLab processors pass ``include_blame=True`` so the Ownership-risk
    tab is populated for normal OAuth orgs, AND the per-sync blame crawl is
    bounded (``BLAME_BACKFILL_MAX_FILES``) so a large repo cannot turn
    onboarding into an unbounded GraphQL/REST crawl.

The connectors package is imported first to avoid the pre-existing
providers._base <-> connectors circular import when this file is collected in
isolation.
"""

from __future__ import annotations

import ast
import textwrap
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.metrics import job_daily
from dev_health_ops.metrics.hotspots import compute_file_risk_hotspots
from dev_health_ops.metrics.schemas import CommitStatRow, FileComplexitySnapshot

DAY = date(2026, 5, 20)
NOW = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)

PROCESSORS_DIR = Path(job_daily.__file__).resolve().parent.parent / "processors"


# ---------------------------------------------------------------------------
# (a) complexity-snapshot loader maps ClickHouse rows -> FileComplexitySnapshot
# ---------------------------------------------------------------------------


class _ComplexitySink:
    """Minimal sink returning canned ``file_complexity_snapshots`` rows."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.queries: list[tuple[str, dict[str, Any]]] = []

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        self.queries.append((query, parameters))
        return self._rows


def test_load_complexity_map_for_repo_maps_rows() -> None:
    repo_id = uuid.uuid4()
    sink = _ComplexitySink(
        [
            {
                "file_path": "app/core.py",
                "language": "python",
                "loc": 120,
                "functions_count": 4,
                "cyclomatic_total": 31,
                "cyclomatic_avg": 7.75,
                "high_complexity_functions": 2,
                "very_high_complexity_functions": 1,
            },
            # A blank path is dropped (defensive against malformed rows).
            {"file_path": "", "cyclomatic_total": 99, "cyclomatic_avg": 9.0},
        ]
    )

    result = job_daily._load_complexity_map_for_repo(
        primary_sink=sink,
        org_id="acme",
        repo_id=repo_id,
        day=DAY,
    )

    assert set(result) == {"app/core.py"}
    snap = result["app/core.py"]
    assert isinstance(snap, FileComplexitySnapshot)
    assert snap.cyclomatic_total == 31
    assert snap.cyclomatic_avg == 7.75
    assert snap.org_id == "acme"
    # Scoped to repo/day/org with a latest-compute argMax read.
    query, params = sink.queries[0]
    assert params["repo_id"] == str(repo_id)
    assert params["day"] == DAY
    assert params["org_id"] == "acme"
    assert "argMax" in query
    assert "file_complexity_snapshots" in query
    # Latest-by-day selection: the argMax temporal key MUST lead with as_of_day
    # so a later-recomputed OLDER snapshot cannot clobber a newer one
    # (CHAOS-2376 round-2). Plain `argMax(field, computed_at)` is the bug.
    assert "(as_of_day, computed_at)" in query
    assert "argMax(cyclomatic_total,               computed_at)" not in query


def test_load_complexity_map_selects_latest_by_as_of_day() -> None:
    """The complexity loader must order snapshots by (as_of_day, computed_at),
    not computed_at alone.

    Regression for CHAOS-2376 round-2: a backfill that recomputes an OLDER
    ``as_of_day`` *after* a newer snapshot was written would, under a
    ``argMax(field, computed_at)`` read, surface the stale older values and
    persist bad risk_score/cyclomatic into ``file_hotspot_daily``. This asserts
    the query argMaxes on the ``(as_of_day, computed_at)`` tuple so the newest
    snapshot day always wins regardless of recompute order.
    """
    captured: dict[str, str] = {}

    class _CaptureSink:
        def query_dicts(
            self, query: str, parameters: dict[str, Any]
        ) -> list[dict[str, Any]]:
            captured["query"] = query
            return []

    job_daily._load_complexity_map_for_repo(
        primary_sink=_CaptureSink(),
        org_id="acme",
        repo_id=uuid.uuid4(),
        day=DAY,
    )

    query = captured["query"]
    # Every projected field must argMax on the (as_of_day, computed_at) tuple.
    for field in (
        "language",
        "loc",
        "functions_count",
        "cyclomatic_total",
        "cyclomatic_avg",
        "high_complexity_functions",
        "very_high_complexity_functions",
    ):
        assert f"AS {field}" in query
    assert query.count("(as_of_day, computed_at)") == 7
    # The buggy single-key form must be gone for every field.
    assert "computed_at) AS" not in query.replace("(as_of_day, computed_at)", "")


def test_load_complexity_map_swallows_query_failure() -> None:
    class _Boom:
        def query_dicts(self, *_: Any, **__: Any) -> list[dict[str, Any]]:
            raise RuntimeError("table missing")

    # A missing/unmigrated table must not abort the daily job.
    result = job_daily._load_complexity_map_for_repo(
        primary_sink=_Boom(),
        org_id="acme",
        repo_id=uuid.uuid4(),
        day=DAY,
    )
    assert result == {}


def test_risk_hotspot_seam_merges_complexity_and_churn() -> None:
    """The loaded complexity map + churn window produce ranked hotspot rows."""
    repo_id = uuid.uuid4()
    complexity_map = job_daily._load_complexity_map_for_repo(
        primary_sink=_ComplexitySink(
            [
                {
                    "file_path": "hot.py",
                    "language": "python",
                    "loc": 300,
                    "functions_count": 10,
                    "cyclomatic_total": 80,
                    "cyclomatic_avg": 8.0,
                    "high_complexity_functions": 5,
                    "very_high_complexity_functions": 2,
                },
                {
                    "file_path": "calm.py",
                    "language": "python",
                    "loc": 40,
                    "functions_count": 2,
                    "cyclomatic_total": 3,
                    "cyclomatic_avg": 1.5,
                    "high_complexity_functions": 0,
                    "very_high_complexity_functions": 0,
                },
            ]
        ),
        org_id="acme",
        repo_id=repo_id,
        day=DAY,
    )

    window_stats: list[CommitStatRow] = [
        {
            "repo_id": repo_id,
            "commit_hash": "c1",
            "file_path": "hot.py",
            "additions": 200,
            "deletions": 100,
            "author_email": "a@ex.com",
            "author_name": "A",
            "committer_when": NOW,
            "old_file_mode": "100644",
            "new_file_mode": "100644",
        },
        {
            "repo_id": repo_id,
            "commit_hash": "c2",
            "file_path": "calm.py",
            "additions": 1,
            "deletions": 0,
            "author_email": "b@ex.com",
            "author_name": "B",
            "committer_when": NOW,
            "old_file_mode": "100644",
            "new_file_mode": "100644",
        },
    ]

    rows = compute_file_risk_hotspots(
        repo_id=repo_id,
        day=DAY,
        window_stats=window_stats,
        complexity_map=complexity_map,
        computed_at=NOW,
    )

    assert {r.file_path for r in rows} == {"hot.py", "calm.py"}
    # High churn + high complexity ranks first.
    assert rows[0].file_path == "hot.py"
    assert rows[0].cyclomatic_total == 80
    assert rows[0].risk_score > rows[1].risk_score


# ---------------------------------------------------------------------------
# (b) blame-map loader -> blame_concentration is non-NULL on the live path
# ---------------------------------------------------------------------------


class _BlameSink:
    """Minimal sink returning canned per-file concentration rows."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.queries: list[tuple[str, dict[str, Any]]] = []

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        self.queries.append((query, parameters))
        return self._rows


def test_load_blame_map_for_repo_maps_concentration() -> None:
    repo_id = uuid.uuid4()
    sink = _BlameSink(
        [
            {"path": "app/core.py", "concentration": 0.9},
            {"path": "app/util.py", "concentration": 0.5},
            # Defensive: blank path and NULL concentration are dropped.
            {"path": "", "concentration": 1.0},
            {"path": "app/empty.py", "concentration": None},
        ]
    )

    result = job_daily._load_blame_map_for_repo(
        primary_sink=sink, org_id="acme", repo_id=repo_id
    )

    assert result == {"app/core.py": 0.9, "app/util.py": 0.5}
    # Scoped to BOTH repo and org with a per-line argMax dedup before the share.
    query, params = sink.queries[0]
    assert params["repo_id"] == str(repo_id)
    assert params["org_id"] == "acme"
    assert "git_blame" in query
    assert "argMax" in query
    assert "org_id = {org_id:String}" in query


def test_load_blame_map_swallows_query_failure() -> None:
    class _Boom:
        def query_dicts(self, *_: Any, **__: Any) -> list[dict[str, Any]]:
            raise RuntimeError("table missing")

    # A missing/unmigrated git_blame table must not abort the daily job.
    result = job_daily._load_blame_map_for_repo(
        primary_sink=_Boom(), org_id="acme", repo_id=uuid.uuid4()
    )
    assert result == {}


def test_load_blame_map_is_org_scoped_no_cross_tenant_leak() -> None:
    """Blame reads MUST filter by org_id so a reused repo_id under another
    tenant (or the 'default' partition) cannot contaminate this org's
    Ownership-risk data (CHAOS-2376 round-2 / Codex no-ship).

    The sink here returns ONLY the rows whose ``org_id`` param matches, proving
    the loader passes a discriminating org filter and that a stale row written
    under a different org is never surfaced for ``org_id='tenant-a'``.
    """
    repo_id = uuid.uuid4()

    # Two org partitions for the SAME repo_id with different ownership shapes.
    rows_by_org = {
        "tenant-a": [{"path": "shared.py", "concentration": 0.2}],
        "tenant-b": [{"path": "shared.py", "concentration": 0.95}],
    }

    class _OrgScopedSink:
        def __init__(self) -> None:
            self.queries: list[tuple[str, dict[str, Any]]] = []

        def query_dicts(
            self, query: str, parameters: dict[str, Any]
        ) -> list[dict[str, Any]]:
            self.queries.append((query, parameters))
            # Emulate ClickHouse honouring the WHERE org_id filter.
            assert "org_id = {org_id:String}" in query
            return rows_by_org.get(parameters.get("org_id", ""), [])

    sink = _OrgScopedSink()

    result_a = job_daily._load_blame_map_for_repo(
        primary_sink=sink, org_id="tenant-a", repo_id=repo_id
    )
    result_b = job_daily._load_blame_map_for_repo(
        primary_sink=sink, org_id="tenant-b", repo_id=repo_id
    )

    # Each tenant sees only its own ownership concentration, never the other's.
    assert result_a == {"shared.py": 0.2}
    assert result_b == {"shared.py": 0.95}
    assert sink.queries[0][1]["org_id"] == "tenant-a"
    assert sink.queries[1][1]["org_id"] == "tenant-b"


def test_blame_concentration_flows_into_hotspot_rows() -> None:
    """The loaded blame map populates blame_concentration (non-NULL live)."""
    repo_id = uuid.uuid4()
    blame_map = job_daily._load_blame_map_for_repo(
        primary_sink=_BlameSink([{"path": "hot.py", "concentration": 0.85}]),
        org_id="acme",
        repo_id=repo_id,
    )

    window_stats: list[CommitStatRow] = [
        {
            "repo_id": repo_id,
            "commit_hash": "c1",
            "file_path": "hot.py",
            "additions": 50,
            "deletions": 10,
            "author_email": "a@ex.com",
            "author_name": "A",
            "committer_when": NOW,
            "old_file_mode": "100644",
            "new_file_mode": "100644",
        },
    ]

    rows = compute_file_risk_hotspots(
        repo_id=repo_id,
        day=DAY,
        window_stats=window_stats,
        complexity_map={},
        blame_map=blame_map,
        computed_at=NOW,
    )

    by_path = {r.file_path: r for r in rows}
    # Without the blame_map wiring this would be None (the bug the fix closes).
    assert by_path["hot.py"].blame_concentration == 0.85


# ---------------------------------------------------------------------------
# (c) default onboarding backfills pass include_blame=True
# ---------------------------------------------------------------------------


def _backfill_include_blame_values(source_path: Path, func_name: str) -> list[bool]:
    """Return the ``include_blame`` literal passed at each call to *func_name*.

    Only inspects keyword args that are constant booleans; a call that omits
    ``include_blame`` contributes ``True`` (the function default).
    """
    tree = ast.parse(source_path.read_text())
    values: list[bool] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        if not (isinstance(target, ast.Name) and target.id == func_name):
            continue
        kw = next(
            (k for k in node.keywords if k.arg == "include_blame"),
            None,
        )
        if kw is None:
            values.append(True)
            continue
        if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, bool):
            values.append(kw.value.value)
    return values


def test_github_backfill_calls_request_blame() -> None:
    values = _backfill_include_blame_values(
        PROCESSORS_DIR / "github.py", "_backfill_github_missing_data"
    )
    assert values, "expected at least one _backfill_github_missing_data call"
    assert all(values), (
        "GitHub onboarding backfill must request blame so the Ownership-risk "
        f"tab is populated (CHAOS-2376); saw include_blame values: {values}"
    )


def test_gitlab_backfill_calls_request_blame() -> None:
    values = _backfill_include_blame_values(
        PROCESSORS_DIR / "gitlab.py", "_backfill_gitlab_missing_data"
    )
    assert values, "expected at least one _backfill_gitlab_missing_data call"
    assert all(values), (
        "GitLab onboarding backfill must request blame so the Ownership-risk "
        f"tab is populated (CHAOS-2376); saw include_blame values: {values}"
    )


def test_backfill_default_include_blame_is_true() -> None:
    """The processor helper signatures default include_blame to True."""
    for path, func in (
        (PROCESSORS_DIR / "github.py", "_backfill_github_missing_data"),
        (PROCESSORS_DIR / "gitlab.py", "_backfill_gitlab_missing_data"),
    ):
        tree = ast.parse(textwrap.dedent(path.read_text()))
        fn = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.AsyncFunctionDef) and node.name == func
        )
        default_by_arg = dict(
            zip(
                [a.arg for a in fn.args.args][-len(fn.args.defaults) :],
                fn.args.defaults,
            )
        )
        include_blame_default = default_by_arg["include_blame"]
        assert isinstance(include_blame_default, ast.Constant)
        assert include_blame_default.value is True


# ---------------------------------------------------------------------------
# (b2) onboarding blame gate is org-scoped (Codex no-ship: stale-org gate)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_has_any_git_blame_is_org_scoped() -> None:
    """``has_any_git_blame`` must scope by ``self.org_id``.

    ``git_blame`` is org-partitioned (migration 027) and ``repo_id`` can be
    reused across tenants. A repo-only existence check would let a stale/default
    org's blame row suppress the fresh blame fetch for a newly-onboarded org,
    leaving its Ownership-risk tab empty (CHAOS-2376 round-2 / Codex no-ship).
    This proves the gate threads org_id into the WHERE clause and that, when a
    row exists only under a different org, the gate reports "no blame" so the
    backfill proceeds.
    """
    from dev_health_ops.storage.clickhouse import ClickHouseStore

    repo_id = uuid.uuid4()

    # Backing store: a blame row exists ONLY under tenant-b.
    existing = {("tenant-b", str(repo_id))}

    captured: list[dict[str, Any]] = []

    def fake_query(query: str, parameters: dict[str, Any]) -> Any:
        captured.append({"query": query, "params": parameters})
        assert "org_id = {org_id:String}" in query
        key = (parameters.get("org_id"), parameters.get("repo_id"))
        result = Mock()
        result.result_rows = [(1,)] if key in existing else []
        return result

    store = ClickHouseStore.__new__(ClickHouseStore)
    store.client = Mock()
    store.client.query = fake_query
    import asyncio as _asyncio

    store._lock = _asyncio.Lock()

    # tenant-a has NO blame even though tenant-b does for the same repo_id ->
    # the gate must report False so onboarding fetches fresh blame.
    store.org_id = "tenant-a"
    assert await store.has_any_git_blame(repo_id) is False

    # tenant-b's own row is visible.
    store.org_id = "tenant-b"
    assert await store.has_any_git_blame(repo_id) is True

    # Every query carried the discriminating org filter.
    assert all("org_id = {org_id:String}" in c["query"] for c in captured)
    assert {c["params"]["org_id"] for c in captured} == {"tenant-a", "tenant-b"}


# ---------------------------------------------------------------------------
# (c) the per-sync blame crawl is BOUNDED (Codex no-ship: unbounded blame)
# ---------------------------------------------------------------------------


class _FakeTreeEntry:
    def __init__(self, path: str, size: int = 100, type_: str = "blob") -> None:
        self.path = path
        self.size = size
        self.type = type_


@pytest.mark.asyncio
async def test_github_blame_backfill_is_capped() -> None:
    """Onboarding blame must stop at BLAME_BACKFILL_MAX_FILES files.

    Drives the real ``_backfill_github_missing_data`` blame branch with a tree
    larger than the cap and asserts ``get_file_blame`` is called at most
    ``BLAME_BACKFILL_MAX_FILES`` times -- proving a large repo cannot turn
    onboarding into an unbounded GraphQL crawl (CHAOS-2376 / Codex no-ship).
    """
    from dev_health_ops.connectors.models import BlameRange, FileBlame
    from dev_health_ops.processors.github import (
        BLAME_BACKFILL_MAX_FILES,
        _backfill_github_missing_data,
    )

    n_files = BLAME_BACKFILL_MAX_FILES + 17

    store = Mock()
    store.has_any_git_files = AsyncMock(return_value=True)
    store.has_any_git_file_contents = AsyncMock(return_value=True)
    store.has_any_git_commit_stats = AsyncMock(return_value=True)
    store.has_any_git_blame = AsyncMock(return_value=False)

    inserted: list[Any] = []

    async def insert_blame(batch: list[Any]) -> None:
        inserted.extend(batch)

    sink = Mock()
    sink.insert_blame_data = AsyncMock(side_effect=insert_blame)

    gh_repo = Mock()
    gh_repo.get_branch.return_value = Mock(commit=Mock(sha="abc"))
    gh_repo.get_git_tree.return_value = Mock(
        tree=[_FakeTreeEntry(f"src/f{i}.py") for i in range(n_files)]
    )

    blame_calls: list[str] = []

    def fake_blame(*, owner: str, repo: str, path: str, ref: str) -> FileBlame:
        blame_calls.append(path)
        return FileBlame(
            file_path=path,
            ranges=[
                BlameRange(
                    starting_line=1,
                    ending_line=1,
                    commit_sha="sha",
                    author="A",
                    author_email="a@ex.com",
                    age_seconds=0,
                )
            ],
        )

    connector = Mock()
    connector.github.get_repo.return_value = gh_repo
    connector.get_file_blame = Mock(side_effect=fake_blame)

    db_repo = Mock()
    db_repo.id = uuid.uuid4()

    await _backfill_github_missing_data(
        store=store,
        ingestion_sink=sink,
        connector=connector,
        db_repo=db_repo,
        repo_full_name="octo/repo",
        default_branch="main",
        max_commits=None,
        include_blame=True,
        include_commit_stats=False,
    )

    assert len(blame_calls) == BLAME_BACKFILL_MAX_FILES
    # Only the capped subset is persisted (no blame for files past the cap).
    assert {row.path for row in inserted} == set(blame_calls)
    assert len(blame_calls) < n_files


@pytest.mark.asyncio
async def test_gitlab_blame_backfill_is_capped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Onboarding blame must stop at BLAME_BACKFILL_MAX_FILES files (GitLab)."""
    import dev_health_ops.processors.gitlab as gitlab_mod
    from dev_health_ops.processors.gitlab import (
        BLAME_BACKFILL_MAX_FILES,
        _backfill_gitlab_missing_data,
    )

    n_files = BLAME_BACKFILL_MAX_FILES + 9

    store = Mock()
    store.has_any_git_files = AsyncMock(return_value=True)
    store.has_any_git_file_contents = AsyncMock(return_value=True)
    store.has_any_git_commit_stats = AsyncMock(return_value=True)
    store.has_any_git_blame = AsyncMock(return_value=False)

    inserted: list[Any] = []

    async def insert_blame(batch: list[Any]) -> None:
        inserted.extend(batch)

    sink = Mock()
    sink.insert_blame_data = AsyncMock(side_effect=insert_blame)

    project = Mock()
    project.id = 123

    connector = Mock()
    connector.gitlab.projects.get.return_value = project

    # _iter_gitlab_repo_tree yields the (oversized) tree of blobs.
    tree_items = [{"type": "blob", "path": f"src/f{i}.py"} for i in range(n_files)]

    blame_calls: list[str] = []

    def fake_rest_blame(project_id: int, path: str, ref: str) -> list[dict[str, Any]]:
        blame_calls.append(path)
        return [{"commit": {"author_email": "a@ex.com"}, "lines": ["x = 1"]}]

    connector.rest_client.get_file_blame = Mock(side_effect=fake_rest_blame)

    db_repo = Mock()
    db_repo.id = uuid.uuid4()
    db_repo.settings = {"default_branch": "main"}

    # Patch the module-level tree iterator to return the oversized blob list.
    monkeypatch.setattr(
        gitlab_mod, "_iter_gitlab_repo_tree", lambda *a, **k: tree_items
    )
    await _backfill_gitlab_missing_data(
        store=store,
        ingestion_sink=sink,
        connector=connector,
        db_repo=db_repo,
        project_full_name="grp/proj",
        default_branch="main",
        max_commits=None,
        include_blame=True,
        include_commit_stats=False,
    )

    assert len(blame_calls) == BLAME_BACKFILL_MAX_FILES
    assert {row.path for row in inserted} == set(blame_calls)
    assert len(blame_calls) < n_files
