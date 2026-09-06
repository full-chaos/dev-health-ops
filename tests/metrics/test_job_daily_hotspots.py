"""CHAOS-2376: git_blame default backfill for the Ownership-risk dimension.

This seam is proven here without a live ClickHouse / GitHub / GitLab: the
default ``backfill_missing and sync_git`` onboarding backfills in the
GitHub / GitLab processors pass ``include_blame=True`` so the Ownership-risk
tab is populated for normal OAuth orgs, AND the per-sync blame crawl is
bounded (``BLAME_BACKFILL_MAX_FILES``) so a large repo cannot turn
onboarding into an unbounded GraphQL/REST crawl.

CHAOS-2376's other two seams -- ``job_daily`` loading the latest complexity
snapshot per file into the risk-hotspot compute, and threading per-file
``git_blame`` ownership concentration into ``blame_concentration`` on
hotspot rows -- used to be proven here too. CHAOS-5234/CHAOS-3092 deleted
both `job_daily.py` call sites (`_load_complexity_map_for_repo`,
`_load_blame_map_for_repo`, `compute_file_hotspots`,
`compute_file_risk_hotspots` are all gone; the native Go executors
FileHotspotsExecutor/FileRiskHotspotsExecutor own the family now), and
their dedicated unit tests were removed in the same PR. The blame-BACKFILL
tests below (loading blame data INTO ``git_blame`` in the first place) are
unrelated to that deletion -- they test the GitHub/GitLab sync processors,
not job_daily.py's now-deleted readers of the already-backfilled data.

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

# Imported for its registration side effects, and *before* dev_health_ops.metrics
# (which transitively imports providers._base) to dodge the pre-existing
# providers._base <-> connectors circular import when this file is collected in
# isolation. The `_CONNECTORS_SIDE_EFFECT` reference below marks it as used so
# ruff/CodeQL don't flag the side-effect-only import.
import dev_health_ops.connectors
from dev_health_ops.metrics import job_daily

# Mark the side-effect-only connectors import (above) as used so ruff/CodeQL
# don't flag it; it is imported purely to pre-empt the circular import.
_CONNECTORS_SIDE_EFFECT = dev_health_ops.connectors

DAY = date(2026, 5, 20)
NOW = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)

PROCESSORS_DIR = Path(job_daily.__file__).resolve().parent.parent / "processors"


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


@pytest.mark.asyncio
async def test_get_blamed_paths_is_org_scoped() -> None:
    """``get_blamed_paths`` must scope by ``self.org_id`` (CHAOS-2376 round-3).

    The coverage-aware backfill diffs the live tree against already-blamed
    paths; a cross-tenant leak here would either falsely mark paths covered
    (silent loss for the new org) or reblame paths the org already has.
    """
    from dev_health_ops.storage.clickhouse import ClickHouseStore

    repo_id = uuid.uuid4()
    captured: list[dict[str, Any]] = []

    def fake_query(query: str, parameters: dict[str, Any]) -> Any:
        captured.append({"query": query, "params": parameters})
        assert "org_id = {org_id:String}" in query
        result = Mock()
        if parameters.get("org_id") == "tenant-a":
            result.result_rows = [("src/a.py",), ("src/b.py",)]
        else:
            result.result_rows = [("other/c.py",)]
        return result

    import asyncio as _asyncio

    store = ClickHouseStore.__new__(ClickHouseStore)
    store.client = Mock()
    store.client.query = fake_query
    store._lock = _asyncio.Lock()

    store.org_id = "tenant-a"
    assert await store.get_blamed_paths(repo_id) == {"src/a.py", "src/b.py"}
    store.org_id = "tenant-b"
    assert await store.get_blamed_paths(repo_id) == {"other/c.py"}
    assert all("org_id = {org_id:String}" in c["query"] for c in captured)


@pytest.mark.asyncio
async def test_has_unblamed_files_is_org_scoped() -> None:
    """``has_unblamed_files`` filters both git_files and git_blame by org_id."""
    from dev_health_ops.storage.clickhouse import ClickHouseStore

    repo_id = uuid.uuid4()
    captured: list[str] = []

    def fake_query(query: str, parameters: dict[str, Any]) -> Any:
        captured.append(query)
        # Both the outer git_files scan and the inner git_blame subquery must
        # carry the org filter so a stale-tenant blame row cannot mask an
        # unblamed file for the current org.
        assert query.count("org_id = {org_id:String}") == 2
        assert parameters.get("org_id") == "tenant-a"
        result = Mock()
        result.result_rows = [(1,)]
        return result

    import asyncio as _asyncio

    store = ClickHouseStore.__new__(ClickHouseStore)
    store.client = Mock()
    store.client.query = fake_query
    store._lock = _asyncio.Lock()
    store.org_id = "tenant-a"

    assert await store.has_unblamed_files(repo_id) is True
    assert captured and "git_files" in captured[0] and "git_blame" in captured[0]


# ---------------------------------------------------------------------------
# (c) the per-sync blame crawl is BOUNDED (Codex no-ship: unbounded blame)
# ---------------------------------------------------------------------------


class _FakeTreeEntry:
    def __init__(self, path: str, size: int = 100, type_: str = "blob") -> None:
        self.path = path
        self.size = size
        self.type = type_


class _FakeGitHubCodeClient:
    """Stand-in for ``GitHubCodeClient`` (CHAOS-2773 CS7): the processor now
    fetches blame through this client's async ``get_file_blame`` instead of
    the frozen connector's sync ``get_file_blame`` (no longer called)."""

    def __init__(self, *, blame_fn: Any) -> None:
        self._blame_fn = blame_fn
        self.close = AsyncMock()

    async def get_file_blame(
        self, *, owner: str, repo: str, path: str, ref: str
    ) -> Any:
        return self._blame_fn(owner=owner, repo=repo, path=path, ref=ref)

    def drain_usage_observations(self) -> list[Any]:
        return []


class _FakeGitLabCodeClientForBlame:
    """Minimal instrumented GitLabCodeClient stand-in for the blame backfill
    branch (CHAOS-2815/CS14): the real backfill now fetches blame via
    ``GitLabCodeClient.get_file_blame``, never ``connector.rest_client.
    get_file_blame`` (frozen, un-instrumented)."""

    def __init__(self, blame_fn, tree_items=None):
        self._blame_fn = blame_fn
        self._tree_items = tree_items if tree_items is not None else []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def get_file_blame(self, project_id, file_path, *, ref):
        return self._blame_fn(project_id, file_path, ref)

    async def list_repository_tree(
        self, project_id, *, ref, per_page=100, max_items=1_000_000
    ):
        return self._tree_items

    def drain_usage_observations(self):
        return []


@pytest.mark.asyncio
async def test_github_blame_backfill_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Onboarding blame must stop at BLAME_BACKFILL_MAX_FILES files.

    Drives the real ``_backfill_github_missing_data`` blame branch with a tree
    larger than the cap and asserts ``get_file_blame`` is called at most
    ``BLAME_BACKFILL_MAX_FILES`` times -- proving a large repo cannot turn
    onboarding into an unbounded GraphQL crawl (CHAOS-2376 / Codex no-ship).
    """
    from dev_health_ops.connectors.models import BlameRange, FileBlame
    from dev_health_ops.processors import github as github_module
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
    # No blame yet: every tree path is unblamed, so the real coverage-aware
    # path selects the first capped batch (not the exception fallback).
    store.get_blamed_paths = AsyncMock(return_value=set())
    store.has_unblamed_files = AsyncMock(return_value=True)

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
    monkeypatch.setattr(
        github_module,
        "_github_code_client_from_connector",
        lambda _connector: _FakeGitHubCodeClient(blame_fn=fake_blame),
    )

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
    from dev_health_ops.providers.gitlab.code_client import (
        GitLabBlameRange,
        GitLabFileBlame,
    )

    n_files = gitlab_mod.BLAME_BACKFILL_MAX_FILES + 9

    store = Mock()
    store.has_any_git_files = AsyncMock(return_value=True)
    store.has_any_git_file_contents = AsyncMock(return_value=True)
    store.has_any_git_commit_stats = AsyncMock(return_value=True)
    store.has_any_git_blame = AsyncMock(return_value=False)
    # No blame yet: every tree path is unblamed, so the real coverage-aware
    # path selects the first capped batch (not the exception fallback).
    store.get_blamed_paths = AsyncMock(return_value=set())
    store.has_unblamed_files = AsyncMock(return_value=True)

    inserted: list[Any] = []

    async def insert_blame(batch: list[Any]) -> None:
        inserted.extend(batch)

    sink = Mock()
    sink.insert_blame_data = AsyncMock(side_effect=insert_blame)

    project = Mock()
    project.id = 123

    connector = Mock()
    connector.gitlab.projects.get.return_value = project

    # list_repository_tree yields the (oversized) tree of blobs.
    tree_items = [{"type": "blob", "path": f"src/f{i}.py"} for i in range(n_files)]

    blame_calls: list[str] = []

    def fake_blame(project_id: int, path: str, ref: str) -> GitLabFileBlame:
        blame_calls.append(path)
        return GitLabFileBlame(
            file_path=path,
            ranges=(GitLabBlameRange(1, 1, "sha", "Ada", "a@ex.com", 0, ("x",)),),
        )

    monkeypatch.setattr(
        gitlab_mod,
        "_gitlab_code_client_from_connector",
        lambda connector: _FakeGitLabCodeClientForBlame(
            fake_blame, tree_items=tree_items
        ),
    )

    db_repo = Mock()
    db_repo.id = uuid.uuid4()
    db_repo.settings = {"default_branch": "main"}
    await gitlab_mod._backfill_gitlab_missing_data(
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

    assert len(blame_calls) == gitlab_mod.BLAME_BACKFILL_MAX_FILES
    assert {row.path for row in inserted} == set(blame_calls)
    assert len(blame_calls) < n_files


# ---------------------------------------------------------------------------
# (d) the capped blame crawl is RESUMABLE across reruns (Codex round-3 no-ship)
#
# The cap is paired with a coverage-aware gate: a second sync must blame the
# files the first sync left unblamed instead of returning early once any blame
# row exists. Without this, every file past BLAME_BACKFILL_MAX_FILES would stay
# without blame_concentration forever, silently truncating Ownership-risk.
# ---------------------------------------------------------------------------


class _StatefulBlameStore:
    """In-memory store that accumulates blamed paths across syncs.

    Models the real coverage-aware contract: ``get_blamed_paths`` returns the
    paths persisted so far and ``has_unblamed_files`` reports whether any
    tracked file still lacks blame, so the backfill keeps making progress.
    """

    def __init__(self, all_paths: set[str]) -> None:
        self._all_paths = set(all_paths)
        self.blamed: set[str] = set()

    async def has_any_git_files(self, repo_id: Any) -> bool:
        return True

    async def has_any_git_file_contents(self, repo_id: Any) -> bool:
        return True

    async def has_any_git_commit_stats(self, repo_id: Any) -> bool:
        return True

    async def has_any_git_blame(self, repo_id: Any) -> bool:
        return bool(self.blamed)

    async def get_blamed_paths(self, repo_id: Any) -> set[str]:
        return set(self.blamed)

    async def has_unblamed_files(self, repo_id: Any) -> bool:
        return bool(self._all_paths - self.blamed)

    def record(self, paths: list[str]) -> None:
        self.blamed.update(paths)


def test_select_unblamed_paths_advances_each_call() -> None:
    """select_unblamed_paths returns the next batch, never the same prefix."""
    import asyncio

    from dev_health_ops.processors.base_git import select_unblamed_paths

    paths = [f"src/f{i}.py" for i in range(7)]

    class _Store:
        def __init__(self) -> None:
            self.blamed: set[str] = set()

        async def get_blamed_paths(self, repo_id: Any) -> set[str]:
            return set(self.blamed)

    store = _Store()

    async def run() -> tuple[list[str], list[str], list[str]]:
        batch1 = await select_unblamed_paths(store, "r", paths, 3)
        store.blamed.update(batch1)
        batch2 = await select_unblamed_paths(store, "r", paths, 3)
        store.blamed.update(batch2)
        batch3 = await select_unblamed_paths(store, "r", paths, 3)
        store.blamed.update(batch3)
        return batch1, batch2, batch3

    batch1, batch2, batch3 = asyncio.run(run())
    assert batch1 == paths[0:3]
    assert batch2 == paths[3:6]
    assert batch3 == paths[6:7]
    # Every path eventually covered, with no overlap (no wasted re-blame).
    assert batch1 + batch2 + batch3 == paths


def test_select_unblamed_paths_empty_when_fully_covered() -> None:
    import asyncio

    from dev_health_ops.processors.base_git import select_unblamed_paths

    paths = ["a.py", "b.py"]

    class _Store:
        async def get_blamed_paths(self, repo_id: Any) -> set[str]:
            return set(paths)

    result = asyncio.run(select_unblamed_paths(_Store(), "r", paths, 10))
    assert result == []


def test_blame_backfill_needed_coverage_aware() -> None:
    """blame_backfill_needed stays True while coverage is partial."""
    import asyncio

    from dev_health_ops.processors.base_git import blame_backfill_needed

    class _Store:
        def __init__(self, unblamed: bool) -> None:
            self._unblamed = unblamed

        async def has_unblamed_files(self, repo_id: Any) -> bool:
            return self._unblamed

    async def run() -> None:
        # include_blame off -> never blame.
        assert (
            await blame_backfill_needed(
                _Store(True), "r", include_blame=False, any_row_needs_blame=True
            )
            is False
        )
        # First sync (no blame yet): any-row gate drives the crawl.
        assert (
            await blame_backfill_needed(
                _Store(False), "r", include_blame=True, any_row_needs_blame=True
            )
            is True
        )
        # Blame exists but coverage partial -> keep crawling.
        assert (
            await blame_backfill_needed(
                _Store(True), "r", include_blame=True, any_row_needs_blame=False
            )
            is True
        )
        # Blame exists and fully covered -> stop.
        assert (
            await blame_backfill_needed(
                _Store(False), "r", include_blame=True, any_row_needs_blame=False
            )
            is False
        )

    asyncio.run(run())


def test_blame_backfill_needed_probe_failure_defers_not_aborts() -> None:
    """A coverage-probe failure defers blame to next sync (no hard abort).

    The wider backfill (files / commit stats) must not be killed by a transient
    coverage probe error; we simply skip blame this run and retry next sync.
    """
    import asyncio

    from dev_health_ops.processors.base_git import blame_backfill_needed

    class _Boom:
        async def has_unblamed_files(self, repo_id: Any) -> bool:
            raise RuntimeError("clickhouse transient")

    result = asyncio.run(
        blame_backfill_needed(
            _Boom(), "r", include_blame=True, any_row_needs_blame=False
        )
    )
    assert result is False


@pytest.mark.asyncio
async def test_github_blame_backfill_resumes_on_second_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second GitHub sync blames the files the first sync left uncovered.

    Proves the cap is not a permanent dead-end: across two runs every file in
    a repo larger than BLAME_BACKFILL_MAX_FILES gets blamed (CHAOS-2376
    round-3 / Codex no-ship).
    """
    from dev_health_ops.connectors.models import BlameRange, FileBlame
    from dev_health_ops.processors import github as github_module
    from dev_health_ops.processors.github import (
        BLAME_BACKFILL_MAX_FILES,
        _backfill_github_missing_data,
    )

    n_files = BLAME_BACKFILL_MAX_FILES + 23
    all_paths = {f"src/f{i}.py" for i in range(n_files)}
    store = _StatefulBlameStore(all_paths)

    def insert_factory() -> tuple[Any, list[Any]]:
        captured: list[Any] = []

        async def insert_blame(batch: list[Any]) -> None:
            captured.extend(batch)

        sink = Mock()
        sink.insert_blame_data = AsyncMock(side_effect=insert_blame)
        return sink, captured

    gh_repo = Mock()
    gh_repo.get_branch.return_value = Mock(commit=Mock(sha="abc"))
    gh_repo.get_git_tree.return_value = Mock(
        tree=[_FakeTreeEntry(f"src/f{i}.py") for i in range(n_files)]
    )

    def fake_blame(*, owner: str, repo: str, path: str, ref: str) -> FileBlame:
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
    monkeypatch.setattr(
        github_module,
        "_github_code_client_from_connector",
        lambda _connector: _FakeGitHubCodeClient(blame_fn=fake_blame),
    )

    db_repo = Mock()
    db_repo.id = uuid.uuid4()

    async def sync() -> set[str]:
        sink, captured = insert_factory()
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
        blamed_now = {row.path for row in captured}
        store.record(list(blamed_now))
        return blamed_now

    first = await sync()
    assert len(first) == BLAME_BACKFILL_MAX_FILES

    second = await sync()
    # The second sync processes ONLY the files the first one skipped.
    assert second == all_paths - first
    assert second.isdisjoint(first)

    # Across the two runs, every file in the repo is now blamed (no silent
    # truncation past the cap).
    assert store.blamed == all_paths

    # A third sync finds nothing left and does no blame work.
    third = await sync()
    assert third == set()


@pytest.mark.asyncio
async def test_gitlab_blame_backfill_resumes_on_second_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second GitLab sync blames the files the first sync left uncovered."""
    import dev_health_ops.processors.gitlab as gitlab_mod
    from dev_health_ops.providers.gitlab.code_client import (
        GitLabBlameRange,
        GitLabFileBlame,
    )

    n_files = gitlab_mod.BLAME_BACKFILL_MAX_FILES + 11
    all_paths = {f"src/f{i}.py" for i in range(n_files)}
    store = _StatefulBlameStore(all_paths)

    tree_items = [{"type": "blob", "path": f"src/f{i}.py"} for i in range(n_files)]

    project = Mock()
    project.id = 123
    connector = Mock()
    connector.gitlab.projects.get.return_value = project

    def fake_blame(project_id: int, path: str, ref: str) -> GitLabFileBlame:
        return GitLabFileBlame(
            file_path=path,
            ranges=(GitLabBlameRange(1, 1, "sha", "Ada", "a@ex.com", 0, ("x",)),),
        )

    monkeypatch.setattr(
        gitlab_mod,
        "_gitlab_code_client_from_connector",
        lambda connector: _FakeGitLabCodeClientForBlame(
            fake_blame, tree_items=tree_items
        ),
    )

    db_repo = Mock()
    db_repo.id = uuid.uuid4()
    db_repo.settings = {"default_branch": "main"}

    async def sync() -> set[str]:
        captured: list[Any] = []

        async def insert_blame(batch: list[Any]) -> None:
            captured.extend(batch)

        sink = Mock()
        sink.insert_blame_data = AsyncMock(side_effect=insert_blame)
        await gitlab_mod._backfill_gitlab_missing_data(
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
        blamed_now = {row.path for row in captured}
        store.record(list(blamed_now))
        return blamed_now

    first = await sync()
    assert len(first) == gitlab_mod.BLAME_BACKFILL_MAX_FILES

    second = await sync()
    assert second == all_paths - first
    assert second.isdisjoint(first)
    assert store.blamed == all_paths

    third = await sync()
    assert third == set()


@pytest.mark.asyncio
async def test_github_blame_gate_alive_when_files_present_blame_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """needs.blame=False but has_unblamed_files=True still triggers the crawl.

    Models a rerun where the any-row gate would say "blame exists, skip" but
    coverage is partial; the coverage gate must keep the blame branch alive.
    """
    from dev_health_ops.connectors.models import BlameRange, FileBlame
    from dev_health_ops.processors import github as github_module
    from dev_health_ops.processors.github import _backfill_github_missing_data

    all_paths = {"src/a.py", "src/b.py", "src/c.py"}
    store = _StatefulBlameStore(all_paths)
    # Simulate a prior partial sync: one file already blamed.
    store.blamed = {"src/a.py"}

    captured: list[Any] = []

    async def insert_blame(batch: list[Any]) -> None:
        captured.extend(batch)

    sink = Mock()
    sink.insert_blame_data = AsyncMock(side_effect=insert_blame)

    gh_repo = Mock()
    gh_repo.get_branch.return_value = Mock(commit=Mock(sha="abc"))
    gh_repo.get_git_tree.return_value = Mock(
        tree=[_FakeTreeEntry(p) for p in sorted(all_paths)]
    )

    def fake_blame(*, owner: str, repo: str, path: str, ref: str) -> FileBlame:
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
    monkeypatch.setattr(
        github_module,
        "_github_code_client_from_connector",
        lambda _connector: _FakeGitHubCodeClient(blame_fn=fake_blame),
    )

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

    # Only the two previously-unblamed files are processed; the already-blamed
    # file is not re-fetched.
    blamed_now = {row.path for row in captured}
    assert blamed_now == {"src/b.py", "src/c.py"}
