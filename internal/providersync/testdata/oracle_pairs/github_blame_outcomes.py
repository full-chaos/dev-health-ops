"""Live per-file GitHub blame failure oracle for CHAOS-3343.

This executes the active Python `_backfill_github_missing_data` loop. Only its
provider, tree, and sink boundaries are replaced. The wrapper returns every
observable item from those boundaries: attempted paths, persisted row paths,
and whether the producer raised out of the per-file loop.
"""

from __future__ import annotations

import asyncio
import pathlib
from types import SimpleNamespace
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/github.py"


class _GitBlame:
    def __init__(self, **values: Any) -> None:
        self.__dict__.update(values)


class _Collector:
    def __init__(self, writer: Any) -> None:
        self.writer = writer
        self.rows: list[Any] = []

    async def __aenter__(self) -> _Collector:
        return self

    def add(self, row: Any) -> None:
        self.rows.append(row)

    async def maybe_flush(self) -> None:
        return None

    async def __aexit__(self, *_args: Any) -> None:
        await self.writer(self.rows)


class _Sink:
    def __init__(self) -> None:
        self.rows: list[Any] = []

    async def insert_blame_data(self, rows: list[Any]) -> None:
        self.rows.extend(rows)


class _CodeClient:
    def __init__(self, failed_paths: set[str]) -> None:
        self.failed_paths = failed_paths
        self.attempted_paths: list[str] = []

    async def get_file_blame(self, *, path: str, **_kwargs: Any) -> Any:
        self.attempted_paths.append(path)
        if path in self.failed_paths:
            raise RuntimeError("per-file GraphQL error")
        return SimpleNamespace(
            ranges=[
                SimpleNamespace(
                    starting_line=1,
                    ending_line=2,
                    author="Ada",
                    author_email="ada@example.com",
                    commit_sha="abc123",
                )
            ]
        )

    def drain_usage_observations(self) -> list[Any]:
        return []

    async def close(self) -> None:
        return None


async def _build_async(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PROCESSOR_SOURCE)

    class _RateLimit(Exception):
        pass

    module.RateLimitException = _RateLimit
    module.RateLimitExceededException = _RateLimit
    module.logging = SimpleNamespace(
        debug=lambda *_args, **_kwargs: None,
        info=lambda *_args, **_kwargs: None,
        warning=lambda *_args, **_kwargs: None,
    )
    module.GitBlame = _GitBlame
    module.AsyncBatchCollector = _Collector
    module.check_backfill_needs = lambda *_args, **_kwargs: asyncio.sleep(
        0, result=SimpleNamespace(files=False, blame=True, commit_stats=False)
    )
    module.blame_backfill_needed = lambda *_args, **_kwargs: asyncio.sleep(
        0, result=True
    )
    module.historical_backfill_day = lambda *_args, **_kwargs: None
    module.select_unblamed_paths = lambda *_args, **_kwargs: asyncio.sleep(
        0, result=list(case["file_paths"])
    )
    client = _CodeClient(set(case["failed_paths"]))
    module._github_code_client_from_connector = lambda _connector: client

    tree = SimpleNamespace(
        tree=[
            SimpleNamespace(type="blob", path=path, size=12)
            for path in case["file_paths"]
        ]
    )
    repo = SimpleNamespace(
        get_branch=lambda _branch: SimpleNamespace(
            commit=SimpleNamespace(sha="tree-sha")
        ),
        get_git_tree=lambda _ref, recursive: tree,
    )
    connector = SimpleNamespace(github=SimpleNamespace(get_repo=lambda _name: repo))
    sink = _Sink()
    raised = False
    try:
        await module._backfill_github_missing_data(
            store=SimpleNamespace(),
            ingestion_sink=sink,
            connector=connector,
            db_repo=SimpleNamespace(id="c7198fbc-1945-3717-05d8-eb78866b4e79"),
            repo_full_name="acme/api",
            default_branch="main",
            max_commits=None,
            include_files=False,
            include_blame=True,
            include_commit_stats=False,
        )
    except Exception:
        raised = True
    return {
        "attempted_paths": client.attempted_paths,
        "persisted_paths": [row.path for row in sink.rows],
        "raised": raised,
    }


def _build(case: dict[str, Any]) -> dict[str, Any]:
    return asyncio.run(_build_async(case))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/blame/outcomes",
        build_row=_build,
        reflected_fields=lambda: frozenset(
            {"attempted_paths", "persisted_paths", "raised"}
        ),
    )
)
