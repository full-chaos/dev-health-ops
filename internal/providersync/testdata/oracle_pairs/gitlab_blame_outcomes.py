"""Live per-file GitLab blame failure oracle for CHAOS-3722.

This executes the active `_backfill_gitlab_missing_data` loop. Provider tree,
blame, and sink boundaries are replaced, while the producer's exception
policy and line construction remain the live implementation.
"""

from __future__ import annotations

import asyncio
import pathlib
from types import SimpleNamespace
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/gitlab.py"


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
    def __init__(self, case: dict[str, Any]) -> None:
        self.case = case
        self.attempted_paths: list[str] = []

    async def __aenter__(self) -> _CodeClient:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def list_repository_tree(
        self, *_args: Any, **_kwargs: Any
    ) -> list[dict[str, Any]]:
        return [{"type": "blob", "path": path} for path in self.case["file_paths"]]

    async def get_file_blame(self, _project: str, path: str, ref: str) -> Any:
        self.attempted_paths.append(path)
        if path in set(self.case["failed_paths"]):
            raise RuntimeError("ordinary per-file failure")
        return SimpleNamespace(
            ranges=[
                SimpleNamespace(
                    starting_line=1,
                    ending_line=1,
                    author="Ada",
                    author_email="ada@example.com",
                    commit_sha="abc123",
                    lines=("line",),
                )
            ]
        )

    def drain_usage_observations(self) -> list[Any]:
        return []


async def _build_async(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PROCESSOR_SOURCE)
    # The loader intentionally omits the application exception package; the
    # live branch's policy is still exercised by making this seam's ordinary
    # failure classification explicit.
    module._is_rate_limit_exception = lambda _exc: False
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
    client = _CodeClient(case)
    module._gitlab_code_client_from_connector = lambda _connector: client
    sink = _Sink()
    raised = False
    try:
        await module._backfill_gitlab_missing_data(
            store=SimpleNamespace(),
            ingestion_sink=sink,
            connector=SimpleNamespace(),
            db_repo=SimpleNamespace(id="c7198fbc-1945-3717-05d8-eb78866b4e79"),
            project_full_name="group/project",
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
        id="gitlab/blame/outcomes",
        build_row=_build,
        reflected_fields=lambda: frozenset(
            {"attempted_paths", "persisted_paths", "raised"}
        ),
    )
)
