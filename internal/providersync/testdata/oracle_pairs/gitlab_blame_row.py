"""Live GitLab blame producer oracle for CHAOS-3722.

The pair executes processors/gitlab.py::_backfill_gitlab_missing_data itself.
Only the provider client, coverage, and sink boundaries are replaced; the
active producer's range expansion and GitBlame construction remain live.
"""

from __future__ import annotations

import asyncio
import pathlib
from types import SimpleNamespace
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/gitlab.py"
_MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"


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

    async def __aenter__(self) -> _CodeClient:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def get_latest_commit_sha(self, *_args: Any, **_kwargs: Any) -> str:
        return self.case.get("ref", "tree-sha")

    async def list_repository_tree(
        self, *_args: Any, **_kwargs: Any
    ) -> list[dict[str, Any]]:
        return [{"type": "blob", "path": self.case["path"]}]

    async def get_file_blame(self, _project: str, path: str, ref: str) -> Any:
        lines = tuple(self.case.get("lines", ["line"]))
        return SimpleNamespace(
            file_path=path,
            ranges=[
                SimpleNamespace(
                    starting_line=1,
                    ending_line=len(lines),
                    author=self.case.get("author_name", "Unknown"),
                    author_email=self.case.get("author_email", ""),
                    commit_sha=self.case.get("commit_id", ""),
                    lines=lines,
                )
            ],
        )

    def drain_usage_observations(self) -> list[Any]:
        return []


def _reflected_fields() -> frozenset[str]:
    return class_annotated_field_names(_MODEL_SOURCE.read_text(), "GitBlame")


async def _build_async(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PROCESSOR_SOURCE)
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
        0, result=[case["path"]]
    )
    client = _CodeClient(case)
    module._gitlab_code_client_from_connector = lambda _connector: client
    sink = _Sink()
    await module._backfill_gitlab_missing_data(
        store=SimpleNamespace(),
        ingestion_sink=sink,
        connector=SimpleNamespace(),
        db_repo=SimpleNamespace(id=case["repo_id"]),
        project_full_name="group/project",
        default_branch="main",
        max_commits=None,
        include_files=False,
        include_blame=True,
        include_commit_stats=False,
    )
    if not sink.rows:
        raise AssertionError("live GitLab blame producer emitted no rows")
    return dict(vars(sink.rows[0]))


def _build(case: dict[str, Any]) -> dict[str, Any]:
    return asyncio.run(_build_async(case))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/blame/row",
        build_row=_build,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "last_synced": "ClickHouseStore.insert_blame_data stamps persistence time after the live producer boundary",
        },
    )
)
