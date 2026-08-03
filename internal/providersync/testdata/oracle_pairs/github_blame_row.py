"""Live github/blame producer oracle for CHAOS-3335.

This pair executes processors/github.py::_backfill_github_missing_data itself.
Only its external tree, GraphQL, coverage, and batch-sink seams are replaced;
the range expansion and GitBlame construction being compared remain the live
production function's own code.

Go tests for this pair must use -count=1 because the production Python source
lives outside Go's embeddable testdata tree.
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
_PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/github.py"
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

    async def get_file_blame(self, **_kwargs: Any) -> Any:
        author = SimpleNamespace()
        return SimpleNamespace(
            ranges=[
                SimpleNamespace(
                    starting_line=self.case["starting_line"],
                    ending_line=self.case["ending_line"],
                    author=self.case.get("author", "Unknown"),
                    author_email=self.case.get("author_email", ""),
                    commit_sha=self.case.get("commit_sha", ""),
                )
            ],
            author=author,
        )

    def drain_usage_observations(self) -> list[Any]:
        return []

    async def close(self) -> None:
        return None


def _reflected_fields() -> frozenset[str]:
    return class_annotated_field_names(_MODEL_SOURCE.read_text(), "GitBlame")


async def _build_row_async(case: dict[str, Any]) -> dict[str, Any]:
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
    code_client = _CodeClient(case)
    module._github_code_client_from_connector = lambda _connector: code_client

    tree = SimpleNamespace(
        tree=[SimpleNamespace(type="blob", path=case["path"], size=12)]
    )
    repo = SimpleNamespace(
        get_branch=lambda _branch: SimpleNamespace(
            commit=SimpleNamespace(sha=case["commit_sha"])
        ),
        get_git_tree=lambda _ref, recursive: tree,
    )
    connector = SimpleNamespace(github=SimpleNamespace(get_repo=lambda _name: repo))
    sink = _Sink()
    await module._backfill_github_missing_data(
        store=SimpleNamespace(),
        ingestion_sink=sink,
        connector=connector,
        db_repo=SimpleNamespace(id=case["repo_id"]),
        repo_full_name="acme/api",
        default_branch="main",
        max_commits=None,
        include_files=False,
        include_blame=True,
        include_commit_stats=False,
    )
    if not sink.rows:
        raise AssertionError("live github blame producer emitted no rows")
    return dict(vars(sink.rows[0]))


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    return asyncio.run(_build_row_async(case))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/blame/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "last_synced": "ClickHouseStore.insert_blame_data stamps persistence time after the live producer boundary",
        },
    )
)
