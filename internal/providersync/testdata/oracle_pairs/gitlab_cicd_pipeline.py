from __future__ import annotations

import ast
import asyncio
import logging
import pathlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/gitlab.py"
BASE_GIT_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/base_git.py"
CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/code_client.py"
MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"


def _active_producer() -> Any:
    """Compile the two live producer functions without importing unrelated app code."""
    tree = ast.parse(PROCESSOR_SOURCE.read_text())
    wanted = {
        "_drain_gitlab_code_usage",
        "_fetch_gitlab_pipelines_sync",
        "_filter_after",
    }
    functions = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in wanted
    ]
    if {node.name for node in functions} != wanted:
        raise ValueError("active GitLab pipeline producer functions were not found")
    base_git = load_live_module(BASE_GIT_SOURCE)
    namespace: dict[str, Any] = {
        "Any": Any,
        "CiPipelineRun": object,
        "datetime": datetime,
        "asyncio": asyncio,
        "logging": logging,
        "timezone": timezone,
        "build_ci_pipeline_run": base_git.build_ci_pipeline_run,
    }
    exec(
        compile(ast.Module(body=functions, type_ignores=[]), PROCESSOR_SOURCE, "exec"),
        namespace,
    )
    return namespace


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    code_client = load_live_module(CODE_CLIENT_SOURCE)
    producer = _active_producer()
    raw = [code_client._map_pipeline(item) for item in case["raw_pipelines"]]

    class Client:
        async def get_pipelines(self, _project_id: object, *, max_pipelines: int):
            return raw[:max_pipelines]

        def drain_usage_observations(self) -> list[dict[str, Any]]:
            return []

    @asynccontextmanager
    async def client_factory(_connector: object):
        yield Client()

    producer["_gitlab_code_client_from_connector"] = client_factory
    since = datetime.fromisoformat(case["since_at"].replace("Z", "+00:00"))
    before = datetime.fromisoformat(case["before_at"].replace("Z", "+00:00"))
    rows = producer["_fetch_gitlab_pipelines_sync"](
        object(), 123, case["repo_id"], len(raw), since, []
    )
    rows = producer["_filter_after"](rows, before, "started_at")
    if len(rows) != 1:
        raise ValueError(f"oracle case expected one accepted row, got {len(rows)}")
    return dict(vars(rows[0]))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/cicd/pipeline",
        build_row=_build_row,
        reflected_fields=lambda: class_annotated_field_names(
            MODEL_SOURCE.read_text(), "CiPipelineRun"
        ),
        excluded_fields={
            "last_synced": "stamped by persistence after the active Python producer returns",
        },
    )
)
