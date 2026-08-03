from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.oracle_pairs.gitlab_cicd_pipeline import (
    CODE_CLIENT_SOURCE,
    MODEL_SOURCE,
    _active_producer,
)
from internal.providersync.testdata.python_oracle_loader import load_live_module


def _build_selection(case: dict[str, Any]) -> dict[str, Any]:
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
    observation = {field: None for field in _REFLECTED_FIELDS}
    observation["run_id"] = [row.run_id for row in rows]
    return observation


_REFLECTED_FIELDS = class_annotated_field_names(
    MODEL_SOURCE.read_text(), "CiPipelineRun"
)

oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/cicd/selection",
        build_row=_build_selection,
        reflected_fields=lambda: _REFLECTED_FIELDS,
        excluded_fields={
            field: "selection boundary compares accepted run identity only; complete persisted-row parity is covered by gitlab/cicd/pipeline"
            for field in _REFLECTED_FIELDS
            if field != "run_id"
        },
    )
)
