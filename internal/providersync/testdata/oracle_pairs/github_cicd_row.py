from __future__ import annotations

import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/github/code_client.py"
_BASE_GIT_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/base_git.py"
_MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"


def _reflected_fields() -> frozenset[str]:
    return class_annotated_field_names(_MODEL_SOURCE.read_text(), "CiPipelineRun")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    code_client = load_live_module(_CODE_CLIENT_SOURCE)
    workflow_run = code_client._workflow_run_from_item(case["raw_run"])
    base_git = load_live_module(_BASE_GIT_SOURCE)
    run = base_git.build_ci_pipeline_run(
        repo_id=case["repo_id"],
        run_id=workflow_run.run_id,
        status=workflow_run.status,
        queued_at=workflow_run.queued_at,
        started_at=workflow_run.started_at,
        finished_at=workflow_run.finished_at,
        retry_count=workflow_run.retry_count,
    )
    return {key: value for key, value in vars(run).items() if not key.startswith("_")}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/cicd/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "last_synced": (
                "stamped by ClickHouseStore.insert_ci_pipeline_runs at persistence time, "
                "not by build_ci_pipeline_run"
            )
        },
    )
)
