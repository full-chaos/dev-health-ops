from __future__ import annotations

import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/github/code_client.py"
_PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/github.py"
_MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"


def _reflected_fields() -> frozenset[str]:
    return class_annotated_field_names(_MODEL_SOURCE.read_text(), "GitCommit")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    code_client = load_live_module(_CODE_CLIENT_SOURCE)
    commit = code_client._commit_from_item(case["raw_commit"])
    processor = load_live_module(_PROCESSOR_SOURCE)
    row = processor._github_commit_to_model(commit, case["repo_id"])
    return {key: value for key, value in vars(row).items() if not key.startswith("_")}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/commits/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "last_synced": "stamped by ClickHouse persistence, not the producer",
        },
    )
)
