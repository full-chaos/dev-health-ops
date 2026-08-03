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
    return class_annotated_field_names(_MODEL_SOURCE.read_text(), "GitCommitStat")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    code_client = load_live_module(_CODE_CLIENT_SOURCE)
    processor = load_live_module(_PROCESSOR_SOURCE)
    file_stat = code_client._commit_stat_from_file(
        case["commit_hash"], case["raw_file"]
    )
    stat = processor._github_commit_stat_to_model(file_stat, case["repo_id"])
    return {key: value for key, value in vars(stat).items() if not key.startswith("_")}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/commit-stats/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "old_file_mode": "ClickHouseStore.insert_git_commit_stats supplies unknown when Python leaves the model field unset",
            "new_file_mode": "ClickHouseStore.insert_git_commit_stats supplies unknown when Python leaves the model field unset",
            "last_synced": "ClickHouseStore.insert_git_commit_stats stamps persistence time after the producer boundary",
        },
    )
)
