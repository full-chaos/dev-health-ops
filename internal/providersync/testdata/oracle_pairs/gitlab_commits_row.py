from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/code_client.py"
_COMMIT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/commits.py"
_MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"


def _reflected_fields() -> frozenset[str]:
    return class_annotated_field_names(_MODEL_SOURCE.read_text(), "GitCommit")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    code_client = load_live_module(_CODE_CLIENT_SOURCE)
    commit = code_client._map_commit(case["raw_commit"])
    commit_builder = load_live_module(_COMMIT_SOURCE)
    normalized_at = datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00"))
    values = commit_builder.build_gitlab_commit_values(
        commit,
        case["repo_id"],
        now=lambda: normalized_at.astimezone(timezone.utc),
    )
    return values


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/commits/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "last_synced": "stamped by ClickHouse persistence, not the producer",
        },
    )
)
