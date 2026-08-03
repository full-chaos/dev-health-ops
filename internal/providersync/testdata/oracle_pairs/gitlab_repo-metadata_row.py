from __future__ import annotations

import pathlib
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import (
    RETURN_LITERAL,
    dict_literal_keys,
)
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_REPOSITORY_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/repository.py"
_MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"
_ROW_ENCODER_SOURCE = REPO_ROOT / "src/dev_health_ops/storage/repository_rows.py"


def _reflected_fields() -> frozenset[str]:
    return dict_literal_keys(
        _ROW_ENCODER_SOURCE.read_text(),
        "build_repository_insert_row",
        (RETURN_LITERAL,),
    )


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    repository = load_live_module(_REPOSITORY_SOURCE)
    values = repository.build_gitlab_repository_values(
        SimpleNamespace(**case["project"]), case["gitlab_url"]
    )
    normalized_at = datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00"))
    model = load_live_module(_MODEL_SOURCE)
    repo = model.Repo(repo_path=None, created_at=normalized_at, **values)
    row_encoder = load_live_module(_ROW_ENCODER_SOURCE)
    return row_encoder.build_repository_insert_row(repo, synced_at=normalized_at)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/repo-metadata/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "source_id": "the Go native repos insert omits this nullable column and ClickHouse defaults it to NULL",
        },
    )
)
