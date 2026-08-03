from __future__ import annotations

import hashlib
import json
import pathlib
import uuid
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


def _reflected_fields() -> frozenset[str]:
    return dict_literal_keys(
        _REPOSITORY_SOURCE.read_text(),
        "build_gitlab_repository_values",
        (RETURN_LITERAL,),
    )


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    repository = load_live_module(_REPOSITORY_SOURCE)
    values = repository.build_gitlab_repository_values(
        SimpleNamespace(**case["project"]), case["gitlab_url"]
    )
    normalized_at = datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00"))
    digest = hashlib.sha256(values["repo"].strip().lower().encode()).digest()[:16]
    return {
        "id": str(uuid.UUID(bytes=digest)),
        "repo": values["repo"],
        "ref": None,
        "created_at": normalized_at,
        "settings": json.dumps(values["settings"], default=str, separators=(",", ":")),
        "tags": json.dumps(values["tags"], default=str, separators=(",", ":")),
        "provider": values["provider"],
        "last_synced": normalized_at,
    }


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/repo-metadata/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
    )
)
