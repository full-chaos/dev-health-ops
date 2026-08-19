from __future__ import annotations

import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/code_client.py"


def _reflected_fields() -> frozenset[str]:
    client = load_live_module(CODE_CLIENT_SOURCE)
    return frozenset(client.SecurityAlertData.__annotations__)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    client = load_live_module(CODE_CLIENT_SOURCE)
    dependency = {"name": case["package_name"]}
    row = client._map_dependency_alert(dependency, case["raw_alert"])
    return vars(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/security/dependency",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "created_at": "the Python dependency mapper uses wall-clock now while Go uses the executor normalized instant",
        },
    )
)
