from __future__ import annotations

import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/github/code_client.py"
_MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"


def _reflected_fields() -> frozenset[str]:
    return class_annotated_field_names(_MODEL_SOURCE.read_text(), "SecurityAlert")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    client = load_live_module(_CODE_CLIENT_SOURCE)
    source = case["source"]
    item = case["raw_alert"]
    builders = {
        "dependabot": client._dependabot_alert_from_item,
        "code_scanning": client._code_scanning_alert_from_item,
        "advisory": client._security_advisory_from_item,
    }
    row = builders[source](item)
    return vars(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/security/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "repo_id": "attached by processors/github.py after the code-client mapping boundary",
            "last_synced": "stamped by the persistence boundary, not the code-client mapping",
        },
    )
)
