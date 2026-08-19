"""Live Python oracle for Linear reference-team resolution."""

from __future__ import annotations

import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import (
    RETURN_LITERAL,
    dict_literal_keys,
)
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PROVIDER_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/linear/provider.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PROVIDER_SOURCE)
    row = module._linear_reference_team(case["reference"], case["team_key"])
    if row is None:
        raise ValueError("production Linear reference resolver rejected oracle case")
    return row


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/team",
        build_row=_build_row,
        reflected_fields=lambda: dict_literal_keys(
            _PROVIDER_SOURCE.read_text(), "_linear_reference_team", (RETURN_LITERAL,)
        ),
    )
)
