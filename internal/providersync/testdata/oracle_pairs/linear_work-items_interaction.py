"""Live Python oracle for Linear comment interaction rows."""

from __future__ import annotations

import dataclasses
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"
_NORMALIZE_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/linear/normalize.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_NORMALIZE_SOURCE)
    from dev_health_ops.providers.identity import IdentityResolver

    row = module.linear_comment_to_interaction_event(
        comment=case["raw_comment"],
        work_item_id=case["work_item_id"],
        identity=IdentityResolver({}),
    )
    if row is None:
        raise ValueError("production Linear comment normalizer rejected oracle case")
    return dataclasses.asdict(dataclasses.replace(row, org_id=case["org_id"]))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/interaction",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItemInteractionEvent"
        ),
        excluded_fields={
            "last_synced": "Python's interaction constructor stamps wall-clock time; Go uses the unit normalizedAt for retry-stable effects.",
        },
    )
)
