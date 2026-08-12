"""Live Python oracle for explicit Linear issue-label AI attribution."""

from __future__ import annotations

import dataclasses
import pathlib
from typing import Any
from uuid import UUID

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/ai_attribution.py"
_NORMALIZE_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/linear/normalize.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_NORMALIZE_SOURCE)
    from dev_health_ops.providers.identity import IdentityResolver
    from dev_health_ops.providers.status_mapping import StatusMapping

    item, _ = module.linear_issue_to_work_item(
        issue=case["raw_issue"],
        status_mapping=StatusMapping({}, {}, {}, {}),
        identity=IdentityResolver({}),
        history=case.get("history", []),
    )
    rows = module.linear_work_item_ai_attributions(
        work_item=item,
        org_id=UUID(case["org_id"]),
    )
    source = case["signal_source"]
    row = next(candidate for candidate in rows if candidate.source.value == source)
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/ai-attribution",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "AIAttributionRecord"
        ),
        excluded_fields={
            "ingested_at": (
                "the Python record constructor stamps wall-clock now; Go stamps the "
                "complete unit normalizedAt and tests its retry stability"
            ),
            "record_id": (
                "the Python record constructor assigns uuid4; Go derives a stable "
                "retry identifier and tests that stability separately"
            ),
        },
    )
)
