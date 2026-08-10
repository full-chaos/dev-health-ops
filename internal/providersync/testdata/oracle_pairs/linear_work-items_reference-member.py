"""Live Python oracle for Linear member catalog normalization."""

from __future__ import annotations

import dataclasses
import pathlib
from datetime import datetime
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PRODUCER_SOURCE = REPO_ROOT / "src/dev_health_ops/workers/team_autoimport_linear.py"
_SCHEMA_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/schemas.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    member = case["member"]
    provider_identity = member.get("email") or member.get("id")
    if not provider_identity:
        raise ValueError("production Linear member identity is empty")
    row = module.MemberRecord(
        member_id=module._member_id("linear", str(provider_identity)),
        name=str(member.get("name") or provider_identity),
        provider_identities=module._provider_identities(
            "linear", str(provider_identity)
        ),
        is_active=1,
        updated_at=datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00")),
        email=member.get("email"),
        org_id=case["org_id"],
    )
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/reference-member",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _SCHEMA_SOURCE.read_text(), "MemberRecord"
        ),
    )
)
