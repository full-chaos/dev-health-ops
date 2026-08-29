"""Live Python oracle for GitHub team membership row construction (CHAOS-4434)."""

from __future__ import annotations

import dataclasses
import pathlib
from datetime import datetime
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PRODUCER_SOURCE = REPO_ROOT / "src/dev_health_ops/workers/team_autoimport_github.py"
_SCHEMA_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/schemas.py"
_IDENTITY_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/identity.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    identity = load_live_module(_IDENTITY_SOURCE)
    now = datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00"))
    team_id = module._team_id(case["team_slug"])
    email = case.get("email")
    # Same ladder _membership_rows itself runs: resolver.membership_facets(...)
    # or [raw_identity] when the ladder returns nothing at all.
    facets = identity.load_identity_resolver().membership_facets(
        provider="github", username=case["login"], email=email
    ) or [case["login"]]
    raw_email = email.strip() if isinstance(email, str) and email.strip() else None
    row = module._github_membership_row(
        org_id=case["org_id"],
        team_id=team_id,
        member_id=f"gh:{case['login']}",
        raw_provider_user_id=facets[0],
        raw_email=raw_email,
        identity_facets=facets,
        now=now,
    )
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/team-catalog/membership",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _SCHEMA_SOURCE.read_text(), "TeamMembershipRecord"
        ),
    )
)
