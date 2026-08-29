"""Live Python oracle for the identity-drift conflict DECISION (CHAOS-4444)
-- clickhouse_identity_drift.py's ``_conflict_for``: which source (an active
manual team_memberships row, an active member-scoped
manual_attribution_fallbacks row, or neither) wins for an incoming
provider_access membership row, and specifically whether a same-team row
confirms while a different-team row conflicts, checked across every
candidate rather than stopping at the first match. The full nested content
of the returned "manual_membership"/"manual_fallback" payload is proven
separately by team_drift_json_test.go's canonical-JSON pins (already run
against real python3); this pair proves the DECISION -- which field wins, or
that neither does -- against the LIVE, checked-in production function.
"""

from __future__ import annotations

import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PRODUCER_SOURCE = (
    REPO_ROOT
    / "src/dev_health_ops/api/services/configuration/clickhouse_identity_drift.py"
)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    conflict = module._conflict_for(
        case["row"],
        manual_memberships=case.get("manual_memberships", []),
        member_fallbacks=case.get("member_fallbacks", []),
    )
    return {"conflict_field": conflict["field"] if conflict else None}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="identity-drift/review/conflict-decision",
        build_row=_build_row,
        # _conflict_for returns a dict-or-None union (two different literal
        # shapes plus a None branch) that dict_literal_keys' single-return
        # AST reflector cannot flatten into one field set -- the flattened
        # decision field is declared directly, same reasoning as the two
        # change-id pairs' scalar declarations.
        reflected_fields=lambda: frozenset({"conflict_field"}),
    )
)
