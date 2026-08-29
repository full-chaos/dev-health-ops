"""Live Python oracle for the identity-drift membership-change id hash
(CHAOS-4444) -- clickhouse_identity_drift.py's
``change_id_for_identity_membership``, the stable identity a staged/
superseded/resolved identity team_drift_changes row is keyed by.
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
    change_id = module.change_id_for_identity_membership(
        org_id=case["org_id"],
        team_id=case["team_id"],
        provider=case["provider"],
        member_id=case["member_id"],
        field=case["field"],
        old_value_json=case["old_value_json"],
        new_value_json=case["new_value_json"],
    )
    return {"change_id": change_id}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="identity-drift/review/change-id",
        build_row=_build_row,
        # Scalar hexdigest return, same reasoning as
        # team-catalog_change-id.py's identical single-field declaration.
        reflected_fields=lambda: frozenset({"change_id"}),
    )
)
