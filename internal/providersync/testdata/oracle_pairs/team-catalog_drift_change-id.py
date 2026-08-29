"""Live Python oracle for the team-level drift-change id hash (CHAOS-4444)
-- clickhouse_team_drift_projector.py's ``change_id_for_team_field``, the
stable identity a staged/superseded/resolved team_drift_changes row is keyed
by. This is what makes Go's canonical-JSON encoder (team_drift_json.go)
matter: any divergence from Python's json.dumps(sort_keys=True,
separators=(",", ":"), default=str) changes this hash.
"""

from __future__ import annotations

import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PRODUCER_SOURCE = (
    REPO_ROOT
    / "src/dev_health_ops/api/services/configuration/clickhouse_team_drift_projector.py"
)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    change_id = module.change_id_for_team_field(
        org_id=case["org_id"],
        team_id=case["team_id"],
        field=case["field"],
        old_value_json=case["old_value_json"],
        new_value_json=case["new_value_json"],
    )
    return {"change_id": change_id}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="team-catalog/drift/change-id",
        build_row=_build_row,
        # change_id_for_team_field returns a bare sha256 hexdigest string,
        # not a dict literal -- dict_literal_keys's AST reflection has no
        # dict return to inspect, so the single-field set is declared
        # directly (mirrors github_team-catalog_facets.py's identical
        # reasoning for `{"facets": ...}`).
        reflected_fields=lambda: frozenset({"change_id"}),
    )
)
