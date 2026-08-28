"""Live Python oracle for GitHub membership identity-facet resolution
(CHAOS-4434). Proves the Go port's simplifying assumption -- that
providers/identity.py's alias_to_canonical map is effectively empty in this
deployment's checked-in src/dev_health_ops/config/identity_mapping.yaml, so
the no-email identity and the provider-qualified identity coincide -- holds
by comparing against the REAL, live identity resolver, not a restated copy
of its ladder logic.
"""

from __future__ import annotations

import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PRODUCER_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/identity.py"


def _build_facets(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    resolver = module.load_identity_resolver()
    facets = resolver.membership_facets(
        provider="github", username=case["login"], email=case.get("email")
    )
    return {"facets": facets}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/team-catalog/facets",
        build_row=_build_facets,
        reflected_fields=lambda: frozenset({"facets"}),
    )
)
