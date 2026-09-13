"""Live Python oracle for providers/identity.py's IdentityResolver.resolve()
and membership_facets(), exercised against a SEEDED alias config rather than
this deployment's checked-in empty default.

The existing github/team-catalog/facets pair only ever runs under the
checked-in, empty identity_mapping.yaml (`identities: []`), so it can prove
the Go port matches Python's UNALIASED fallback ladder but says nothing about
whether an org that actually populates the alias config gets matching
behaviour out of the native Go team-catalog collectors. This pair fills that
gap: the Go test sets IDENTITY_MAPPING_PATH to a temp file it seeds with
alias entries before invoking this CLI, so both the live resolver constructed
here and internal/identityalias's Go port read the SAME config.
"""

from __future__ import annotations

import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PRODUCER_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/identity.py"


def _build_resolve(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    resolver = module.load_identity_resolver()
    resolved = resolver.resolve(
        provider=case["provider"],
        email=case.get("email"),
        username=case.get("username"),
        account_id=case.get("account_id"),
        display_name=case.get("display_name"),
    )
    facets = resolver.membership_facets(
        provider=case["provider"],
        username=case.get("username"),
        account_id=case.get("account_id"),
        email=case.get("email"),
    )
    return {"resolved": resolved, "facets": facets}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="identity/alias/resolve",
        build_row=_build_resolve,
        reflected_fields=lambda: frozenset({"resolved", "facets"}),
    )
)
