"""Python client for the Auth Control Plane v1 wire contracts.

Wave 1 scope (CHAOS-4884) is the ``principal.v1`` surface: schema-validated
parsing of a resolved principal. The verification and decision client grows
here as the authorization request/decision/batch surface lands; this package
deliberately ships no decision call yet rather than a stub that would have to
be believed.
"""

from dev_health_ops.authclient.contracts import (
    ContractError,
    Violation,
    contracts_dir,
    repo_root,
    validate,
    validator_for,
    violations,
)
from dev_health_ops.authclient.principal import (
    SCHEMA_VERSION,
    Impersonation,
    Principal,
    Revisions,
)

__all__ = [
    "SCHEMA_VERSION",
    "ContractError",
    "Impersonation",
    "Principal",
    "Revisions",
    "Violation",
    "contracts_dir",
    "repo_root",
    "validate",
    "validator_for",
    "violations",
]
