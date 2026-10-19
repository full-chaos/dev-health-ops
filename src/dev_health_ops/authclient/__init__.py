"""Python client for the Auth Control Plane v1 wire contracts.

Wave 1 scope (CHAOS-4884) is the ``principal.v1`` surface: schema-validated
parsing of a resolved principal. The verification and decision client grows
here as the authorization request/decision/batch surface lands; this package
deliberately ships no decision call yet rather than a stub that would have to
be believed.
"""

from dev_health_ops.authclient.authorization import (
    AuthorizationDecision,
    AuthorizationObligations,
    parse_batch_response_bytes,
    parse_decision_bytes,
)
from dev_health_ops.authclient.contracts import (
    ContractError,
    Violation,
    contracts_dir,
    parse_rfc3339,
    repo_root,
    validate,
    validator_for,
    violations,
)
from dev_health_ops.authclient.credential import (
    CredentialBody,
    CredentialEntry,
    CredentialList,
    CredentialReceipt,
    check_credential_transition,
    parse_credential_entry,
    parse_credential_list,
    parse_credential_receipt,
)
from dev_health_ops.authclient.discovery import Discovery, parse_discovery
from dev_health_ops.authclient.error_envelope import (
    ErrorEnvelope,
    parse_bytes,
    parse_decoded,
)
from dev_health_ops.authclient.principal import (
    SCHEMA_VERSION,
    Authentication,
    Credential,
    Delegation,
    Principal,
)
from dev_health_ops.authclient.provisioning_event import (
    AuditEvent,
    ConsumerCursor,
    ProvisioningEvent,
    parse_audit_event,
    parse_provisioning_event,
)

__all__ = [
    "AuthorizationDecision",
    "AuthorizationObligations",
    "parse_batch_response_bytes",
    "parse_decision_bytes",
    "CredentialBody",
    "CredentialEntry",
    "CredentialList",
    "CredentialReceipt",
    "check_credential_transition",
    "parse_credential_entry",
    "parse_credential_list",
    "parse_credential_receipt",
    "AuditEvent",
    "ConsumerCursor",
    "ProvisioningEvent",
    "parse_audit_event",
    "parse_provisioning_event",
    "ErrorEnvelope",
    "SCHEMA_VERSION",
    "Authentication",
    "ContractError",
    "Credential",
    "Delegation",
    "Discovery",
    "Principal",
    "Violation",
    "contracts_dir",
    "parse_bytes",
    "parse_decoded",
    "parse_rfc3339",
    "parse_discovery",
    "repo_root",
    "validate",
    "validator_for",
    "violations",
]
