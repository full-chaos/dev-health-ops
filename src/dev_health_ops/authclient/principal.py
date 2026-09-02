"""The ``principal.v1`` wire type, parsed only after schema validation.

``from_wire`` validates BEFORE it parses. The order is the point: a parser
that reads fields first and validates afterwards has already made decisions
on unvalidated input by the time the validator speaks, and a parser that
never validates at all is how a wire field nobody declared reaches business
logic.

Deliberately absent, and load-bearing: there is no ``tier`` and no
``licensed_features``. ACP-ADR-07 decision 2 (Accepted 2026-09-02) makes
entitlement an input to a decision and never a claim in a credential, and
G-14 forbids entitlement travelling inside a credential by name. The existing
effective-principal envelope carries both today
(``cmd/query-api/internal/principal/claims.go``); their absence here is the
ADR, not an omission to be repaired. Entitlement reaches a caller through an
authorization decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dev_health_ops.authclient.contracts import ContractError, validate

SCHEMA_VERSION = "principal.v1"
SURFACE = "principal.v1"


def _parse_timestamp(raw: str, field: str) -> datetime:
    """Parse one RFC 3339 timestamp that the schema has already accepted.

    The schema's ``format: date-time`` assertion runs first (and refuses to
    run at all if its backing implementation is missing -- see
    ``contracts._require_format_assertion``), so this is a parse of
    already-validated input, not a second, weaker validation. It is still
    guarded, because ``datetime.fromisoformat`` and RFC 3339 are not the same
    grammar in every direction and a disagreement must surface as an error
    naming the field rather than as a traceback.
    """
    try:
        return datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ContractError(
            f"{SURFACE}: /{field} passed schema format validation but is not "
            f"parseable by datetime.fromisoformat: {raw!r} ({exc})"
        ) from exc


@dataclass(frozen=True, slots=True)
class Impersonation:
    """An active impersonation session bounded independently of the principal.

    ACP-ADR-03 fixes the delegated/impersonation session at 15 minutes and
    independently revocable, shorter than the base session (G-52). Neither
    ``expires_at`` here nor the enclosing principal's widens the other: the
    effective bound is the earlier of the two, and a caller that checks only
    one of them is checking the wrong one half the time.
    """

    impersonated_by: str
    started_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class Revisions:
    """The monotonic revisions a principal was resolved against.

    ACP-ADR-05 decision 3 requires every allow-cache key to bind all four
    (G-31), so a revision bump invalidates by construction rather than by an
    explicit purge. ``entitlement_revision`` says WHICH entitlement snapshot
    was current; it never says what that snapshot granted.
    """

    policy_revision: int
    membership_revision: int
    grant_revision: int
    entitlement_revision: int


@dataclass(frozen=True, slots=True)
class Principal:
    """A resolved ``principal.v1`` document."""

    principal_id: str
    principal_type: str
    organization_id: str | None
    role: str | None
    permissions: tuple[str, ...]
    is_superuser: bool
    is_superuser_verified: bool
    token_version: int
    impersonation: Impersonation | None
    revisions: Revisions
    issued_at: datetime
    expires_at: datetime
    issuer: str
    audience: str

    @classmethod
    def from_wire(cls, document: Any, root: Path | None = None) -> Principal:
        """Validate *document* against ``principal.v1``, then parse it."""
        validate(SURFACE, document, root)

        raw_impersonation = document["impersonation"]
        impersonation = None
        if raw_impersonation is not None:
            impersonation = Impersonation(
                impersonated_by=raw_impersonation["impersonated_by"],
                started_at=_parse_timestamp(
                    raw_impersonation["started_at"], "impersonation/started_at"
                ),
                expires_at=_parse_timestamp(
                    raw_impersonation["expires_at"], "impersonation/expires_at"
                ),
            )

        raw_revisions = document["revisions"]
        return cls(
            principal_id=document["principal_id"],
            principal_type=document["principal_type"],
            organization_id=document["organization_id"],
            role=document.get("role"),
            permissions=tuple(document["permissions"]),
            is_superuser=document["is_superuser"],
            is_superuser_verified=document["is_superuser_verified"],
            token_version=document["token_version"],
            impersonation=impersonation,
            revisions=Revisions(
                policy_revision=raw_revisions["policy_revision"],
                membership_revision=raw_revisions["membership_revision"],
                grant_revision=raw_revisions["grant_revision"],
                entitlement_revision=raw_revisions["entitlement_revision"],
            ),
            issued_at=_parse_timestamp(document["issued_at"], "issued_at"),
            expires_at=_parse_timestamp(document["expires_at"], "expires_at"),
            issuer=document["issuer"],
            audience=document["audience"],
        )

    def has_permission(self, action: str) -> bool:
        """Membership test against the resolved action set.

        This is a lookup, not an authorization decision. ACP-ADR-05 decision 4
        requires an explicit action name at the call site and forbids an
        ``is_admin``-style check as the FINAL decision (G-27) -- so a caller
        uses this to read the principal's own claim, and takes the actual
        decision from the authorization surface, which also binds entitlement
        (ACP-ADR-07 decision 3: entitlement and authorization are independent
        gates and BOTH must pass; a role grants no product and a paid product
        grants no action).
        """
        return action in self.permissions

    @property
    def is_impersonated(self) -> bool:
        """True when an operator is acting as this principal."""
        return self.impersonation is not None
