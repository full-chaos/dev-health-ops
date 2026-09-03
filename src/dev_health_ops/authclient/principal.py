"""The ``principal.v1`` wire type, parsed only after schema validation.

Implements TRD section 8's canonical principal document, extended only where
guardrail G-31 requires more than the TRD's example shows (all four revisions
rather than two).

``from_wire`` validates BEFORE it parses. The order is the point: a parser
that reads fields first and validates afterwards has already made decisions on
unvalidated input by the time the validator speaks.

FOUR DELIBERATE OMISSIONS, none an oversight to repair. There is no ``tier``
or ``licensed_features`` (ACP-ADR-07 decision 2, G-14, and TRD section 8's own
"Grants and entitlements are not identity claims"); no ``permissions`` (TRD
section 11, G-14); no ``role`` (ACP-ADR-05 decision 4, G-27); no
``is_superuser`` (TRD section 3, G-23 -- platform authority moves to a
separate namespace resolved against current state). The existing
effective-principal envelope
(``cmd/query-api/internal/principal/claims.go``) carries all four; it is the
compatibility implementation this migration retires.
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
    """Parse one RFC 3339 timestamp the schema has already accepted.

    Both a ``pattern`` (the cross-language floor all three validators enforce)
    and a ``format`` assertion have already run, so this is a parse of
    validated input rather than a second, weaker validation. It is still
    guarded: ``fromisoformat`` and RFC 3339 are not the same grammar in every
    direction, and a disagreement must surface as an error naming the field
    rather than as a traceback.
    """
    try:
        return datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ContractError(
            f"{SURFACE}: /{field} passed schema validation but is not parseable "
            f"by datetime.fromisoformat: {raw!r} ({exc})"
        ) from exc


@dataclass(frozen=True, slots=True)
class Credential:
    """The credential that authenticated the principal.

    Present because TRD section 2 principle 1 makes credential class part of
    the route contract, and G-2/G-3 forbid treating every bearer value as one
    class or discovering the class by validator fan-out. ``credential_id`` is
    an identifier, never the secret (G-16, G-17).
    """

    cls: str
    credential_id: str
    issuer: str
    audience: str
    scopes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Authentication:
    """How and when the principal authenticated.

    ``assurance`` is an authorization input, not decoration: G-31 names it as
    part of an allow-cache key and G-51 requires step-up gating for high-risk
    actions during impersonation.
    """

    methods: tuple[str, ...]
    authenticated_at: datetime
    assurance: str


@dataclass(frozen=True, slots=True)
class Delegation:
    """One hop in the actor chain.

    ACP-ADR-03 bounds a delegated session at 15 minutes; G-52 requires an
    independent id, a reason, an explicit permitted action set and independent
    revocability. G-50: while delegated, permissions derive from the effective
    subject -- real-actor platform authority does not implicitly authorize
    target actions.
    """

    actor_principal_id: str
    delegation_id: str
    reason: str
    started_at: datetime
    expires_at: datetime
    permitted_actions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Principal:
    """A resolved ``principal.v1`` document (TRD section 8)."""

    principal_id: str
    principal_type: str
    subject_id: str | None
    organization_id: str | None
    credential: Credential
    authentication: Authentication
    actor_chain: tuple[Delegation, ...]
    membership_revision: int
    policy_revision: int
    grant_revision: int
    entitlement_revision: int
    issued_at: datetime
    expires_at: datetime

    @classmethod
    def from_wire(cls, document: Any, root: Path | None = None) -> Principal:
        """Validate *document* against ``principal.v1``, then parse it."""
        validate(SURFACE, document, root)

        raw_credential = document["credential"]
        raw_authentication = document["authentication"]
        return cls(
            principal_id=document["principal_id"],
            principal_type=document["principal_type"],
            subject_id=document["subject_id"],
            organization_id=document["organization_id"],
            credential=Credential(
                cls=raw_credential["class"],
                credential_id=raw_credential["credential_id"],
                issuer=raw_credential["issuer"],
                audience=raw_credential["audience"],
                scopes=tuple(raw_credential["scopes"]),
            ),
            authentication=Authentication(
                methods=tuple(raw_authentication["methods"]),
                authenticated_at=_parse_timestamp(
                    raw_authentication["authenticated_at"],
                    "authentication/authenticated_at",
                ),
                assurance=raw_authentication["assurance"],
            ),
            actor_chain=tuple(
                Delegation(
                    actor_principal_id=hop["actor_principal_id"],
                    delegation_id=hop["delegation_id"],
                    reason=hop["reason"],
                    started_at=_parse_timestamp(
                        hop["started_at"], f"actor_chain/{index}/started_at"
                    ),
                    expires_at=_parse_timestamp(
                        hop["expires_at"], f"actor_chain/{index}/expires_at"
                    ),
                    permitted_actions=tuple(hop["permitted_actions"]),
                )
                for index, hop in enumerate(document["actor_chain"])
            ),
            membership_revision=document["membership_revision"],
            policy_revision=document["policy_revision"],
            grant_revision=document["grant_revision"],
            entitlement_revision=document["entitlement_revision"],
            issued_at=_parse_timestamp(document["issued_at"], "issued_at"),
            expires_at=_parse_timestamp(document["expires_at"], "expires_at"),
        )

    @property
    def is_delegated(self) -> bool:
        """True when at least one actor is acting as this principal."""
        return bool(self.actor_chain)

    @property
    def real_actor_principal_id(self) -> str:
        """The originating real actor, or this principal when not delegated.

        G-49 requires the real actor and the effective subject to remain
        distinguishable through token, decision, audit and downstream
        execution. Exposed as one named accessor so that question has one
        answer rather than being re-derived at each call site.
        """
        if self.actor_chain:
            return self.actor_chain[0].actor_principal_id
        return self.principal_id

    def effective_deadline(self) -> datetime:
        """The earliest of this principal's expiry and every delegation's.

        Takes the minimum rather than trusting ACP-ADR-03's requirement that a
        delegated session be strictly shorter: a bound that relies on an
        invariant it does not check is only a bound while the invariant holds.
        """
        deadlines = [self.expires_at, *(hop.expires_at for hop in self.actor_chain)]
        return min(deadlines)

    def cache_key_dimensions(self) -> tuple[object, ...]:
        """Every dimension G-31 requires an allow-cache key to bind.

        G-31: "An allow cache key includes principal, actor chain,
        credential/session, organization, action, resource, policy revision,
        membership/grant revisions, entitlement revision when applicable, and
        relevant assurance." Action and resource belong to the decision, not
        to the principal, so a caller appends those; everything the PRINCIPAL
        contributes is returned here, in one place, so a consumer cannot bind
        a subset by accident. Binding a subset is a cache a revision bump
        cannot invalidate.
        """
        return (
            self.principal_id,
            tuple(hop.delegation_id for hop in self.actor_chain),
            self.credential.credential_id,
            self.organization_id,
            self.policy_revision,
            self.membership_revision,
            self.grant_revision,
            self.entitlement_revision,
            self.authentication.assurance,
        )
