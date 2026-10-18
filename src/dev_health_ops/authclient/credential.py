"""Python client for the credential metadata surfaces.

``credential-entry.v1``, ``credential-list.v1`` and ``credential-receipt.v1``.
Mirrors ``internal/auth/contracts/credential.go``. Only the receipt carries a
secret; the entry and list parsers have no field for one.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

from dev_health_ops.authclient.contracts import (
    ContractError,
    parse_rfc3339,
    validate,
)

ENTRY_SURFACE: Final = "credential-entry.v1"
LIST_SURFACE: Final = "credential-list.v1"
RECEIPT_SURFACE: Final = "credential-receipt.v1"


def _refuse_duplicate_members(surface: str) -> Any:
    def hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        seen: set[str] = set()
        for key, _ in pairs:
            if key in seen:
                raise ContractError(
                    f"{surface}: duplicate object member {key!r}. Parsers keep the "
                    "last value, so an earlier one would never reach validation"
                )
            seen.add(key)
        return dict(pairs)

    return hook


def _load(raw: bytes | str, surface: str) -> dict[str, Any]:
    try:
        document = json.loads(raw, object_pairs_hook=_refuse_duplicate_members(surface))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ContractError(f"{surface}: document is not valid JSON: {exc}") from exc
    validate(surface, document)
    assert isinstance(document, dict)
    return document


@dataclass(frozen=True, slots=True)
class CredentialBody:
    """A registry entry without its schema_version. No secret member exists."""

    id: str
    principal_id: str
    credential_class: str
    display_prefix: str
    domain_owner: str
    audiences: tuple[str, ...]
    scopes: tuple[str, ...]
    status: str
    created_at: datetime
    updated_at: datetime
    fingerprint: str | None = None
    external_reference: str | None = None
    rotation_parent_id: str | None = None
    expires_at: datetime | None = None
    last_used_at: datetime | None = None
    revoked_at: datetime | None = None
    revoked_reason: str | None = None
    created_by: str | None = None


def _opt_ts(body: dict[str, Any], name: str, surface: str) -> datetime | None:
    value = body.get(name)
    return None if value is None else parse_rfc3339(value, surface, name)


def _body(body: dict[str, Any], surface: str, now: datetime) -> CredentialBody:
    parsed = CredentialBody(
        id=body["id"],
        principal_id=body["principal_id"],
        credential_class=body["credential_class"],
        display_prefix=body["display_prefix"],
        domain_owner=body["domain_owner"],
        audiences=tuple(body["audiences"]),
        scopes=tuple(body["scopes"]),
        status=body["status"],
        created_at=parse_rfc3339(body["created_at"], surface, "created_at"),
        updated_at=parse_rfc3339(body["updated_at"], surface, "updated_at"),
        fingerprint=body.get("fingerprint"),
        external_reference=body.get("external_reference"),
        rotation_parent_id=body.get("rotation_parent_id"),
        expires_at=_opt_ts(body, "expires_at", surface),
        last_used_at=_opt_ts(body, "last_used_at", surface),
        revoked_at=_opt_ts(body, "revoked_at", surface),
        revoked_reason=body.get("revoked_reason"),
        created_by=body.get("created_by"),
    )
    _check_status_at(parsed, surface, now)
    return parsed


def _check_status_at(body: CredentialBody, surface: str, now: datetime) -> None:
    """Status is derived: revoked_at set -> revoked; else expires_at <= now ->
    expired; else active. The schema pins its shape; only a clock compares."""
    want = "active"
    if body.revoked_at is not None:
        want = "revoked"
    elif body.expires_at is not None and body.expires_at <= now:
        want = "expired"
    if body.status != want:
        raise ContractError(
            f"{surface}: status {body.status!r} disagrees with the timestamps at "
            f"{now.isoformat()}, which derive {want!r} (revoked_at set -> revoked; "
            "else expires_at <= now -> expired; else active)"
        )


def _now(now: datetime | None) -> datetime:
    return now if now is not None else datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class CredentialEntry:
    body: CredentialBody


def parse_credential_entry(
    raw: bytes | str, now: datetime | None = None
) -> CredentialEntry:
    document = _load(raw, ENTRY_SURFACE)
    body = {k: v for k, v in document.items() if k != "schema_version"}
    return CredentialEntry(body=_body(body, ENTRY_SURFACE, _now(now)))


@dataclass(frozen=True, slots=True)
class CredentialList:
    items: tuple[CredentialBody, ...]


def parse_credential_list(
    raw: bytes | str, now: datetime | None = None
) -> CredentialList:
    document = _load(raw, LIST_SURFACE)
    return CredentialList(
        items=tuple(_body(item, LIST_SURFACE, _now(now)) for item in document["items"])
    )


@dataclass(frozen=True, slots=True)
class CredentialReceipt:
    kind: str
    credential: CredentialBody
    secret: str


def parse_credential_receipt(
    raw: bytes | str, now: datetime | None = None
) -> CredentialReceipt:
    document = _load(raw, RECEIPT_SURFACE)
    credential = _body(document["credential"], RECEIPT_SURFACE, _now(now))
    if not document["secret"].startswith(credential.display_prefix):
        raise ContractError(
            f"{RECEIPT_SURFACE}: the secret does not begin with the credential's "
            "display_prefix (the secret is not repeated here)"
        )
    return CredentialReceipt(
        kind=document["kind"], credential=credential, secret=document["secret"]
    )


def check_credential_transition(prev: CredentialBody, nxt: CredentialBody) -> None:
    """Whether *nxt* may follow *prev* for one credential.

    Revocation is idempotent and monotonic (TRD section 12): once revoked the
    status, instant and reason never change, so active-after-revoked is refused.
    Identity, class, domain owner, external reference, lineage, display prefix,
    fingerprint and creation never change; scopes and audiences may narrow but
    never widen in place. Mirrors the database rules of migration 0007 and the Go
    ``CheckCredentialTransition``.
    """
    if prev.id != nxt.id:
        raise ContractError(
            f"{ENTRY_SURFACE}: transition between different credentials "
            f"{prev.id!r} and {nxt.id!r}"
        )
    if prev.status == "revoked" and (
        nxt.status != "revoked"
        or nxt.revoked_at != prev.revoked_at
        or nxt.revoked_reason != prev.revoked_reason
    ):
        raise ContractError(
            f"{ENTRY_SURFACE}: credential {prev.id} was revoked; revocation is "
            "monotonic and idempotent (status, revoked_at and revoked_reason never change)"
        )
    immutable = (
        "principal_id",
        "credential_class",
        "domain_owner",
        "external_reference",
        "rotation_parent_id",
        "display_prefix",
        "fingerprint",
        "created_by",
        "created_at",
    )
    if any(getattr(prev, name) != getattr(nxt, name) for name in immutable):
        raise ContractError(
            f"{ENTRY_SURFACE}: credential {prev.id} identity, lineage and "
            "verifier-adjacent members are immutable"
        )
    if not (
        set(nxt.scopes) <= set(prev.scopes)
        and set(nxt.audiences) <= set(prev.audiences)
    ):
        raise ContractError(
            f"{ENTRY_SURFACE}: credential {prev.id} scopes and audiences may "
            "narrow in place, never widen"
        )
