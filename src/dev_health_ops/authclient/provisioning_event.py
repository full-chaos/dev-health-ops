"""Python client for the ``provisioning-event.v1`` and ``audit-event.v1`` surfaces.

Mirrors ``internal/auth/contracts/provisioning_event.go``. Raw bytes are
parsed so duplicate members can be refused before they are collapsed, and
every integer field is guarded on its raw token: ``json`` returns a ``float``
for exactly the decimal- or exponent-form tokens, so a float is refused.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Final, Literal

from dev_health_ops.authclient.contracts import (
    ContractError,
    refuse_bool,
    refuse_decimal_form,
    validate,
)

PROVISIONING_SURFACE: Final = "provisioning-event.v1"
AUDIT_SURFACE: Final = "audit-event.v1"

Disposition = Literal["apply", "ignore", "gap"]


def _refuse_duplicate_members(
    surface: str,
) -> Any:
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


def _require_int(document: dict[str, Any], name: str, surface: str) -> int | None:
    value = document.get(name)
    if value is None:
        return None
    refuse_bool(value, surface, name, "an integer")
    refuse_decimal_form(value, surface, name)
    assert isinstance(value, int)
    return value


@dataclass(frozen=True, slots=True)
class ProvisioningEvent:
    """One outbox row as a consumer receives it."""

    aggregate_type: str
    aggregate_id: str
    event_type: str
    idempotency_key: str
    created_at: str
    revision: int
    change: dict[str, Any] = field(default_factory=dict)
    organization_id: str | None = None
    actor_principal_id: str | None = None
    request_id: str | None = None

    def dedupe_key(self) -> str:
        """aggregate_type:aggregate_id:revision, what idempotency_key must equal."""
        return f"{self.aggregate_type}:{self.aggregate_id}:{self.revision}"


def parse_provisioning_event(raw: bytes | str) -> ProvisioningEvent:
    document = _load(raw, PROVISIONING_SURFACE)
    payload = document["payload"]
    revision = _require_int(payload, "revision", PROVISIONING_SURFACE)
    assert revision is not None  # required by the schema
    event = ProvisioningEvent(
        aggregate_type=document["aggregate_type"],
        aggregate_id=document["aggregate_id"],
        event_type=document["event_type"],
        idempotency_key=document["idempotency_key"],
        created_at=document["created_at"],
        revision=revision,
        change=payload["change"],
        organization_id=payload.get("organization_id"),
        actor_principal_id=payload.get("actor_principal_id"),
        request_id=payload.get("request_id"),
    )
    # A pattern cannot compare two members. A key that disagrees with the
    # payload would let two revisions collide in the UNIQUE index, or one
    # revision be delivered under two keys.
    if event.idempotency_key != event.dedupe_key():
        raise ContractError(
            f"{PROVISIONING_SURFACE}: idempotency_key {event.idempotency_key!r} does "
            f"not equal {event.dedupe_key()!r} "
            "(aggregate_type:aggregate_id:payload.revision)"
        )
    return event


@dataclass(frozen=True, slots=True)
class AuditEvent:
    event_type: str
    occurred_at: str
    outcome: str
    policy_revision: int | None
    aggregate_revision: int | None


def parse_audit_event(raw: bytes | str) -> AuditEvent:
    document = _load(raw, AUDIT_SURFACE)
    return AuditEvent(
        event_type=document["event_type"],
        occurred_at=document["occurred_at"],
        outcome=document["outcome"],
        policy_revision=_require_int(document, "policy_revision", AUDIT_SURFACE),
        aggregate_revision=_require_int(
            document["attributes"], "aggregate_revision", AUDIT_SURFACE
        ),
    )


class ConsumerCursor:
    """Last applied revision per aggregate. Orders nothing across aggregates."""

    def __init__(self) -> None:
        self._last: dict[str, int] = {}

    def observe(
        self, aggregate_type: str, aggregate_id: str, revision: int
    ) -> Disposition:
        """Classify an event; advance the cursor only on ``apply``."""
        key = f"{aggregate_type}:{aggregate_id}"
        last = self._last.get(key, 0)
        if revision <= last:
            return "ignore"
        if revision == last + 1:
            self._last[key] = revision
            return "apply"
        return "gap"
