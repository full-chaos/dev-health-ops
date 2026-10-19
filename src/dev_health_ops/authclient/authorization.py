"""The authorization wire types, parsed only after schema validation.

``authorization_decision.v1`` and the batch forms of TRD sections 8 and 14.
The Go twin is ``internal/auth/contracts/authorization.go``; both read the SAME
schemas and golden fixtures.

THE FAIL-CLOSED RULE: any failure here means "no valid decision" and the caller
denies. A malformed document is never read as an allow, so every check raises
rather than returning a default.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dev_health_ops.authclient.contracts import ContractError, validate
from dev_health_ops.authclient.error_envelope import _refuse_duplicate_members
from dev_health_ops.authclient.principal import _parse_revision, _parse_timestamp

REQUEST_SURFACE = "authorization_request.v1"
DECISION_SURFACE = "authorization_decision.v1"
BATCH_REQUEST_SURFACE = "authorization_batch_request.v1"
BATCH_RESPONSE_SURFACE = "authorization_batch_response.v1"


@dataclass(frozen=True, slots=True)
class AuthorizationObligations:
    tenant_scope: str | None
    repository_allowlist: tuple[str, ...] | None
    redaction: tuple[str, ...]
    minimum_assurance: str | None
    step_up: bool


@dataclass(frozen=True, slots=True)
class AuthorizationDecision:
    decision_id: str
    allowed: bool
    reason_code: str
    principal_id: str
    organization_id: str | None
    policy_revision: int
    expires_at: datetime
    obligations: AuthorizationObligations | None


def _load(raw: bytes | str, surface: str) -> Any:
    """Decode RAW bytes, refusing duplicate members, then validate the schema.

    Duplicates must be refused here and not after decoding: a repeated
    ``allowed`` keeps the last value, so the evidence is destroyed by the decode.
    """
    try:
        document = json.loads(raw, object_pairs_hook=_refuse_duplicate_members)
    except json.JSONDecodeError as exc:
        raise ContractError(f"{surface}: document is not valid JSON: {exc}") from exc
    validate(surface, document)
    return document


def _decision(document: dict[str, Any]) -> AuthorizationDecision:
    obligations = document.get("obligations")
    allowlist = None if obligations is None else obligations.get("repository_allowlist")
    return AuthorizationDecision(
        decision_id=document["decision_id"],
        allowed=document["allowed"] is True,
        reason_code=document["reason_code"],
        principal_id=document["principal_id"],
        organization_id=document["organization_id"],
        policy_revision=_parse_revision(document["policy_revision"], "policy_revision"),
        expires_at=_parse_timestamp(document["expires_at"], "expires_at"),
        obligations=None
        if obligations is None
        else AuthorizationObligations(
            tenant_scope=obligations.get("tenant_scope"),
            repository_allowlist=None if allowlist is None else tuple(allowlist),
            redaction=tuple(obligations.get("redaction", ())),
            minimum_assurance=obligations.get("minimum_assurance"),
            step_up=obligations.get("step_up") is True,
        ),
    )


def _refuse_expired(
    decision: AuthorizationDecision, now: datetime, surface: str
) -> AuthorizationDecision:
    """Refuse a decision whose ``expires_at`` is not strictly after *now*.

    Expiry is the bound on a stale allow (TRD section 14), so a parser that only
    read the timestamp would return ``allowed=True`` for a decision nobody may
    reuse. An expired deny is refused too, so one rule holds for every verdict.
    *now* is required, with no default: the caller states which instant it
    means, and a test does not depend on the date it runs.
    """
    if now.tzinfo is None:
        raise ContractError(f"{surface}: `now` must be timezone-aware")
    if decision.expires_at <= now:
        raise ContractError(
            f"{surface}: decision {decision.decision_id} expired at "
            f"{decision.expires_at.isoformat()} (now {now.isoformat()}); "
            "it must not be reused"
        )
    return decision


def parse_decision_bytes(raw: bytes | str, *, now: datetime) -> AuthorizationDecision:
    """Parse one decision from the RAW response body, or raise."""
    return _refuse_expired(
        _decision(_load(raw, DECISION_SURFACE)), now, DECISION_SURFACE
    )


def parse_batch_response_bytes(
    request_raw: bytes | str, response_raw: bytes | str, *, now: datetime
) -> list[AuthorizationDecision]:
    """Parse a batch response against the batch request it answers.

    The response must carry exactly one decision per request, and the decision
    at position ``i`` must declare ``index == i``. These compare TWO documents,
    so no schema can express them. A response failing either is refused WHOLE:
    returning the items that happen to line up would let one item's allow be
    applied to another item's resource.
    """
    request = _load(request_raw, BATCH_REQUEST_SURFACE)
    response = _load(response_raw, BATCH_RESPONSE_SURFACE)
    items = response["decisions"]
    if len(items) != len(request["requests"]):
        raise ContractError(
            f"{BATCH_RESPONSE_SURFACE}: {len(items)} decisions for "
            f"{len(request['requests'])} requests; refusing the whole response"
        )
    decisions = []
    for position, item in enumerate(items):
        index = _parse_revision(item["index"], "index")
        if index != position:
            raise ContractError(
                f"{BATCH_RESPONSE_SURFACE}: decision at position {position} "
                f"declares index {index}; refusing the whole response"
            )
        decisions.append(
            _refuse_expired(_decision(item["decision"]), now, BATCH_RESPONSE_SURFACE)
        )
    return decisions
