"""The ``error.v1`` wire type, parsed only after schema validation.

Named ``error_envelope`` rather than ``error`` deliberately: a module called
``error`` inside a client package reads as "this package's exception type",
which is :class:`~dev_health_ops.authclient.contracts.ContractError` and lives
elsewhere. This module is about the wire DOCUMENT a server sends.

WHAT THIS ADDS OVER SCHEMA VALIDATION. Two checks, and both exist because a
validator is handed a document rather than an exchange:

* the envelope's ``status`` must equal the HTTP status the response arrived
  with. The schema cannot see the response line, so it cannot check this, and
  the duplicated field is either enforced here or decorative.
* ``occurred_at`` must not be implausibly far in the future. The schema
  constrains the SHAPE of a timestamp, never its plausibility.

Neither is expressible in JSON Schema, which is exactly why the fixtures for
them are filed under ``reject_by_client`` in the manifest and not under
``reject`` -- they are valid documents that a client must nonetheless refuse.
Filing them as ``reject`` would assert the schema catches something it
provably does not.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final

from dev_health_ops.authclient.contracts import ContractError, validate

SURFACE: Final = "error.v1"
SCHEMA_VERSION: Final = "error.v1"

#: How far ahead of the reader's clock an ``occurred_at`` may sit before the
#: envelope is refused.
#:
#: Five minutes is the conventional allowance for unsynchronised hosts and is
#: the same order as the token-validation leeway elsewhere in this system. The
#: bound is one-directional ON PURPOSE: an error stamped in the PAST is normal
#: (queueing, retries, slow logs) and is not checked, while one stamped in the
#: future cannot be explained by anything but skew or a wrong clock, and it
#: reorders an audit trail silently once stored.
#:
#: This is a client policy, not a contract term. A different deployment may
#: reasonably choose a different tolerance; what it may not do is skip the
#: check, because "no bound" makes the timestamp unusable for ordering.
MAX_CLOCK_SKEW: Final = timedelta(minutes=5)

#: The statuses TRD section 18 defines as transient, and therefore the ones the
#: schema's ``if``/``then`` requires ``retry_after_seconds`` on. Duplicated here
#: only so :attr:`ErrorEnvelope.is_transient` can answer without re-reading the
#: schema; the schema remains the authority and
#: ``test_transient_statuses_match_the_schema`` asserts the two agree, so this
#: cannot drift into a second source of truth.
TRANSIENT_STATUSES: Final = frozenset({429, 503})


def _refuse_duplicate_members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """``object_pairs_hook`` that rejects a repeated member instead of collapsing it.

    Every JSON parser collapses duplicates to one survivor -- Python and Go both
    keep the LAST -- so by the time a validator sees a decoded object, the
    earlier value is gone. The schema then validates the survivor and reports
    success about a document whose bytes contained something else.

    That defeats the subtractive contract directly. ``reason_code``'s pattern
    makes prose and addresses unrepresentable, but::

        {"reason_code": "credential_for_bob@example.com",
         "reason_code": "grant_absent"}

    validates, because only ``grant_absent`` survives to be checked. The
    disclosure TRD section 18 forbids is sitting in the response body, and this
    is a WIRE contract -- the guarantee is about bytes, not about the object
    they decode to. Found by codex round 2.

    RFC 8259 permits duplicate names and calls the behaviour unpredictable,
    which is precisely why a security contract refuses them rather than
    inheriting whichever survivor the parser happened to pick.
    """
    seen: set[str] = set()
    for key, _ in pairs:
        if key in seen:
            raise ContractError(
                f"{SURFACE}: duplicate object member {key!r}. Parsers keep the "
                f"last value, so an earlier one would never reach validation -- "
                f"a document can carry a forbidden value on the wire and still "
                f"validate"
            )
        seen.add(key)
    return dict(pairs)


def parse_bytes(
    raw: bytes | str,
    http_status: int,
    *,
    now: datetime | None = None,
) -> ErrorEnvelope:
    """Parse an ``error.v1`` document from the RAW response body.

    **Prefer this over :func:`parse` wherever the bytes are available.** It is
    the only entry point that can refuse duplicate members, because that check
    is impossible once the document has been decoded -- the evidence is
    destroyed by the decode itself.

    :func:`parse` remains for callers holding an already-decoded object, and it
    is documented there that such a caller has already lost this protection.
    That is a real boundary rather than an oversight: a function handed a
    ``dict`` cannot know what the bytes behind it said.
    """
    try:
        document = json.loads(raw, object_pairs_hook=_refuse_duplicate_members)
    except json.JSONDecodeError as exc:
        raise ContractError(f"{SURFACE}: document is not valid JSON: {exc}") from exc
    return parse(document, http_status, now=now)


def _require_wire_int(document: Any, field: str) -> int | None:
    """Return an integer field, refusing a zero-fraction decimal.

    JSON Schema's ``integer`` admits ``403.0`` -- it means "a number with no
    fractional part", not "a JSON integer literal" -- so the schema accepts it
    in all three languages and cannot be the place this is caught.

    Go's client re-decodes the raw bytes into an ``int`` field and refuses
    ``403.0`` outright. Python's ``json`` hands back a ``float`` and, without
    this check, carried it into a parsed envelope: the SAME document accepted
    by one client and rejected by the other. Codex round 1 found it, and it is
    the same cross-language split CHAOS-4884 already fixed for principal.v1
    revisions -- rediscovered here because a new surface re-derived the shape
    instead of reusing the guard.

    Refusing in Python (rather than teaching Go to coerce) is the direction
    that keeps both clients agreeing on the STRICTER reading, and a status of
    ``403.0`` is a producer defect in any case. The corresponding fixtures are
    ``reject_by_client``, not ``reject``: the schema validates these documents,
    and filing them as ``reject`` would assert a check that provably does not
    exist.
    """
    raw = document.get(field)
    if raw is None:
        return None
    # bool is a subclass of int in Python; a JSON `true` here is not a number.
    if isinstance(raw, bool):
        raise ContractError(f"{SURFACE}: /{field} is a boolean, not an integer")
    if isinstance(raw, int):
        return raw
    raise ContractError(
        f"{SURFACE}: /{field} = {raw!r} is a decimal, not an integer. The schema "
        f"accepts it because JSON Schema's `integer` admits a zero fractional "
        f"part, and Go's client refuses the identical document -- so accepting "
        f"it here would split the two clients on one wire format"
    )


def _parse_timestamp(raw: str, field: str) -> datetime:
    """Parse one RFC 3339 timestamp the schema has already accepted.

    Both a ``pattern`` and a ``format`` assertion have already run, so this is
    a parse of validated input rather than a second, weaker validation. It is
    still guarded: ``fromisoformat`` and RFC 3339 are not the same grammar in
    every direction, and a disagreement must surface as an error naming the
    field rather than as a traceback.
    """
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:  # pragma: no cover - unreachable via the schema
        raise ContractError(
            f"{SURFACE}: /{field} passed schema validation but is not "
            f"parseable as RFC 3339: {raw!r}"
        ) from exc
    if parsed.tzinfo is None:  # pragma: no cover - the pattern requires a zone
        raise ContractError(f"{SURFACE}: /{field} carries no timezone: {raw!r}")
    return parsed


@dataclass(frozen=True, slots=True)
class ErrorEnvelope:
    """One ``error.v1`` document, validated and parsed."""

    status: int
    reason_code: str
    request_id: str
    occurred_at: datetime
    retry_after_seconds: int | None

    @property
    def is_transient(self) -> bool:
        """Whether the caller may usefully retry.

        Reads the status rather than the presence of ``retry_after_seconds``.
        Those coincide today because the schema's conditional makes them
        coincide -- but keying off the field would silently invert this
        property if the conditional were ever relaxed, and a caller asking
        "should I retry" deserves an answer that does not depend on that.
        """
        return self.status in TRANSIENT_STATUSES


def parse(
    document: Any,
    http_status: int,
    *,
    now: datetime | None = None,
) -> ErrorEnvelope:
    """Validate and parse an ``error.v1`` document from an HTTP response.

    *http_status* is the status of the response line the document arrived on.
    It is REQUIRED rather than optional: making it optional would let every
    caller skip the one check the schema cannot perform, and a check that is
    easy to omit is one that will be omitted.

    *now* is injectable so the skew check is testable without sleeping or
    mocking the clock.

    **This entry point CANNOT refuse duplicate object members**, because it is
    handed an already-decoded object and the decode is what discarded the
    evidence. Callers holding the raw body should use :func:`parse_bytes`, which
    can. Stated here rather than left implicit: a caller who reaches this
    function with a ``dict`` has already lost that protection, and no amount of
    checking inside this function can recover it.
    """
    validate(SURFACE, document)

    status = _require_wire_int(document, "status")
    retry_after = _require_wire_int(document, "retry_after_seconds")
    if status != http_status:
        raise ContractError(
            f"{SURFACE}: envelope status {status} disagrees with the HTTP "
            f"status {http_status} it arrived on; refusing rather than "
            f"choosing one -- trusting the body would let a server contradict "
            f"its own response line, and on a 404 that discloses the "
            f"existence the status withholds"
        )

    occurred_at = _parse_timestamp(document["occurred_at"], "occurred_at")
    reference = now if now is not None else datetime.now(timezone.utc)
    if occurred_at > reference + MAX_CLOCK_SKEW:
        raise ContractError(
            f"{SURFACE}: /occurred_at {occurred_at.isoformat()} is more than "
            f"{MAX_CLOCK_SKEW} ahead of {reference.isoformat()}; a future "
            f"timestamp reorders an audit trail silently"
        )

    return ErrorEnvelope(
        status=status,
        reason_code=document["reason_code"],
        request_id=document["request_id"],
        occurred_at=occurred_at,
        retry_after_seconds=retry_after,
    )
