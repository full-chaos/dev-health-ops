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
    """
    validate(SURFACE, document)

    status = document["status"]
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
        retry_after_seconds=document.get("retry_after_seconds"),
    )
