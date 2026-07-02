"""Client-side batch size guardrails for `push batch`/`push validate` (CHAOS-2700).

Hardcoded fallback limits are imported directly from the server's own
``schemas.py`` constants (master-spec CC3: 1000 records / 10MB body) -- not
re-declared -- so an offline pre-check can never silently drift from what
``POST /batches`` actually enforces. ``push batch`` prefers the
server-reported ``GET /schemas`` ``limits`` object when reachable (brief
decision 6, amended by master-spec CC3: the endpoint exposes ``limits`` from
day one, so the CLI prefers it immediately, no "until then" hedge); these
constants are the fallback when that call fails, times out, or
``--skip-limits-check`` is passed.
"""

from __future__ import annotations

from typing import NamedTuple

from dev_health_ops.api.external_ingest.schemas import (
    MAX_BODY_BYTES_DEFAULT,
    MAX_RECORDS_DEFAULT,
)


class BatchLimits(NamedTuple):
    max_records_per_batch: int
    max_body_bytes: int


DEFAULT_LIMITS = BatchLimits(
    max_records_per_batch=MAX_RECORDS_DEFAULT,
    max_body_bytes=MAX_BODY_BYTES_DEFAULT,
)

#: Hard client-side ceiling on ANY body-size limit this CLI will ever honor
#: as a read cap, including a server-reported one (Codex adversarial-review
#: finding, round 2): `GET /schemas` is unauthenticated and reachable at
#: whatever `--api-url` the caller supplied -- a misconfigured URL or a
#: malicious/MITM responder could otherwise advertise an enormous
#: `maxBodyBytes` and re-open the unbounded-read hole the bounded-read fix
#: (`push/cli.py::_read_payload_arg`) closed. 10x the current server
#: default: generous enough for a legitimate admin-raised
#: `EXTERNAL_INGEST_MAX_BODY_BYTES`, small enough to keep a CI runner's
#: memory bounded regardless of what the server claims.
ABSOLUTE_MAX_BODY_BYTES = 100_000_000


def limits_from_schema_response(document: dict | None) -> BatchLimits:
    """Extract ``limits`` from a `GET /schemas` response body, falling back
    to ``DEFAULT_LIMITS`` field-by-field for anything missing/malformed --
    a partially-populated or absent ``limits`` object should never crash the
    CLI, only lose the benefit of the live values (brief decision 6).
    ``max_body_bytes`` is always clamped to ``ABSOLUTE_MAX_BODY_BYTES``,
    even when the server reports something larger."""
    limits = (document or {}).get("limits")
    if not isinstance(limits, dict):
        return DEFAULT_LIMITS
    max_records = limits.get("maxRecordsPerBatch")
    max_bytes = limits.get("maxBodyBytes")
    resolved_max_bytes = (
        max_bytes
        if isinstance(max_bytes, int) and max_bytes > 0
        else DEFAULT_LIMITS.max_body_bytes
    )
    return BatchLimits(
        max_records_per_batch=(
            max_records
            if isinstance(max_records, int) and max_records > 0
            else DEFAULT_LIMITS.max_records_per_batch
        ),
        max_body_bytes=min(resolved_max_bytes, ABSOLUTE_MAX_BODY_BYTES),
    )


__all__ = [
    "ABSOLUTE_MAX_BODY_BYTES",
    "BatchLimits",
    "DEFAULT_LIMITS",
    "limits_from_schema_response",
]
