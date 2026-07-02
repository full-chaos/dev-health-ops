"""Raw-SQL accessors for ``external_ingest_batch_payloads`` (CHAOS-2693 D2).

The durable stream carries a metadata *pointer* only -- never the raw batch
JSON (see ``docs/architecture/external-ingest-stream-design.md``). The full
payload lives here, in Postgres, keyed by ``ingestion_id``; the worker
(CHAOS-2697) fetches it by pointer and deletes it on terminal status (D7).

Table/model DDL is hosted by CHAOS-2694's migration ``0033`` and
``models/external_ingest.py::ExternalIngestBatchPayload`` (master-spec
CC19) -- this module owns only the read/write helpers, using raw
parameterized ``text()`` SQL (house rule: no ORM-only paths for API
persistence) so it stays portable across the sqlite-in-memory engine used
by unit tests and real Postgres in production (no ``RETURNING``/
``ON CONFLICT``).

``upsert_payload`` is a SELECT-then-UPDATE-or-INSERT in the caller's own
transaction (master-spec CC22, post-critique CC22): a RETRY accept (same
``ingestion_id`` reused for both the ``stream_unavailable`` case -- row
still exists, worker never ran -- and the ``failed`` case -- worker may have
already deleted the row) must not collide on the primary key. The
idempotency row (``external_ingest_batches``, unique-indexed, written first
in the same accept transaction by the caller) is the serialization point
for concurrent same-key accepts; the residual sub-ms concurrent-insert race
surfaces as ``503 ingest_temporarily_unavailable`` upstream (CHAOS-2695),
not as a constraint violation here.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_TABLE = "external_ingest_batch_payloads"


async def upsert_payload(
    session: AsyncSession,
    *,
    ingestion_id: uuid.UUID | str,
    org_id: str,
    schema_version: str,
    payload_bytes: bytes,
) -> None:
    """Write (or refresh) the raw-payload row for ``ingestion_id``.

    Does NOT commit -- caller commits once the accept sequence's other
    writes (the ``external_ingest_batches`` status row) also succeed, so a
    payload row is never durable without its corresponding status row (and
    vice versa is fine: the status row is written first in CC22's pinned
    sequence, so a crash between the two leaves an orphaned status row, not
    an orphaned payload -- see brief Risk G3).
    """
    ingestion_id_str = str(ingestion_id)
    existing = (
        await session.execute(
            text(
                f"SELECT 1 FROM {_TABLE} "
                "WHERE ingestion_id = :ingestion_id AND org_id = :org_id"
            ),
            {"ingestion_id": ingestion_id_str, "org_id": org_id},
        )
    ).first()

    params = {
        "ingestion_id": ingestion_id_str,
        "org_id": org_id,
        "schema_version": schema_version,
        "payload_json": payload_bytes,
        "byte_size": len(payload_bytes),
        # Python-side UTC timestamp, not SQL now() -- keeps this portable
        # across the sqlite-in-memory engine used by unit tests and real
        # Postgres in prod (matches tests/test_rate_limit_observations.py's
        # convention).
        "created_at": datetime.now(timezone.utc),
    }

    if existing:
        await session.execute(
            text(
                f"UPDATE {_TABLE} SET schema_version = :schema_version, "
                "payload_json = :payload_json, byte_size = :byte_size, "
                "created_at = :created_at "
                "WHERE ingestion_id = :ingestion_id AND org_id = :org_id"
            ),
            params,
        )
    else:
        await session.execute(
            text(
                f"INSERT INTO {_TABLE} "
                "(ingestion_id, org_id, schema_version, payload_json, byte_size, created_at) "
                "VALUES (:ingestion_id, :org_id, :schema_version, :payload_json, "
                ":byte_size, :created_at)"
            ),
            params,
        )


async def payload_exists(
    session: AsyncSession, *, ingestion_id: uuid.UUID | str, org_id: str
) -> bool:
    """Cheap indexed existence check -- ``SELECT 1``, no payload bytes.

    Backs ``streams.enqueue_batch()``'s fail-closed invariant (a pointer
    must never become visible on the stream before its payload is durable
    in Postgres): unlike :func:`fetch_payload`, this never transfers the
    (possibly multi-MB) blob just to answer a yes/no question.
    """
    row = (
        await session.execute(
            text(
                f"SELECT 1 FROM {_TABLE} "
                "WHERE ingestion_id = :ingestion_id AND org_id = :org_id"
            ),
            {"ingestion_id": str(ingestion_id), "org_id": org_id},
        )
    ).first()
    return row is not None


async def fetch_payload(
    session: AsyncSession, *, ingestion_id: uuid.UUID | str, org_id: str
) -> bytes | None:
    """Read the raw payload bytes for ``ingestion_id``.

    ``org_id`` is included in the predicate even though ``ingestion_id``
    alone is already a unique primary key -- defense in depth against a
    leaked/guessed UUID crossing tenants (house rule: org_id in every
    lookup predicate).
    """
    row = (
        await session.execute(
            text(
                f"SELECT payload_json FROM {_TABLE} "
                "WHERE ingestion_id = :ingestion_id AND org_id = :org_id"
            ),
            {"ingestion_id": str(ingestion_id), "org_id": org_id},
        )
    ).first()
    if row is None:
        return None
    value = row[0]
    # sqlite (unit tests) round-trips LargeBinary as bytes already; guard
    # only for a driver returning e.g. memoryview.
    return bytes(value) if not isinstance(value, bytes) else value


async def delete_payload(
    session: AsyncSession, *, ingestion_id: uuid.UUID | str
) -> None:
    """Delete the payload row. Idempotent: a no-op if already deleted.

    No ``org_id`` predicate -- callers (the worker, on terminal status; the
    beat-scheduled prune sweep, D7) already resolve ``ingestion_id``
    tenant-scoped upstream, and this table has no secondary index that
    would benefit from one.
    """
    await session.execute(
        text(f"DELETE FROM {_TABLE} WHERE ingestion_id = :ingestion_id"),
        {"ingestion_id": str(ingestion_id)},
    )


__all__ = ["upsert_payload", "payload_exists", "fetch_payload", "delete_payload"]
