"""Deterministic scheduled-sync occurrence identity (CHAOS-4602 design page).

Ports ``internal/scheduler/sync/transaction.go``'s ``newOccurrence`` plus
``snapshot.go``'s ``writeDigestField``/``canonicalTime`` byte-for-byte, so a
Python-inserted ``scheduled_sync_occurrences`` row (a manual "Sync Now" or
backfill trigger) computes the SAME ``occurrence_id`` the Go reconciler's
``occurrenceIdentityIsValid`` (``occurrence_reconciler.go:495-510``)
recomputes and checks on pickup -- the same identity space a scheduled
occurrence already uses, not a new one.

Only three fields feed the digest on the Go side: ``identity_version`` (the
hardcoded constant below, never itself a parameter -- ``org_id``/``job_id``/
``observed_at``/``next_run_at`` are NOT part of the hash, confirmed by Go's
own ``TestOccurrenceIdentityIsDeterministicForConfigAndCronOccurrence``,
which proves an org/job change under a retry leaves the identity unchanged).

Cross-language proof without invoking Go: that same Go test pins a golden
vector -- ``config_id="config-a"``, ``scheduled_for=2026-01-01T11:00:00Z`` ->
``sha256:27478ac7c7bbcfc33caa3922492910d97220984911632d754944fdeaf405f0f9``
(``internal/scheduler/sync/transaction_test.go:363``). This module's own
test suite (``tests/test_occurrence_identity.py``) asserts the identical
value.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

__all__ = ["OCCURRENCE_IDENTITY_VERSION", "canonical_time", "occurrence_id"]

# Matches transaction.go:17's OccurrenceIdentityVersion constant exactly.
# Never parameterized on the Go side either -- a real identity-version bump
# would be a coordinated change on both sides, not a per-call argument.
OCCURRENCE_IDENTITY_VERSION = "sync_scheduler_occurrence_v1"


def _write_digest_field(name: str, value: str) -> bytes:
    """Mirrors snapshot.go's writeDigestField: length-prefixed name and
    value, terminated with a newline, so no pair of distinct field values
    can concatenate into the same byte sequence."""
    name_bytes = name.encode("utf-8")
    value_bytes = value.encode("utf-8")
    return (
        str(len(name_bytes)).encode("ascii")
        + b":"
        + name_bytes
        + str(len(value_bytes)).encode("ascii")
        + b":"
        + value_bytes
        + b"\n"
    )


def canonical_time(value: datetime) -> str:
    """Mirrors snapshot.go's canonicalTime:
    ``value.UTC().Format("2006-01-02T15:04:05.000000000Z")`` -- always 9
    fractional digits.

    Python's ``datetime`` has microsecond (not nanosecond) resolution, so
    the last 3 digits are always forced to ``"000"``. This is not a
    precision mismatch in practice: Postgres ``timestamptz`` is itself
    microsecond-resolution, and the value Go's reconciler reads back is the
    SAME stored instant this function was given -- never independently
    recomputed "now" on the Go side -- so both sides format the identical
    underlying instant.

    Requires a timezone-AWARE ``value``. Codex review (round 2, P2): a
    naive ``datetime``'s ``.astimezone()`` silently interprets it as the
    HOST's local timezone, not UTC -- a worker running in a non-UTC
    timezone would then compute a different occurrence_id for the exact
    same wall-clock instant than a UTC worker (or than Go's own
    recomputation off the stored, unambiguous ``timestamptz`` value) would.
    Rather than guess, this rejects a naive value outright.
    """
    if value.tzinfo is None:
        raise ValueError(
            "canonical_time() requires a timezone-aware datetime; a naive "
            "value's timezone is ambiguous (astimezone() would silently "
            "assume the host's local timezone, not UTC)"
        )
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S") + f".{value.microsecond:06d}000Z"


def occurrence_id(config_id: str, scheduled_for: datetime) -> str:
    """The deterministic ``sha256:``-prefixed digest Go's ``newOccurrence``
    computes from exactly ``identity_version``, ``config_id``, and the
    canonical ``scheduled_for`` -- the same three fields
    ``occurrenceIdentityIsValid`` recomputes and checks. ``scheduled_for``
    must be the EXACT instant that will be persisted to
    ``scheduled_sync_occurrences.scheduled_for`` (a ``timestamptz`` column):
    computing this before rounding/truncation drift would produce an id the
    Go side's readback recomputation cannot reproduce.
    """
    digest = hashlib.sha256()
    digest.update(_write_digest_field("identity_version", OCCURRENCE_IDENTITY_VERSION))
    digest.update(_write_digest_field("config_id", config_id))
    digest.update(_write_digest_field("scheduled_for", canonical_time(scheduled_for)))
    return "sha256:" + digest.hexdigest()
