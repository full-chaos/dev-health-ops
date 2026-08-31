"""Cross-language parity for occurrence_identity.py (CHAOS-4602 design page).

Proves the Python port of Go's ``newOccurrence``
(``internal/scheduler/sync/transaction.go:430``) produces byte-identical
occurrence ids from the SAME golden inputs Go's own test pins -- without
invoking Go. See ``occurrence_identity.py``'s module docstring for the full
citation chain.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.sync.occurrence_identity import canonical_time, occurrence_id

_GOLDEN_SCHEDULED_FOR = datetime(2026, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
_GOLDEN_ID = "sha256:27478ac7c7bbcfc33caa3922492910d97220984911632d754944fdeaf405f0f9"


def test_occurrence_id_matches_go_golden_vector():
    """internal/scheduler/sync/transaction_test.go:363
    (TestOccurrenceIdentityIsDeterministicForConfigAndCronOccurrence) pins
    this exact hash for config_id="config-a",
    scheduled_for=2026-01-01T11:00:00Z. This is the cross-language proof: if
    this ever fails, the two sides have silently diverged and a
    Python-inserted occurrence would be quarantined by Go's
    occurrenceIdentityIsValid, not planned.
    """
    assert occurrence_id("config-a", _GOLDEN_SCHEDULED_FOR) == _GOLDEN_ID


def test_occurrence_id_is_unaffected_by_input_timezone():
    """Go's own test proves org_id/job_id changes (a retry under a
    replacement job) leave the identity unchanged -- this function does not
    even accept those fields, so there is nothing to vary there. What this
    function DOES need to get right is that a differently-timezoned
    representation of the SAME instant produces the same id (canonical_time
    always normalizes to UTC first).
    """
    offset_scheduled_for = _GOLDEN_SCHEDULED_FOR.astimezone(
        timezone(timedelta(hours=-8))
    )
    assert occurrence_id("config-a", offset_scheduled_for) == _GOLDEN_ID


def test_occurrence_id_differs_for_different_config_or_time():
    later = _GOLDEN_SCHEDULED_FOR + timedelta(hours=1)
    assert occurrence_id("config-a", later) != _GOLDEN_ID
    assert occurrence_id("config-b", _GOLDEN_SCHEDULED_FOR) != _GOLDEN_ID


def test_canonical_time_matches_go_format():
    assert canonical_time(_GOLDEN_SCHEDULED_FOR) == "2026-01-01T11:00:00.000000000Z"
    # Sub-second precision: Python's microsecond resolution pads to 9 digits
    # with 3 trailing zeros, matching Go's nanosecond format field-for-field.
    with_micros = _GOLDEN_SCHEDULED_FOR.replace(microsecond=123456)
    assert canonical_time(with_micros) == "2026-01-01T11:00:00.123456000Z"


def test_canonical_time_rejects_naive_datetime():
    """Codex review (round 2, P2): a naive datetime's .astimezone() silently
    assumes the HOST's local timezone, not UTC -- a non-UTC worker would
    then compute a different occurrence_id for the same wall-clock instant
    than Go's own recomputation off the stored timestamptz value. Must fail
    loud instead.
    """
    naive = datetime(2026, 1, 1, 11, 0, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        canonical_time(naive)
