"""CHAOS-3544 PR2: the committed world snapshot must agree with the
generators that produce it.

WHY THIS EXISTS. The acceptance stack does not run the fixture generators --
it restores a committed snapshot (``tests/acceptance/world/ask-dev-world.v1/
snapshot/``). So a change to a generator does not change what the stack
seeds, and the two can silently disagree.

That is not hypothetical: CHAOS-3544 fixed
``retention_conversations.build_retention_aged_conversation`` to stamp an
expiry on 0-day rows, and **the entire unit suite stayed green** while the
committed snapshot went on carrying the un-purgeable ``expires_at: NULL``
shape the fix exists to remove. The divergence would have surfaced only as a
``WORLD_DIGEST`` mismatch during an armed corpus run -- a measurement
failure discovered at 2am, attributed to whatever else changed that day.

WHAT IT ASSERTS. Every snapshotted ``dev_conversations`` row must carry the
expiry the real generator PRODUCES for it today, obtained by RUNNING that
generator against the row's own ``created_at`` and ``retention_days`` -- not
by re-deriving the rule here. A test that re-implemented the arithmetic
would be a second implementation of it, and two implementations drift
together in precisely the way that leaves a differential check green while
both are wrong.

Deliberately bidirectional: it fails if the generator changes without a
re-mint, AND if a snapshot is minted from a tree whose generator disagrees.
Either way the remedy is the same and is named in the failure message.
"""

from __future__ import annotations

import gzip
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dev_health_ops.fixtures.generators import retention_conversations as conv_gen

_SNAPSHOT = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "acceptance"
    / "world"
    / "ask-dev-world.v1"
    / "snapshot"
    / "postgres"
    / "dev_conversations.json.gz"
)


def _unwrap(value: Any) -> Any:
    """Snapshot cells are ``{"__t__": ..., "v": ...}`` for typed columns."""

    return value["v"] if isinstance(value, dict) and "v" in value else value


def _produced_expiry(created_at: datetime, retention_days: int) -> datetime | None:
    """The expiry the REAL generator produces for this row.

    EXECUTED, never transcribed. Re-deriving the rule here in the test's own
    arithmetic would make this a second implementation of it -- and two
    implementations of the same rule can drift together in exactly the way
    that leaves a differential check passing while both are wrong. The point
    of this guard is to compare the snapshot against the producer, so the
    producer has to actually run.

    ``build_retention_aged_conversation`` is that producer: it is the
    function ``fixtures/world.py`` calls to mint these rows. Passing
    ``pinned_now=created_at`` with ``age_days=0`` reproduces the row's own
    creation instant, so whatever it returns is what a re-mint would write
    today.
    """

    bundle = conv_gen.build_retention_aged_conversation(
        org_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        id_seed="snapshot-agreement-probe",
        retention_days=retention_days,
        age_days=0,
        pinned_now=created_at,
        title="snapshot agreement probe",
    )
    return bundle.conversation.expires_at


def test_snapshot_conversation_expiries_match_the_production_rule() -> None:
    payload = json.loads(gzip.open(_SNAPSHOT).read())
    columns = payload["columns"]
    idx = {name: position for position, name in enumerate(columns)}

    disagreements: list[str] = []
    for row in payload["rows"]:
        created_raw = _unwrap(row[idx["created_at"]])
        created_at = datetime.fromisoformat(created_raw)
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=UTC)
        retention_days = _unwrap(row[idx["retention_days"]])
        actual_raw = _unwrap(row[idx["expires_at"]])
        title = _unwrap(row[idx["title"]])

        expected = _produced_expiry(created_at, retention_days)
        if actual_raw is None and expected is None:
            # Genuine agreement: the generator produces no expiry for this
            # row and the snapshot has none. Checked BEFORE the NULL branch
            # below, which otherwise reported "snapshot has NULL, the
            # generator produces None" as a disagreement -- a guard that
            # cries wolf in a legitimately agreeing state. Found by mutating
            # the generator back to its pre-fix rule and reading the failure
            # rather than trusting the pass/fail bit.
            continue
        if actual_raw is None:
            disagreements.append(
                f"{title!r} (retention_days={retention_days}): snapshot has "
                f"expires_at=NULL, the generator produces "
                f"{expected.isoformat() if expected else None}"
            )
            continue
        actual = datetime.fromisoformat(actual_raw)
        if actual.tzinfo is None:
            actual = actual.replace(tzinfo=UTC)
        if actual != expected:
            disagreements.append(
                f"{title!r} (retention_days={retention_days}): snapshot has "
                f"{actual.isoformat()}, the generator produces "
                f"{expected.isoformat() if expected else None}"
            )

    assert not disagreements, (
        "the committed world snapshot disagrees with the generators that "
        "produce it, so the acceptance stack seeds rows production can no "
        "longer create. Re-mint the snapshot and re-pin WORLD_DIGEST in the "
        "same change (they must never disagree across a merge boundary):\n  "
        + "\n  ".join(disagreements)
    )


def test_the_snapshot_actually_carries_rows_to_check() -> None:
    """Rule 4: a guard that silently checks nothing is worse than no guard.

    If the snapshot moves, is renamed, or is emptied, the test above passes
    vacuously over zero rows while reporting agreement.
    """

    payload = json.loads(gzip.open(_SNAPSHOT).read())
    assert payload["rows"], "the snapshot has no dev_conversations rows to check"
    assert any(
        _unwrap(row[payload["columns"].index("retention_days")]) == 0
        for row in payload["rows"]
    ), (
        "the snapshot carries no 0-day rows, so this guard could not have "
        "caught the CHAOS-3544 divergence it exists for"
    )
