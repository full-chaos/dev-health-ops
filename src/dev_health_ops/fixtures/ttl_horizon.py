"""CHAOS-3432/3544: keep generated fixture history clear of ClickHouse TTLs.

THE DEFECT THIS EXISTS TO PREVENT. Several ClickHouse tables carry
``TTL <column> + INTERVAL n DAY DELETE``. The fixture generators wrote
history right up to the edge of the tightest one -- flags created
``randint(7, 90)`` days before ``pinned_now`` against a 90-day TTL -- so the
oldest rows sat exactly on the boundary the moment the world was generated.

Every day of real time afterwards, more rows crossed the horizon and
ClickHouse deleted them on a background merge. The committed snapshot bytes
never changed; the RESTORED table did, because the database removes rows on
load. A world minted on one day therefore failed its own content oracle days
later, with no code change anywhere.

Measured on 2026-08-07 against the committed snapshot: 1045 rows, of which 1
had already expired and 24 more were due within a week.

That is also the most likely explanation for CHAOS-3432's "cross-generation
nondeterminism": TTL deletion runs on ASYNCHRONOUS background merges, so two
generations minutes apart differ exactly when a merge fires between them --
timing-dependence that is in ClickHouse, not in our generation path. It
accounts for the recorded row-count differences, for the affected table set
SHIFTING run to run (four TTL tables with different horizons), and for
``PYTHONHASHSEED=0`` failing to converge.

THE RULE. Generated history must end at least ``TTL_SAFETY_MARGIN`` inside
the tightest TTL horizon in the schema. The margin is the snapshot's honest
SHELF LIFE: a world generated today stays restorable for that long, and the
typed expiry preflight in ``world_snapshot`` fails loudly once it is past.
"""

from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path

#: How far inside the tightest TTL horizon generated history must stop.
#:
#: This IS the snapshot's shelf life. A world generated today restores
#: cleanly for this long; past it, rows begin crossing the TTL boundary on
#: load and the content oracle fails. Thirty days is chosen to be long
#: enough that re-minting is a routine chore rather than a weekly emergency,
#: and short enough that a stale snapshot is caught while the reason is
#: still obvious.
#:
#: Raising it shortens generated history; lowering it shortens shelf life.
#: Neither is free, which is why this is one named number rather than a
#: literal scattered across generators.
TTL_SAFETY_MARGIN = timedelta(days=30)

_TTL_PATTERN = re.compile(
    r"TTL\s+.*?\+\s*INTERVAL\s+(?P<days>\d+)\s+DAY\s+DELETE", re.IGNORECASE
)


def _migrations_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "migrations" / "clickhouse"


def clickhouse_ttl_horizons(migrations_dir: Path | None = None) -> dict[str, int]:
    """Every ``TTL … INTERVAL n DAY DELETE`` in the ClickHouse schema.

    DERIVED FROM THE SCHEMA, never transcribed. A hardcoded list of "the
    tables that have TTLs" is exactly how a future migration rejoins the
    decay class in silence -- the list would still look complete, and nothing
    would connect the new table to the failure it eventually causes.

    Returns ``{migration_filename: days}``; callers care about the MINIMUM,
    which is the horizon that binds.
    """

    directory = migrations_dir or _migrations_dir()
    horizons: dict[str, int] = {}
    for path in sorted(directory.glob("*.sql")):
        for match in _TTL_PATTERN.finditer(path.read_text(encoding="utf-8")):
            days = int(match.group("days"))
            # The tightest clause in a file is the one that binds it.
            horizons[path.name] = min(horizons.get(path.name, days), days)
    return horizons


def tightest_ttl_days(migrations_dir: Path | None = None) -> int:
    """The binding TTL horizon across the whole schema, in days."""

    horizons = clickhouse_ttl_horizons(migrations_dir)
    if not horizons:
        raise RuntimeError(
            "no ClickHouse TTL clauses found -- this helper exists because "
            "the schema HAS them, so finding none means the pattern stopped "
            "matching (a migration reworded its TTL) rather than that the "
            "risk went away"
        )
    return min(horizons.values())


def max_generated_history_days(migrations_dir: Path | None = None) -> int:
    """How far back generators may write, to stay inside the shelf life."""

    return tightest_ttl_days(migrations_dir) - TTL_SAFETY_MARGIN.days
