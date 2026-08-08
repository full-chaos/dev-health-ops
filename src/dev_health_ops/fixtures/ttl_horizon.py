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
    # BOTH suffixes. This repo's ClickHouse migrations are .sql AND .py, and
    # a .py migration carries its DDL as SQL strings -- so a TTL can arrive
    # through either. Globbing *.sql alone is a known way to reach a
    # wrong-schema conclusion here, and it would defeat this helper's entire
    # purpose: a future TTL table that the parser cannot see rejoins the
    # decay class in silence, which is the failure this exists to prevent.
    # Proven by a control that feeds it a .py-sourced TTL, rather than
    # asserted.
    sources = sorted(
        [*directory.glob("*.sql"), *directory.glob("*.py")],
        key=lambda path: path.name,
    )
    for path in sources:
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


def max_generated_age_days_for_table(table: str) -> int | None:
    """A PER-TABLE generation ceiling that stays compatible with THIS
    module's shelf-life contract -- ``None`` if ``table`` carries no TTL.

    CHAOS-3602 port, codex finding (HIGH, confirmed): a generator clamped
    against ``ttl_registry.py``'s own margin constants
    (``TTL_SAFETY_MARGIN_DAYS`` = 7, ``GENERATOR_SLACK_DAYS`` = 3) produces a
    ceiling incompatible with the 30-day margin ``_assert_snapshot_within_
    shelf_life`` actually enforces at restore: measured live,
    ``telemetry_signal_bucket`` at that looser ceiling (80d) plus this
    module's own advertised 30-day restore shelf life reaches 110 days old
    against a 90-day TTL -- 20 days PAST the horizon, silently losing rows
    to a TTL merge during a restore main's own manifest says is still safe.
    ``release_impact_daily`` (355d + 30d vs a 365d TTL) and
    ``product_telemetry_events`` (170d + 30d vs a 180d TTL) have the same
    defect.

    This combines ``ttl_registry.py``'s PER-TABLE retention data (more
    precise than this module's single tightest-global horizon) with THIS
    module's own ``TTL_SAFETY_MARGIN`` -- the one number
    ``_assert_snapshot_within_shelf_life`` actually promises callers -- so
    every generator's ceiling stays honest about what a restore up to the
    full advertised shelf life can still find intact.
    """

    from dev_health_ops.fixtures.ttl_registry import clickhouse_ttl_retentions

    retentions = clickhouse_ttl_retentions()
    if not retentions:
        raise RuntimeError(
            f"no per-table TTL retentions found while computing a generation "
            f"ceiling for {table!r} -- this schema is known to carry several "
            "TTL'd tables, so an empty registry means the parser or its "
            "migrations-directory path broke, not that the risk went away "
            "(see tightest_ttl_days for the same rule applied globally)"
        )
    retention = retentions.get(table)
    if retention is None:
        return None
    return max(0, retention.retention_days - TTL_SAFETY_MARGIN.days)
