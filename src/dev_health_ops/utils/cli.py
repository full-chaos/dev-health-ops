"""Shared CLI argument helpers for sync and metrics commands.

All sync and metrics subcommands should use these helpers to ensure
consistent flag names and behavior.
"""

from __future__ import annotations

import argparse
import logging
import warnings
from datetime import date, datetime, time, timedelta, timezone

from dev_health_ops.utils.datetime import utc_today

logger = logging.getLogger(__name__)


def add_date_range_args(parser: argparse.ArgumentParser) -> None:
    """Add unified ``--since``, ``--before``, and ``--backfill`` flags.

    These three flags define the time window for sync/metrics operations:

    - ``--since <ISO-date>`` — inclusive start (mutually exclusive with ``--backfill``).
    - ``--before <ISO-date>`` — exclusive end (defaults to today + 1 day).
    - ``--backfill <N>`` — convenience: N days ending *before* ``--before``
      (mutually exclusive with ``--since``).

    Default behaviour (no flags): equivalent to ``--backfill 1`` (today only).
    """
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--since",
        type=date.fromisoformat,
        default=None,
        help="Start date (inclusive, ISO YYYY-MM-DD). Mutually exclusive with --backfill.",
    )
    group.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Process N days ending before --before (default: 1 = today only). "
        "Mutually exclusive with --since.",
    )
    parser.add_argument(
        "--before",
        type=date.fromisoformat,
        default=None,
        help="End date (exclusive, ISO YYYY-MM-DD). Defaults to tomorrow (i.e. through today).",
    )

    # Hidden deprecated aliases — emit warnings when used
    parser.add_argument(
        "--day", type=date.fromisoformat, default=None, help=argparse.SUPPRESS
    )
    parser.add_argument(
        "--date", type=date.fromisoformat, default=None, help=argparse.SUPPRESS
    )


def add_sink_arg(parser: argparse.ArgumentParser) -> None:
    """Add ``--sink`` flag for analytics backend (ClickHouse only).

    Deprecated backends (mongo, sqlite, postgres) are rejected with a
    clear migration message.
    """
    parser.add_argument(
        "--sink",
        default="clickhouse",
        help="Analytics sink backend. Only 'clickhouse' is supported. "
        "Connection string via CLICKHOUSE_URI env or --analytics-db.",
    )


_DEPRECATED_BACKENDS = frozenset({"mongo", "sqlite", "postgres", "both"})


def resolve_date_range(ns: argparse.Namespace) -> tuple[date, int]:
    """Resolve CLI date flags into ``(end_day, backfill_days)`` for internal APIs.

    Returns
    -------
    end_day : date
        The last day in the range (inclusive).
    backfill_days : int
        Number of days in the range (≥ 1).

    The returned pair is directly usable with the existing ``_date_range()``
    helpers in ``job_daily``, ``job_work_items``, etc.
    """
    # Handle deprecated --day / --date aliases
    _handle_deprecated_day_flag(ns)

    before = ns.before
    since: date | None = ns.since
    backfill: int = getattr(ns, "backfill", 1)

    if before is None:
        # Default: tomorrow (so end_day = today)
        before = utc_today() + timedelta(days=1)

    # end_day is the last inclusive day (before is exclusive)
    end_day = before - timedelta(days=1)

    if since is not None:
        # Explicit --since: compute backfill_days from the range
        if since > end_day:
            raise SystemExit(f"--since ({since}) must be before --before ({before}).")
        backfill_days = (end_day - since).days + 1
    else:
        # --backfill N (default path)
        backfill_days = max(1, backfill)

    return end_day, backfill_days


def resolve_since_datetime(ns: argparse.Namespace) -> datetime | None:
    """Resolve CLI date flags into a UTC ``since`` datetime.

    Used by ``sync git`` and other commands that accept a start timestamp
    but not an explicit end (they sync to HEAD).

    Returns ``None`` when no date constraint was provided and backfill is 1
    (the default "sync everything recent" mode).
    """
    _handle_deprecated_day_flag(ns)

    before = ns.before
    since_date: date | None = ns.since
    backfill: int = getattr(ns, "backfill", 1)

    if since_date is not None:
        return datetime.combine(since_date, time.min, tzinfo=timezone.utc)

    if backfill > 1:
        if before is None:
            before = utc_today() + timedelta(days=1)
        start_day = before - timedelta(days=backfill)
        return datetime.combine(start_day, time.min, tzinfo=timezone.utc)

    # No --since and --backfill=1: return None (default "recent" mode)
    return None


def resolve_max_commits(ns: argparse.Namespace) -> int | None:
    """Resolve max commits per repo from argparse namespace."""
    max_commits = getattr(ns, "max_commits_per_repo", None)
    has_date_constraint = (
        getattr(ns, "since", None) is not None or getattr(ns, "backfill", 1) > 1
    )
    if has_date_constraint:
        return max_commits if max_commits is not None else None
    return max_commits or 100


def validate_sink(ns: argparse.Namespace) -> None:
    """Validate ``--sink`` value, rejecting deprecated backends."""
    sink = getattr(ns, "sink", "clickhouse") or "clickhouse"
    sink = sink.strip().lower()

    if sink in _DEPRECATED_BACKENDS:
        raise SystemExit(
            f"Backend '{sink}' is no longer supported for analytics. "
            "ClickHouse is the only supported analytics backend. "
            "Set CLICKHOUSE_URI and use --sink clickhouse (or omit --sink)."
        )

    if sink not in {"clickhouse", "auto"}:
        raise SystemExit(f"Unknown sink '{sink}'. Only 'clickhouse' is supported.")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _handle_deprecated_day_flag(ns: argparse.Namespace) -> None:
    """Translate deprecated ``--day`` / ``--date`` into ``--before``."""
    day_val = getattr(ns, "day", None)
    date_val = getattr(ns, "date", None)
    deprecated_val = day_val or date_val
    flag_name = "--day" if day_val else "--date"

    if deprecated_val is not None:
        warnings.warn(
            f"{flag_name} is deprecated. Use --since/--before/--backfill instead. "
            f"Interpreting {flag_name} {deprecated_val} as --before {deprecated_val + timedelta(days=1)} --backfill 1.",
            DeprecationWarning,
            stacklevel=3,
        )
        # Treat --day/--date X as "process day X" → --before X+1 --backfill 1
        if ns.before is None:
            ns.before = deprecated_val + timedelta(days=1)
        if ns.since is None and getattr(ns, "backfill", 1) == 1:
            # If --backfill was also explicitly set, respect it
            pass
