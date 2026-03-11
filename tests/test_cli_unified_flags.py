"""Tests for unified CLI flag helpers in utils/cli.py.

Covers: add_date_range_args, add_sink_arg, resolve_date_range,
resolve_since_datetime, resolve_max_commits, validate_sink.
"""

from __future__ import annotations

import argparse
import warnings
from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest

from dev_health_ops.utils.cli import (
    add_date_range_args,
    add_sink_arg,
    resolve_date_range,
    resolve_max_commits,
    resolve_since_datetime,
    validate_sink,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_parser() -> argparse.ArgumentParser:
    """Create a minimal parser with date range + sink args."""
    parser = argparse.ArgumentParser()
    add_date_range_args(parser)
    add_sink_arg(parser)
    return parser


def _ns(**kwargs) -> argparse.Namespace:
    """Build a bare Namespace with sensible defaults for resolve_* functions."""
    defaults = dict(
        since=None,
        before=None,
        backfill=1,
        day=None,
        date=None,
        sink="clickhouse",
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# add_date_range_args — parser integration
# ---------------------------------------------------------------------------


class TestAddDateRangeArgs:
    def test_backfill_default_is_1(self):
        parser = _make_parser()
        ns = parser.parse_args([])
        assert ns.backfill == 1
        assert ns.since is None
        assert ns.before is None

    def test_since_parses_iso_date(self):
        parser = _make_parser()
        ns = parser.parse_args(["--since", "2025-03-01"])
        assert ns.since == date(2025, 3, 1)

    def test_before_parses_iso_date(self):
        parser = _make_parser()
        ns = parser.parse_args(["--before", "2025-03-15"])
        assert ns.before == date(2025, 3, 15)

    def test_backfill_parses_int(self):
        parser = _make_parser()
        ns = parser.parse_args(["--backfill", "7"])
        assert ns.backfill == 7

    def test_since_and_backfill_mutually_exclusive(self):
        parser = _make_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--since", "2025-01-01", "--backfill", "7"])

    def test_since_and_before_together(self):
        parser = _make_parser()
        ns = parser.parse_args(["--since", "2025-01-01", "--before", "2025-01-10"])
        assert ns.since == date(2025, 1, 1)
        assert ns.before == date(2025, 1, 10)

    def test_deprecated_day_alias_parsed_hidden(self):
        parser = _make_parser()
        ns = parser.parse_args(["--day", "2025-06-15"])
        assert ns.day == date(2025, 6, 15)

    def test_deprecated_date_alias_parsed_hidden(self):
        parser = _make_parser()
        ns = parser.parse_args(["--date", "2025-06-15"])
        assert ns.date == date(2025, 6, 15)


# ---------------------------------------------------------------------------
# add_sink_arg
# ---------------------------------------------------------------------------


class TestAddSinkArg:
    def test_default_is_clickhouse(self):
        parser = _make_parser()
        ns = parser.parse_args([])
        assert ns.sink == "clickhouse"

    def test_explicit_clickhouse(self):
        parser = _make_parser()
        ns = parser.parse_args(["--sink", "clickhouse"])
        assert ns.sink == "clickhouse"


# ---------------------------------------------------------------------------
# resolve_date_range
# ---------------------------------------------------------------------------

_FIXED_TODAY = date(2025, 3, 10)


class TestResolveDateRange:
    @patch("dev_health_ops.utils.cli.utc_today", return_value=_FIXED_TODAY)
    def test_defaults_to_today_only(self, _mock):
        """No flags → end_day=today, backfill_days=1."""
        ns = _ns()
        end_day, backfill_days = resolve_date_range(ns)
        assert end_day == _FIXED_TODAY
        assert backfill_days == 1

    def test_explicit_before_and_backfill(self):
        ns = _ns(before=date(2025, 3, 11), backfill=7)
        end_day, backfill_days = resolve_date_range(ns)
        assert end_day == date(2025, 3, 10)
        assert backfill_days == 7

    def test_explicit_since_and_before(self):
        ns = _ns(since=date(2025, 3, 1), before=date(2025, 3, 11))
        end_day, backfill_days = resolve_date_range(ns)
        assert end_day == date(2025, 3, 10)
        assert backfill_days == 10  # Mar 1..10 inclusive

    @patch("dev_health_ops.utils.cli.utc_today", return_value=_FIXED_TODAY)
    def test_since_without_before_defaults_end_to_today(self, _mock):
        ns = _ns(since=date(2025, 3, 5))
        end_day, backfill_days = resolve_date_range(ns)
        assert end_day == _FIXED_TODAY
        assert backfill_days == 6  # Mar 5..10

    def test_since_after_before_raises(self):
        ns = _ns(since=date(2025, 3, 15), before=date(2025, 3, 11))
        with pytest.raises(SystemExit, match="--since.*must be before"):
            resolve_date_range(ns)

    def test_deprecated_day_flag_translates(self):
        ns = _ns(day=date(2025, 6, 15))
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            end_day, backfill_days = resolve_date_range(ns)
        assert end_day == date(2025, 6, 15)
        assert backfill_days == 1
        assert any("--day is deprecated" in str(warning.message) for warning in w)

    def test_deprecated_date_flag_translates(self):
        ns = _ns(date=date(2025, 6, 15))
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            end_day, backfill_days = resolve_date_range(ns)
        assert end_day == date(2025, 6, 15)
        assert backfill_days == 1
        assert any("--date is deprecated" in str(warning.message) for warning in w)

    def test_backfill_minimum_is_1(self):
        ns = _ns(before=date(2025, 3, 11), backfill=0)
        end_day, backfill_days = resolve_date_range(ns)
        assert backfill_days == 1


# ---------------------------------------------------------------------------
# resolve_since_datetime
# ---------------------------------------------------------------------------


class TestResolveSinceDatetime:
    @patch("dev_health_ops.utils.cli.utc_today", return_value=_FIXED_TODAY)
    def test_defaults_return_none(self, _mock):
        """No flags, backfill=1 → None (sync recent data)."""
        ns = _ns()
        assert resolve_since_datetime(ns) is None

    def test_explicit_since_returns_utc_datetime(self):
        ns = _ns(since=date(2025, 3, 1))
        result = resolve_since_datetime(ns)
        assert result == datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc)

    @patch("dev_health_ops.utils.cli.utc_today", return_value=_FIXED_TODAY)
    def test_backfill_greater_than_1_returns_start(self, _mock):
        ns = _ns(backfill=7)
        result = resolve_since_datetime(ns)
        # before defaults to tomorrow (Mar 11), backfill=7 → start = Mar 11 - 7 = Mar 4
        assert result == datetime(2025, 3, 4, 0, 0, 0, tzinfo=timezone.utc)

    def test_deprecated_day_flag_works(self):
        ns = _ns(day=date(2025, 6, 15))
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = resolve_since_datetime(ns)
        # --day 2025-06-15 → before=2025-06-16, backfill=1 → returns None (single day mode)
        assert result is None


# ---------------------------------------------------------------------------
# resolve_max_commits
# ---------------------------------------------------------------------------


class TestResolveMaxCommits:
    def test_no_date_constraint_defaults_to_100(self):
        ns = _ns()
        assert resolve_max_commits(ns) == 100

    def test_explicit_max_commits_without_date(self):
        ns = _ns(max_commits_per_repo=50)
        assert resolve_max_commits(ns) == 50

    def test_date_constraint_returns_none_if_unset(self):
        ns = _ns(since=date(2025, 1, 1))
        assert resolve_max_commits(ns) is None

    def test_date_constraint_preserves_explicit_max(self):
        ns = _ns(since=date(2025, 1, 1), max_commits_per_repo=200)
        assert resolve_max_commits(ns) == 200

    def test_backfill_greater_than_1_is_date_constraint(self):
        ns = _ns(backfill=7)
        assert resolve_max_commits(ns) is None


# ---------------------------------------------------------------------------
# validate_sink
# ---------------------------------------------------------------------------


class TestValidateSink:
    def test_clickhouse_accepted(self):
        ns = _ns(sink="clickhouse")
        validate_sink(ns)  # Should not raise

    def test_auto_accepted(self):
        ns = _ns(sink="auto")
        validate_sink(ns)  # Should not raise

    def test_none_defaults_to_clickhouse(self):
        ns = _ns(sink=None)
        validate_sink(ns)  # Should not raise

    @pytest.mark.parametrize("backend", ["mongo", "sqlite", "postgres", "both"])
    def test_deprecated_backends_rejected(self, backend):
        ns = _ns(sink=backend)
        with pytest.raises(SystemExit, match="no longer supported"):
            validate_sink(ns)

    def test_unknown_backend_rejected(self):
        ns = _ns(sink="mysql")
        with pytest.raises(SystemExit, match="Unknown sink"):
            validate_sink(ns)

    def test_case_insensitive(self):
        ns = _ns(sink="CLICKHOUSE")
        validate_sink(ns)  # Should not raise

    def test_whitespace_stripped(self):
        ns = _ns(sink="  clickhouse  ")
        validate_sink(ns)  # Should not raise
