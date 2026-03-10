"""Tests for CHAOS-849: utc_today() helper and date.today() elimination."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import patch

from dev_health_ops.utils.datetime import utc_today


def test_utc_today_returns_date():
    result = utc_today()
    assert isinstance(result, date)


def test_utc_today_uses_utc_not_local():
    """When UTC date differs from local date, utc_today must return UTC."""
    # Simulate a moment where UTC is Jan 2 but local (e.g. PST) is still Jan 1
    fake_utc_dt = datetime(2026, 1, 2, 1, 0, 0, tzinfo=timezone.utc)
    with patch("dev_health_ops.utils.datetime.datetime") as mock_dt:
        mock_dt.now.return_value = fake_utc_dt
        result = utc_today()
    assert result == date(2026, 1, 2)
    mock_dt.now.assert_called_once_with(timezone.utc)


def test_utc_today_matches_datetime_now_utc_date():
    """utc_today() should be equivalent to datetime.now(timezone.utc).date()."""
    result = utc_today()
    expected = datetime.now(timezone.utc).date()
    assert result == expected


def test_no_date_today_in_production_code():
    """Ensure date.today() is not used anywhere in production source code."""
    import pathlib
    import re

    src_root = pathlib.Path(__file__).parent.parent / "src" / "dev_health_ops"
    violations = []
    pattern = re.compile(r"\bdate\.today\(\)")

    for py_file in src_root.rglob("*.py"):
        content = py_file.read_text()
        for i, line in enumerate(content.splitlines(), 1):
            if pattern.search(line) and not line.strip().startswith("#"):
                violations.append(f"{py_file.relative_to(src_root.parent.parent)}:{i}")

    assert violations == [], (
        "Found date.today() in production code (use utc_today() instead):\n"
        + "\n".join(violations)
    )
