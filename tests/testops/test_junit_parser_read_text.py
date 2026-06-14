from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.parsers.junit import _read_text, parse_junit_xml

FIXTURES_DIR = Path(__file__).parent / "fixtures"

_MINIMAL_JUNIT_XML = """<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="suite" tests="1" failures="0" errors="0" skipped="0">
    <testcase name="test_one" classname="mod::test_one" time="0.1"/>
  </testsuite>
</testsuites>
"""


def _make_long_junit_xml(extra_chars: int = 300) -> str:
    padding = "x" * extra_chars
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="long-content-suite" tests="1" failures="0" errors="0" skipped="0">
    <properties>
      <property name="padding" value="{padding}"/>
    </properties>
    <testcase name="test_long" classname="mod::test_long" time="0.05"/>
  </testsuite>
</testsuites>
"""


def test_read_text_returns_short_xml_string_as_content() -> None:
    result = _read_text(_MINIMAL_JUNIT_XML)

    assert result == _MINIMAL_JUNIT_XML


def test_read_text_long_xml_string_does_not_raise() -> None:
    long_xml = _make_long_junit_xml(extra_chars=500)
    result = _read_text(long_xml)

    assert result == long_xml


def test_read_text_preserves_existing_path_read_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "report.xml"
    path.write_text(_MINIMAL_JUNIT_XML, encoding="utf-8")

    original_read_text = Path.read_text

    def fail_read_text(self: Path, *args: Any, **kwargs: Any) -> str:
        if self == path:
            raise PermissionError("cannot read report")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", fail_read_text)

    with pytest.raises(PermissionError, match="cannot read report"):
        _read_text(str(path))


def test_parse_junit_xml_accepts_long_xml_content_string() -> None:
    long_xml = _make_long_junit_xml(extra_chars=500)
    suites = parse_junit_xml(long_xml)

    assert len(suites) == 1
    assert suites[0].suite_name == "long-content-suite"
    assert suites[0].total_count == 1


def test_parse_junit_xml_accepts_fixture_content_as_string() -> None:
    content = (FIXTURES_DIR / "sample_junit.xml").read_text(encoding="utf-8")
    suites = parse_junit_xml(content)

    assert len(suites) == 2
    assert suites[0].suite_name == "services.api.tests.test_api"
