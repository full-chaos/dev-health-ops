"""Regression coverage for CHAOS-3406.

``parsers/coverage.py::_read_text`` accepted either a filesystem path or the
full report body inline. A realistically-sized inline report (well over
PATH_MAX/NAME_MAX) made the unguarded ``Path(source).exists()`` call raise
``OSError(ENAMETOOLONG)`` instead of returning ``False``. That exception
propagated out of ``process_coverage_report`` and was swallowed by
``ingest_report_members``'s broad ``except Exception`` in
``processors/testops_ingest.py``, silently dropping the coverage row for
every artifact-sized report -- exactly the failure mode that left
``coverage_snapshots`` empty.

``parsers/junit.py::_read_text`` already carries the correct guard
(added for CHAOS-2412); this file mirrors its
``tests/testops/test_junit_parser_read_text.py`` companion, sized so the
regression test cannot pass by accident: a short fixture stays under
PATH_MAX and would never have triggered the bug in the first place (that
gap is exactly how the existing unit tests missed CHAOS-3406), so the
content built here is tens of KB -- far past PATH_MAX (4096 on Linux, 1024
on macOS) and NAME_MAX (255) on any platform CI runs on.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.parsers.coverage import (
    _read_text,
    parse_cobertura_xml,
    parse_coverage_report,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"

_MINIMAL_COBERTURA_XML = (
    '<?xml version="1.0" ?>'
    '<coverage lines-valid="1" lines-covered="1" branches-valid="0" '
    'branches-covered="0">'
    '<packages><package name="pkg"><classes>'
    '<class name="mod" filename="pkg/mod.py">'
    '<lines><line number="1" hits="1" /></lines>'
    "</class></classes></package></packages></coverage>"
)


def _make_long_cobertura_xml(class_repeats: int = 500) -> str:
    """Build a realistically-structured cobertura XML well past PATH_MAX.

    Repeats a real ``<class>`` block (with the ``/``-bearing filename
    attributes a real report has) rather than padding with opaque
    characters, so the length check exercises the same "many path
    separators, many short components" shape a real coverage.xml has --
    the shape that makes the aggregate-length check (not the per-component
    NAME_MAX check) the one doing the work on most platforms.
    """
    class_block = (
        '<class name="mod{i}" filename="services/api/src/module_{i}.py">'
        '<lines><line number="1" hits="1"/>'
        '<line number="2" hits="1" branch="true" condition-coverage="50% (1/2)"/>'
        "</lines></class>"
    )
    classes = "".join(class_block.format(i=i) for i in range(class_repeats))
    return (
        '<?xml version="1.0" ?>'
        f'<coverage lines-valid="{class_repeats * 2}" '
        f'lines-covered="{class_repeats * 2}" branches-valid="{class_repeats}" '
        f'branches-covered="{class_repeats}">'
        f'<packages><package name="services.api"><classes>{classes}'
        "</classes></package></packages></coverage>"
    )


def test_long_cobertura_xml_exceeds_path_max() -> None:
    """Sanity check the fixture itself is large enough to be load-bearing."""
    long_xml = _make_long_cobertura_xml()
    # PATH_MAX is 4096 on Linux, 1024 on macOS; NAME_MAX is 255 everywhere.
    assert len(long_xml) > 4096


def test_read_text_returns_short_xml_string_as_content() -> None:
    result = _read_text(_MINIMAL_COBERTURA_XML)

    assert result == _MINIMAL_COBERTURA_XML


def test_read_text_long_xml_string_does_not_raise() -> None:
    """The load-bearing regression test for CHAOS-3406.

    Before the fix, this raised ``OSError(ENAMETOOLONG)`` instead of
    returning the content -- plant the defect (remove the guard) and this
    test goes red while a short-fixture test stays green, which is exactly
    what makes this test prove something the existing suite didn't.
    """
    long_xml = _make_long_cobertura_xml()

    result = _read_text(long_xml)

    assert result == long_xml


def test_read_text_preserves_existing_path_read_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "coverage.xml"
    path.write_text(_MINIMAL_COBERTURA_XML, encoding="utf-8")

    original_read_text = Path.read_text

    def fail_read_text(self: Path, *args: Any, **kwargs: Any) -> str:
        if self == path:
            raise PermissionError("cannot read report")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", fail_read_text)

    with pytest.raises(PermissionError, match="cannot read report"):
        _read_text(str(path))


def test_parse_cobertura_xml_accepts_long_xml_content_string() -> None:
    long_xml = _make_long_cobertura_xml()

    report = parse_cobertura_xml(long_xml)

    assert report.report_format == "cobertura"
    assert report.lines_total == 1000
    assert report.lines_covered == 1000
    assert len(report.files) == 500


def test_parse_coverage_report_accepts_long_xml_content_string() -> None:
    """The end-to-end shape of the bug: dispatch through ``parse_coverage_report``
    (the entrypoint ``process_coverage_report`` actually calls) with content
    sized like a real downloaded artifact, not a hand-authored fixture."""
    long_xml = _make_long_cobertura_xml()

    report = parse_coverage_report(long_xml)

    assert report.report_format == "cobertura"
    assert len(report.files) == 500


def test_parse_coverage_report_accepts_fixture_content_as_string() -> None:
    content = (FIXTURES_DIR / "sample_cobertura.xml").read_text(encoding="utf-8")

    report = parse_coverage_report(content)

    assert report.report_format == "cobertura"
    assert len(report.files) == 2
