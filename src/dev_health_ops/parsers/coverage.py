from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET


@dataclass(frozen=True)
class CoverageFileRecord:
    file_path: str
    lines_total: int | None
    lines_covered: int | None
    branches_total: int | None = None
    branches_covered: int | None = None
    functions_total: int | None = None
    functions_covered: int | None = None


@dataclass(frozen=True)
class CoverageReport:
    report_format: str
    lines_total: int | None
    lines_covered: int | None
    branches_total: int | None = None
    branches_covered: int | None = None
    functions_total: int | None = None
    functions_covered: int | None = None
    files: list[CoverageFileRecord] = field(default_factory=list)


def _read_text(source: str | bytes | Path) -> str:
    if isinstance(source, Path):
        return source.read_text(encoding="utf-8")
    if isinstance(source, bytes):
        return source.decode("utf-8")
    path = Path(source)
    if path.exists():
        return path.read_text(encoding="utf-8")
    return source


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _condition_counts(value: str | None) -> tuple[int | None, int | None]:
    if not value or "(" not in value or "/" not in value:
        return None, None
    counts = value.split("(", 1)[1].rstrip(")")
    covered, total = counts.split("/", 1)
    return _safe_int(total), _safe_int(covered)


def parse_lcov_report(source: str | bytes | Path) -> CoverageReport:
    content = _read_text(source)
    files: list[CoverageFileRecord] = []
    current_path: str | None = None
    line_totals: set[int] = set()
    covered_lines: set[int] = set()
    lines_total: int | None = None
    lines_covered: int | None = None
    branches_total: int | None = None
    branches_covered: int | None = None
    functions_total: int | None = None
    functions_covered: int | None = None

    def flush() -> None:
        nonlocal current_path, line_totals, covered_lines, lines_total, lines_covered
        nonlocal branches_total, branches_covered, functions_total, functions_covered
        if current_path is None:
            return
        file_lines_total = lines_total if lines_total is not None else len(line_totals)
        file_lines_covered = (
            lines_covered if lines_covered is not None else len(covered_lines)
        )
        files.append(
            CoverageFileRecord(
                file_path=current_path,
                lines_total=file_lines_total,
                lines_covered=file_lines_covered,
                branches_total=branches_total,
                branches_covered=branches_covered,
                functions_total=functions_total,
                functions_covered=functions_covered,
            )
        )
        current_path = None
        line_totals = set()
        covered_lines = set()
        lines_total = None
        lines_covered = None
        branches_total = None
        branches_covered = None
        functions_total = None
        functions_covered = None

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line == "end_of_record":
            flush()
            continue
        prefix, _, value = line.partition(":")
        if prefix == "SF":
            flush()
            current_path = value.strip()
        elif prefix == "DA":
            parts = value.split(",")
            line_number = _safe_int(parts[0])
            hits = _safe_int(parts[1]) if len(parts) > 1 else None
            if line_number is not None:
                line_totals.add(line_number)
                if hits and hits > 0:
                    covered_lines.add(line_number)
        elif prefix == "LF":
            lines_total = _safe_int(value)
        elif prefix == "LH":
            lines_covered = _safe_int(value)
        elif prefix == "BRF":
            branches_total = _safe_int(value)
        elif prefix == "BRH":
            branches_covered = _safe_int(value)
        elif prefix == "FNF":
            functions_total = _safe_int(value)
        elif prefix == "FNH":
            functions_covered = _safe_int(value)
    flush()

    return CoverageReport(
        report_format="lcov",
        lines_total=sum(record.lines_total or 0 for record in files) or None,
        lines_covered=sum(record.lines_covered or 0 for record in files) or None,
        branches_total=sum(record.branches_total or 0 for record in files) or None,
        branches_covered=sum(record.branches_covered or 0 for record in files) or None,
        functions_total=sum(record.functions_total or 0 for record in files) or None,
        functions_covered=sum(record.functions_covered or 0 for record in files)
        or None,
        files=files,
    )


def parse_cobertura_xml(source: str | bytes | Path) -> CoverageReport:
    root = ET.fromstring(_read_text(source))
    files: dict[str, CoverageFileRecord] = {}

    for class_element in root.findall(".//class"):
        filename = class_element.get("filename")
        if not filename:
            continue
        lines = class_element.findall("./lines/line")
        line_total = len(lines)
        line_covered = sum(
            1 for line in lines if (_safe_int(line.get("hits")) or 0) > 0
        )
        branch_total = 0
        branch_covered = 0
        for line in lines:
            total, covered = _condition_counts(line.get("condition-coverage"))
            branch_total += total or 0
            branch_covered += covered or 0
        files[filename] = CoverageFileRecord(
            file_path=filename,
            lines_total=line_total or None,
            lines_covered=line_covered,
            branches_total=branch_total or None,
            branches_covered=branch_covered or None,
        )

    file_records = list(files.values())
    lines_total = _safe_int(root.get("lines-valid"))
    lines_covered = _safe_int(root.get("lines-covered"))
    branches_total = _safe_int(root.get("branches-valid"))
    branches_covered = _safe_int(root.get("branches-covered"))

    if lines_total is None:
        lines_total = sum(record.lines_total or 0 for record in file_records) or None
    if lines_covered is None:
        lines_covered = (
            sum(record.lines_covered or 0 for record in file_records) or None
        )
    if branches_total is None:
        branches_total = (
            sum(record.branches_total or 0 for record in file_records) or None
        )
    if branches_covered is None:
        branches_covered = (
            sum(record.branches_covered or 0 for record in file_records) or None
        )

    return CoverageReport(
        report_format="cobertura",
        lines_total=lines_total,
        lines_covered=lines_covered,
        branches_total=branches_total,
        branches_covered=branches_covered,
        files=file_records,
    )


def parse_coverage_report(
    source: str | bytes | Path,
    report_format: str | None = None,
) -> CoverageReport:
    content = _read_text(source)
    normalized_format = (report_format or "").strip().lower()

    if normalized_format == "lcov" or content.lstrip().startswith(("TN:", "SF:")):
        return parse_lcov_report(content)
    if normalized_format == "cobertura" or "<coverage" in content:
        return parse_cobertura_xml(content)
    raise ValueError("Unsupported coverage report format")
