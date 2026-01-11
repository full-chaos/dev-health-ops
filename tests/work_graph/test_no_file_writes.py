from __future__ import annotations

import re
from pathlib import Path


FORBIDDEN_PATTERNS = [
    re.compile(r"open\([^)]*['\"]w"),
    re.compile(r"open\([^)]*['\"]a"),
    re.compile(r"open\([^)]*['\"]x"),
    re.compile(r"\.write_text\("),
    re.compile(r"\.write_bytes\("),
    re.compile(r"json\.dump\("),
    re.compile(r"yaml\.dump\("),
]


def test_no_file_writes_in_work_graph():
    root = Path(__file__).resolve().parents[2] / "work_graph"
    offenders = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_PATTERNS:
            if pattern.search(text):
                offenders.append(str(path))
                break
    assert not offenders, f"Forbidden file-write patterns in: {offenders}"
