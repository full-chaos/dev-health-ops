"""Radon oracle for the native Go cyclomatic-complexity port (CHAOS-4971a).

The corpus files are named ``*.py.txt``, not ``*.py``, ON PURPOSE. They are
parser INPUT, not source: they carry one-line suites, backslash/bracket line
continuations and single-quoted strings because those are the lexical shapes
the Go tokenizer must handle. ruff-format rewrote all three the first time they
were committed as ``.py`` -- and every test still passed, because formatting
does not change complexity, so the corpus silently stopped testing what it
claims to test. A ruff ``exclude`` entry does NOT prevent this: ``exclude``
governs file discovery, and lefthook passes staged files to ruff explicitly,
which bypasses it unless ``force-exclude`` is set. Renaming is the fix that
needs no repo-wide config change and that no future tool can undo by accident.

Emits, for every ``*.py.txt`` file in the corpus directory, exactly what
``analytics/complexity.py::_analyze_python`` derives from radon: the block
list (name, kind, complexity) plus the aggregates the family stores.

This script is the ORACLE and never the executor. Nothing here is allowed
to reimplement a rule -- it calls ``radon.complexity.cc_visit`` and reports
what radon said, so a disagreement is always the Go side's to explain.

VERSION PROVENANCE. The oracle version of record is ``uv.lock`` (radon
6.0.1), not whatever happens to be importable in an ambient environment.
``pyproject.toml`` declares ``radon>=6.0.1``, which is a FLOOR and not a
pin -- so the resolved lock entry is the only authority on which radon
produced these numbers. The emitted ``radon_version`` field is what lets
the Go rot-guard fail loudly when the installed radon moves off that lock,
rather than absorbing a rules change as a parity "fix".

Run:
    python3 python_radon_cc_oracle.py <corpus-dir>

Output: one JSON object on stdout, sorted by file name so the bytes are
stable across runs and a golden diff is reviewable.
"""

from __future__ import annotations

import json
import pathlib
import sys

from radon.complexity import cc_visit
from radon.visitors import Class, Function


def _kind(block: object) -> str:
    """Map radon's block objects to the Go side's BlockKind names.

    radon models a method as a Function whose ``is_method`` is True, which
    is the same distinction Go's BlockMethod draws.
    """
    if isinstance(block, Class):
        return "class"
    if isinstance(block, Function):
        return "method" if block.is_method else "function"
    return "unknown"


def analyze(path: pathlib.Path) -> dict | None:
    code = path.read_text(encoding="utf-8")
    try:
        blocks = cc_visit(code)
    except Exception as exc:  # noqa: BLE001 -- mirrors _analyze_python
        # analytics/complexity.py:219-222 catches EVERY exception and
        # returns None, dropping the file. Reporting the skip explicitly
        # lets the Go test assert that it skips the same files, rather
        # than assuming both sides fail on the same input.
        return {"skipped": True, "reason": type(exc).__name__, "detail": str(exc)}

    complexities = [b.complexity for b in blocks]
    functions_count = len(complexities)
    cyclomatic_total = sum(complexities)
    return {
        "skipped": False,
        "loc": len(code.splitlines()),
        "functions_count": functions_count,
        "cyclomatic_total": cyclomatic_total,
        "cyclomatic_avg": (
            cyclomatic_total / functions_count if functions_count > 0 else 0.0
        ),
        # Thresholds are the defaults from ComplexityAnalyzer.__init__
        # (analytics/complexity.py:91-92) and the comparison is strict `>`,
        # matching _build_result.
        "high_complexity_functions": sum(1 for c in complexities if c > 15),
        "very_high_complexity_functions": sum(1 for c in complexities if c > 25),
        "blocks": sorted(
            (
                {
                    "name": b.name,
                    "kind": _kind(b),
                    "lineno": b.lineno,
                    "col_offset": b.col_offset,
                    "complexity": b.complexity,
                }
                for b in blocks
            ),
            key=lambda entry: (entry["lineno"], entry["col_offset"], entry["name"]),
        ),
    }


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: python_radon_cc_oracle.py <corpus-dir>", file=sys.stderr)
        return 2
    corpus = pathlib.Path(sys.argv[1])
    if not corpus.is_dir():
        print(f"not a directory: {corpus}", file=sys.stderr)
        return 2

    import radon

    results: dict[str, object] = {}
    for path in sorted(corpus.glob("*.py.txt")):
        results[path.name] = analyze(path)

    json.dump(
        {"radon_version": radon.__version__, "files": results},
        sys.stdout,
        indent=2,
        sort_keys=True,
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
