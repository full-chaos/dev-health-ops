"""lizard oracle for the native Go C-family cyclomatic-complexity port
(CHAOS-5156 PR2a).

The corpus files are named ``<name>.<real-ext>.txt``, e.g.
``basic_control_flow.cpp.txt`` or ``plain_c.c.txt``: the embedded extension
(the second-to-last dot segment) is the filename lizard must see to select
its reader, and the trailing ``.txt`` keeps no Python/C/C++ tool from ever
treating the corpus as its own source (the exact reason pycc's corpus is
``*.py.txt`` rather than ``*.py`` -- see python_radon_cc_oracle.py's own
docstring for the formatter-silently-rewrites-it incident this convention
exists to prevent).

This script is the ORACLE and never the executor: it calls
``lizard.analyze_file.analyze_source_code`` and reports what real lizard
said, so a disagreement in the Go parity test is always the Go side's to
explain. Nothing here reimplements a single complexity rule.

VERSION PROVENANCE. The oracle version of record is ``uv.lock`` (lizard
1.23.0), not whatever happens to be importable in an ambient environment --
pyproject.toml's ``lizard>=1.17.10`` is a FLOOR, not a pin. The emitted
``lizard_version`` field is what lets the Go rot-guard fail loudly when the
installed lizard moves off that lock, rather than silently absorbing a
rules change as a parity "fix" (the exact same shape as pycc's radon guard).

Run:
    python3 python_lizard_cc_oracle.py <corpus-dir>

Output: one JSON object on stdout, sorted by file name so the bytes are
stable across runs and a golden diff is reviewable.
"""

from __future__ import annotations

import json
import pathlib
import sys

import lizard


def real_filename(corpus_name: str) -> str:
    """Strip the trailing ``.txt`` so lizard's own extension dispatch sees
    the language the corpus file is actually testing, e.g.
    ``basic_control_flow.cpp.txt`` -> ``basic_control_flow.cpp``."""
    if not corpus_name.endswith(".txt"):
        raise ValueError(f"corpus file {corpus_name!r} does not end in .txt")
    return corpus_name[: -len(".txt")]


def analyze(path: pathlib.Path) -> dict:
    code = path.read_text(encoding="utf-8")
    fake_name = real_filename(path.name)
    info = lizard.analyze_file.analyze_source_code(fake_name, code)
    complexities = [f.cyclomatic_complexity for f in info.function_list]
    functions_count = len(complexities)
    cyclomatic_total = sum(complexities)
    return {
        "loc": info.nloc,
        "functions_count": functions_count,
        "cyclomatic_total": cyclomatic_total,
        "cyclomatic_avg": (
            cyclomatic_total / functions_count if functions_count > 0 else 0.0
        ),
        # Thresholds and the strict ">" comparison match
        # ComplexityAnalyzer.__init__'s defaults (analytics/complexity.py:91-92)
        # and _build_result, which compute.go's BuildFileResult also uses.
        "high_complexity_functions": sum(1 for c in complexities if c > 15),
        "very_high_complexity_functions": sum(1 for c in complexities if c > 25),
        "functions": sorted(
            (
                {
                    "name": f.name,
                    "long_name": f.long_name,
                    "lineno": f.start_line,
                    "complexity": f.cyclomatic_complexity,
                }
                for f in info.function_list
            ),
            key=lambda entry: (entry["lineno"], entry["name"]),
        ),
    }


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: python_lizard_cc_oracle.py <corpus-dir>", file=sys.stderr)
        return 2
    corpus = pathlib.Path(sys.argv[1])
    if not corpus.is_dir():
        print(f"not a directory: {corpus}", file=sys.stderr)
        return 2

    results: dict[str, object] = {}
    for path in sorted(corpus.glob("*.txt")):
        results[path.name] = analyze(path)

    json.dump(
        {"lizard_version": lizard.version, "files": results},
        sys.stdout,
        indent=2,
        sort_keys=True,
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
