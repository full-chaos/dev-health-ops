"""Oracle for FnMatch: what CPython's fnmatch actually matches.

`_load_repos` (metrics/job_complexity_db.py) filters repository names with
``fnmatch.fnmatch``, and the pattern reaches the worker through
ComplexityScope. Go's path.Match is not equivalent, so the Go port is compared
against CPython rather than against a reading of the docs.

Reads a JSON array of [pattern, name] pairs on stdin -- JSON rather than
line-separated text ON PURPOSE: a name containing a newline cannot survive a
line-based protocol, and `*` and `?` crossing a newline is exactly one of the
behaviours under test. A harness that silently drops those cases would report
parity it never checked.

Emits the match RESULT and CPython's own translated regular expression; the
latter because two different expressions can agree on a small sample and
diverge on the input nobody thought to try.

This script is the ORACLE and never the implementation.
"""

from __future__ import annotations

import fnmatch
import json
import sys


def main() -> int:
    pairs = json.load(sys.stdin)
    results = []
    for pattern, name in pairs:
        results.append(
            {
                "pattern": pattern,
                "name": name,
                "match": fnmatch.fnmatch(name, pattern),
                "matchcase": fnmatch.fnmatchcase(name, pattern),
                "translated": fnmatch.translate(pattern),
            }
        )
    json.dump(results, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
