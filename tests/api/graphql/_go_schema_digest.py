"""The canonical GO_API_SCHEMA_DIGEST producer for the go_api dual-run /
live-local harnesses (CHAOS-4696 PR2).

**Ruling (team-lead, 2026-08-31):** ``GO_API_SCHEMA_DIGEST`` used to be an
opaque, operator-supplied string with no canonical algorithm -- a wrong
value made every ``PostgresSwitch`` routing-state lookup miss and fail
closed, SILENTLY, indistinguishable from "not canaried yet". Every harness
in this directory hand-typed its OWN throwaway literal for this value (e.g.
``"sha256:wave1-dual-run-test-schema-digest"``, ``"sha256:lane-go-api-
livelocal-schema-digest"`` -- nine different scratch strings across nine
files before this module existed), and nothing stopped one of those
reaching a real environment.

CHAOS-4696 PR2 closes that gap TWO ways:

1. ``cmd/query-api/internal/digest.Schema`` is now the ONE canonical
   algorithm (``sha256:<hex of raw contracts/graphql/v1/schema.graphql
   bytes>``), shared code both a running ``query-api`` process and
   ``cmd/query-api/tools/registrydump -schema-digest`` call.
2. A running ``query-api`` process now VERIFIES its configured
   ``GO_API_SCHEMA_DIGEST`` against that same computation at startup and
   refuses to start (loudly, fail-closed) on a mismatch -- so a harness
   that spawns a real ``query-api`` binary
   (``test_go_api_livelocal.py``'s ``_start_go_server``) with one of the
   OLD hand-typed literals would now crash it immediately.

:func:`producer_schema_digest` is the ONE place every harness in this
directory gets this value from now -- never a hand-typed literal again.
It shells out to the exact same producer ``query-api`` verifies against,
so a value this returns can never disagree with what a spawned binary
requires.
"""

from __future__ import annotations

import functools
import os
import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRYDUMP_DIR = REPO_ROOT / "cmd" / "query-api" / "tools" / "registrydump"


@functools.lru_cache(maxsize=1)
def producer_schema_digest() -> str:
    """Runs ``registrydump -schema-digest`` and returns its stdout,
    trimmed. Cached: the value is a pure function of the checked-out
    ``contracts/graphql/v1/schema.graphql`` for the lifetime of one test
    process, so every call site in every harness gets the same value
    without re-shelling out per call.

    Deliberately raises rather than falling back to a literal on any
    failure (missing ``go`` toolchain, a genuine registrydump error) --
    root ``AGENTS.md``'s "a measurement that did not happen must FAIL,
    loudly" applies here exactly as it does to
    ``test_go_api_livelocal.py``'s document enumeration. Lazy and cached
    rather than a module-level constant so it never runs at IMPORT/
    COLLECTION time (which would break collection on a machine with no
    ``go`` toolchain even for a test that would otherwise skip) -- only
    when a test that actually needs it runs, after its own skip
    preconditions have already applied.
    """
    go = shutil.which("go")
    if go is None:
        raise RuntimeError(
            "go toolchain not on PATH -- required to compute the canonical "
            "GO_API_SCHEMA_DIGEST via `registrydump -schema-digest`. There "
            "is deliberately no hand-typed fallback (CHAOS-4696 PR2's "
            "whole point is removing those)."
        )
    result = subprocess.run(
        [go, "run", str(REGISTRYDUMP_DIR), "-schema-digest"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "GOWORK": "off"},
    )
    if result.returncode != 0:
        raise RuntimeError(
            "registrydump -schema-digest failed -- this names a real "
            "problem (a bad SDL embed, a build failure), NOT something to "
            f"work around with a literal:\nstdout={result.stdout}\n"
            f"stderr={result.stderr}"
        )
    value = result.stdout.strip()
    if not value.startswith("sha256:"):
        raise RuntimeError(
            "registrydump -schema-digest returned an unexpected value "
            f'(want a "sha256:"-prefixed digest): {value!r}'
        )
    return value
