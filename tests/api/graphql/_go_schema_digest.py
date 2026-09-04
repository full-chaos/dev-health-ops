"""The canonical schema-digest producer for the go_api dual-run /
live-local harnesses (CHAOS-4696 PR2).

**Ruling (team-lead, 2026-08-31):** the schema digest used to be an
opaque, operator-supplied string (``GO_API_SCHEMA_DIGEST``) with no
canonical algorithm -- a wrong value made every ``PostgresSwitch``
routing-state lookup miss and fail closed, SILENTLY, indistinguishable
from "not canaried yet". Every harness in this directory hand-typed its
OWN throwaway literal for this value (e.g.
``"sha256:wave1-dual-run-test-schema-digest"``, ``"sha256:lane-go-api-
livelocal-schema-digest"`` -- nine different scratch strings across nine
files before this module existed), and nothing stopped one of those
reaching a real environment.

CHAOS-4696 PR2 made ``cmd/query-api/internal/digest.Schema`` the ONE
canonical algorithm (``sha256:<hex of raw contracts/graphql/v1/
schema.graphql bytes>``), shared code both a running ``query-api``
process and ``cmd/query-api/tools/registrydump -schema-digest`` call.

**CHAOS-5013 (2026-09-04) removed ``GO_API_SCHEMA_DIGEST`` and the
startup verification against it** (chris: "you version a schema, but to
start something -- no") **and the Python edge's per-request comparison**
alongside it: a running ``query-api`` process now always COMPUTES this
value internally for its own ``PostgresSwitch`` routing key, never reads
or verifies an operator-supplied env var for it, and never crashes over
it. This module and :func:`producer_schema_digest` still exist and are
still needed: something has to independently produce the value used to
SEED ``go_api_routing_state``/``go_api_candidate_build`` rows so they
carry the same digest a spawned ``query-api`` binary computes -- shelling
out to the same producer the binary uses internally is what keeps that
guaranteed, exactly as CHAOS-4696 PR2 established.

:func:`producer_schema_digest` is the ONE place every harness in this
directory gets this value from -- never a hand-typed literal.
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
