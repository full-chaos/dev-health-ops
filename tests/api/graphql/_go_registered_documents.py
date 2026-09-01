"""Shared registered-document text lookup for the go_api dual-run harnesses.

**EXECUTED regression found running this repo's OWN real dual-run proof**
(CHAOS-4696 PR2, 2026-08-31): CHAOS-4696 PR1 regenerated all 12
``registered*Document`` consts in ``cmd/query-api/query_route.go`` to the
TRUE wire form a real urql client sends (``print()`` reflow AND
``cacheExchange``'s ``__typename`` injection) -- but every
``test_go_api_dual_run_*.py`` file's own ``<OPERATION>_DOCUMENT`` constant
was a HAND-TYPED COPY of the OLD web-source-text form. Running
``test_go_api_dual_run_feature_flags.py`` against a REAL spawned
``query-api`` binary post-PR1-merge returns HTTP 404 on every request:
the harness's own hardcoded document text no longer digest-matches the
registered const. Every dual-run harness's ``_post_graphql``-style helper
IS a real query-api-facing HTTP client -- exactly the class of caller
CHAOS-4696's own evidence bar says must never hand-type a copy of
registered document text (root ``AGENTS.md``'s "ask what the harness
cannot see" -- these harnesses could not see their OWN staleness because
nothing compared their literal against the real source).

:func:`registered_document` is the ONE place these harnesses read a
document's text from now: ``cmd/query-api/tools/registrydump``'s own
enumeration (a ``go/ast`` parse of ``query_route.go``, the exact source
the running binary compiles from -- the same mechanism
``test_go_api_livelocal.py``'s own ``_enumerate_registered_documents``
already used, generalized here into a by-name lookup other harnesses can
import). A future PR that changes any registered document's text needs
NO change in this directory: every dual-run harness picks up the new text
automatically on its next run.
"""

from __future__ import annotations

import functools
import json
import os
import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
QUERY_ROUTE_GO = REPO_ROOT / "cmd" / "query-api" / "query_route.go"
REGISTRYDUMP_DIR = REPO_ROOT / "cmd" / "query-api" / "tools" / "registrydump"


@functools.lru_cache(maxsize=1)
def _enumerate() -> dict[str, str]:
    """Runs registrydump once per test process and caches the
    operation -> document-text mapping. Lazy (not a module-level call)
    so importing this module never runs `go run` at collection time --
    only when a test that actually needs a document's text runs, after
    its own skip preconditions have already applied (same discipline as
    _go_schema_digest.producer_schema_digest).
    """
    go = shutil.which("go")
    if go is None:
        raise RuntimeError(
            "go toolchain not on PATH -- required to read registered "
            "document text via `registrydump`. There is deliberately no "
            "hand-typed fallback; CHAOS-4696's whole point is removing "
            "those."
        )
    result = subprocess.run(
        [go, "run", str(REGISTRYDUMP_DIR), "-file", str(QUERY_ROUTE_GO)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "GOWORK": "off"},
    )
    if result.returncode != 0:
        raise RuntimeError(
            "registrydump failed to enumerate registered documents -- a "
            "real gap in the registration source itself, not a bug in "
            f"this helper:\nstdout={result.stdout}\nstderr={result.stderr}"
        )
    docs = json.loads(result.stdout)
    if not docs:
        raise RuntimeError(
            f"registrydump enumerated ZERO registered documents from {QUERY_ROUTE_GO}"
        )
    return {d["operation"]: d["document"] for d in docs}


def registered_document(operation: str) -> str:
    """Returns the REGISTERED document text for `operation` (e.g.
    "featureFlags"), read live from query_route.go via registrydump --
    never a hand-typed copy that can silently drift from the real const.

    Raises KeyError naming every currently-known operation if `operation`
    is not registered -- a real finding (the operation was renamed or
    removed), not something to skip past.
    """
    docs = _enumerate()
    if operation not in docs:
        raise KeyError(
            f"{operation!r} is not a currently registered document "
            f"(registrydump reports: {sorted(docs)})"
        )
    return docs[operation]
