#!/usr/bin/env python3
"""Regenerate the Python edge's Go-API operation catalog (CHAOS-4697).

The edge dispatcher (``api/graphql/go_api_dispatcher.py``) must know, given
an incoming request's document digest, which ``selected_operation`` string
to use for the ``go_api_routing_state`` lookup -- that string is
query-api's OWN internal registry key (``digestByOperation`` in
``cmd/query-api/query_route.go``), not derivable from the GraphQL
operation name (see that map's doc comment: "investmentBreakdown"/
"investmentFull" both invoke the `analytics` root field).

Rather than hand-maintain that mapping (the CHAOS-4466/CHAOS-4495 drift
class the runbook and ``registrydump`` itself warn about), this script
regenerates it from the ONE canonical producer -- ``registrydump -file
cmd/query-api/query_route.go``, a ``go/ast`` parse of the real route
source -- and writes ONLY ``operation`` and ``digest`` to the checked-in
catalog (``api/graphql/go_api_operations.json``). The raw document text is
deliberately NOT persisted here: the edge never needs it at runtime (only
the digest, to match against a request's own computed digest), and
carrying a second copy of the query text would be exactly the kind of
extra copy CHAOS-4696 warns about, for no runtime benefit.

Run this whenever ``cmd/query-api/query_route.go``'s registered documents
change. ``tests/api/graphql/test_go_api_operation_catalog.py`` asserts the
checked-in file has NOT drifted from what this script would produce right
now -- a CI failure there means this script needs to be re-run and the
diff committed, not that the test is wrong.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
QUERY_ROUTE_GO = REPO_ROOT / "cmd" / "query-api" / "query_route.go"
REGISTRYDUMP_DIR = REPO_ROOT / "cmd" / "query-api" / "tools" / "registrydump"
CATALOG_PATH = (
    REPO_ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "go_api_operations.json"
)


def generate() -> list[dict[str, str]]:
    go = shutil.which("go")
    if go is None:
        raise RuntimeError(
            "go toolchain not on PATH -- required to regenerate the operation "
            "catalog via `registrydump`. There is deliberately no hand-typed "
            "fallback (same discipline CHAOS-4696 established for the schema "
            "digest producer)."
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
            f"registrydump failed:\nstdout={result.stdout}\nstderr={result.stderr}"
        )
    docs = json.loads(result.stdout)
    if not docs:
        raise RuntimeError(
            f"registrydump enumerated ZERO documents from {QUERY_ROUTE_GO}"
        )
    catalog = sorted(
        ({"operation": d["operation"], "digest": d["digest"]} for d in docs),
        key=lambda d: d["operation"],
    )
    seen_digests = {d["digest"] for d in catalog}
    if len(seen_digests) != len(catalog):
        raise RuntimeError(
            "registrydump enumerated two operations with the SAME document "
            "digest -- the edge catalog cannot disambiguate them by digest "
            f"alone: {catalog}"
        )
    return catalog


def main() -> int:
    catalog = generate()
    CATALOG_PATH.write_text(json.dumps(catalog, indent=2) + "\n")
    print(f"wrote {len(catalog)} operations to {CATALOG_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
