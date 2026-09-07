"""The schema digest must mean the same thing in all four places that use it.

``go_api_routing_state``'s primary key includes ``schema_digest``. Four
independent pieces of code produce or consume that value:

* ``api/graphql/go_api_schema_digest.current_schema_digest`` -- the runtime
  producer (dispatcher reads, CLI writes, startup drift check).
* ``cmd/query-api/internal/digest.Schema`` -- what the deployed Go binary
  computes for its own ``PostgresSwitch`` lookups, reachable from a
  checkout via ``registrydump -schema-digest``.
* ``ci/check_go_api_routing_digest.py`` -- the auditor, which deliberately
  reimplements the algorithm so it can catch the producer being wrong.
* ``contracts/graphql/v1/schema-digest.json`` -- the checked-in pin.

If any two of those disagree, rows are written under one key and read
under another: every lookup misses, both planes fall back to Python, and
nothing errors. That is not a hypothetical failure mode -- it is what
happened on 2026-09-01 and stayed invisible for six days.

Before this file existed, **nothing pinned the Python producer against the
Go one**. The algorithms were documented as identical and never measured
against each other.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from dev_health_ops.api.graphql.go_api_schema_digest import (
    SCHEMA_DIGEST_PREFIX,
    current_schema_digest,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
SDL_PATH = REPO_ROOT / "contracts" / "graphql" / "v1" / "schema.graphql"
PIN_PATH = REPO_ROOT / "contracts" / "graphql" / "v1" / "schema-digest.json"
REGISTRYDUMP_DIR = REPO_ROOT / "cmd" / "query-api" / "tools" / "registrydump"


def test_digest_is_sha256_of_the_raw_sdl_bytes() -> None:
    """No parsing, no normalisation, no reprint -- raw bytes.

    Stated independently of the implementation so a "helpful" future edit
    (strip trailing whitespace, normalise line endings, parse-and-reprint
    the SDL) fails here rather than silently diverging from the Go plane,
    which hashes its ``go:embed``ed bytes with no such step.
    """
    expected = SCHEMA_DIGEST_PREFIX + hashlib.sha256(SDL_PATH.read_bytes()).hexdigest()
    assert current_schema_digest() == expected


def test_checked_in_pin_matches_the_live_sdl() -> None:
    """The pin ``ci/check_go_api_routing_digest.py`` gates on is current."""
    pinned = json.loads(PIN_PATH.read_text())["schema_digest"]
    assert pinned == current_schema_digest(), (
        "contracts/graphql/v1/schema-digest.json is stale. The SDL moved, so "
        "every go_api_routing_state row keyed to the pinned digest is now "
        "unreachable. See the 'When the schema digest moves' section of "
        "docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md"
    )


def test_python_producer_matches_the_go_producer() -> None:
    """The cross-language pin: Python's value == ``registrydump -schema-digest``.

    ``registrydump -schema-digest`` calls the SAME ``digest.Schema`` a
    running ``query-api`` uses internally for its ``PostgresSwitch``
    routing key, so agreement here is agreement with the binary's actual
    lookup key -- the property that was assumed but never measured.

    Skips without a Go toolchain rather than falling back to a hand-typed
    literal: a literal here would assert only that this file agrees with
    itself, which is precisely the kind of self-confirming test that let
    the digest drift go unnoticed.
    """
    go = shutil.which("go")
    if go is None:
        pytest.skip("go toolchain not on PATH -- cannot run registrydump")

    result = subprocess.run(
        [go, "run", str(REGISTRYDUMP_DIR), "-schema-digest"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        # Inherit the environment (GOCACHE/HOME/PATH) and override only
        # GOWORK, exactly as tests/api/graphql/_go_schema_digest.py does --
        # a stripped env fails with "build cache is required".
        env={**os.environ, "GOWORK": "off"},
    )
    if result.returncode != 0:
        pytest.fail(
            "registrydump -schema-digest failed -- that names a real problem "
            "(a bad SDL embed, a build failure), not something to skip past:\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )

    assert result.stdout.strip() == current_schema_digest(), (
        "The Python edge and the Go binary compute DIFFERENT schema digests "
        "from the same SDL. Rows written by one are invisible to the other."
    )


def test_ci_checker_reimplementation_agrees_with_the_runtime_producer() -> None:
    """The auditor and the producer must agree on the current tree.

    ``ci/check_go_api_routing_digest.py`` reimplements the algorithm on
    purpose -- an auditor that imports the thing it audits cannot catch it
    being wrong. That independence is only worth having if the two are
    pinned together somewhere, which is here.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_check_go_api_routing_digest",
        REPO_ROOT / "ci" / "check_go_api_routing_digest.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.compute_schema_digest(SDL_PATH) == current_schema_digest()


def test_producer_is_importable_without_the_web_stack() -> None:
    """The CLI and the CI checker must be able to import this producer.

    ``go_api_dispatcher`` imports httpx/starlette/strawberry at module
    scope, so importing it from a bare interpreter raises
    ``ModuleNotFoundError``. That is the whole reason the producer lives
    in its own module -- a regression that moves it back, or adds a
    third-party import here, breaks ``dev-hops go-api routing`` in the
    production image rather than in CI.
    """
    source = (
        REPO_ROOT
        / "src"
        / "dev_health_ops"
        / "api"
        / "graphql"
        / "go_api_schema_digest.py"
    ).read_text()
    for forbidden in (
        "import httpx",
        "import strawberry",
        "import starlette",
        "from starlette",
        "from strawberry",
        "import sqlalchemy",
        "from sqlalchemy",
    ):
        assert forbidden not in source, (
            f"go_api_schema_digest.py must stay stdlib-only; found {forbidden!r}"
        )
