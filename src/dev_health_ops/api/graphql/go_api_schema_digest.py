"""The Python plane's canonical Go-API schema digest.

Extracted from :mod:`dev_health_ops.api.graphql.go_api_dispatcher` (which
still calls it, unchanged) for one concrete reason: ``go_api_dispatcher``
imports ``httpx``, ``starlette`` and ``strawberry`` at module scope, so
importing it costs the whole web stack. The ``dev-hops go-api routing``
CLI and ``ci/check_go_api_routing_digest.py`` both need this ONE value
and nothing else from that module, and the CI checker in particular must
import cleanly in a bare interpreter. **This module is stdlib-only on
purpose** -- ``contract_artifacts`` (its single first-party import) is
itself stdlib-only. Do not add a third-party import here.

**Why there is exactly one producer.** ``go_api_routing_state``'s primary
key is ``(schema_digest, document_digest, selected_operation)``. If the
value used to WRITE a row and the value used to READ it are computed by
two different pieces of code, they can drift and every lookup silently
misses -- fails closed, serves Python, reports nothing. That is not
hypothetical: it is exactly what happened between 2026-09-01 and
2026-09-07 (see the "When the schema digest moves" section of
``docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md``).
The dispatcher's read, the CLI's write, the startup drift check and the
CI pin all call :func:`current_schema_digest` -- there is no second
implementation to drift against.

**Agreement with the Go plane** is a separate claim, and a weaker one:
``cmd/query-api`` embeds its OWN copy of the SDL via ``go:embed`` and
computes ``digest.Schema(schemav1.SDL)``. The algorithm is identical
(``"sha256:" + hex(sha256(raw file bytes))``, no parsing, no reprinting,
so the CHAOS-4696 two-printer trap does not apply), but the two planes
agree only while the running query-api IMAGE and this checkout carry the
same ``contracts/graphql/v1/schema.graphql``. A checkout that has moved
ahead of the deployed image produces rows the image can never read. Two
things enforce that:

* ``tests/api/graphql/test_go_api_schema_digest.py`` pins this function's
  output against ``registrydump -schema-digest`` (the same producer the
  binary uses internally), so the ALGORITHMS provably match. (The DOCUMENT
  digest already had cross-language parity coverage in
  ``test_go_api_document_digest.py``; it was the SCHEMA digest producer
  that nothing measured against Go -- codex r1 corrected an overbroad
  claim here.)
* ``dev-hops go-api routing enable`` refuses unless the RUNNING
  query-api's ``GET /registry`` reports this exact value, so the
  DEPLOYMENTS provably match at the moment rows are written.
"""

from __future__ import annotations

import functools
import hashlib

from dev_health_ops.contract_artifacts import contract_directory

__all__ = ["current_schema_digest", "SCHEMA_DIGEST_PREFIX"]

#: Every schema digest is prefixed this way, on both planes -- Go's
#: ``digest.Schema`` emits it too. Exported so callers can validate an
#: operator-supplied or wire-received value without re-typing the literal.
SCHEMA_DIGEST_PREFIX = "sha256:"


@functools.lru_cache(maxsize=1)
def current_schema_digest() -> str:
    """``"sha256:"`` + hex sha256 of ``contracts/graphql/v1/schema.graphql``'s
    RAW bytes, unmodified -- the exact algorithm and the exact file
    ``cmd/query-api/internal/digest.Schema(schemav1.SDL)`` hashes on the Go
    side.

    Cached: a pure function of the checked-out SDL for the lifetime of one
    process, so a per-request dispatch after the first reuses the value
    rather than re-reading and re-hashing the file. Deliberately NOT
    cached on failure (``lru_cache`` does not memoize a raised exception)
    -- a missing or unreadable SDL keeps failing loudly on every call
    instead of latching one bad read into a permanent silent fallback.
    """
    sdl_path = contract_directory("graphql", "v1") / "schema.graphql"
    return SCHEMA_DIGEST_PREFIX + hashlib.sha256(sdl_path.read_bytes()).hexdigest()
