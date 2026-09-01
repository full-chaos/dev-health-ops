"""The Python edge's copy of query-api's registered-document inventory
(CHAOS-4697).

Maps a request's own computed document digest to the ``selected_operation``
string ``go_api_routing_state`` is keyed by -- query-api's OWN internal
registry key (see ``cmd/query-api/query_route.go``'s ``digestByOperation``
doc comment: it is never compared against GraphQL operation/field names,
only used internally and by ``PostgresSwitch``'s lookups). There is no way
to derive it from the request otherwise.

Loaded from ``go_api_operations.json``, a CHECKED-IN artifact regenerated
by ``scripts/go_api/generate_operation_catalog.py`` from query-api's own
``registrydump`` tool -- never hand-maintained (CHAOS-4466/CHAOS-4495 is
the drift class that produces). It is NOT regenerated at runtime: the
production Python edge image has no Go toolchain (query-api ships from a
separate, distroless-based image build -- see ``docker/query-api.Dockerfile``
vs ``docker/Dockerfile``), so shelling out to ``go run`` at process
startup is not an option here the way it is for a test harness.
``tests/api/graphql/test_go_api_operation_catalog.py`` is the drift check
that keeps the checked-in file honest, the same shape as
``api/graphql/export_schema.py``'s schema-drift contract.

Fails closed by construction: if the catalog file is missing, empty, or
malformed, :func:`operation_for_digest` returns ``None`` for every digest
(logged once, loudly) -- indistinguishable from "no operations are
Go-eligible", which is exactly the safe default (every request stays on
Python).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

__all__ = ["operation_for_digest", "known_operations"]

_CATALOG_PATH = Path(__file__).parent / "go_api_operations.json"

_catalog_loaded = False
_digest_to_operation: dict[str, str] = {}


def _load() -> None:
    global _catalog_loaded
    if _catalog_loaded:
        return
    _catalog_loaded = True  # never retry per-request; a bad file stays bad
    try:
        raw = _CATALOG_PATH.read_text()
        entries = json.loads(raw)
        if not isinstance(entries, list) or not entries:
            raise ValueError("catalog must be a non-empty JSON array")
        mapping: dict[str, str] = {}
        for entry in entries:
            operation = entry["operation"]
            digest = entry["digest"]
            if digest in mapping:
                raise ValueError(
                    f"duplicate digest {digest!r} for operations "
                    f"{mapping[digest]!r} and {operation!r}"
                )
            mapping[digest] = operation
        _digest_to_operation.update(mapping)
        logger.info(
            "go_api_operation_catalog.loaded",
            extra={"operation_count": len(mapping)},
        )
    except Exception:
        logger.exception(
            "go_api_operation_catalog.load_failed -- every request will stay "
            "on Python (fail-closed: no operation can be recognized as "
            "Go-eligible without this catalog)",
            extra={"catalog_path": str(_CATALOG_PATH)},
        )


def operation_for_digest(document_digest: str) -> str | None:
    """The ``selected_operation`` registered under ``document_digest``, or
    ``None`` if no known operation matches -- either a genuinely
    unregistered document (safe default: stay on Python) or a catalog
    that failed to load (also stays on Python, loudly logged once above).
    """
    _load()
    return _digest_to_operation.get(document_digest)


def known_operations() -> frozenset[str]:
    """The full set of operation names currently in the catalog. Used by
    tests and diagnostics; never by per-request dispatch logic."""
    _load()
    return frozenset(_digest_to_operation.values())
