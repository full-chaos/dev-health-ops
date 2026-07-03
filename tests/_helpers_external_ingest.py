"""Shared fixture loader + batch-envelope builder for external-ingest tests
(CHAOS-2702).

Single source of truth for the 9 v1 record-kind fixtures under
``tests/fixtures/external_ingest/v1/*.json`` — consumed by the live e2e
module (``tests/test_external_ingest_customer_push_live.py``) and the cheap
offline shape guard (``tests/test_external_ingest_fixtures_shape.py``).

Naming note: the brief called for ``tests/_helpers/external_ingest_fixtures.py``,
but this repo already has ``tests/_helpers.py`` as a plain module (not a
package) — a same-named ``tests/_helpers/`` directory would shadow/collide
with it (ambiguous namespace-package vs. module resolution). This module is
named ``tests/_helpers_external_ingest.py`` instead to avoid that collision;
see the CHAOS-2702 PR description for the reconciliation note.

Each fixture file has the shape::

    {
      "kind": "<kind>.v1",
      "valid": {"kind": "<kind>.v1", "externalId": "...", "payload": {...}},
      "invalid": {
        "kind": "<kind>.v1", "externalId": "...", "payload": {...},
        "expectedError": {"code": "missing_required_field", "field": "..."}
      }
    }

Per the CHAOS-2690 synthesizer reconciliation (brief-2702-e2e-docs.md §
SYNTHESIZER RECONCILIATION #3), each fixture's ``valid.payload`` is byte/
structurally IDENTICAL to the canonical package example shipped at
``src/dev_health_ops/api/external_ingest/examples/<kind>.v1.json`` (CHAOS-2692)
-- there is no fourth, hand-copied duplicate. ``load_package_example`` reads
that same file directly so callers (and the shape-guard test) can assert
equality instead of trusting it by convention.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import dev_health_ops.api.external_ingest.examples as _examples_pkg

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "external_ingest" / "v1"
EXAMPLES_DIR = Path(_examples_pkg.__file__).resolve().parent

#: Bare (unversioned) kind names -- fixture filenames and the batch envelope
#: builder's ``kinds=`` argument use these; wire ``kind`` values are always
#: the ``.v1``-suffixed form (e.g. ``"pull_request.v1"``).
ALL_KINDS: list[str] = [
    "repository",
    "identity",
    "team",
    "work_item",
    "work_item_transition",
    "work_item_dependency",
    "pull_request",
    "review",
    "commit",
]

DEFAULT_SCHEMA_VERSION = "external-ingest.v1"


def load_fixture(kind: str) -> dict[str, Any]:
    """Load ``tests/fixtures/external_ingest/v1/<kind>.json`` (bare kind name,
    e.g. ``"pull_request"``, not ``"pull_request.v1"``)."""
    return json.loads((FIXTURE_DIR / f"{kind}.json").read_text())


def load_package_example(kind: str) -> dict[str, Any]:
    """Load the canonical CHAOS-2692 package example payload for ``kind``."""
    return json.loads((EXAMPLES_DIR / f"{kind}.v1.json").read_text())


def _record_envelope(fixture_record: dict[str, Any]) -> dict[str, Any]:
    """Strip fixture-only bookkeeping keys (``expectedError``) down to the
    real wire shape (``RecordEnvelope``: ``kind``/``externalId``/``payload``,
    ``extra="forbid"``) before this record is placed on the wire."""
    return {
        "kind": fixture_record["kind"],
        "externalId": fixture_record["externalId"],
        "payload": fixture_record["payload"],
    }


def build_batch_envelope(
    *,
    idempotency_key: str,
    source_system: str = "github",
    source_instance: str = "acme/api",
    kinds: list[str] | None = None,
    invalid_kind: str | None = None,
    window_started_at: str = "2026-06-25T00:00:00Z",
    window_ended_at: str = "2026-06-26T00:00:00Z",
    producer: str = "pytest-e2e",
    producer_version: str = "0.0.0",
) -> dict[str, Any]:
    """Build a ``POST /validate`` / ``POST /batches`` request body.

    ``kinds`` (bare names, default ``ALL_KINDS``) contribute their ``valid``
    record; ``invalid_kind``, if given, additionally appends that kind's
    ``invalid`` record (deliberately malformed, to exercise rejection
    diagnostics).
    """
    selected_kinds = kinds if kinds is not None else ALL_KINDS
    records = [_record_envelope(load_fixture(kind)["valid"]) for kind in selected_kinds]
    if invalid_kind is not None:
        records.append(_record_envelope(load_fixture(invalid_kind)["invalid"]))
    return {
        "schemaVersion": DEFAULT_SCHEMA_VERSION,
        "idempotencyKey": idempotency_key,
        "source": {
            "type": "customer_push",
            "system": source_system,
            "instance": source_instance,
            "producer": producer,
            "producerVersion": producer_version,
        },
        "window": {"startedAt": window_started_at, "endedAt": window_ended_at},
        "records": records,
    }


__all__ = [
    "ALL_KINDS",
    "DEFAULT_SCHEMA_VERSION",
    "FIXTURE_DIR",
    "EXAMPLES_DIR",
    "load_fixture",
    "load_package_example",
    "build_batch_envelope",
]
