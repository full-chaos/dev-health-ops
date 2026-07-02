"""Schema discovery registry for external-ingest (CHAOS-2692).

Builds the versioned JSON Schema bundle documented by ``GET /schemas`` and
``GET /schemas/{schema_version}`` once per process from CHAOS-2691's
Pydantic models in ``schemas.py`` — the single source of truth (master-spec
CC17). This module never redeclares or hand-writes a JSON Schema; it only
generates one via ``pydantic.json_schema.models_json_schema()`` and wraps it
with an ETag, a per-record-kind ``$ref`` index, and canonical examples
(``examples/*.json``, CC18) for discovery/export/CLI consumers.

See ``docs/architecture/adr-005-external-ingest-schema-discovery.md`` for
the versioning/caching/static-export rationale.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from typing import Any

from pydantic import BaseModel
from pydantic.json_schema import JsonSchemaMode, models_json_schema

from . import schemas as s

SUPPORTED_SCHEMA_VERSIONS: tuple[str, ...] = (s.SCHEMA_VERSION,)


@dataclass(frozen=True)
class SchemaBundle:
    schema_version: str
    document: dict[str, Any]  # full JSON Schema doc, incl. $defs — ETag'd
    etag: str
    record_kinds: tuple[str, ...]


def compute_etag(document: dict[str, Any]) -> str:
    """Quoted sha256 over the canonical (sorted, compact) JSON of ``document``.

    Public so ``router.py`` can hash the *actual served representation*
    (schema document + live ``limits``) per request for ``If-None-Match``
    comparisons — not just the cached ``SchemaBundle.etag``, which only
    covers the schema shape (adversarial-review finding: a 304 must be a
    correct HTTP validator for the whole response body, not a subset of it,
    or a client could cache stale ``limits`` under an unchanged ETag).
    """
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f'"{digest}"'


def _tighten_server_enforced_literals(definitions: dict[str, Any]) -> None:
    """Add ``const``/``enum`` for two fields the server rejects on but whose
    Pydantic type is a bare ``str`` (adversarial-review finding, round 2):
    ``BatchEnvelope.schema_version`` and ``RecordEnvelope.kind`` are typed
    ``str`` in ``schemas.py`` (not ``Literal``) so ``POST /batches`` can 400
    with a precise ``unsupported_schema_version``/``unknown_record_kind``
    message instead of FastAPI's generic 422 — but that means the
    *generated* schema for those two fields was just ``{"type": "string"}``,
    so a customer validating against it could certify a batch the server
    actually rejects (e.g. ``schemaVersion: "external-ingest.v99"``).

    This does not hand-write a schema (D5) or redeclare a model (CC17) — it
    tightens two already-generated field schemas using data this registry
    already owns (``SUPPORTED_SCHEMA_VERSIONS``, ``RECORD_KIND_MODELS``
    keys), post-``models_json_schema()``, to match documented, tested server
    behavior (``router.py``'s ``_check_schema_version_or_400`` /
    ``_check_all_kinds_known_or_400``).
    """
    defs = definitions["$defs"]

    schema_version_field = defs["BatchEnvelope"]["properties"]["schemaVersion"]
    if len(SUPPORTED_SCHEMA_VERSIONS) == 1:
        schema_version_field["const"] = SUPPORTED_SCHEMA_VERSIONS[0]
    else:
        schema_version_field["enum"] = list(SUPPORTED_SCHEMA_VERSIONS)

    defs["RecordEnvelope"]["properties"]["kind"]["enum"] = sorted(s.RECORD_KIND_MODELS)


def _load_example(kind: str) -> dict[str, Any]:
    if kind not in s.RECORD_KIND_MODELS:
        raise KeyError(f"Unknown record kind: {kind!r}")
    data = (
        resources.files("dev_health_ops.api.external_ingest.examples")
        .joinpath(f"{kind}.json")
        .read_text()
    )
    return json.loads(data)


@lru_cache(maxsize=1)
def _build_v1_bundle() -> SchemaBundle:
    # models_json_schema (not per-model model_json_schema() stitched by
    # hand) gives collision-free $defs across all models in one pass (D5) —
    # verified against installed pydantic 2.13.4: default ref_template
    # already produces "#/$defs/{model}" refs, so no override is needed.
    mode: JsonSchemaMode = "validation"
    models_and_modes: list[tuple[type[BaseModel], JsonSchemaMode]] = [
        (s.BatchEnvelope, mode)
    ] + [(model, mode) for model in s.RECORD_KIND_MODELS.values()]
    refs, definitions = models_json_schema(models_and_modes)
    _tighten_server_enforced_literals(definitions)

    record_index: dict[str, Any] = {}
    for kind, model in s.RECORD_KIND_MODELS.items():
        ref = refs[(model, "validation")]
        record_index[kind] = {**ref, "examples": [_load_example(kind)]}

    document: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": (
            "https://api.fullchaos.dev/api/v1/external-ingest/schemas/"
            f"{s.SCHEMA_VERSION}"
        ),
        "schemaVersion": s.SCHEMA_VERSION,
        "title": s.SCHEMA_VERSION,
        "description": (
            "FullChaos external customer-push ingestion contract, v1. "
            "'envelope' validates batch shape only — records[].payload is "
            "intentionally unconstrained there (see $defs.RecordEnvelope); "
            "validate each record's payload against recordKinds[kind].$ref "
            "for kind-specific field requirements (adversarial-review "
            "finding: envelope-only validation can pass a payload the "
            "server's POST /validate rejects)."
        ),
        "envelope": refs[(s.BatchEnvelope, "validation")],
        "recordKinds": record_index,
        **definitions,  # contributes "$defs": {...}
    }

    return SchemaBundle(
        schema_version=s.SCHEMA_VERSION,
        document=document,
        # Structural ETag over the schema shape alone (stable across
        # process restarts, independent of dict insertion order, D6) — the
        # HTTP-served ETag used for If-None-Match is computed fresh per
        # request in router.py over document + live limits (compute_etag()).
        etag=compute_etag(document),
        record_kinds=tuple(s.RECORD_KIND_MODELS),
    )


def get_bundle(schema_version: str) -> SchemaBundle | None:
    if schema_version == s.SCHEMA_VERSION:
        return _build_v1_bundle()
    return None


def list_versions() -> list[dict[str, Any]]:
    bundle = _build_v1_bundle()
    return [
        {
            "schemaVersion": bundle.schema_version,
            "recordKinds": list(bundle.record_kinds),
        }
    ]


def load_example(kind: str) -> dict[str, Any]:
    """Public accessor for CHAOS-2700's ``dev-hops push sample --kind``."""
    return _load_example(kind)


def iter_record_kinds() -> list[tuple[str, type[BaseModel]]]:
    """Public (kind, model) pairs — avoids importing the private dict."""
    return list(s.RECORD_KIND_MODELS.items())


__all__ = [
    "SUPPORTED_SCHEMA_VERSIONS",
    "SchemaBundle",
    "compute_etag",
    "get_bundle",
    "list_versions",
    "load_example",
    "iter_record_kinds",
]
