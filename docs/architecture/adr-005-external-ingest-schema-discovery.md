# ADR-005: External-ingest schema discovery and JSON Schema export

## Status

Accepted.

## Context

CHAOS-2691 ships the frozen wire contract for `/api/v1/external-ingest/*` as
Pydantic v2 models in `api/external_ingest/schemas.py` — the envelope
(`BatchEnvelope`), its wrapper (`RecordEnvelope`), and the 9 record-kind
payload models in `RECORD_KIND_MODELS`. Customers need a machine-readable,
versioned way to discover that contract (`GET /schemas`, `GET
/schemas/{schema_version}`) without hand-reading Python source, so their own
CI pipelines and SDKs can validate payloads before pushing — and `dev-hops
push validate`/`push sample` (CHAOS-2700) need the same generated shapes to
work offline. This ADR records the generation, caching, and static-export
decisions CHAOS-2692 makes on top of CHAOS-2691's models (mirroring
`docs/architecture/adr-003-external-ingest-rest-boundary.md`'s "record
decisions in-tree" convention).

## Decision 1: JSON Schema is generated, never hand-written

`schema_registry.py` builds the discovery bundle from
`pydantic.json_schema.models_json_schema()` over `BatchEnvelope` plus all 9
`RECORD_KIND_MODELS` values in one call, not per-model `model_json_schema()`
calls stitched together by hand. Verified against the installed pydantic
(2.13.4): the default `ref_template` already emits `#/$defs/{ModelName}`
refs and the returned `$defs` are collision-free across all 10 models, so no
`ref_template` override or manual `$ref` merging is needed. This is the same
"single source of truth" property CC17 establishes for validation — the
registry imports `RECORD_KIND_MODELS`/`BatchEnvelope` and never redeclares a
field.

## Decision 2: One bundled document per envelope version, `$ref`-based record index

`GET /schemas/{schema_version}` returns one JSON Schema document containing:

- `envelope`: `{"$ref": "#/$defs/BatchEnvelope"}`
- `recordKinds`: a map of versioned kind (`"commit.v1"`, …) to
  `{"$ref": "#/$defs/<Model>", "examples": [...]}`
- `$defs`: every referenced model's schema, generated once by
  `models_json_schema()`

`schema_version` is the envelope-level API version (`external-ingest.v1`),
not a per-record-kind version — bumping a single record kind's shape is
expressed by renaming its Pydantic model / `kind` literal (`commit.v2`)
while the bundle's `schema_version` stays `v1`; bumping the envelope shape
itself requires a new `external-ingest.v2` bundle. `$ref`-based indexing
(over inlining each record kind's full schema under `recordKinds`) lets a
standard validator (ajv, the Python `jsonschema` package) resolve
`$defs.CommitV1` directly from the one document — proven in this issue's
live verification by validating each kind's example against
`{**doc, "$ref": doc["recordKinds"][kind]["$ref"]}` with an independent
`jsonschema` validator, not Pydantic itself.

`GET /schemas/{schema_version}` does not support a `?kind=` narrowing query
param in v1 — the full bundle (9 kinds) is small and standard `$ref`
resolution already lets a customer validate a single record kind from it.

## Decision 3: Compute once per process, ETag over canonical JSON

`schema_registry._build_v1_bundle()` is `@lru_cache(maxsize=1)`'d and forced
eagerly at `router.py` import time (a bare `get_bundle(SCHEMA_VERSION)`
call, result discarded) — `models_json_schema()` reflection and example-file
loading are non-trivial and deterministic per process, so a bad example
fixture or a schema-generation regression fails app startup, not a
customer's first request.

`schema_registry.compute_etag(document) = sha256(json.dumps(document,
sort_keys=True, separators=(",", ":")))`, wrapped in quotes.
`SchemaBundle.etag` (computed once, cached with the rest of the bundle) is a
*structural* hash over the schema shape alone — useful for content-addressing
independent of runtime config. The **served** `ETag` header is a different,
per-request value: `router.py` hashes the full response body actually
returned (`{**bundle.document, "limits": _limits_payload()}`, live env-var
values included) via that same `compute_etag()` helper. This distinction
closes an adversarial-review finding in this issue's own review: an earlier
draft hashed only `bundle.document` and appended live `limits` outside the
hash, so a `maxRecordsPerBatch`/`maxBodyBytes` change (env var) could return
a `304 Not Modified` for a response whose body had actually changed — an
incorrect HTTP validator for the representation served. Hashing the whole
body per request is cheap (a few KB, no re-reflection — `bundle.document` is
still cache-built once) and keeps `If-None-Match` correct for the entire
representation, not just the versioned-schema portion of it.

`GET /schemas/{schema_version}` sets `ETag` and `Cache-Control: public,
max-age=3600, must-revalidate`; a matching `If-None-Match` request returns
`304` with only the `ETag` header (no body).

No new caching middleware is added — no generic ETag/Cache-Control handling
existed anywhere in the app before this issue, and schemas are the first
genuinely cacheable, static-per-deploy REST resource; a narrow, local
implementation in the two `GET /schemas*` handlers avoids scope-creeping a
decision for every other route.

## Decision 4: Discovery endpoints stay public; rate-limited against anonymous DoS by IP, never by an unvalidated bearer token

Per CHAOS-2691's D2 (unchanged by this issue): `GET /schemas` and `GET
/schemas/{schema_version}` are not gated behind `require_ingest_scope`.
`schema:read` is reserved for a future per-org-customization state and is a
no-op scope for these two routes. Both routes keep CHAOS-2691's
`INGEST_READ_LIMIT` (`120/minute`) — public + ETag'd is still a
registry-render per request, a cheap anonymous DoS surface without a
limiter.

They do **not**, however, reuse `rate_limit.get_ingest_token_key` as their
limiter key function — a second adversarial-review finding in this issue's
own review. `get_ingest_token_key` hashes any `Authorization: Bearer <value>`
header into its own limiter bucket, which is the *correct* behavior for
`POST /batches`/`POST /validate` once a bearer value has been checked against
a real credential (CHAOS-2696/2712): distinct valid tokens should not share
one global bucket. But `GET /schemas*` never validates the bearer value at
all (that's the whole point of D2 — no auth dependency runs) — so keying on
an unvalidated header lets a caller mint a fresh 120/minute allowance on
every request just by rotating a random string, defeating the limiter
entirely. `router.py` instead defines `_schema_discovery_rate_limit_key`, a
small IP-only key function (`rate_limit.get_forwarded_ip`) scoped to just
these two routes — the only identity that actually exists for an
unauthenticated request. `POST /batches`/`POST /validate` are unaffected and
keep `get_ingest_token_key`.

## Decision 5: Static export checked into `docs/api/external-ingest/v1/`, enforced by a no-drift pytest

`export_schemas.py` (`python3 -m
dev_health_ops.api.external_ingest.export_schemas --out <path>`) mirrors
`api/graphql/export_schema.py`'s shape (argparse, `--out`, stdout fallback)
and serializes the same `schema_registry.get_bundle(...).document` used by
the live endpoint (`json.dumps(..., indent=2, sort_keys=True)`), so the
committed `docs/api/external-ingest/v1/schema.json` and the live response
can never structurally diverge except by an unstaged regeneration.
`tests/api/external_ingest/test_schema_export_no_drift.py` fails the gate
(not a new CI workflow — the existing unit-test tier already runs it) if the
committed file doesn't byte-match a fresh export, with a message telling the
implementer the exact regeneration command.

Rationale for checking in a static artifact instead of "customers just call
the live endpoint": (a) customers/CI can vendor the file into their own repo
for offline `ajv`/`jsonschema` validation without a live FullChaos
dependency, (b) docs get a stable linkable artifact
(`docs/examples/external-ingest/` in CHAOS-2701 links to it), (c) the
no-drift test is cheap insurance against silent registry/docs skew at PR
time, since generation is deterministic and fast.

One deliberate deviation from the GraphQL precedent: `export_schemas.py`'s
`main()` runs under `if __name__ == "__main__":` rather than unconditionally
at module scope, so the no-drift test can `import render_schema_json`
without argparse consuming pytest's own argv.

## Decision 6: Canonical examples live under `api/external_ingest/examples/`, one JSON file per kind

`api/external_ingest/examples/<kind>.json` (e.g. `commit.v1.json`) holds one
realistic **payload-shaped** example per record kind — not a full
`RecordEnvelope` wrapper — loaded via `importlib.resources` (so it works
once packaged as a wheel, not just in a source checkout) and attached to
each `recordKinds` entry's `"examples"` array in both the live bundle and
the static export. This is CC18's single canonical fixture home for the
whole epic: CHAOS-2700's `dev-hops push sample --kind` calls
`schema_registry.load_example(kind)` directly rather than duplicating
payloads; CHAOS-2701 copies these files to
`docs/examples/external-ingest/` under its own byte-identity drift test;
CHAOS-2702's e2e valid fixtures assert equality against this same package
directory.

## Decision 7: Post-generation tightening of two server-enforced literals (`schemaVersion`, `kind`)

A second adversarial-review round found that `BatchEnvelope.schema_version`
and `RecordEnvelope.kind` are typed as bare `str` in `schemas.py` (not
`Literal`) — deliberately, so `POST /batches` can 400 with a precise
`unsupported_schema_version`/`unknown_record_kind` message rather than
FastAPI's generic 422 for a bad enum value. The side effect: the naively
generated schema for those two fields was just `{"type": "string"}`, so a
customer's offline validator could certify a batch with
`schemaVersion: "external-ingest.v99"` or an invented `kind` that the live
server actually rejects — the opposite of what a schema-discovery endpoint
is for.

`schema_registry._tighten_server_enforced_literals()` closes this by adding
`const: SUPPORTED_SCHEMA_VERSIONS[0]` (or `enum` if a second version is ever
added) to `$defs.BatchEnvelope.properties.schemaVersion`, and
`enum: sorted(RECORD_KIND_MODELS)` to
`$defs.RecordEnvelope.properties.kind`, as a post-processing step on the
`models_json_schema()` output — **not** by hand-writing a schema (D5) or
adding a `Literal` to the frozen `schemas.py` model (CC17, out of this
issue's ownership). The values used are the same `SUPPORTED_SCHEMA_VERSIONS`
tuple and `RECORD_KIND_MODELS` keys the registry already treats as its
source of truth elsewhere (`list_versions()`, `iter_record_kinds()`), so
this can't drift from the rest of the bundle. Regression tests
(`test_schema_version_field_is_pinned_to_supported_versions`,
`test_kind_field_is_pinned_to_known_record_kinds`) lock in the constraint.

This is a narrower, lower-risk fix than the still-open Decision-6/Consequences
gap below (`records[].payload` not being tied to `kind`): `schemaVersion`
and `kind` are each a single scalar field with one server-enforced value
set, so a `const`/`enum` addition is a faithful, mechanical tightening. Fully
closing the payload-vs-kind gap would require a hand-built `oneOf`
discriminated union the registry does not attempt, per Decision 6's
rationale.

## Consequences

- Adding a 10th record kind or an `external-ingest.v2` envelope is additive:
  extend `RECORD_KIND_MODELS`/`SUPPORTED_SCHEMA_VERSIONS` and drop in an
  example file — no hand-written JSON Schema to keep in sync.
- The registry has zero database/network dependency (pure Pydantic
  reflection + packaged JSON files), so none of this issue's tests need a
  `clickhouse`/`postgres` marker.
- Validating a whole batch against just the top-level `envelope` ref cannot
  catch a kind-specific violation (e.g. a `commit.v1` record with an empty
  payload) — `RecordEnvelope.payload` is a bare, unconstrained object in
  `schemas.py` (payload/kind are tied together only in Python validation
  code — `router.py`'s kind allowlist and `validate.py` — not in the wire
  model itself, and that model is frozen/owned by CHAOS-2691, not
  redeclarable here per CC17). This is a real, deliberate fidelity gap
  (adversarial-review finding), not a silent one: the bundle's top-level
  `description` states it explicitly, and
  `tests/api/external_ingest/test_schema_registry.py::
  test_envelope_level_payload_is_documented_as_kind_unconstrained` locks it
  in. Customers/CI needing kind-accurate validation must validate each
  record's payload against `recordKinds[kind].$ref` (the documented,
  D9-intended usage — `GET /schemas/{version}` deliberately returns per-kind
  `$ref`s for exactly this reason), not the envelope alone.
