# External-ingest REST contract (CHAOS-2691)

Design decisions behind `src/dev_health_ops/api/external_ingest/` — the
`/api/v1/external-ingest/*` REST contract, its 9 versioned record-kind
schemas, and the two interim seams (`streams.py`, `auth.py`) later tickets
harden. See [ADR-003](adr-003-external-ingest-rest-boundary.md) for the
cross-epic REST-vs-GraphQL and ownership-model decisions this ticket does
not own.

## Idempotency-Key: body is canonical, header is an optional alias

`BatchEnvelope.idempotencyKey` (body) is required and authoritative — it
participates in the batch-identity tuple (`org_id + source_system +
source_instance + idempotencyKey`) that later tickets use for
replay/conflict detection, so it needs to live inside the payload a CLI can
replay offline. The HTTP `Idempotency-Key` header is an optional
Stripe-style alias for generic cURL/CI ergonomics; if both are present they
must match, or the request is rejected with `400 idempotency_key_mismatch`.
This ticket only implements the header/body consistency check — durable
conflict detection (payload-hash comparison, 409 semantics) is CHAOS-2695.

## 400 vs 422: the router parses the envelope itself

The app-wide convention (`api/_errors.py`) maps FastAPI's automatic
Pydantic-body validation failures to `422`. External-ingest's documented
contract requires `400` for malformed envelopes. Rather than fight the
app-wide convention, the endpoints take a raw `Request`, read bytes
themselves (needed anyway for the body-size check below), and call
`BatchEnvelope.model_validate_json(raw)` inside a try/except that maps
`pydantic.ValidationError` to `ExternalIngestError(400, "invalid_envelope",
...)`. Every other router in the app keeps the generic 422 convention
untouched.

## Error envelope: `{"error": {"code", "message", "errors"?}}`

A dedicated shape (`api/external_ingest/errors.py::ExternalIngestError` +
handler), not the app's `{"detail": ...}` convention — external-ingest
responses are consumed by customer SDKs/CI scripts and need a stable,
documented, machine-parseable contract distinct from the internal
convention. Starlette dispatches exception handlers by exact type, so this
handler wins over the app's generic `Exception` catch-all regardless of
registration order. The canonical `code` vocabulary is pinned epic-wide
(master-spec CC16); this ticket raises `invalid_envelope`,
`unsupported_schema_version`, `unknown_record_kind`,
`idempotency_key_mismatch`, `batch_too_large`, `payload_too_large`,
`stream_unavailable`, `auth_not_configured`, `invalid_token`,
`insufficient_scope` (dead code in interim auth), and `rate_limited` (429,
wired via a path-prefix branch in the shared `api/_errors.py::_rate_limit_handler`
rather than a second
`RateLimitExceeded` handler registration). Auth failures use this envelope
too (master-spec CC16 explicitly includes them) — a missing/malformed
bearer token is `401 invalid_token`; a missing `X-Org-Id` header is
`400 missing_org_header`, a code that exists **only** for this interim
mechanism (the header itself disappears once CHAOS-2712 derives `org_id`
from a validated token, so it is deliberately not in CC16's permanent
vocabulary). A genuinely unexpected exception (not one of the deliberate
`ExternalIngestError`s above) also gets this envelope — `api/_errors.py`'s
shared generic handler branches on the `/api/v1/external-ingest` path
prefix and returns `500 internal_error` with the same sanitized "Internal
Server Error" message it uses app-wide, so customer SDKs never need a
special-case parser for the one failure mode that would otherwise use the
app's `{"detail": ...}` shape (adversarial-review finding). This does not
change `ExternalIngestError`'s own handler winning by exact-type dispatch
for every deliberate error above. The remaining codes (`source_not_registered`,
`source_disabled`, `source_owned_by_fullchaos_sync`, `source_mismatch`,
`not_found`, `idempotency_conflict`, `ingest_temporarily_unavailable`) are
reserved for CHAOS-2694/2695/2696/2712.

## Batch limits: 1000 records / 10 MB body, both env-overridable

`EXTERNAL_INGEST_MAX_RECORDS` (default 1000) is checked against
`len(envelope.records)` after parse (`400 batch_too_large` — a semantic
limit, not a transport one). `EXTERNAL_INGEST_MAX_BODY_BYTES` (default
10,000,000) is checked against `Content-Length` first (fast rejection) and
against the actual streamed byte count if `Content-Length` is absent or the
request is chunked (`413 payload_too_large`). `POST /validate` enforces the
same two limits. `GET /schemas` exposes both as `limits.maxRecordsPerBatch`
/ `limits.maxBodyBytes` so a CLI pre-check reads live server values instead
of hardcoding them.

## `/batches` checks envelope + kind allowlist only; `/validate` deep-validates eagerly

`POST /batches` rejects the entire batch with `400` for: unsupported
`schemaVersion`, any `records[i].kind` outside the 9 known kinds, or
envelope-level shape errors. It does **not** reject the batch for a
malformed individual record *payload* (e.g. a `pull_request.v1` missing
`title`) — those are accepted into the stream and, once CHAOS-2694/2697
land, surface as per-record rejections after the worker's full validation
pass. This means a customer's momentary schema drift on a handful of
records out of a large batch doesn't drop the rest.

`POST /validate` performs the deep per-record check eagerly via
`dev_health_ops.external_ingest.validate.validate_records` — the **single**
owner of deep validation (master-spec CC17): CHAOS-2697's worker imports
this function unchanged, so "the API's /validate and the durable worker
agree on what's valid" holds by construction rather than by keeping two
implementations in sync. `RecordEnvelope.payload` is typed as a plain
`dict` at the envelope level (not a discriminated union) specifically so one
malformed record's shape doesn't abort parsing the rest of the batch —
per-record diagnostics need to report *which* record failed without
throwing away the other 999.

## `Repo.provider` stores the source system, not the ingestion mode

`repository.v1.sourceSystem` is written directly into `Repo.provider`
(`"github"`, `"gitlab"`, `"custom"`) by the CHAOS-2697/2698 worker — no new
`"customer_push"` provider value is introduced. Ingestion-mode / ownership
tracking lives exclusively in the CHAOS-2696 source-registration table
(`external_ingest_sources`, keyed on `org_id, system, instance`), not
overloaded onto `repos.provider`. Provenance (which specific registered
source wrote a row) is a separate nullable `source_id` column added by
CHAOS-2698's ClickHouse migration, not `provider` itself.

## `repository.v1.externalId` is the provider full name, not a URL

Verified in code (not assumed): `processors/github.py:1572` passes
`repo=repo_info.full_name`, `processors/gitlab.py:1815` passes
`path_with_namespace`, and `get_repo_uuid_from_repo()`
(`models/git.py:72-93`) lowercases/strips whatever string it's given as the
deterministic-UUID seed. So `externalId` for github/gitlab must be exactly
the string FullChaos's own connector would have derived (`owner/repo` /
`group/subgroup/project`) for identity continuity across a
fullchaos_sync-to-customer_push handoff to work; for `system="custom"` the
seed is `custom:{source.instance}:{externalId}` instead. This is a genuine,
unenforced footgun: Pydantic validates string *shape*, not "is this the
same string FullChaos's connector would derive" — a customer sending a
differently-formatted identifier for the same logical repo silently creates
a duplicate `repos` row instead of reconciling with the existing one.
`repository.v1.externalId` must also equal `source.instance` for git
systems (repo/project-grain sources, one batch per source instance).

## `work_item.v1` takes `externalKey`, not a customer-supplied namespaced ID

Customers send the provider-native key only (`ABC-123`, `CHAOS-123`, an
issue/PR number) — never the internal namespaced `work_item_id`
(`jira:ABC-123`, `gh:{repo}#{n}`, etc.), which CHAOS-2698's
`external_ingest/ids.py` derives server-side. `WorkItemTransitionV1` and
`WorkItemDependencyV1` mirror this with `externalKey`
(`sourceExternalKey`/`targetExternalKey` for the dependency's two sides)
plus an optional `workItemType` per side, since GitHub/GitLab need it to
disambiguate an issue namespace from a PR/MR namespace sharing the same
number.

## `review.v1.state` is a validated allow-list, not a free string

The internal `GitPullRequestReview.state` has no normalized enum — it's a
raw provider string. External-ingest constrains the wire schema to
`Literal["APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED",
"PENDING"]` instead, because a customer payload is untrusted input in a way
a native sync provider's response is not; an unrecognized value is a loud
`422`-shaped validation error, not a silent pass-through.

## `extra="forbid"` on all 9 record models — and on the envelope/wrapper models too

A deliberate deviation from most Pydantic models elsewhere in this
codebase, which tend to be looser. This is a versioned, external,
customer-SDK contract — a customer's field-name typo should be a loud
validation error, not silently dropped data. If a future reviewer expects
laxer validation (matching `product_telemetry`'s free-form `payload: dict`
field), the strictness is intentional and load-bearing: CHAOS-2692's
"schemas match API validation behavior" acceptance criterion depends on
validation being meaningful.

`SourceDescriptor`, `IngestWindow`, `RecordEnvelope`, and `BatchEnvelope`
(the envelope/wrapper layer, as opposed to the 9 per-kind payload models)
also carry `extra="forbid"` — an adversarial-review fix: strict payloads
wrapped in a loose envelope would let a typo in `schemaVersion`'s sibling
fields, `source`, `window`, or a record wrapper's own keys pass through
silently while `/schemas` advertises exact-match validation. `POST
/batches` and `POST /validate` both reject unknown fields anywhere in the
envelope with `400 invalid_envelope` accordingly.

## `streams.py` / `auth.py` are real interim implementations, not mocks

`enqueue_batch()` is a real Valkey `XADD` writer that raises
`StreamUnavailableError` on any failure — the router maps this to `503
stream_unavailable`, never accept-and-warn (contrast the legacy
`api/ingest/streams.py`, which accepts silently on Redis failure). Stream
naming (`external-ingest:{org_id}:batches`, DLQ
`external-ingest:{org_id}:dlq`) and the signature (including the
`record_count`/`window_started_at`/`window_ended_at` pointer fields added
per master-spec CC9) are a pinned contract CHAOS-2693 extends in place
without changing.

**Updated by CHAOS-2712**: `require_ingest_scope()`'s body now resolves a
real, DB-backed `IngestToken` row (sha256-hashed `fcpush_` bearer against
`external_ingest_tokens`) — the `EXTERNAL_INGEST_INSECURE_AUTH` flag and
`X-Org-Id` header path described in this section's original (CHAOS-2691
interim) form are deleted entirely. See
[ADR-003](adr-003-external-ingest-rest-boundary.md#decision-4-interim-auth-is-a-mechanically-gated-stopgap-not-a-real-credential-system)
for the interim-era history and
[docs/architecture/customer-push-authz.md](customer-push-authz.md) for the
real auth/token model.

`get_ingest_token_key` (`api/middleware/rate_limit.py`) now keys the
limiter on `request.state.ingest_token_id` — set by `require_ingest_scope`
only once a bearer has been resolved against a real token row — falling
back to IP for public/unauthenticated requests. This closes the
adversarial-review finding against the old interim design (keying on a hash
of the raw, unvalidated bearer text would have let a caller rotate
arbitrary strings for a fresh 60/minute bucket on every request).

## `GET /schemas` / `GET /schemas/{version}`: minimal, real, off the Pydantic models directly

Both endpoints return live output from `BatchEnvelope.model_json_schema()`
and each `RECORD_KIND_MODELS[kind].model_json_schema()` — no separate
schema-registry module. CHAOS-2692 is additive polish (examples, ETag,
versioned history) on top of this, importing `RECORD_KIND_MODELS` from this
ticket's `schemas.py` rather than redeclaring the models, so the two
tickets' schemas can never drift apart.
