# Customer Push Ingestion: REST API Reference

All routes are mounted under the prefix `/api/v1/external-ingest`. See
[Overview](overview.md) for the request lifecycle and
[Schemas & Idempotency](schemas-and-idempotency.md) for the batch envelope and record-kind
field reference.

## Authentication

Every route below except the two `GET /schemas*` discovery routes requires:

```
Authorization: Bearer fcpush_<token>
```

Tokens are minted by an org admin (see [Setup Guide](setup-guide.md)) and are **not** JWTs —
they're independent, hashed bearer credentials resolved by a dedicated auth dependency, scoped
to one of three scopes:

| Scope | Grants |
|---|---|
| `schema:read` | `POST /validate`, `GET /schemas`, `GET /schemas/{version}` |
| `ingest:write` | `POST /batches` (also requires the token to be bound to a specific source) |
| `ingest:status` | `GET /batches`, `GET /batches/{ingestion_id}` |

A token can be **org-wide** (not bound to a source) only if its scopes are a subset of
`{schema:read, ingest:status}` — `ingest:write` always requires a source-bound token, and a
source-bound token can only push data for that exact `(system, instance)` (a mismatch is
`403 source_mismatch`).

`GET /schemas` and `GET /schemas/{schema_version}` do not validate the bearer value at all
(they're public schema-discovery routes) — rate-limited by client IP instead of by token.

## Error envelope

Every error response (including an unexpected `500`) uses this shape:

```json
{
  "error": {
    "code": "source_mismatch",
    "message": "Payload source does not match the token's bound source",
    "errors": [ ]
  }
}
```

`errors` is present (a list of per-field/per-record problem objects) only for `400
invalid_envelope` (Pydantic field errors) — see [Troubleshooting](troubleshooting.md) for the
full error-code vocabulary and remediation per code.

## Rate limits

| Route(s) | Limit | Keyed by |
|---|---|---|
| `POST /batches` | 60/minute | validated token |
| `POST /validate` | 60/minute | validated token |
| `GET /batches`, `GET /batches/{id}`, `GET /schemas`, `GET /schemas/{version}` | 120/minute | validated token (or forwarded IP for the two public schema routes) |
| Ingest-auth attempts (any bearer, before validation) | 100/minute per IP | forwarded IP |
| Ingest-auth *failures* specifically | 30/minute per IP | forwarded IP |

Exceeding any limit returns `429` with `{"error": {"code": "rate_limited", ...}}`.

---

## `POST /api/v1/external-ingest/validate`

Validates a batch envelope (shape + per-record payload) without writing anything or enqueueing
work. Use this in CI, or via `dev-hops push validate`, before submitting a real batch.

- **Auth scope:** `schema:read`
- **Request body:** a [batch envelope](schemas-and-idempotency.md#the-batch-envelope)
- **Success — `200`:**

```json
{
  "valid": false,
  "itemsAccepted": 8,
  "itemsRejected": 2,
  "errors": [
    {
      "index": 3,
      "kind": "pull_request.v1",
      "code": "missing_required_field",
      "message": "Field required",
      "path": "records[3].payload.state"
    }
  ]
}
```

- **Failure modes:**
  - `400 invalid_envelope` — malformed JSON or envelope shape (bad `schemaVersion` field type,
    missing `source`, empty `records`, etc.)
  - `400 unsupported_schema_version` — `schemaVersion` isn't `external-ingest.v1`
  - `400 batch_too_large` — more than `maxRecordsPerBatch` records (default 1000)
  - `413 payload_too_large` — request body exceeds `maxBodyBytes` (default 10MB)
  - `401 invalid_token`, `403 insufficient_scope`, `429 rate_limited`

Per-record problems (unknown kind, missing/invalid fields) are reported as `200` with
`valid: false` and one `errors[]` item per problem — they do **not** produce a 4xx, since a
partially-invalid batch is exactly what this endpoint exists to surface.

---

## `POST /api/v1/external-ingest/batches`

Accepts a batch for durable, asynchronous processing.

- **Auth scope:** `ingest:write` (token must be bound to the `source.system`/`source.instance`
  in the request body)
- **Headers:** optional `Idempotency-Key` — if present, it must match the body's
  `idempotencyKey` exactly (`400 idempotency_key_mismatch` otherwise). The body field is
  canonical; the header is an optional Stripe-style alias for generic HTTP client ergonomics.
- **Request body:** a [batch envelope](schemas-and-idempotency.md#the-batch-envelope)
- **Success — `202 Accepted`:**

```json
{
  "ingestionId": "b6c1e6b0-...-uuid",
  "status": "accepted",
  "itemsReceived": 250,
  "stream": "external-ingest:<org_id>:batches"
}
```

  A replayed request (same idempotency key + identical payload, see
  [Idempotency](schemas-and-idempotency.md#idempotency)) instead returns **`200`** with the
  *current* full status envelope (the same shape as `GET /batches/{id}` below) — not the
  narrow 202 shape, since the batch may already be `completed`/`partial`.

- **Failure modes:**
  - `400` — `invalid_envelope`, `idempotency_key_mismatch`, `unsupported_schema_version`,
    `unknown_record_kind` (any record's `kind` isn't one of the 9 known kinds — the whole batch
    is rejected, no partial acceptance), `batch_too_large`
  - `401 invalid_token`
  - `403` — `source_mismatch` (payload source doesn't match the token's bound source),
    `source_disabled`, `source_not_registered`, `source_owned_by_fullchaos_sync`,
    `insufficient_scope`
  - `409 idempotency_conflict` — same idempotency key already used with a **different**
    payload
  - `413 payload_too_large`
  - `429 rate_limited`
  - `503` — `ingest_temporarily_unavailable` (a concurrent request for the same idempotency key
    is in flight; retry) or `stream_unavailable` (the batch was durably recorded but could not
    be enqueued; retry with the same `idempotencyKey`)

See [Troubleshooting](troubleshooting.md) for remediation per code.

---

## `GET /api/v1/external-ingest/batches/{ingestion_id}`

Fetches the current status and diagnostics for one batch.

- **Auth scope:** `ingest:status`
- **Query params:** `errorLimit` (1-200, default 50), `errorOffset` (default 0) — paginate the
  `errors[]` list.
- **Success — `200`:**

```json
{
  "ingestionId": "b6c1e6b0-...-uuid",
  "status": "partial",
  "attempts": 1,
  "itemsReceived": 250,
  "itemsAccepted": 248,
  "itemsRejected": 2,
  "source": {"system": "github", "instance": "acme/api"},
  "window": {"startedAt": "2026-06-25T00:00:00Z", "endedAt": "2026-06-26T00:00:00Z"},
  "producer": "dev-hops-cli",
  "producerVersion": "0.12.0",
  "createdAt": "2026-06-26T00:01:00Z",
  "updatedAt": "2026-06-26T00:01:05Z",
  "completedAt": "2026-06-26T00:01:05Z",
  "errorSummary": {
    "total_rejected": 2,
    "stored_rejections": 2,
    "truncated": false,
    "top_codes": [{"code": "missing_required_field", "count": 2}]
  },
  "errors": [
    {"index": 3, "kind": "pull_request.v1", "externalId": "acme/api#482",
     "code": "missing_required_field", "message": "Field required",
     "path": "records[3].payload.state"}
  ],
  "errorsTotal": 2,
  "errorsLimit": 50,
  "errorsOffset": 0,
  "recompute": {
    "status": "dispatched",
    "scope": {"repoIds": ["..."], "teamIds": [], "windowStartedAt": "...",
              "windowEndedAt": "...", "cappedDays": false, "cappedRepos": false},
    "dispatchedAt": "2026-06-26T00:01:06Z",
    "completedAt": null,
    "error": null,
    "jobs": [{"task": "run_daily_metrics", "taskId": "...", "queue": "metrics", "repoId": "..."}]
  }
}
```

  `status` is one of `accepted | stream_unavailable | processing | completed | partial |
  failed` — see [Troubleshooting](troubleshooting.md#status-polling) for the full transition
  diagram. `recompute.status` is one of `not_applicable | pending | dispatched |
  skipped_no_scope | failed`.

- **Failure modes:** `404 not_found` (unknown id, or belongs to a different org — both map to
  the identical 404 to avoid leaking cross-org existence), `401 invalid_token`,
  `403 insufficient_scope`, `429 rate_limited`.

## `GET /api/v1/external-ingest/batches`

Lists batches for the caller's org.

- **Auth scope:** `ingest:status`
- **Query params:** `sourceSystem`, `sourceInstance`, `status`, `createdAfter`, `createdBefore`
  (all optional filters), `limit` (1-200, default 50), `offset` (default 0).
- **Success — `200`:**

```json
{
  "items": [
    {
      "ingestionId": "b6c1e6b0-...-uuid",
      "status": "partial",
      "itemsReceived": 250,
      "itemsAccepted": 248,
      "itemsRejected": 2,
      "source": {"system": "github", "instance": "acme/api"},
      "window": {"startedAt": "2026-06-25T00:00:00Z", "endedAt": "2026-06-26T00:00:00Z"},
      "producer": "dev-hops-cli",
      "createdAt": "2026-06-26T00:01:00Z",
      "completedAt": "2026-06-26T00:01:05Z"
    }
  ],
  "total": 1,
  "limit": 50,
  "offset": 0
}
```

  Each list item is a **narrower** shape than a `GET /batches/{id}` response — it has exactly
  these 10 fields (`ingestionId`, `status`, `itemsReceived`, `itemsAccepted`, `itemsRejected`,
  `source`, `window`, `producer`, `createdAt`, `completedAt`) and none of the detail-only
  fields: no `attempts`, `producerVersion`, `updatedAt`, `errors`, `errorSummary`, or
  `recompute`. Fetch `GET /batches/{id}` for any of those.
- **Failure modes:** `401 invalid_token`, `403 insufficient_scope`, `429 rate_limited`.

---

## `GET /api/v1/external-ingest/schemas`

Lists supported schema versions and record kinds. **Public** (no bearer validation) —
rate-limited by client IP.

- **Success — `200`:**

```json
{
  "schemaVersions": ["external-ingest.v1"],
  "recordKinds": ["commit.v1", "identity.v1", "pull_request.v1", "repository.v1",
                  "review.v1", "team.v1", "work_item.v1", "work_item_dependency.v1",
                  "work_item_transition.v1"],
  "limits": {"maxRecordsPerBatch": 1000, "maxBodyBytes": 10000000}
}
```

## `GET /api/v1/external-ingest/schemas/{schema_version}`

Returns the full JSON Schema bundle (envelope + per-record-kind `$ref`s + `$defs` + canonical
examples) for a schema version, generated directly from the server's Pydantic models. **Public**
(no bearer validation), rate-limited by client IP. Supports conditional `If-None-Match` /
`ETag` caching (`304` on a matching ETag; the ETag covers the schema *and* the live `limits`
block, so a limits change is never masked behind a stale 304).

- **Success — `200`** (headers: `ETag`, `Cache-Control: public, max-age=3600,
  must-revalidate`) — see [Schemas & Idempotency](schemas-and-idempotency.md) for how to read
  the bundle.
- **`304 Not Modified`** if `If-None-Match` matches the current ETag.
- **Failure modes:** `404 unsupported_schema_version` — unknown `schema_version` (note: this
  is the same error `code` string used for the *400* `POST /batches`/`POST /validate` case,
  but with a `404` status here since the version is a URL path segment).

---

## Admin validate proxy (session-authed, not token-authed)

`POST /api/v1/admin/customer-push/sources/{source_id}/validate` is a **session-JWT +
admin-role** authenticated twin of `POST /validate`, used by the web console. It mirrors the
same envelope/version/size/per-record checks, but reports every failure — including malformed
envelopes and version mismatches — as a `200` with `valid: false` (never a 4xx), since the
console renders each response as a validation result row. The `source_id` path segment is a
tenant-scope check only; it does not verify the payload's `source` field against it (that
check only applies to the token-authed data-plane write path). See
[Setup Guide](setup-guide.md) for the source-registration admin routes this proxy sits next to.
