# Customer Push Ingestion: Troubleshooting

## Status polling

`GET /api/v1/external-ingest/batches/{ingestion_id}` (or `dev-hops push status <id> --poll` /
`dev-hops push batch <file> --poll`) reports `status` as one of:

```
accepted ──▶ processing ──▶ completed
   │                    ├──▶ partial
   │                    └──▶ failed
   └──▶ stream_unavailable ──▶ (retry same idempotencyKey) ──▶ accepted
```

| Status | Meaning |
|---|---|
| `accepted` | Durably recorded in Postgres; not yet picked up by the worker. |
| `stream_unavailable` | The batch was durably recorded, but the durable stream enqueue failed (Redis/Valkey outage). Recoverable: resubmit `POST /batches` with the **same** `idempotencyKey` once the stream is available again — this resolves as a **retry**, re-enqueueing the same `ingestion_id`. |
| `processing` | A worker has picked up the batch and is normalizing/writing records. |
| `completed` | All records accepted; `itemsRejected == 0`. |
| `partial` | Some records were accepted, some rejected; `itemsAccepted + itemsRejected == itemsReceived`, both positive. Check `errors[]`/`errorSummary`. |
| `failed` | The batch failed at a system level before producing per-record results (e.g. worker crash, schema-version mismatch caught late) — `itemsAccepted = 0`, `itemsRejected = itemsReceived`, `errorSummary.system_failure = true`. Retryable (same idempotency key). |

A same-key retry is always safe — it never double-processes a batch: `accepted`/
`stream_unavailable`/`failed` are the only retryable states, and a stale `accepted` (worker
never saw the pointer) only becomes retryable after 15 minutes (see
[Idempotency](schemas-and-idempotency.md#idempotency)).

## Rejected-record diagnostics

`GET /batches/{id}` includes:

```json
"errorSummary": {
  "total_rejected": 8,
  "stored_rejections": 8,
  "truncated": false,
  "top_codes": [{"code": "missing_required_field", "count": 6}, {"code": "invalid_field", "count": 2}]
},
"errors": [
  {"index": 12, "kind": "pull_request.v1", "externalId": "acme/api#501",
   "code": "missing_required_field", "message": "Field required",
   "path": "records[12].payload.state"}
]
```

- `errors[]` is paginated (`errorLimit`/`errorOffset` query params, 1-200, default 50) and
  capped server-side at a maximum number of stored rejection rows per batch — if
  `errorSummary.truncated` is `true`, `total_rejected` exceeds what was persisted; the
  first-N are always the ones stored.
- Per-record error `code` vocabulary (identical between `POST /validate` and worker-side
  rejections, since both call the same validation function):

| Code | Meaning |
|---|---|
| `unknown_kind` | `records[i].kind` isn't one of the 9 supported kinds. |
| `missing_required_field` | A required field is absent from `payload`. |
| `invalid_literal` | A field's value isn't one of its allowed literal/enum values. |
| `invalid_field` | Any other field-level validation failure (wrong type, failed length/range constraint, etc). |

- `path` points at the offending field: `records[<index>].payload.<field.path>` (or
  `records[<index>].kind` for `unknown_kind`).

Always run `POST /validate` (or `dev-hops push validate`) before `POST /batches` in CI to catch
these before they cost a real batch attempt.

## Common failure modes and remediation

| HTTP | `code` | Cause | Remediation |
|---|---|---|---|
| 400 | `invalid_envelope` | Malformed JSON, or the envelope itself fails schema validation (bad types, missing `source`, empty `records`). | Fix the envelope; check `errors[]` for the specific Pydantic field errors. |
| 400 | `idempotency_key_mismatch` | `Idempotency-Key` header doesn't match the body's `idempotencyKey`. | Send the same value in both, or omit the header (body is canonical). |
| 400 | `unsupported_schema_version` | `schemaVersion` isn't `external-ingest.v1`. | Update the client / check `GET /schemas` for the current supported version. |
| 400 | `unknown_record_kind` | A record's `kind` isn't one of the 9 known kinds (only on `POST /batches`; `/validate` reports this per-record instead). | Fix the record's `kind`, or run `POST /validate` first to catch this before submitting. |
| 400 | `batch_too_large` | More than `maxRecordsPerBatch` records (default 1000). | Split into multiple batches. |
| 401 | `invalid_token` | Missing/malformed `Authorization: Bearer fcpush_...` header, unknown token, or a revoked/expired token. | Check the token is present, correctly prefixed, and not revoked/expired — rotate if needed (see [Setup Guide](setup-guide.md)). |
| 403 | `insufficient_scope` | Token lacks the scope the route requires. | Mint a token with the needed scope (`schema:read`/`ingest:write`/`ingest:status`). |
| 403 | `source_mismatch` | Token is bound to a different `(system, instance)` than the envelope's `source`. | Use the token minted for this exact source, or fix `source.system`/`source.instance` in the envelope. |
| 403 | `source_not_registered` | No source is registered for this `(system, instance)` in this org. | Register the source first (admin API — see [Setup Guide](setup-guide.md)). |
| 403 | `source_disabled` | The registered source exists but is disabled, or not in `customer_push` mode. | Enable the source / set its mode to `customer_push`. |
| 403 | `source_owned_by_fullchaos_sync` | FullChaos-managed sync is actively connected to the same `(system, instance)`. | Disable managed sync for that instance before pushing customer data, or contact support. |
| 409 | `idempotency_conflict` | Same `idempotencyKey` was already used with a **different** payload. | Use a new `idempotencyKey` for genuinely different data, or resend the exact original payload. |
| 413 | `payload_too_large` | Request body exceeds `maxBodyBytes` (default 10MB). | Split into multiple batches, or trim payload fields. |
| 429 | `rate_limited` | Exceeded the per-token or per-IP rate limit (see [API Reference](api-reference.md#rate-limits)). | Back off and retry after a short delay; check you're not retrying in a hot loop. |
| 503 | `ingest_temporarily_unavailable` | A concurrent request for the same idempotency key is already in flight. | Retry shortly with the same key. |
| 503 | `stream_unavailable` | The batch was durably recorded, but the stream (Valkey/Redis) enqueue failed. | Retry with the **same** `idempotencyKey`; this resolves as a retry against the same `ingestion_id`. |
| 500 | `internal_error` | Unexpected server error. | Retry with backoff; contact support with the `ingestionId` (if one was returned) if it persists. |

## Operational troubleshooting

- **Batch stuck in `accepted` for a long time:** the stream enqueue may have failed silently
  from the client's perspective (a 503 the client didn't see/retry). Poll `GET
  /batches/{id}` — after `EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES` (default 15 min) a same-key
  retry will pick it back up.
- **Batch stuck in `processing`:** the worker consumer group may be behind or the worker may
  have died mid-processing. This does not require client action — the consumer's redelivery
  and DLQ machinery (per-org streams, `external-ingest:<org_id>:batches` /
  `external-ingest:<org_id>:dlq`) handles recovery; if a batch never resolves, it eventually
  reaches a terminal `failed` status which is itself retryable.
- **Recompute never shows `dispatched`:** check `recompute.status` — `skipped_no_scope` means
  the batch didn't touch any repos/teams that map to a recompute scope (e.g. it only carried
  `identity.v1`/`team.v1` records); `not_applicable` is the initial value before the worker has
  made a recompute decision.
- **Cross-org 404s:** `GET /batches/{id}` returns an identical `404 not_found` whether the id
  doesn't exist or belongs to a different org — this is deliberate (avoids leaking cross-org
  existence), not a bug.
