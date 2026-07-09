# External ingest: idempotency + source-ownership policy (CHAOS-2695)

Part of the CHAOS-2690 external customer-push ingestion epic. This document
is the durable record of the batch idempotency policy and the
one-active-owner rule enforced by `POST /api/v1/external-ingest/batches`,
plus the admin validate proxy. Companion docs:
`external-ingest-rest-contract.md` (envelope/endpoints),
`external-ingest-status-store.md` (2694 store),
`external-ingest-stream-design.md` (2693 stream/payload),
`customer-push-authz.md` (2696/2712 tokens + registration).

## Modules

| Module | Owns |
|---|---|
| `api/external_ingest/idempotency.py` | `compute_payload_hash`, 4-way `resolve_batch_idempotency` (NEW/REPLAY/CONFLICT/RETRY) |
| `external_ingest/ownership.py` | CC5 per-provider instance matching + `resolve_effective_mode` (shared by admin registration and accept path) |
| `api/external_ingest/status.py` | store: `create_batch`, `find_existing_batch`, `reset_for_retry`, transitions |
| `api/external_ingest/router.py` | the CC22 accept sequence wiring |
| `api/admin/routers/customer_push.py` | registration-time 409 policy (reuses ownership.py predicates) + `POST .../sources/{id}/validate` |

`idempotency.py` lives in the API package rather than the brief's
`dev_health_ops/external_ingest/` because it is accept-time-only and its
store dependency lives there; importing the store from the sibling package
would recurse through `api/external_ingest/__init__` (which imports
`router`, which imports `idempotency`) into a circular ImportError.
`ownership.py` (models-only imports, worker-shareable) keeps the brief's
placement.

## Batch identity + canonicalization

Identity key: `(org_id, source_system, source_instance, idempotency_key)`,
unique **forever** (no TTL — a deliberate deviation from the legacy
`/api/v1/ingest` 24h Redis cache; durable dedup is what makes reprocessing
safe). Reusing a key string against a different `source.instance` is a
different logical batch, never a conflict.

Payload hash: SHA-256 over
`json.dumps(envelope.model_dump(mode="json"), sort_keys=True,
separators=(",", ":"), ensure_ascii=True)` — computed on the
**schema-validated Pydantic model**, never raw bytes. Pydantic's
`mode="json"` dump normalizes timestamp spellings (`...Z` vs `...+00:00`);
`sort_keys` (recursive) + `separators` remove field-order and whitespace
variance. The `records` array is position-significant (NOT sorted): CLI
exports are deterministically ordered, and a synthetic cross-kind sort key
isn't worth the complexity — a nondeterministic exporter should be fixed,
not papered over.

## Outcomes (4-way, not the plan's literal 3-way)

| Outcome | Condition | Response |
|---|---|---|
| `NEW` | no existing row | insert row (`accepted`, `attempts=1`) → payload upsert → COMMIT → enqueue → **202** |
| `REPLAY` | same hash, row not retryable | **200** with the full `GET /batches/{id}` status envelope (a replayed batch may already be `completed`; the 202 shape would misreport it, and 200 lets `dev-hops push --poll` short-circuit) |
| `CONFLICT` | different hash | **409** `idempotency_conflict`; the original row is never touched |
| `RETRY` | same hash AND (`status ∈ {stream_unavailable, failed}` OR `accepted` older than `EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES` = 15) | `reset_for_retry` (same ingestion_id, `attempts += 1`, prior outcome fields + rejection rows cleared) → payload re-upsert → COMMIT → re-enqueue → **202** |

`RETRY` exists because the 503-on-stream-unavailable contract leaves a
durable row the client is told to resubmit against; treating that resubmit
as `REPLAY` would return the stale status forever. The stale-`accepted`
extension (post-critique CC13) closes the crash-before-XADD / stream-trim
fail-open where `accepted` was otherwise unrecoverable; a *young* accepted
row replays instead, so an in-flight enqueue is never raced by an
impatient client. Hash mismatch dominates status: a different payload
against a `failed` row is still a 409.

**Stale `processing` deliberately REPLAYs** — a narrowing of the
reconciliation header's literal "accepted/processing" (adversarial-review
finding). A row stuck in `accepted` is invisible to every worker (the
pointer never reached the stream), so the client resubmit is the *only*
recovery — that is CC13's actual target. A `processing` row proves a
worker holds the pointer; dead-worker recovery is the CHAOS-2693 stream
reclaim path and the `failed` terminal status (already retryable), while a
client-driven retry would race a slow-but-alive worker's terminal CAS with
no attempt fence (double-apply, superseded attempt's counters winning). If
operational need appears, CHAOS-2697+ can extend with attempt-fenced
retries (attempt number carried in the stream entry and CAS'd through
`mark_processing`/`complete_batch`).

`reset_for_retry` is a CAS on the observed status (loser re-reads and
replays) and deletes the prior attempt's rejection rows — without that, the
retry's own `complete_batch` would violate the `(ingestion_id,
record_index)` unique index. `recompute_*` fields are left for the worker
to overwrite (CHAOS-2699).

True same-key insert race (pre-check miss + unique-constraint collision +
winner's row not yet visible): **503** `ingest_temporarily_unavailable` —
the client's idempotent retry resolves it. The insert runs in a SAVEPOINT
(`create_batch`), so the collision never poisons the caller's session.

**Duplicate stream pointers are expected, not a bug.** A stale-`accepted`
RETRY during a long consumer outage can re-enqueue a pointer whose first
XADD actually succeeded — this is indistinguishable from crash-before-XADD
on the accept side, and it is deliberately NOT "solved" with a durable
enqueue marker (a crash after XADD but before the marker write just
inverts the bug; exactly-once across Postgres + Valkey is not achievable
at this boundary). The pipeline is at-least-once end to end — the
CHAOS-2693 reclaim machinery itself redelivers entries — and dedup lives
at processing time: single-replica consumer (CC11), `mark_processing` /
`complete_batch` compare-and-swap transitions, ReplacingMergeTree sinks,
and coalesced recompute (CHAOS-2699). A redelivered pointer for a
terminal batch is skipped; for an in-flight one it processes serially and
no-ops at the terminal CAS.

## Accept sequence (master-spec CC22)

```
parse/size/version/kind checks (400/413)
→ require_matching_source (token↔envelope source binding, 2712)
→ resolve_effective_mode (403 source_not_registered / source_disabled /
  source_owned_by_fullchaos_sync)
→ compute_payload_hash
→ resolve_batch_idempotency        # FIRST Postgres write
→ [CONFLICT → 409 | REPLAY → 200]
→ [RETRY → reset_for_retry (CAS; loser → 200 replay)]
→ upsert_payload (same txn) → COMMIT
→ enqueue_batch (pointer only; fail-closed payload-durability check)
→ [StreamUnavailableError → mark_stream_unavailable → COMMIT → 503]
→ 202
```

On the 503 path the payload row is **kept** (supersedes CHAOS-2693's
interim orphan-delete): it is referenced by the durable
`stream_unavailable` status row, the same-key retry reuses it via the same
ingestion_id, and deleting it could black-hole an enqueue whose XADD
actually landed before the error surfaced. Never-retried leftovers are the
CHAOS-2769 reconciler's job.

## One-active-owner resolution

`resolve_effective_mode(session, org_id, system, instance)` →
`fullchaos_sync | customer_push | disabled | unclaimed`. Precedence:

1. **Explicit `external_ingest_sources` row wins** — `disabled` if disabled
   (or `mode=disabled`), else its mode — EXCEPT a `customer_push` row is
   overridden by a managed source that **actively owns** the same instance
   (source `is_enabled` AND parent `Integration.is_active`). This
   accept-time re-check is deliberate defense-in-depth: nothing on the
   `api/admin/routers/sync.py` side knows about `external_ingest_sources`,
   so managed sync can be connected to the same repo *after* registration.
2. No explicit row: an actively-owning managed source implies
   `fullchaos_sync` (derived at read time, never mirrored/backfilled —
   touching every provider-connect path to dual-write would be far riskier
   than a read-time derivation).
3. Otherwise `unclaimed`.

Instance matching is per-provider (master-spec CC5) because
`integration_sources.external_id` is not uniformly the human-readable name:

| system | matches instance against |
|---|---|
| github, jira | `external_id`, `full_name` |
| gitlab | `full_name`, `metadata.path_with_namespace`, `external_id` (numeric project id) |
| linear | org-wide placeholder (`external_id == "linear"` or `metadata.org_wide_placeholder`) matches **any** instance; else `external_id`/`full_name`/`name` |
| custom | never conflicts |

Provider comparison is `func.lower()`d on both sides (managed rows may
carry `"GitHub"`), and instance comparison is case-insensitive on both
sides (adversarial-review finding): GitHub full names, GitLab paths, and
Jira/Linear keys are case-insensitive identifiers on their providers — no
two managed entities can differ only by case — while sync stores them as
the provider API returned them, so an exact match would let `Acme/API`
fail to block `acme/api`. The same predicates run at registration time
(2696's 409 + warnings policy in the admin router) and accept time —
single source of truth, brief decision 12.

Registration itself also rejects **case-variant duplicate customer-push
sources** (409, adversarial-review finding): the DB unique constraint is
on the raw `(org, system, instance)`, so without the app-level
case-folded pre-check one org could register `Acme/API` and `acme/api`
as two enabled sources for the same logical repository, splitting the
owner and idempotency namespaces. The stored instance keeps the user's
casing (token binding and envelope matching stay exact against it); a
DB-level canonical unique index is a tracked follow-up (needs a
migration, which this changeset deliberately ships none of).

On the data plane, `require_matching_source` already binds the token to a
registered write-eligible row, so `unclaimed`/`disabled` are practically
unreachable at accept time; the load-bearing outcome is the
`fullchaos_sync` override.

## Error codes raised by this changeset (CC16 envelope)

| HTTP | code | when |
|---|---|---|
| 409 | `idempotency_conflict` | same key, different payload hash |
| 503 | `ingest_temporarily_unavailable` | sub-ms concurrent same-key race |
| 503 | `stream_unavailable` | enqueue failed; row marked, retry same key |
| 403 | `source_not_registered` | resolved mode `unclaimed` |
| 403 | `source_disabled` | resolved mode `disabled` |
| 403 | `source_owned_by_fullchaos_sync` | resolved mode `fullchaos_sync` |

All via `ExternalIngestError` (`{"error": {code, message}}`), not
HTTPException detail-dicts (brief D13 overruled by reconciliation).

## Admin validate proxy (CC25)

`POST /api/v1/admin/customer-push/sources/{source_id}/validate` —
session-auth twin of the data-plane `POST /validate` for the web console's
Screen 5 (validate-only; the console-push proxy was cut from v1). Same
`validate_records` + size/version/count checks; snake_case
`AdminValidateResponse` matching the web's `CustomerPushValidateResponse`,
with `external_id` enriched from the record wrapper. Envelope-level
failures return **200 `valid: false`** with synthetic error rows (the
console renders them as results; the web mock contract pinned this shape).
It does NOT check the envelope's `source` against the path's source — that
binding is a token-auth concept enforced at push time.

## Record-level identity (contract for 2697/2698)

No separate dedup table: per-record identity delegates to each ClickHouse
sink's existing ReplacingMergeTree `ORDER BY` + sink-stamped version column
(`synced_at`/`last_synced` at write time — never customer-supplied
`updatedAt`). Customer-pushed repos MUST derive `Repo.id` via the same
`get_repo_uuid_from_repo` composition native sync uses, or a mode switch
silently forks the repo row (flagged epic-wide; owned by CHAOS-2697).

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES` | `15` | age beyond which an `accepted`/`processing` row is presumed lost and a same-key resubmit becomes RETRY |
