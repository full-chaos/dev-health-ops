# External ingest: worker processor (CHAOS-2697)

Part of the CHAOS-2690 external customer-push ingestion epic. This document
records the worker-side processing contract and the decisions behind
`external_ingest/normalize.py` + `external_ingest/processor.py`. Companion
docs: `external-ingest-rest-contract.md` (envelope/endpoints),
`external-ingest-stream-design.md` (2693 stream/consumer/DLQ),
`external-ingest-status-store.md` (2694 store),
`external-ingest-idempotency-ownership.md` (2695 accept-path policy),
`external-ingest-sink-writes.md` (2698 sink writes),
`external-ingest-bounded-recompute.md` (2699 bounded recompute).

## Modules

| Module | Owns |
|---|---|
| `external_ingest/normalize.py` | pure record validation/partitioning: CC17 shape delegation + CC6 matrix + instance scope → `NormalizedBatch` + collapsed rejections |
| `external_ingest/processor.py` | the impure orchestrator: `process_batch` (CC23) + `mark_batch_failed` (give-up path) |

Merging `processor.py` is what **arms the consumer**: 2693's
`_processor_available()` deployment-order guard keys on this module's
importability and refuses to claim entries until it exists. No consumer
config change was needed.

## `process_batch` sequence (master-spec CC23)

```
schema_version / source_system / ingestion_id sanity     (Permanent on fail)
→ require_clickhouse_uri()                               (RuntimeError = transient)
→ get_batch: terminal? → return 0 (idempotent skip, CC11 post-critique)
→ mark_processing (CAS accepted|stream_unavailable → processing) → COMMIT
→ re-read: not processing? → yield (return 0; another actor owns the row)
→ fetch_payload (missing → Permanent)
→ resolve source_id from external_ingest_sources        (missing → Permanent)
→ parse envelope; pointer↔payload source + count checks (mismatch → Permanent)
→ normalize_batch (per-record rejections, never raises)
→ [any accepted] write_batch + retry ladder             (exhausted → transient)
→ complete_batch (CAS processing → completed|partial|failed, + record_counts)
→ delete_payload (same txn) → COMMIT
→ schedule_or_coalesce(...) ONCE                         (best-effort, never fails ingest)
→ return items_accepted
```

## Decisions

**Permanent vs transient classification.** `PermanentProcessingError`
(immediate DLQ + `mark_batch_failed`) is reserved for states no retry can
fix: wrong schema version, non-UUID pointer, missing status row, missing
payload row, unregistered source, pointer/payload disagreement, corrupt
envelope. Everything else — including `TransientSinkWriteError` after the
ladder and a missing `CLICKHOUSE_URI` — leaves the entry un-ACKed for
2693's reclaim ladder (15 min idle, max 5 deliveries). Erring transient is
deliberate: a wrongly-permanent classification destroys a batch, a
wrongly-transient one costs at most ~75 minutes of reclaim cycles before
the give-up path produces the same DLQ outcome.

**Sink retry ladder keys off `SinkWriteResult.errors`, not exceptions.**
`sinks.write_batch()` never raises — per-kind failures come back as error
entries (batch-call granularity). The ladder (initial attempt + retries at
2s/4s/8s, CC11) re-runs the WHOLE batch each attempt; that is safe because
every sink is a ReplacingMergeTree upsert on natural keys, and only the
final attempt's outcome counts. Sink errors never become per-record
rejections: a kind-level ClickHouse failure is a system problem, not a data
problem, so the batch must not complete `partial` with valid-but-unwritten
records counted as rejected.

**`stream_unavailable` pointers are processed, not wedged.** A 503'd accept
whose XADD actually landed leaves a live pointer for a `stream_unavailable`
row (the expected-duplicate-pointer case in the 2695 doc). `mark_processing`
therefore CASes from `accepted` OR `stream_unavailable`. Processing it is
strictly better than skipping: the payload row is durable by the accept
sequence's ordering, and the client's same-key retry then REPLAYs the
terminal outcome instead of re-accepting. A concurrent stale-`accepted`
RETRY serializes against the same row CAS; the processor re-reads after
`mark_processing` and yields (`return 0`) if it did not win.

**Rejections collapse to one per record index.** The status store's
`(ingestion_id, record_index)` unique constraint permits exactly one
persisted diagnostic per record; multi-field shape failures keep the first
`validate_records` error (field order). `items_rejected` counts rejected
RECORDS, so counts always reconcile with `items_received`
(`complete_batch` enforces the sum).

**CC6 enforcement lives in `normalize.py`; instance scope is
case-insensitive.** `validate.py` (CC17, imported unchanged) owns shape
only; `sinks.py` asserts-but-never-rejects. The kind×system matrix and the
git-family `repositoryExternalId == source.instance` rule reject here. The
instance comparison casefolds both sides — same rationale as 2695's
ownership matching: provider identifiers are case-insensitive and
`derive_repo_uuid` lower-cases its seed, so a case-variant identifier is
the same repo and cannot fork identity. (The sink's exact-match
`record_outside_source_instance` *warning* may still fire for accepted
case-variants; diagnostic noise, accepted.)

**Recompute vocabulary: FULL kind names.** `schedule_or_coalesce` /
`plan_recompute` intersect `record_kinds` against `.v1`-suffixed sets
(`pull_request.v1`), while `SinkWriteResult.affected_scope.record_kinds`
carries bare names (`pull_request`). The processor passes the
normalization-level kind names (`NormalizationResult.record_counts` keys) —
passing the sink scope through would silently plan zero recompute. The
dispatch is wrapped best-effort (log-and-continue): the batch is already
terminal and durable; a Valkey/Celery hiccup must not fail ingestion or
un-ACK the entry.

**`record_counts` is written by the worker only.** `complete_batch` gained
an optional `record_counts` parameter (per-kind accepted counts, full kind
names) — the column existed since 2694 with no writer.

**Payload cleanup on terminal status (CC9).** `process_batch` deletes the
payload row in the same transaction as `complete_batch`; `mark_batch_failed`
does the same on the give-up path. A subsequent same-key resubmission
(RETRY) re-upserts the payload under the same `ingestion_id`. Never-retried
orphans are the CHAOS-2769 reconciler's job.

**Give-up ordering: idempotent DLQ write FIRST, then `mark_batch_failed`
(which raises on failure); ACK only after both.** Three adversarial rounds
converged here. Round 1: the ACK must gate on the mark result — ACKing past
a raised `mark_batch_failed` strands the batch non-terminal with the
pointer gone (a `processing` row REPLAYs, never RETRYs). Round 2: naive
DLQ-first XADDed a duplicate DLQ row on every mark-failure retry during a
Postgres outage, eventually evicting distinct failures from the capped
approximate DLQ stream. Round 3: the mark-first "fix" for that LOSES the
DLQ row entirely when the mark lands but the XADD fails — the now-terminal
batch makes every redelivery take the consumer's idempotent-skip guard,
which ACKs without ever retrying the DLQ write. Round 4: the marker cannot be treated
as proof the DLQ row still exists — the DLQ is `maxlen`-capped, so under
sustained pressure a high-volume org can trim the original row while the
mark outage keeps the entry pending, and a marker-only short-circuit would
then mark-and-ACK with no surviving DLQ record. Resolution: DLQ-first with
a per-entry `…:dlq:written:<entry_id>` marker that stores the DLQ
**stream-id** of the row it wrote (set best-effort right after a successful
XADD; check-then-write is safe under the CC11 single-consumer invariant). On
a marker hit the code re-reads that exact stream-id and only short-circuits
if it is still present; a trimmed row falls through to a fresh XADD. Net: a
failed XADD leaves the batch untouched and the entry pending (self-healing),
a failed mark retries with the marker suppressing duplicate rows *while the
row survives*, a trimmed row is rewritten, and only full success ACKs. `status.mark_failed` transitions from ANY
non-terminal status (permanent failures raised before `mark_processing`
leave `accepted`), forces the counter invariant (`items_accepted=0`,
`items_rejected=items_received`, `record_counts=NULL` — failed means
nothing accepted, and GET/list consumers key off these), and is a no-op for
terminal/unknown batches, so DLQ re-drives and duplicate pointers stay
idempotent.

**Give-up with unrecoverable entry fields.** `reclaim_stale` hands
`move_to_dlq` only a message ID; the fields are re-read via XRANGE. A
transient re-read failure is NOT safe to ACK (return `False`, retry later —
2697 round-2 HIGH: collapsing it with "absent" let a Redis blip ACK away an
unmarked batch). An authoritatively-absent entry (MAXLEN-trimmed while
pending) has nothing addressable to mark: a tombstone DLQ row is written
and the entry ACKed; the possibly-stranded batch row is the CHAOS-2769
orphan-batch reconciler's territory.

**Source provenance resolution.** `source_id` (CC8) is looked up from
`external_ingest_sources` case-insensitively (2695 blocks case-variant
duplicates, so at most one logical source matches). A source disabled
*after* accept still resolves — the batch was accepted while write-eligible;
a *deleted* source is a `PermanentProcessingError` (rows would be
unattributable).

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `CLICKHOUSE_URI` | — (required) | sink DSN passed to `sinks.write_batch` |

Retry ladder timing is code-constant (`SINK_RETRY_BACKOFF_SECONDS =
(2, 4, 8)`), pinned by CC11 alongside 2693's `reclaim_idle_ms=900_000` —
changing one without the other reopens the duplicate-concurrent-processing
window the 15-minute reclaim idle was sized against.
