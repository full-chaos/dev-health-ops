# CHAOS-3034: River compatibility and cross-language enqueue boundary

**Status:** Accepted for direct PostgreSQL queue control; PollOnly-only deployment rejected; session mode unverified  
**Date:** 2026-07-20  
**Owners:** Dev Health Ops architecture  
**Supersedes:** The proposed direct Python-to-River production path in the
initial Go worker migration TRD  
**Evidence:** [Go worker migration evidence](../architecture/evidence/go-worker-migration/README.md)

## Context

The Go worker migration needs a PostgreSQL-backed bounded-job engine that works
through the deployed PgBouncer transaction-mode topology and allows Python
domain producers to enqueue atomically during the transition. It must also
support a rolling N/N-1 deployment window, preserve queue policy, and avoid a
public repository dependency on private ACR source.

Phase 0 tested `riverqueue` 0.7.0 against the River 0.40.0 PostgreSQL schema
with SQLAlchemy 2.0.49 and `asyncpg` 0.31.0 on Python 3.13.14. Both direct
PostgreSQL and PgBouncer transaction mode were exercised. The PgBouncer path
used `NullPool`, disabled statement caches, and unique prepared-statement names;
the direct path used SQLAlchemy's ordinary async engine configuration.

Standard inserts passed: a commit produced exactly one job with queue,
priority, maximum-attempt, and scheduled-state values intact, while rollback
produced none. Unique insertion failed with PostgreSQL SQLSTATE `42P10`. The
Python client targets
`ON CONFLICT (kind, unique_key)`, while [River migration
006](https://github.com/riverqueue/river/blob/v0.40.0/riverdriver/riverpgxv5/migration/main/006_bulk_unique.up.sql)
removes that qualifying index and moves uniqueness to the `unique_states`
design. The sanitized matrix is
[`v1-river-spike/compatibility-matrix.json`](../architecture/evidence/go-worker-migration/v1-river-spike/compatibility-matrix.json).

The Go harness then ran with Go 1.25.9, River 0.40.0, River N-1 0.39.0,
direct PostgreSQL, and PgBouncer 1.25.2 transaction mode. Direct execution and
running cancellation passed. PollOnly execution, retry, scheduling, load,
rolling-version, interoperability, and process-crash recovery passed, but
neither cross-client nor same-client `JobCancel` reached an already-running
worker context. The sanitized measurements are in
[`local-harness-results.json`](../architecture/evidence/go-worker-migration/v1-river-spike/local-harness-results.json).

## Candidate version lock

These are the Phase 1 candidate pins. Changing one reopens the affected
compatibility rows before implementation or image promotion.

| Component | Candidate pin |
|---|---|
| Go | 1.25.9 |
| River | 0.40.0 |
| `riverpgxv5` | 0.40.0 |
| River rolling N-1 | 0.39.0 |
| pgx | 5.10.0 |
| pgx rolling N-1 | 5.9.2 |
| ClickHouse Go driver | 2.47.0 |
| `valkey-go` | 1.0.76 |
| OpenTelemetry Go | 1.44.0 |
| `testcontainers-go` | 0.43.0 |
| Python `riverqueue` spike only | 0.7.0 |
| PostgreSQL test image | `sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15` |
| PgBouncer test image | 1.25.2 at `sha256:4c1ca296ef525f108f5d3552cc337c0c09587cf8dae7f0067fd93349e47dc1cd` |

SQLAlchemy 2.0.49, `asyncpg` 0.31.0, and Python 3.13.14 are recorded as the
spike environment, not new production dependencies selected by this ADR.

## Decision

1. **GO for River/riverpgxv5 0.40.0 with direct PostgreSQL queue control.**
   The direct, N/N-1, load, and worker-failure harness rows pass. A session-mode
   pooler is not covered by this evidence and may replace direct PostgreSQL only
   after it passes the same execution, cancellation, failure, and load matrix.
2. **NO-GO for transaction-mode PgBouncer PollOnly as the sole production
   queue-control path.** It does not propagate cancellation to running worker
   contexts with the selected pgx driver. Phase 1 may proceed only if the
   direct PostgreSQL prerequisite is accepted, or if a session-mode endpoint
   or separate cancellation
   control plane is designed and passes equivalent failure tests. Otherwise
   the broker decision reopens.
3. **NO-GO for `riverqueue` as a production dependency.** Passing standard
   transaction tests does not offset a required uniqueness path that is
   incompatible with the selected River schema. Python must not write River
   tables directly or bind to their internal indexes.
4. **Select a generic `worker_job_outbox` bridge.** Python and future
   non-Go producers write an ops-owned, language-neutral row in the same
   transaction as domain state. A Go relay translates the versioned envelope
   through River's supported Go API.
5. **Use a public clean-room implementation boundary.** ACR may inform
   requirements and patterns, but no private source is copied or imported.
6. **Separate compatibility and production-evidence gates.** The local
   compatibility matrix is complete with the explicit PollOnly blocker.
   Production canary routing stays blocked until real Celery baseline data is
   recorded and parity thresholds are approved.

<a id="worker-job-outbox-contract"></a>

## `worker_job_outbox` contract

The migration that implements this table may choose repository-specific SQL
types and index names, but it must preserve the following language-neutral
contract.

| Field | Contract |
|---|---|
| `id` | Immutable UUID primary key; also carried as the River relay identity |
| `dedupe_key` | Stable, non-secret text with a global unique constraint |
| `job_kind` | Stable registered job kind |
| `contract_version` | Positive payload/envelope version |
| `args` | Bounded JSON object validated against that version; identifiers and safe options only |
| `payload_hash` | Canonical hash used to reject reuse of a dedupe key with different kind/version/args |
| `queue` | Registered target queue/profile |
| `priority` | Validated River priority |
| `max_attempts` | Validated positive handler attempt limit |
| `scheduled_at` | Earliest business availability in UTC |
| `status` | `pending`, `claimed`, `delivered`, or terminal `dead` |
| `claim_token` | Opaque per-claim token, nullable outside `claimed` |
| `claimed_at`, `claim_expires_at` | Claim timestamp and renewable/reclaimable lease boundary |
| `attempt_count` | Relay attempts, separate from River handler attempts |
| `first_attempt_at`, `last_attempt_at` | Relay attempt timestamps |
| `next_attempt_at` | Backoff boundary for a pending relay retry |
| `last_error_code`, `last_error_detail`, `last_error_at` | Bounded, redacted relay failure evidence; never raw driver text containing values |
| `river_job_id` | River row ID after a verified insert/reconciliation |
| `delivered_at` | Timestamp at which River insertion and delivered state committed |
| `created_at`, `updated_at` | Audit timestamps |

The JSON envelope contains `contract_version`, tenant/domain identifiers,
correlation and idempotency identifiers, and a bounded `payload`. Credentials,
DSNs, source records, SQL, headers, rendered reports, and webhook bodies are
never embedded.

### Producer transaction

The producer writes the authoritative domain change and one outbox row in the
same PostgreSQL transaction. Commit makes both visible; rollback makes neither
visible. Reusing `dedupe_key` with the same kind, version, and `payload_hash`
returns the existing logical dispatch. Reusing it with different content fails
closed and emits a bounded conflict signal.

### Relay and reconciliation

1. Relays claim due `pending` rows with `FOR UPDATE SKIP LOCKED`, a fresh
   `claim_token`, and a bounded `claim_expires_at` lease.
2. The owning relay starts a PostgreSQL transaction, re-locks the row by
   `id + claim_token`, and verifies the lease and payload hash.
3. In that transaction it calls River 0.40's supported Go `InsertTx` API. The
   River args/metadata carry the outbox ID, and River uniqueness is derived
   deterministically from the outbox/dedupe identity through supported options.
4. The same transaction records `river_job_id`, sets `status=delivered` and
   `delivered_at`, clears claim fields, and commits. A crash or error before
   commit persists neither the River insert nor the delivered mark.
5. An expired claim returns to reconciliation. If the supported River API
   reports an existing unique job, the relay verifies the outbox identity,
   kind, contract version, and payload hash before marking delivered. Any
   mismatch fails closed; the relay never guesses from River table internals.
6. Transient failures increment `attempt_count`, set bounded error evidence and
   `next_attempt_at`, and release the claim. Policy exhaustion moves the row to
   `dead` for audited operator action.

This is guarded at-least-once relay delivery that converges to one River job.
It does not make handler side effects exactly once; each job family still owns
its domain claim, ledger, or idempotency policy.

## PgBouncer and migration conditions

- River migrations run only in the one-shot migration job and are pinned to
  0.40.0.
- Direct PostgreSQL queue-control connectivity uses notifications and
  leadership normally and is required for production running-job cancellation.
  A session-mode endpoint remains unverified until it passes this matrix.
- Transaction-mode PgBouncer with `PollOnly` may not be the sole queue-control
  endpoint. At a 250 ms interval its 20-sample queue-start p95 and
  connection/load gates passed their compatibility limits, but both
  running-cancel propagation paths failed. Exact measurements are retained in
  the generated local result rather than duplicated here.
- River 0.40.0 and 0.39.0 passed the migration-prefix and rolling-client
  checks: schema 6 was upgraded to 7, and on schema 7 each version inserted the
  v1 contract and the other version consumed it.
- The relay uses River APIs, not copied SQL or assumptions about the
  `river_job` index layout.

The result follows River's public implementation boundary: PollOnly skips the
listener, while the local no-listener control helper returns early when the
driver advertises listener support. River 0.40's changelog also limits its
poll-only running-cancel fix to one process. See the
[listener startup](https://github.com/riverqueue/river/blob/v0.40.0/client.go#L914-L932),
[`JobCancel` path](https://github.com/riverqueue/river/blob/v0.40.0/client.go#L1423-L1471),
[local-control helper](https://github.com/riverqueue/river/blob/v0.40.0/client.go#L2848-L2868),
and [0.40 changelog](https://github.com/riverqueue/river/blob/v0.40.0/CHANGELOG.md#fixed).

## Licensing and provenance

River 0.40.0 is licensed under the
[Mozilla Public License 2.0](https://github.com/riverqueue/river/blob/v0.40.0/LICENSE).
The spike-only [`riverqueue` 0.7.0
wheel](https://github.com/riverqueue/riverqueue-python/blob/v0.7.0/LICENSE)
also includes an MPL-2.0 license; even though it is rejected for production
use, source or developer-environment redistribution must retain the same
notice/source-availability obligations.
For externally distributed binaries or images, the engineering release process
must preserve license/notices, identify where recipients can obtain the exact
MPL-covered source, and make modifications to MPL-covered files available
under MPL-2.0. Separate ops files in a larger work are not relicensed merely by
linking the dependency, but copying River code into an ops file can make that
file covered. The SBOM/license inventory must retain the exact River version.
See the [MPL 2.0 license](https://www.mozilla.org/en-US/MPL/2.0/) and
[Mozilla's MPL FAQ](https://www.mozilla.org/en-US/MPL/2.0/FAQ/); legal review
remains the authority for a distribution question.

The public implementation is clean-room:

- no private ACR package or build dependency;
- no copied or mechanically translated private source, tests, comments, or
  configuration;
- requirements come from public contracts, this ADR, and independently
  authored behavior tests;
- provenance review is required for any proposed shared helper;
- a future extraction requires a separately approved public repository and
  explicit license.

## Consequences

The outbox adds an ops-owned table, relay, and one asynchronous hop for
transitional producers. In return it preserves atomic domain enqueue, survives
River schema evolution behind the supported Go client, and avoids carrying a
production Python dependency already incompatible with required uniqueness.

Phase 1 must implement the table, relay, cleanup/retention policy, metrics, and
failure-injection tests. The existing sync dispatch outbox remains authoritative
for sync orchestration and is not replaced by this generic bridge.

Infrastructure must expose bounded direct PostgreSQL worker queue control
before any production worker profile starts. A session-mode pooler may replace
it only after equivalent compatibility evidence is recorded. If operations
instead choose a separate application-owned cancellation channel, that channel
becomes a new P0 compatibility gate and must prove cross-process delivery,
worker-crash behavior, and bounded cancellation latency before the direct
requirement can be removed.

## Gate disposition

Phase 0 compatibility evidence is complete:

- direct PostgreSQL execution/cancellation/load: pass;
- transaction-mode PollOnly execution/retry/scheduling/load: pass;
- transaction-mode PollOnly running cancellation: fail, architecture blocker;
- River 0.40.0/0.39.0 rolling window: pass;
- Python transaction and Go consumption: pass;
- Python unique insertion: fail, client rejected;
- worker `SIGKILL` rescue in both profiles: pass.

Phase 1 foundation has a conditional GO only with direct PostgreSQL queue
control, a session-mode endpoint that separately passes the same matrix, or a
separately approved and verified cancellation plane. This is a deployment
condition, not a pending harness row.

Phase 1 foundation and canary gates:

- Implement the generic relay and prove every outbox claim/insert/mark crash
  window converges idempotently before the foundation gate closes.
- Populate and review the real production
  [`v0-celery-baseline/capture.json`](../architecture/evidence/go-worker-migration/v0-celery-baseline/capture.json)
  before the first production canary.
