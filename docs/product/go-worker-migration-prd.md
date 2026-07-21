# Product Requirements: Go Worker Runtime Migration

**Status:** Proposed  
**Owner:** Dev Health Ops  
**Linear:** CHAOS-3033 / Go Worker Runtime Migration  
**Last updated:** 2026-07-20  
**Technical design:** [Go Worker Runtime TRD](../architecture/go-worker-runtime-trd.md)  
**Delivery plan:** Repository-only [Go Worker Migration Implementation Plan](https://github.com/full-chaos/dev-health-ops/blob/main/docs/plans/go-worker-migration-implementation-plan.md)

## 1. Executive summary

`dev-health-ops` should migrate bounded background jobs from Celery to a typed Go runtime backed by PostgreSQL, while moving long-lived ingestion loops into dedicated Go stream-runner processes.

The recommended product and platform outcome is:

- **River OSS on PostgreSQL** for bounded jobs, retries, scheduled availability, queue isolation, and operator-visible job state.
- **Dedicated Go stream runners** for internal ingest, product telemetry, and external ingest over the existing Valkey/Redis Streams consumer groups.
- **A Go scheduler/reconciler** for periodic dispatch and repair, preserving the current PostgreSQL locking, `SyncRun`, `SyncRunUnit`, lease, and durable dispatch-outbox semantics.
- **Valkey database 1 remains** for cache, distributed provider rate-limit state, and streams.
- **Celery broker and result use on Valkey database 0 is retired** only after all task families pass canary, replay, and rollback gates.
- **ACR Go patterns are reused as patterns or clean-room/publicly owned code**, not by directly importing the private `dev-health-acr` repository into the public `dev-health-ops` build.

This is not a mechanical language translation. It is a controlled replacement of the execution runtime, task contracts, deployment topology, and operator experience while preserving the domain behavior that already makes sync and analytics processing reliable.

## 2. Problem statement

The current worker platform has accumulated several compensating mechanisms around Celery:

- queue-specific worker pools to prevent blocking stream consumers and heavy jobs from starving user-facing work;
- disabled prefetch and explicit queue routing for fairness;
- a singleton Beat process plus database locks for scheduler correctness;
- a durable PostgreSQL dispatch outbox and reconciler because broker publication alone is not durable enough;
- task-specific replay exclusions because global late acknowledgement is unsafe;
- long global shutdown windows because task limits are expressed at the Celery process level;
- a synchronous Celery task boundary that repeatedly bridges into async Python application code;
- a separate Redis/Valkey database for broker and result metadata even though the durable execution truth already lives in PostgreSQL domain tables.

These measures are individually valid, but together they reveal that Celery is no longer the system of record or orchestration model. For the most important sync path, Celery is primarily a wake-up and execution transport around a database-backed state machine.

Long-lived stream consumers are also a poor fit for periodic queue tasks. Historical incidents showed that repeatedly scheduling blocking consumer loops can create duplicate backlog and monopolize worker slots. Dedicated processes are a clearer execution model.

Finally, the current task registry mixes unlike workloads—coordinators, bounded commands, stream loops, schedulers, health probes, and queue telemetry—behind one decorator-based abstraction. That makes policy and ownership inconsistent and raises the cost of testing, operating, and safely evolving workers.

## 3. Product users and needs

### Operators

Operators need to:

- deploy and scale worker profiles without predicting idle windows;
- see queued, running, retrying, canceled, discarded, and completed work without exposing credentials or payload secrets;
- identify queue age, backlog, throughput, attempts, lease expiry, and stream lag;
- pause or drain one workload class without stopping unrelated work;
- retry only work that is proven replay-safe;
- understand whether a failed queue record affects a `JobRun`, `SyncRun`, `ReportRun`, backfill, or stream checkpoint;
- roll a migrated task family back to Python/Celery during the transition.

### Backend engineers

Engineers need to:

- define a versioned, typed job contract once;
- declare queue, timeout, retry, idempotency, concurrency, and sensitivity policy next to the handler;
- test handlers without a live broker;
- run integration tests against real PostgreSQL, ClickHouse, and Valkey dependencies;
- share lifecycle, configuration, storage, logging, tracing, and health patterns across Go binaries;
- migrate task families incrementally rather than maintaining two complete worker platforms indefinitely.

### Product users

Customers and internal users need:

- “Sync now,” webhook, report, and admin-triggered work to start predictably;
- no missing work after a broker, process, or rollout failure;
- no duplicated provider side effects or analytics generations;
- progress and failure states that remain coherent across UI and API surfaces;
- no credential leakage across organizations.

## 4. Goals

1. Replace Celery for all bounded production jobs with a typed Go worker runtime.
2. Replace Beat-launched stream-consumer tasks with lifecycle-owned Go stream processes.
3. Preserve the existing sync domain state machine and delivery guarantees.
4. Make task policy explicit and uniform through a single job registry.
5. Remove the Celery broker/result dependency after a reversible migration.
6. Reduce infrastructure concepts by using the existing PostgreSQL database for bounded job state.
7. Improve deployment isolation through explicit worker profiles rather than ad hoc queue lists.
8. Provide safe, payload-redacted operator controls and telemetry.
9. Reuse proven ACR Go platform patterns without violating repository and licensing boundaries.
10. Keep Python and Go interoperable during the migration through versioned wire contracts.

## 5. Non-goals

This project will not:

- rewrite the FastAPI, Strawberry GraphQL, or web application in Go;
- replace PostgreSQL or ClickHouse;
- change provider ownership of authentication, pagination, retry interpretation, normalization, or sink persistence boundaries;
- change product metric definitions, attribution rules, investment taxonomy, or evidence semantics;
- introduce Temporal, NATS JetStream, Kafka, or another new infrastructure service in the first migration;
- remove Valkey database 1 while it remains the cache, distributed rate-limit, and stream backend;
- make all workloads exactly-once; execution remains at-least-once where replay-safe and explicitly at-most-once where duplicate effects remain unsafe;
- directly copy private ACR source into the public repository without an approved ownership and licensing path;
- port every Python library line-for-line where a stable service or data boundary is more appropriate.

## 6. Product principles and non-negotiable invariants

### Domain state remains authoritative

`SyncRun`, `SyncRunUnit`, their claim/lease tokens, the sync dispatch outbox, and finalization ledgers remain execution truth. A queue job is a wake-up, attempt record, and operator surface—not a replacement workflow state machine.

### Delivery policy is per task family

Every job kind must declare one of:

- **replay-safe at-least-once**;
- **guarded at-least-once**, where a database claim, idempotency key, or ledger prevents duplicate effects;
- **at-most-once**, where the job must be marked before dispatch because duplicate execution is unsafe;
- **non-retryable**, where failures require a new operator action.

Global retry or acknowledgement switches are prohibited.

### Credentials are explicit and scoped

Worker payloads contain identifiers, not provider tokens or API keys. Credentials are resolved inside the handler, scoped to the organization/integration, passed explicitly to clients, and excluded from logs, traces, UI, and operator commands.

### One execution model per workload shape

The platform supports four explicit modes:

1. **Command jobs** — bounded, independently retryable work.
2. **Coordinator jobs** — database-backed planning, fan-out, finalization, and repair.
3. **Stream runners** — long-lived consumer loops with checkpoints and reclaim semantics.
4. **Scheduler runners** — leader-elected periodic dispatch only.

A health check or queue monitor is not modeled as a background job when it can be a process endpoint or runtime metric.

### Migration is reversible

Until a task family completes its stability gate, routing can be returned to its prior Python/Celery handler without data repair. Dual execution that writes the same analytics or provider side effect is prohibited unless the write path is proven deduplicated.

## 7. Scope

### 7.1 In scope

#### Runtime foundation

- Go module and worker binaries in `dev-health-ops`.
- Typed job registry and versioned JSON payload contracts.
- River client, migrations, middleware, retries, cancellation, timeouts, job metadata, and queue profiles.
- Python insert-only enqueue integration for transitional producers, subject to compatibility validation.
- HTTP health, readiness, metrics, and sanitized operator endpoints or CLI.
- Structured logging, OpenTelemetry traces/metrics, Sentry-equivalent error capture if retained, and correlation propagation.
- PostgreSQL and ClickHouse storage adapters.
- Secret-file and environment configuration patterns.

#### Workload migration

- sync scheduler, reconciler, dispatcher, unit execution, finalizer, post-sync relay, and provider team-drift projection;
- daily metrics, complexity, DORA, release impact, capacity, recommendations, work graph, investment, and membership jobs;
- reports and report scheduling;
- webhooks, billing notifications, heartbeat, retention, and administrative background work;
- internal ingest, product telemetry, and external ingest stream consumers, including external-ingest recompute debounce and health behavior;
- queue monitoring and health behavior, converted to native runtime telemetry/endpoints;
- provider-specific and cost-class routing and concurrency policy.

#### Infrastructure

- PostgreSQL River schema migrations.
- A small direct/session PostgreSQL pool for River notifications and leadership, with a poll-only PgBouncer transaction-mode fallback.
- Existing PgBouncer path retained for domain traffic.
- Compose, production Compose, Kubernetes, Helm, Docker Swarm, image build, migration job, CI, and deployment tests.
- HPA and alerting changes from Redis queue length to job age/depth and stream lag.
- Celery/Beat and Valkey database 0 retirement after cutover.

#### Documentation and operations

- architecture diagrams;
- task migration matrix;
- replay/idempotency matrix;
- deployment and rollback runbooks;
- operator inspection, pause, retry, cancel, and drain procedures;
- platform-contract and worker documentation updates.

### 7.2 Explicitly deferred

- replacement of Redis Streams with NATS JetStream;
- adoption of Temporal for cross-service workflows;
- durable re-drive of post-sync work before CHAOS-2596 closes;
- extraction of a shared public Go platform module until ownership and licensing are approved;
- removal of Python from domain areas whose libraries or algorithms have not yet been ported or isolated behind a stable boundary.

## 8. Functional requirements

### FR-1: Versioned job contracts

Every bounded job must have:

- a stable `kind`;
- an integer or semantic contract version;
- a JSON schema;
- generated or hand-maintained Go and Python types;
- golden request fixtures;
- compatibility tests that reject breaking payload changes without a new version.

Payloads must contain only primitive identifiers and bounded metadata. Large source data, credentials, and rendered reports stay in authoritative stores.

### FR-2: Unified job definition

Every job kind must register:

- handler and owning package;
- execution mode;
- queue/profile;
- priority;
- default timeout and cancellation behavior;
- maximum attempts and backoff classifier;
- idempotency policy and key derivation;
- concurrency policy;
- sensitive fields;
- domain run linkage;
- migration route and rollback route.

Startup must fail if a deployed profile references an unknown job kind or if a registered kind has incomplete policy.

### FR-3: Transactional enqueue

When a business-state write and job creation belong to one outcome, they must commit atomically in PostgreSQL.

During the Python/Go transition, Python producers may use the River insert-only SQLAlchemy client within the existing transaction after a compatibility spike. Existing sync dispatch-outbox rows remain the durable language-neutral bridge for sync orchestration.

### FR-4: Scheduling

The Go scheduler must:

- run safely with multiple replicas through leader election or row locking;
- use existing per-organization cron and `next_run_at` semantics;
- create bounded jobs transactionally;
- skip or defer work for paused, deleted, unentitled, or invalid organizations;
- expose last evaluation, next due time, dispatch result, and error telemetry;
- avoid running business workloads in the scheduler process.

### FR-5: Retry and failure classification

Handlers must classify failures as:

- success;
- retryable transient;
- retryable after a specific time;
- rate-limited/deferred;
- canceled;
- discarded/non-retryable;
- terminal domain failure.

Backoff must be bounded and include jitter. Provider rate limits continue to use the shared distributed gate and budget reservations rather than multiplying with worker replicas.

### FR-6: Idempotency and replay

Before migration, every task family must document:

- the side effects it performs;
- its deduplication key or database claim;
- whether partial work can be repeated;
- how a process crash is detected;
- how stale work is repaired;
- whether operator retry is allowed.

`post_sync` remains at-most-once until CHAOS-2596 proves all affected ClickHouse reads/writes safe under re-drive.

### FR-7: Cancellation, drain, and shutdown

The runtime must:

- stop fetching new jobs on `SIGTERM`;
- cancel cooperative handlers through context deadlines;
- allow a profile-specific drain budget;
- preserve or requeue work according to the job’s declared policy;
- expose active job identifiers without payloads;
- distinguish operator cancellation from timeout, crash, and domain failure.

Stream runners must stop reading, finish or safely abandon the current message, flush checkpoints, and close storage clients in order.

### FR-8: Workload isolation

The default deployment profiles are:

- `latency`: webhooks, reports, user-triggered lightweight coordinators;
- `sync`: provider sync units and sync coordination;
- `heavy`: metrics, work graph, investment, complexity, and large backfills;
- `stream-ingest`: internal ingest and product telemetry;
- `stream-external`: customer external ingest;
- `scheduler`: periodic evaluation and reconciliation.

Profiles may share one binary but have separate deployment, resource, autoscaling, and concurrency configuration.

### FR-9: Operator visibility and controls

Operators must be able to:

- list queue depth and oldest-job age by profile and kind;
- inspect active jobs with safe metadata only;
- view attempts and terminal reason;
- pause/resume a queue;
- cancel or retry eligible jobs;
- drain one deployment;
- inspect stream pending counts, oldest pending age, lag, reclaim count, and consumer identity;
- correlate a job to domain rows and traces.

The product must never expose serialized payloads by default.

### FR-10: Progress and product status

Queue state must not replace product state. Jobs that represent product-visible operations must update or derive from their authoritative rows:

- sync: `JobRun`, `SyncRun`, `SyncRunUnit`, backfill records;
- reports: `ReportRun`;
- other long-running jobs: a canonical `JobRun` or dedicated domain run row where missing today.

The migration must close existing observability gaps where work is visible only in logs.

### FR-11: Shadow and canary validation

Each task family must support one or more of:

- read-only shadow execution;
- output comparison to a canonical Python run;
- canary organization routing;
- dual calculation with only one writer;
- deterministic fixture comparison;
- live failure-injection.

Promotion requires documented parity thresholds and zero unresolved correctness differences.

## 9. Task-family product scope

| Family | Current shape | Target mode | Product requirement |
|---|---|---|---|
| Sync planning and dispatch | Celery coordinators + DB outbox | Coordinator jobs | Preserve outbox, claim/lease, cost class, provider routing, and bounded fan-out |
| Sync unit execution | Long bounded provider work | Command jobs in `sync` profile | Explicit credentials, rate budgets, lease heartbeats, retry matrix |
| Sync finalization/post-sync | Celery callbacks/relay | Coordinator jobs | Preserve once-only ledgers; post-sync remains at-most-once until CHAOS-2596 |
| Team drift/autoimport | Provider discovery + ClickHouse projection tasks | Sync commands/coordinators | Preserve provider matrix, fail-closed auth, and ClickHouse authority |
| Daily and extra metrics | Fan-out/batch/finalize | Coordinator + heavy command jobs | Deterministic partitions, generation identity, ClickHouse dedup |
| Work graph/investment | Heavy compute and LLM calls | Heavy command jobs | Persisted run identity, LLM adapter parity, bounded concurrency |
| Reports | Scheduled and on-demand | Latency/heavy command jobs | `ReportRun` is authoritative; schedule uniqueness |
| Webhooks/billing | Short side effects, including PagerDuty stream-backed processing | Latency command jobs | Idempotency keys plus stream delete/retry/dead-letter parity |
| Heartbeat/retention | Periodic operations | Scheduler + command jobs | Unique schedule occurrence and safe retry policy |
| Ingest/product telemetry | Repeated blocking Celery tasks | Stream runners | Long-lived lifecycle, consumer groups, bounded checkpoint/reclaim |
| External ingest | Singleton Celery consumer + recompute debounce + health task | Dedicated stream runner + bounded control job/native telemetry | Preserve singleton and atomic debounce consumption until reclaim is redesigned and validated |
| Queue monitoring | Celery monitoring task | Native metrics/exporter | No monitoring job competing with monitored work |
| Worker health | Celery task/inspect | HTTP health/readiness + CLI | No broker round trip required for process health |

## 10. Infrastructure scope changes

### Remove

After full cutover:

- Celery worker and Beat containers/deployments;
- Celery task registry, decorators, signals, inspect command, and async bridge;
- Valkey database 0 broker queues, unacked keys, and result metadata;
- Celery-specific queue purge and worker inspect runbooks;
- Celery/Sentry/OTel instrumentation packages and configuration;
- blanket one-hour process assumptions where job-specific policy replaces them.

### Keep

- PostgreSQL and the existing migration job;
- PgBouncer for domain traffic;
- ClickHouse;
- Valkey database 1 for cache, provider rate limiting, and Streams;
- existing sync state, outbox, claims, leases, concurrency buckets, and budget guards;
- separated deployment profiles and workload-specific resource limits;
- explicit credential threading.

### Add or change

- River schema in PostgreSQL.
- A proposed `WORKER_DATABASE_URI`, introduced by CHAOS-3037, for a small direct/session queue-control pool; `PollOnly` is the fallback for transaction-mode PgBouncer deployments.
- Go worker and scheduler images or targets.
- job queue and stream telemetry exporters.
- HPA signals based on oldest job age, available/running count, execution saturation, and stream lag.
- safe operator API/CLI and River UI evaluation; any UI must enforce payload redaction and existing auth boundaries.
- versioned job contract artifacts and compatibility checks.

## 11. Success criteria

### Correctness

- Zero known duplicate provider side effects caused by job redelivery.
- Zero lost committed sync dispatches in broker/process failure tests.
- All migrated jobs have passing replay and crash-window tests.
- Product-visible run states remain consistent with job states.
- CHAOS-2596 is completed before durable post-sync re-drive is enabled.
- Credentials and payload secrets do not appear in logs, metrics, traces, job UI, or operator output.

### Reliability and latency

Before the first production canary, establish baselines for:

- enqueue-to-start latency by profile;
- oldest queue age;
- job success/retry/discard rates;
- process crash recovery time;
- sync unit lease expiry rate;
- stream pending count and oldest pending age;
- worker resource usage.

A migrated family must meet or improve its baseline SLO and must not increase unrelated profile latency.

### Operability

- Every profile exposes `/healthz`, `/readyz`, and `/metrics`.
- Operators can drain and roll a profile without a global worker shutdown.
- Queue depth and age telemetry survives failure of any one workload profile.
- Every terminal job has a bounded, redacted failure reason and correlation ID.
- Runbooks cover pause, retry, cancel, drain, rollback, and queue/schema recovery.

### Simplification

- No Celery or Beat process remains in production.
- No Celery publish/import path remains in application code.
- Valkey database 0 is no longer required by `dev-health-ops`.
- One job registry replaces decorator and deployment queue drift.
- Stream consumers are not scheduled as recurring bounded jobs.

## 12. Rollout policy

Migration occurs by task family, not by global runtime switch.

For each family:

1. freeze and version the payload contract;
2. capture current behavior and production-like fixtures;
3. implement the Go handler and policy;
4. shadow or compare without duplicate writes;
5. canary selected organizations or schedule occurrences;
6. promote producer routing through a feature flag;
7. retain the Celery rollback route through the stability gate;
8. remove the Python handler only after no rollback use for two stable releases.

A family that fails parity or reliability gates returns to the prior route. Database migrations must remain additive until the final decommission phase.

## 13. Dependencies

- CHAOS-3033 — migration epic.
- CHAOS-2521 / CHAOS-2581 — durable sync dispatch and repair semantics.
- CHAOS-2596 — post-sync read/write idempotency.
- CHAOS-2305 — horizontal scaling invariants.
- CHAOS-2522 — deploy-safe worker behavior.
- CHAOS-2277 — stream-consumer starvation lessons.
- CHAOS-2923 — ACR Go service-shell and storage-boundary reference.
- A licensing/ownership decision for any shared Go foundation extracted from ACR.
- A River, PgBouncer, migration, and Python enqueue compatibility spike.

## 14. Major risks

| Risk | Impact | Mitigation |
|---|---|---|
| Port changes domain behavior | Incorrect analytics or provider coverage | Golden fixtures, shadow comparison, provider-by-provider parity gates |
| Queue DB load competes with semantic traffic | API/worker latency regression | Dedicated small direct pool, bounded workers, indexes, load tests, PgBouncer retained for domain traffic |
| Python client and Go River schema drift | Jobs fail to decode or enqueue | Pin compatible versions, contract tests against migrated schema, fallback bridge decision in phase 0 |
| Go lacks equivalent provider/analytics libraries | Scope expansion | Port by capability, retain stable Python service boundary temporarily where justified, do not shell out per job |
| Replay assumptions are wrong | Duplicate external or ClickHouse effects | Required idempotency matrix, kill tests, CHAOS-2596 gate |
| Private ACR code leaks into public ops | Licensing and distribution failure | Clean-room/public implementation; explicit extraction decision |
| New profiles recreate queue drift | Unconsumed jobs | Registry-generated profile manifests and CI coverage checks |
| Long jobs ignore cancellation | Unsafe deploy or duplicate work | Context-aware clients, checkpoints/leases, profile-specific drain tests |
| External ingest scales before reclaim redesign | Duplicate or stranded stream messages | Preserve singleton and concurrency 1 until dedicated scaling issue closes |

## 15. Decisions required before implementation lock

1. Confirm River OSS as the bounded-job engine after the compatibility spike.
2. Select the exact Go and River versions and support policy.
3. Approve direct/session `WORKER_DATABASE_URI` as the production default, with PollOnly as the pooler fallback.
4. Approve the Python insert-only client or select a language-neutral relay fallback.
5. Decide whether generic ACR platform patterns live directly under `dev-health-ops/internal/platform` or in a separately licensed public module.
6. Define the first canary organizations and production parity thresholds.
7. Decide whether River UI is deployed; a custom sanitized operator surface remains required if River UI cannot meet payload and authorization constraints.
8. Assign owners for provider ports, analytics ports, runtime foundation, and infrastructure.

## 16. Approval criteria

The PRD is approved when product, backend, data, security, and infrastructure owners agree that:

- the target runtime and infrastructure direction are acceptable;
- the non-goals prevent an uncontrolled whole-platform rewrite;
- the delivery semantics and CHAOS-2596 dependency are explicit;
- every current task family has a target execution mode;
- migration, rollback, observability, and credential requirements are sufficient for production.
