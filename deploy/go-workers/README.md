# Go worker deployment

> **Running the cutover?** Use
> [`CUTOVER-RUNBOOK.md`](./CUTOVER-RUNBOOK.md). It is the single end-to-end
> procedure for cutover day: preconditions, Celery drain order, the `0066`
> authorization (set, run, verify, **unset**), per-kind verification, and
> rollback. This file remains the deployment and coexistence reference it
> depends on.

`deployment.json` is the deployment manifest shared by the job contract,
connection-budget checks, and stack renderers. Its River entries are deployment
groups. They select registered queues; they are not application worker types
and do not change the canonical job-kind-to-queue mapping.

Each `dev-health-worker` process receives its explicit, static queue set,
per-queue concurrency, group label, and shutdown budget as command arguments.
The set is normalized and checked against the registry before readiness. Empty,
unknown, duplicate, malformed, or conflicting selections fail closed. Runtime
queue reconfiguration is not supported. Environment equivalents remain
fallbacks for existing supervisors and conflict with the matching argument.

The deployment owns the group name, queue concurrency, replica count, resource
limits, autoscaling policy, and shutdown budget. A worker process constructs one
River client for all of its selected queues. If a separate process boundary is
needed, deploy another group; do not hide another client inside the process.

Groups can be disjoint or intentionally overlap:

```text
sync-workers      -> sync, sync_provider
analytics-workers -> investment, metrics, reports, workgraph
metrics-overflow  -> metrics, webhooks   (overlaps metrics intentionally)
```

River distributes claims safely among all consumers of an overlapping queue.
There is no global unique-queue-owner requirement.

The manifest also budgets one concurrent `dev-health-workerctl` invocation
with two domain and two queue-control session connections. The operator is a
one-shot authenticated CLI, not a replica-bearing process, and its dedicated
image target receives the operator token only when an operator invokes it.
`runtime_role_env` is the shared non-secret identity contract for every future
Go workload renderer: runtime DSN usernames must match both declared role names
before a process can become ready.

The default Compose, Swarm, Kubernetes, and Helm stacks still render only the
Celery topology. Additive Go workload overlays render the declared deployment
groups at zero replicas and require explicit deployment selection. Static
deployment-contract tests bind those overlays, the shared PgBouncer budget,
and one-shot migration wiring to the real manifests.

The contract gate validates that:

- every registered River job kind keeps its canonical queue mapping;
- every deployment group selects only registered queues;
- every selected queue has explicit deployment-owned `queue_workers` capacity,
  and queue telemetry uses that same denominator;
- disjoint and overlapping queue groups both validate;
- each worker process has exactly one River client;
- every started River client registers type-only rescue workers for every
  bounded job kind it does not execute. River elects one maintenance leader
  per schema, not per queue, so a partial registry can otherwise discard
  another queue's stuck job as unhandled;
- `MIGRATION_DATABASE_URI` is available only to the one-shot migration job;
- the one-shot operator has an exact token/DSN/config surface and is included
  in both direct and PgBouncer client connection budgets;
- every River/control process receives separate domain and queue-control DSNs;
- maximum River session-pool connections plus the transaction PgBouncer server pool and
  server reserve stay below PostgreSQL `max_connections`; the PgBouncer term
  multiplies `default_pool_size` by the declared `(database,user)` pool count;
  and
- maximum domain client connections stay below the PgBouncer client budget.

`postgres_budget.server_max_connections` in `deployment.json` must equal the
real, configured `max_connections` of the shared PostgreSQL server — it is not
a Go-stack-only figure, because the transaction pool it counts is the same
endpoint Python/Celery domain traffic uses too (CHAOS-3945). It was raised
100 -> 200 to match a measured production value; the prior 100 predates that
measurement and was already stale, independent of any pooler resize.
`server_reserved_connections` is a flat, deliberately approximate buffer for
everything this budget does not enumerate by name (operator psql sessions,
managed-Postgres incidentals); it is not a per-consumer reconciliation.

The budget is calculated from each group's `max_replicas`, including groups
disabled by default, and one client per process. Enabling the complete declared
topology cannot silently exceed the checked-in ceiling. Groups may remain at
zero replicas until their readiness dependencies, ownership route, and canary
evidence are approved.

`desired_replicas` is the reviewed replica request for each deployment group.
Validation rejects a desired count outside `min_replicas..max_replicas`, and
the deployment tests require Compose, Swarm, Kubernetes, and Helm to render
that same value. Change the manifest and all renderer outputs in one review.

River groups also declare `shutdown_grace_seconds`. It must cover the longest
registered job timeout for that group plus 60 seconds for terminal claim
finalization. Set it per group; do not inherit it from a fixed application
topology.

### River maintenance and premature terminal delivery recovery

Queue separation controls execution only. River's maintenance election is
schema-wide, so every started worker client carries type-only rescue coverage
for the job kinds owned by the other queues. These workers cannot perform
domain effects: an incorrectly queued job is cancelled fail-closed. Their
purpose is to give JobRescuer the real kind, timeout, and retry shape even when
the elected maintenance client consumes a different queue.

The sync reconciler also repairs the historical failure produced by a partial
maintenance registry. It re-arms a dispatched sync outbox row only when the
linked River row is finalized as `discarded`, still has attempts remaining,
and its latest error is exactly River's unhandled JobRescuer error. The current
unpaused River route generation must still match. Ordinary provider failures,
explicit cancellation, and exhausted jobs remain terminal. The replacement is
published through the existing outbox attempt and route-generation fences; do
not reset these rows manually or broaden the recovery predicate.

## CHAOS-3052 deployment runbook

The deployment artifacts are additive and default-off. `compose.yml` remains
untouched; the existing Celery workers, singleton Beat, and Valkey database 0
remain the baseline in every default deployment. `deployment_state` is still
`coexistence_disabled`, so rendering or scaling a Go workload does **not**
transfer a job, queue, or scheduler marker to Go.

### Images and topology

Publish one immutable image per target in `docker/go-worker.Dockerfile`:
`dev-health-go-worker` (deployment-selected queue groups),
`dev-health-go-reconciler`, `dev-health-go-scheduler`, and
`dev-health-go-stream-runner` (external, ingest). All workload definitions
run as UID/GID `65532`, deny privilege escalation, use a read-only root
filesystem, and expose only the operator HTTP surface on port 8080:
`/healthz`, `/readyz`, and `/metrics`.

The separately deployable `sync-workers` group can consume `sync`,
`sync_provider`, or both, as selected by deployment. A group that consumes both
queues uses one River client and must have handlers for every selected queue.
Two groups may instead select disjoint queues, or may intentionally overlap a
queue when River claim sharing and the combined capacity budget are reviewed.
Both queues and all provider routes remain Celery-owned unless a reviewed route
release says otherwise.

### Stream-runner profiles remain separate

`dev-health-go-stream-runner` keeps its existing runtime profiles. The
`external`, `ingest`, and `pagerduty` stream profiles are separate process
roles, use stream-specific configuration, and are not queue groups. Do not use
their profile setting to configure `dev-health-worker`.

Porting a provider/dataset pair to a `route_ready` Go complete-route handler
(the `CompleteRouteHandler`/`EffectSink` pattern `launchdarkly/feature-flags`,
`github/repo-metadata`, `github/commits`, `github/prs`,
`gitlab/repo-metadata`, `gitlab/commits`, `gitlab/commit-stats`, and the
mutually exclusive `gitlab/cicd` + `gitlab/tests` aliases already ship)
is a separate,
code-level recipe, not a deployment-manifest concern — see
[`provider-sync-porting-recipe.md`](./provider-sync-porting-recipe.md) and
`contracts/provider-matrix/v1/README.md`'s per-pair "Activation status"
sections.

### Coexistence canary

1. Compose runs a fail-closed bootstrap chain before any Go workload:
   canonical `migrate` (Alembic and ClickHouse), `go-river-provision`
   (idempotent post-Alembic runtime-role grants), `go-river-migrate` (the
   pinned River migration followed by `dev-health-worker-migrate --check`),
   then `go-contractcheck` (the embedded job registry and deployment groups).
   Every edge requires `service_completed_successfully`; a failed
   one-shot leaves the Go services unstarted. The elevated DSN is confined to
   `go-river-migrate` as `GO_WORKER_MIGRATION_DATABASE_URI`; a Go workload
   receives `POSTGRES_URI` and `WORKER_DATABASE_URI`, never
   `MIGRATION_DATABASE_URI`.
2. Verify the deployed immutable image contains the matching deployment
   manifest and `contracts/jobs/v1/registry.json`; then deploy the
   coexistence topology:

   ```bash
   docker compose -f deploy/docker-compose/compose.production.yml \
     -f deploy/docker-compose/compose.go-workers.yml \
     --profile go-workers up -d --build
   docker stack deploy -c deploy/docker-swarm/stack.yml \
     -c deploy/docker-swarm/stack.go-workers.yml dev-health
   ```

   **Helm/Kubernetes (CHAOS-4195):** the Celery `worker`/`beat` templates and
   manifests, and the `values-go-workers-coexistence.yaml` overlay that
   staged the Go path beside them, are deleted -- there is no Celery fleet
   left to coexist with. `helm upgrade --install dev-health
   deploy/helm/dev-health` and `kubectl apply -k deploy/kubernetes/` render
   the Go topology (every group at `replicas: 0`) with no extra values file
   or separate `apply`; scale groups per step 3 below.

   This bootstrap validates schema and contracts only. It does not invoke
   `dev-health-workerctl`, mutate a worker route, or transfer Celery/Beat
   ownership. For an existing local Postgres volume, the post-Alembic
   provision step is what grants access to tables that did not exist when
   `/docker-entrypoint-initdb.d` originally ran.

3. Scale each reviewed group independently, never above its declared maximum.
   Wait for `/readyz` and confirm its selected queue set, per-queue
   concurrency, worker identity, one-client count, and connection budgets
   before allowing an autoscaler or adding a second replica. Swarm has no
   native HPA; use the same signals for a manual one-at-a-time scale and wait
   through its start-first rolling update.
4. Run `dev-health-workerctl workers queues status`. Confirm each group's
   `queues`, `desired_replicas`, expiring `live_replicas`, `queue_backlog`,
   `active_jobs`, `drain_state`, and connection-budget headroom.
5. Scrape `/metrics` and alert on all three capacity signals before proceeding:
   `worker_jobs_available` (depth), `worker_job_oldest_age_seconds` (oldest
   age), and `worker_execution_saturation_ratio` (configured worker capacity).
   The Kubernetes/Helm HPAs require a Prometheus Adapter mapping those exact
   metric names; they stay at zero if the adapter cannot read them. Also watch
   `worker_database_pool_saturation_ratio` and the checked-in Go-worker
   Grafana dashboard.
6. Keep Celery consumers and Beat running during coexistence. A failed Go
   readiness, queue age threshold, or saturation threshold means scale the
   affected group back to zero; do not reroute work as a recovery action.

Normal replica shutdown is process-local: mark that instance draining, stop
its River client, then remove its presence row. Do not use a queue-set drain
for an ordinary rollout or downscale because it pauses the named queues for
the whole group.

Use an explicit queue set for a deliberate drain or resume. The group and every
queue are required, and the action is audited:

```bash
dev-health-workerctl workers queues drain \
  --group analytics-workers \
  --queue metrics --queue reports \
  --reason deploy_drain \
  --correlation-id rollout-2026-08-15

dev-health-workerctl workers queues undrain \
  --group analytics-workers \
  --queue metrics --queue reports \
  --reason deploy_resume \
  --correlation-id rollout-2026-08-15
```

### Go-only is a release gate, not a switch

The Compose `compose.go-workers-only.yml` overlay is a deliberately explicit
topology overlay: it scales Celery worker/Beat consumers to zero but does not
delete their definitions or Valkey DB 0. **Helm/Kubernetes are Go-only by
construction since CHAOS-4195** (the Celery `worker`/`beat` templates,
manifests, and the `go-workers-only.yaml`/`values-go-workers-only.yaml`
scale-to-zero overlays that used to gate them are deleted, not retained at
zero) -- there is nothing left to scale down on those two paths. This does
not relax the release gate below: whichever path you deploy on, use it only
after all of the following are recorded in the owning route release:
executable River handlers, explicit route/rollback ownership, cross-process
quiescence, scheduler policy parity,
provider sync (`sync_provider`) contract support where applicable, and a
successful coexistence canary. The current checked-in contract fails these
conditions, so Go-only is not production-authorized.

## Running a Go worker topology against local Postgres (CHAOS-3142)

These are the wiring facts discovered while exercising the additive Go path
(`go-worker-migrate` → `go-worker` → `go-reconciler`) end to end against a
local Postgres/ClickHouse/Valkey stack. They apply to any compose project
that starts these services, not to one specific file — see that project's
own compose file(s) for exactly which services exist and where.

> **`docker compose config` prints every resolved secret in cleartext**,
> including `SETTINGS_ENCRYPTION_KEY`, database passwords, and any real
> credential env vars — it resolves and dumps the fully-interpolated
> compose document, not a redacted one. Do not run it against a real
> environment's `.env` and paste the output anywhere, including into an
> agent transcript or a chat log. To check whether a variable is wired at
> all without revealing its value, grep the rendered `services.<name>` block
> for the *key* name only, or use `docker compose config --services` /
> `--format json` piped through `jq 'del(...)'` to strip the fields you
> don't need before looking at it.

### Bring-up order: the Python schema is a prerequisite, never a `depends_on`

**`depends_on` ignores profiles.** A profiled service that declares
`depends_on: migrate` pulls the *unprofiled* Python migrator into the plan. The
application migrator advances ordinary schema on a separate branch and leaves
`0066` pending unless `DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1`, but a
deployment carrying that one-shot authorization would still run
`0066_activate_river_worker_job_routes` before the downstream Go runtimes can
start. That is precisely the ordering `0066`'s own docstring forbids: routes
flip to River before any River consumer is available, and envelopes then
accumulate in `worker_job_outbox` with nothing to execute them.

`go-worker-migrate` therefore does **not** depend on `migrate`, and standing up
the Go observation path cannot move real traffic as a side effect.
`tests/test_compose_config.py::test_go_profile_overlay_never_depends_on_python_migrate`
is the regression barrier.

Bring the Python application schema current without activating the cutover,
first:

```bash
# Without the cutover opt-in, ordinary schema advances and 0066 remains pending.
docker compose run --rm --entrypoint sh migrate -c \
  'python -m dev_health_ops.cli migrate postgres upgrade'

# Confirm the application schema is current before going further.
docker compose run --rm --entrypoint sh migrate -c \
  'python -m dev_health_ops.cli migrate postgres status --check'

# Only then start the Go path.
docker compose --profile go up -d go-worker go-reconciler
```

If the database is behind `0065`, nothing fails loudly at migration time — the
River grants are `to_regclass`-guarded so a migration can never fail on a
missing table, they simply no-op. What you get instead is `go-reconciler`
staying unready. Since CHAOS-3142 that path logs the precise missing
`table.privilege` gaps via `DiagnoseRolePosture`, so the cause is legible
rather than a bare "dependency unavailable"; see the migrations section below.

### One compose project owns one Postgres cluster

Docker Compose tracks container ownership by `(project name, service name)`.
Two different compose files that both declare the same top-level `name:`
believe they own the same containers — an `up` from either one can silently
recreate the other's already-running `postgres` container in place, with
whatever role/database/PGDATA layout *that* file happens to declare. This
is not a hypothetical: it is the CHAOS-3142 incident (2026-07-25) that
motivated hardening `ops/compose.yml`'s postgres service (digest-pinned
image, `PGDATA` spelled correctly instead of the inert `PG_DATA`, its own
project name). If you are pointing a Go worker topology at an *existing*
local Postgres cluster rather than starting a fresh one, confirm which
compose project actually owns that container (`docker inspect <container>
--format '{{index .Config.Labels "com.docker.compose.project"}}'`) before
running any compose command against it from a second file.

**What the rename does and does not change.** `ops/compose.yml` is now project
`dev-health-ops`. It never owned the long-running local data: those volumes are
labelled `com.docker.compose.project=dev-health` and belong to the repo-root
`compose.yml`, whose project name is deliberately unchanged. No
`dev-health-ops_*` volume existed before this change, so nothing was stranded
or orphaned — the rename removes `ops/compose.yml`'s ability to hijack the root
project's containers, which is the whole point.

Two consequences worth knowing:

- Starting `ops/compose.yml` now creates **fresh, empty** `dev-health-ops_*`
  volumes rather than reattaching the root project's data. That is intended —
  it is an isolated stack now, not a second view of the shared one. If you
  actually want the shared cluster, use the root `compose.yml`.
- `ops/compose.yml` still sets fixed `container_name:` values, several of them
  bare (`postgres`, `clickhouse`, `valkey`, `worker`, `beat`). Container names
  are global to the Docker daemon, not scoped per project, so the two projects
  cannot run *simultaneously*: the second `up` fails with a name conflict.
  This is a strict improvement over the previous behaviour — the old failure
  mode was a *silent* recreate of the running container with a foreign
  definition, and a loud name conflict is a far better outcome than that — but
  it does mean "rename the project" is not the same as "the two stacks now
  coexist". Run one at a time, or drop the fixed names.

### River control DSNs must use session semantics

`COORDINATOR_DATABASE_URI` (read by `go-reconciler`, `go-scheduler`, and `dev-health-workerctl`) and
`dev-health-worker-migrate`'s `MIGRATION_DATABASE_URI`, which is a distinct,
more-privileged DSN — never reused as a coordinator runtime identity) must
point at the dedicated PgBouncer session endpoint (`6434` locally) or direct
Postgres (`5432` locally), never the transaction-mode pool (`6432` locally). The coordinator holds
cross-statement row and table locks (`FOR UPDATE`, `LOCK TABLE ... IN SHARE
ROW EXCLUSIVE MODE`) that a transaction-mode pooler can hand to a different
server session mid-transaction, silently breaking the lock. Startup rejects
this explicitly: `internal/storage/postgres/runtime.go`'s
`ErrCoordinatorTransactionMode` fires when the domain endpoint is
PgBouncer-pooled and the coordinator DSN resolves to that same endpoint. The
domain DSN (`POSTGRES_URI`) continues through transaction-mode PgBouncer. The
queue-control DSN (`WORKER_DATABASE_URI`) uses the dedicated session endpoint
(`6433` locally) or direct PostgreSQL for the same session-semantics reason.

### ClickHouse: the Go worker needs the native port, not the HTTP port

Python's `CLICKHOUSE_URI` (via `clickhouse-connect`) speaks ClickHouse's
HTTP interface, port `8123` locally. The Go worker's `CLICKHOUSE_URI` (via
`internal/storage/clickhouse`, built on `ClickHouse/clickhouse-go/v2`)
speaks the **native wire protocol**, port `9000` locally, and eagerly
`Ping()`s at construction time. Pointing the Go worker's `CLICKHOUSE_URI` at
the HTTP port fails immediately with `ClickHouse readiness check failed`
(`internal/storage/clickhouse.ErrUnavailable`) — the two processes' env var
has the same name but must resolve to a different port.

The `sync` queue now requires ClickHouse (CHAOS-4175: native reference-discovery
readback verification reads `teams`/`sprints`). A process selecting `sync`
without `CLICKHOUSE_URI` configured refuses to build at startup rather than
silently skipping the verification step.

### Explicit queue selection fails closed when the selected handlers are incomplete

This is intentional. With `POSTGRES_URI` and `WORKER_DATABASE_URI` configured,
`dev-health-worker` still refuses readiness when its selected queue set cannot
be served by the handlers it actually constructed. A deployment that selects
`sync,sync_provider`, for example, must construct every admitted handler for
both queues; a worker that selects only `sync` must not claim `sync_provider`.
The same exact queue and handler checks apply to every group.

Use `-Q/--queues`, `-c/--concurrency`, `--worker-group`, and
`--shutdown-timeout` to declare the process topology. Do not use a named worker
preset or rely on a service name to select queues. The process reports the
canonical queue set, per-queue concurrency, worker identity, one River client,
and effective database limits in its startup and readiness evidence.

Since CHAOS-4020 the same is true of the *rest* of the configuration: run
`dev-health-worker --help` for the full option list, the environment variable
each flag falls back to, and its default. Resolution is flag > environment >
default, an unknown flag is rejected at startup with exit status 2, and the
manifests in this directory pass their configuration in `command:` so
`docker compose config` shows what each container actually runs.

The `sync.team_autoimport` handler still needs an operational bridge when the
selected queue requires it: pass `--operational-bridge-url` and set
`WORKER_OPERATIONAL_BRIDGE_TOKEN` in the environment (the token is a credential
and has no flag). The constructor fails closed on an empty origin or token. If
the origin is plain HTTP rather than HTTPS, also pass
`--operational-bridge-allow-insecure=true`, matching the deployment examples.
These settings are dependency requirements for the selected queue; they are not
queue-selection aliases.

### Provider credentials the Go executor can decrypt

`internal/providerfoundation.CredentialResolver` reads
`integration_credentials.credentials_encrypted` and decrypts it with the
same v1 Fernet-over-PBKDF2 scheme as
`src/dev_health_ops/core/encryption.py` (`SETTINGS_ENCRYPTION_KEY`, 600,000
PBKDF2-HMAC-SHA256 iterations, default salt
`dev-health-ops-settings-encryption-v1`) — the two are wire-compatible, and
`decrypt_value`/`encrypt_value` in that Python module are the reference
implementation for constructing a test row by hand. The decrypted
**plaintext must be a JSON object** (`{"token": "..."}` for a GitHub PAT;
`ValidateCredentialShape` accepts exactly one of a PAT `token` or the three
GitHub App fields `app_id`/`private_key`/`installation_id`), not a bare
token string — decrypting a bare string fails
`decodeCredential`'s `json.Unmarshal` and surfaces identically to a wrong
key (`ErrCredentialInvalid`, "provider credential is invalid"). The claim
row's `credential_id` comes from `COALESCE(sync_runs.credential_id,
integrations.credential_id)`, so a credential row that exists but isn't
linked from `integrations.credential_id` (or the sync run) is invisible to
a claim even though `PostgresCredentialRepository.ResolveEncrypted` would
find it directly by `(org_id, provider)`.

### A coordinator (or domain) readiness failure with no stated reason usually means the database is behind on migrations

**Symptom:** `/readyz` reports `{"failed_checks":["coordinator_postgres"], ...}` (or
`domain_postgres`) and nothing else — no table name, no missing privilege,
just the check name. There is nothing wrong with the grants your migration
ran, the role's authentication, or its ownership; every one of those can
check out individually and the failure still won't clear.

**Why this happens, structurally, and why it isn't a bug:**
`coordinatorGrantStatements`/`runtimeGrantStatements`
(`internal/storage/river/migrate.go`) guard every `GRANT` with
`to_regclass('public.<table>') IS NOT NULL`, by design — a table the current
Alembic revision hasn't created yet must not fail the whole migration, it
must just skip that one grant. `CheckRolePosture`
(`internal/storage/postgres/domain_authorization.go`), the readiness side,
has no matching leniency: it asserts the FULL declared posture
(`domainPosture()`/`coordinatorPosture()`) unconditionally, because a table
that's missing from a production database *is* a real problem to report, not
one to shrug off. The asymmetry is intentional on both sides individually —
skip-if-missing on the write side, require-unconditionally on the read side —
but their combination means: **any database that is behind on Alembic head,
by even one migration that creates a table either posture requires, gets a
coordinator or domain readiness check that fails forever, with no stated
reason, until that specific migration is applied.** This is not a one-off;
it will recur for the next posture-required table added by a future
migration, on any database that hasn't caught up yet.

**How to identify it (read-only, safe against a real database):**

```sql
-- What revision is this database actually at?
SELECT version_num FROM alembic_version;
```

Compare that against the highest-numbered file in
`src/dev_health_ops/alembic/versions/`. If it's behind, check whether any
migration between the current revision and head creates a table either
`domainPosture()` or `coordinatorPosture()`
(`internal/storage/postgres/domain_authorization.go`) requires — grep those
functions' `RequiredTables`/`ColumnScoped` entries against the pending
migrations' `op.create_table(...)` calls. You can also confirm a specific
table directly:

```sql
SELECT to_regclass('public.<table_name>');  -- NULL means it does not exist
```

As of CHAOS-3142, `cmd/dev-health-reconciler` logs this automatically: a
`coordinator_postgres` readiness failure now also emits a redacted ERROR log
line per unsatisfied requirement — `postgres.DiagnoseRolePosture`, wired at
`logCoordinatorPostureGaps` — naming the table (and privilege, or
"table does not exist") without any DSN, host, or credential. Check the
reconciler's own logs first; the query above is for when you need to trace
it back to a specific migration yourself.

**Before you reach for direct `alembic upgrade head` to fix this: read every
pending migration first, not
just the one that creates the table you need.** "Behind on migrations" and
"the next migration is inert DDL" are not the same claim, and treating
`migrate` as a safe, idempotent, always-correct-to-run step because it
*usually* only adds a column or table is exactly the mistake that cost real
debugging time while building this ticket — the pending migration on the
database this was diagnosed against was `0065_add_fixed_schedule_occurrences.py`
(safe, adds one table), immediately followed by
`0066_activate_river_worker_job_routes.py` — the CHAOS-3033 Celery-to-River
cutover migration, which flips checked-in job kinds from `transport='celery'`
to `transport='river'` and, per its own docstring, requires Go consumers to
already be running for every affected queue before it commits. Direct
`alembic upgrade head` on a database with no Go workers running would silently
stop background processing for every one of those job kinds. The application
`migrate postgres` command advances the separate `application_schema` branch
while leaving the sibling `0066` River activation pending unless
`DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1`. Do not use direct
`alembic upgrade head`: the graph intentionally has multiple heads. Use the
application migrator, and confirm first that the
`api`/`migrate` container actually has your target revision's file available
(root compose mounts `./ops:/app`, so it does if you're on a branch with that
migration file; it does not against an unmodified `origin/main` checkout).

### `go-river-provision` is bootstrap-only — never reach it without `go-river-migrate`

**CHAOS-4261 (prod incident, 2026-08-25):** `scripts/worker/provision_river_roles.sql`
used to `REVOKE ALL PRIVILEGES` on the domain and queue roles and then
re-`GRANT` a hand-maintained subset of tables — a second copy of the grant
manifest that silently drifted behind `domainPosture()`/`coordinatorPosture()`
(`internal/storage/postgres/domain_authorization.go`) as tables were added
over time. `go-river-migrate` (`internal/storage/river/migrate.go`'s
`runtimeGrantStatements`/`coordinatorGrantStatements`, applied by
`cmd/dev-health-worker-migrate`) always ran afterward and restored the full
posture, so a normal two-pass deploy masked the problem — but any compose
service that reached `go-river-provision` **without** `go-river-migrate`
(a deploy that stopped after pass 1, `pgbouncer-river-queue`/
`pgbouncer-river-coordinator` starting up, or an operator running
`docker compose run go-workerctl …`) silently wiped whatever grants a prior
`go-river-migrate` run had established down to that stale subset. In prod
this produced a `go-reconciler` crash loop ("worker outbox database
unavailable") and a `dev-health-workerctl` `runtime_role_unauthorized`
failure on every invocation, including `--help`.

**The fix and the resulting contract:** `provision_river_roles.sql` is now
role-creation-and-connectivity bootstrap ONLY — it creates the three runtime
logins (idempotently, guarded by `WHERE NOT EXISTS`) and grants each one
database `CONNECT` and schema `USAGE`, nothing more. It never touches a
table or a sequence and never issues `REVOKE ALL`. **The single authority
for every per-table/sequence privilege on all three runtime roles —
domain, queue, and coordinator — is `go-river-migrate`.** It is safe to run
any number of times, in any order relative to `go-river-provision`, because
each run re-derives the full declared posture from scratch inside one
transaction rather than layering onto whatever state it finds. After
applying grants, `cmd/dev-health-worker-migrate` also runs an
executed-proof gate (`checkExecutedGrantPosture`): it re-derives the live
posture via `postgresstore.DiagnoseRolePosture` (NOT
`CheckDomainAuthorization`/`CheckCoordinatorAuthorization`/
`CheckQueueAuthorization` — those assert `current_user = expectedRole` and
this one-shot command, connected solely as the migration/admin identity,
holds no domain/queue/coordinator password to satisfy that) and exits
non-zero, naming every missing `(table, privilege)` pair, if a declared
requirement is not actually satisfied — closing the gap where a
`to_regclass` guard had silently skipped a required table (see the
readiness-failure section above) and the command still reported success.
**This is a narrower claim than full authorization:** it proves nothing
about excess privileges, River-schema grants, or role membership — that
half of the property is still proven only by each runtime binary's own
strict, `current_user`-bound check at startup
(`CheckDomainAuthorization`/`CheckQueueAuthorization`/
`CheckCoordinatorAuthorization`). A clean `go-river-migrate` run does not
substitute for a healthy `/readyz`.

**Operator rule:** never run `go-workerctl`, or anything else that reaches
`go-river-provision`, without also running `go-river-migrate` in the same
maintenance window. A deploy that stops after provisioning is not a partial
success — it is a fleet running on schema-and-USAGE only. If it happens
anyway, the recovery is to force a fresh migrate run and read the posture
back, never to hand-edit grants:

```sh
docker compose --profile go-workers up -d --no-deps --force-recreate go-river-migrate
```

(`--no-deps` so provisioning does not run again first; `--force-recreate`
because `go-river-migrate` is a `build:` service, so a previously-completed
container can satisfy `depends_on: service_completed_successfully` without
re-executing.) Confirm with a direct read of
`information_schema.role_table_grants` for each runtime role against
`domainPosture()`/`coordinatorPosture()` — never by trusting `workerctl`'s
own posture gate to have already caught it, since that gate only runs at
process start.

**The production host's compose file is hand-maintained, not a git
checkout of this repository**, and can diverge from what is reviewed here —
CHAOS-4261 traced the incident partly to a prod-only `depends_on` edge from
`pgbouncer-river-queue`/`pgbouncer-river-coordinator` onto
`go-river-provision` that does not exist in this repo's
`deploy/docker-compose/compose.production.yml`, and to `provision_river_roles.sql`
being read from a bind-mounted host directory that `docker compose pull &&
up -d` never refreshes. Diff the host's compose file and its bind-mounted
SQL against this repository before every deploy; do not assume `pull`
brought either one current.

### Live landmine: direct Alembic and authorized migration runs can still cut over

Independent of everything else in this document: if the compose project
running this stack mounts a working copy of this repository into its
`migrate` service (as the coexistence overlays and CHAOS-3142's own
repo-root wiring do, so a nested `migrate` target and other Go/Alembic
artifacts on a feature branch are visible without a rebuild), direct Alembic
still applies every explicitly targeted pending revision. The application
migrator prevents an unattended `0066` cutover by targeting only the
`application_schema` branch unless `DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1`;
that one-shot authorization must therefore be scoped to the reviewed cutover
run and removed afterward. There is no separate interactive confirmation once
the authorization is present.
This is a general operational hazard of mounting a live checkout into a
migration-running service, not specific to CHAOS-3142 or to this migration —
it deserves review as its own item, independent of the Go execution path
this document otherwise covers. Tracked as CHAOS-3143.

What CHAOS-3142 *did* close is the narrower case where bringing up the Go path
was itself the trigger: `go-worker-migrate` no longer declares
`depends_on: migrate`, so `--profile go up` cannot pull the Python migrator
into the plan (see "Bring-up order" above, and its regression test). The
general hazard remains — any `up` that restarts `migrate` for any other reason
still runs to head — which is why CHAOS-3143 stays open.

### `SETTINGS_ENCRYPTION_KEY` delivery: the `env_file` path must NOT be parameterized alongside the build context

`go-worker` receives its real, non-placeholder `SETTINGS_ENCRYPTION_KEY` (and
whatever else a developer's `.env` sets) through an `env_file:` entry, not
through an `environment:` reference — the service's `environment:` block
does not name that variable at all, on purpose (see "Provider credentials"
above: `environment:` wins over `env_file:` when both set the same key, so
adding `SETTINGS_ENCRYPTION_KEY: ${SETTINGS_ENCRYPTION_KEY:-}` there would
let an unset shell variable silently override a real key from `env_file`
with an empty string for anyone who doesn't happen to export it — a strictly
worse failure mode than the one below, and the fix that was *not* taken for
that reason).

When a compose project parameterizes `go-worker`'s build context to follow a
worktree (e.g. `context: ${DEV_HEALTH_OPS_ROOT:-./ops}`, so the branch with
the `migrate` Dockerfile target and current Go source actually gets built),
it is tempting to parameterize the adjacent `env_file:` path the same way.
**Don't.** The build context should follow the worktree; the developer's
real configuration should not — it lives at a fixed, well-known location
(`./ops/.env` in this repository) regardless of which worktree's source is
being built. A worktree checkout has no `.env` of its own. If the
`env_file:` path is parameterized alongside the build context, it resolves
to a nonexistent file under a worktree override, and `required: false`
(there specifically so a missing dev `.env` doesn't hard-fail the service)
skips it **in complete silence** — no warning, no log line, just a
container that starts with `SETTINGS_ENCRYPTION_KEY` absent from its
environment entirely and crash-loops with the generic
`"dependency_configuration_failed"` message this document already covers.
This cost real debugging time while building CHAOS-3142: the symptom looked
identical to (and was initially misdiagnosed as) the "both switches off"
crash-loop above, and the two have to be told apart by inspecting the
container's actual environment (`docker inspect --format
'{{range .Config.Env}}{{println .}}{{end}}'`), not by reading the compose
file.

### Daily-metrics compatibility bridge: a child OOM kill inside `api` shows up nowhere `docker`/SigNoz look (CHAOS-4264)

The daily/remaining-metrics compatibility bridge (`internal/jobs/metrics/daily`,
`internal/jobs/metrics/remaining`) still runs its Python compute as a child
process of the `api` container (`worker_metrics._run_compatibility_process`
spawns `python -m dev_health_ops.api.internal.worker_metrics_runner`), sharing
that container's `deploy.resources.limits.memory` cgroup with the API process
itself. On 2026-08-25 that child alone reached 1.7 GB RSS inside a 2 GiB `api`
container and was killed by the kernel memcg OOM killer. `docker inspect`'s
`OOMKilled` field only reflects PID 1 (the API process), so it read `false`;
SigNoz's `docker_stats` metrics showed memory usage climbing but recorded no
kill event at all. The only host-level evidence was the kernel ring buffer:

```
sudo dmesg -T | rg -i 'killed process|oom'
```

(a short ring buffer — capture same day) or, for a still-running container,
`/sys/fs/cgroup/memory.events`'s `oom_kill` counter and `memory.peak`.
Recreating the container resets both.

Two independent mitigations exist as of CHAOS-4264, neither of which raises
the `api` container's own memory limit:

- The runner subprocess self-limits via `RLIMIT_AS`/`RLIMIT_DATA`
  (`worker_metrics_runner._apply_memory_limit`), configurable with
  `DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES` (default 1 GiB — see the
  `api`/`worker` service `environment:` block in `compose.yml` and
  `deploy/docker-compose/compose.production.yml`). Hitting this bound raises
  a `MemoryError` the runner catches and reports as a classified
  `resource_exhausted` exit, instead of relying on the kernel to notice the
  container-wide ceiling first.
- The compatibility bridge no longer collapses a signaled/resource-exhausted
  runner with zero recorded progress into the `ambiguous` state that used to
  require a human `/metric-executions/v1/{id}/repair` call before any retry
  could re-claim it; it authorizes the retry itself (see
  `worker_metrics._mark_retry_authorized`), so a single OOM kill no longer
  permanently fails the partition.

`DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES` bounds the *runner*, not the
container: alert on the container-level `memory.events` `oom_kill` counter
(cgroup v2) regardless, since a runaway allocation anywhere else in the `api`
process is invisible to the runner's own rlimit and still only ever surfaces
in `dmesg`/`memory.events`, never in `docker inspect` or the metrics this
stack already exports.

## CHAOS-3142 end-to-end proof

The durable record of what CHAOS-3142 proved — against a real shared local
stack and an isolated throwaway one — and exactly where the chain stopped, has
moved to
[`chaos-3142-local-bringup-report.md`](./chaos-3142-local-bringup-report.md).
It is a verification record rather than a procedure, and it was crowding the
operational content in this file.

For cutover day, see [`CUTOVER-RUNBOOK.md`](./CUTOVER-RUNBOOK.md).
