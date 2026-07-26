# Go worker deployment profiles

`profiles.json` is the Phase 1 deployment source of truth shared by contract,
connection-budget, and future stack renderers. It is deliberately disabled
during coexistence: a checked-in process or queue does not route production
work away from Celery.

The manifest also budgets one concurrent `dev-health-workerctl` invocation
with two domain and two direct queue-control connections. The operator is a
one-shot authenticated CLI, not a replica-bearing process, and its dedicated
image target receives the operator token only when an operator invokes it.
`runtime_role_env` is the shared non-secret identity contract for every future
Go workload renderer: runtime DSN usernames must match both declared role names
before a process can become ready.

The default Compose, Swarm, Kubernetes, and Helm stacks still render only the
Celery topology. Additive Go workload overlays now render the declared
`processes` at zero replicas and require an explicit coexistence or Go-only
selection. Static deployment-contract tests bind those overlays, the shared
PgBouncer budget, and one-shot migration wiring to the real manifests.

The contract gate validates that:

- every registered River job kind and queue appears in exactly one matching
  worker profile;
- undeclared queues and kinds cannot appear in a River profile;
- every River queue has one explicit `queue_workers` capacity, and the queue
  telemetry denominator must use that same value when a River client is
  composed;
- `MIGRATION_DATABASE_URI` is available only to the one-shot migration job;
- the one-shot operator has an exact token/DSN/config surface and is included
  in both direct and PgBouncer client connection budgets;
- every River/control process receives separate domain and queue-control DSNs;
- maximum direct queue-control connections plus the PgBouncer server pool and
  server reserve stay below PostgreSQL `max_connections`; the PgBouncer term
  multiplies `default_pool_size` by the declared `(database,user)` pool count;
  and
- maximum domain client connections stay below the PgBouncer client budget.

The budget is calculated from `max_replicas`, including profiles disabled by
default, so enabling the complete declared topology cannot silently exceed the
checked-in ceiling. Phase 1 keeps every `min_replicas` at zero until its
readiness dependencies, ownership route, and canary evidence are approved.

## CHAOS-3052 deployment runbook

The deployment artifacts are additive and default-off. `compose.yml` remains
untouched; the existing Celery workers, singleton Beat, and Valkey database 0
remain the baseline in every default deployment. `deployment_state` is still
`coexistence_disabled`, so rendering or scaling a Go workload does **not**
transfer a job, queue, or scheduler marker to Go.

### Images and topology

Publish one immutable image per target in `docker/go-worker.Dockerfile`:
`dev-health-go-worker` (sync, heavy, ops),
`dev-health-go-reconciler`, `dev-health-go-scheduler`, and
`dev-health-go-stream-runner` (external, ingest). All workload definitions
run as UID/GID `65532`, deny privilege escalation, use a read-only root
filesystem, and expose only the operator HTTP surface on port 8080:
`/healthz`, `/readyz`, and `/metrics`.

The separately deployable `sync-provider` topology starts the checked-in
`sync` runtime profile and consumes the isolated `sync_provider` River queue
when the provider-unit contract/handler release is present. It must never be
combined with the coordinator's `sync` queue: the two clients have disjoint
handlers. Both queues and all provider routes remain Celery-owned unless a
reviewed route release says otherwise.

### Coexistence canary

1. Compose runs a fail-closed bootstrap chain before any Go workload:
   canonical `migrate` (Alembic and ClickHouse), `go-river-provision`
   (idempotent post-Alembic runtime-role grants), `go-river-migrate` (the
   pinned River migration followed by `dev-health-worker-migrate --check`),
   then `go-contractcheck` (the embedded job registry and deployment
   profile). Every edge requires `service_completed_successfully`; a failed
   one-shot leaves the Go services unstarted. The elevated DSN is confined to
   `go-river-migrate` as `GO_WORKER_MIGRATION_DATABASE_URI`; a Go workload
   receives `POSTGRES_URI` and `WORKER_DATABASE_URI`, never
   `MIGRATION_DATABASE_URI`.
2. Verify the deployed immutable image contains the matching
   `profiles.json` and `contracts/jobs/v1/registry.json`; then deploy the
   coexistence topology:

   ```bash
   docker compose -f deploy/docker-compose/compose.production.yml \
     -f deploy/docker-compose/compose.go-workers.yml \
     --profile go-workers up -d --build
   helm upgrade --install dev-health deploy/helm/dev-health \
     -f deploy/helm/dev-health/values-go-workers-coexistence.yaml
   kubectl -n dev-health apply -f deploy/kubernetes/go-workers.yaml
   docker stack deploy -c deploy/docker-swarm/stack.yml \
     -c deploy/docker-swarm/stack.go-workers.yml dev-health
   ```

   This bootstrap validates schema and contracts only. It does not invoke
   `dev-health-workerctl`, mutate a worker route, or transfer Celery/Beat
   ownership. For an existing local Postgres volume, the post-Alembic
   provision step is what grants access to tables that did not exist when
   `/docker-entrypoint-initdb.d` originally ran.

3. Scale one reviewed profile only, never above `profiles.json.max_replicas`.
   Wait for `/readyz` and confirm its build metadata and profile labels before
   allowing an autoscaler or adding a second replica. Swarm has no native HPA;
   use the same signals for a manual one-at-a-time scale and wait through its
   start-first rolling update.
4. Scrape `/metrics` and alert on all three capacity signals before proceeding:
   `worker_jobs_available` (depth), `worker_job_oldest_age_seconds` (oldest
   age), and `worker_execution_saturation_ratio` (configured worker capacity).
   The Kubernetes/Helm HPAs require a Prometheus Adapter mapping those exact
   metric names; they stay at zero if the adapter cannot read them. Also watch
   `worker_database_pool_saturation_ratio` and the checked-in Go-worker
   Grafana dashboard.
5. Keep Celery consumers and Beat running during coexistence. A failed Go
   readiness, queue age threshold, or saturation threshold means scale the Go
   profile back to zero; do not reroute work as a recovery action.

### Go-only is a release gate, not a switch

The `compose.go-workers-only.yml`, `go-workers-only.yaml`, and
`values-go-workers-only.yaml` are deliberately explicit topology overlays.
They scale Celery worker/Beat consumers to zero but do not delete their
definitions or Valkey DB 0. Use them only after all of the following are
recorded in the owning route release: executable River handlers, explicit
route/rollback ownership, cross-process quiescence, scheduler policy parity,
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
`depends_on: migrate` pulls the *unprofiled* Python migrator into the plan, so
`docker compose --profile go up -d` runs Alembic to **head** — and head is
`0066_activate_river_worker_job_routes`, the cutover that retargets 23 of 24
job kinds from Celery to River. Both Go runtimes sit downstream of
`go-worker-migrate`, so that edge guaranteed precisely the ordering `0066`'s
own docstring forbids: routes flip to River *before* any River consumer can
start, and envelopes then accumulate in `worker_job_outbox` with nothing to
execute them.

`go-worker-migrate` therefore does **not** depend on `migrate`, and standing up
the Go observation path cannot move real traffic as a side effect.
`tests/test_compose_config.py::test_go_profile_overlay_never_depends_on_python_migrate`
is the regression barrier.

Bring the Python schema to the last pre-cutover revision yourself, first:

```bash
# Explicit, separately authorized, and stops one revision short of the cutover.
# `revision` is a positional on `upgrade`; omitting it means head, i.e. 0066.
docker compose run --rm --entrypoint sh migrate -c \
  'python -m dev_health_ops.cli migrate postgres upgrade 0065'

# Confirm where you actually landed before going further.
docker compose run --rm --entrypoint sh migrate -c \
  'python -m dev_health_ops.cli migrate postgres current'

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

### The coordinator DSN must be direct Postgres, never PgBouncer

`COORDINATOR_DATABASE_URI` (read by `go-reconciler` and by
`dev-health-worker-migrate`'s `MIGRATION_DATABASE_URI`, which is a distinct,
more-privileged DSN — never reused as a coordinator runtime identity) must
point at Postgres's own port (`5432` locally), never through PgBouncer's
transaction-mode pool (`6432` locally). The coordinator holds
cross-statement row and table locks (`FOR UPDATE`, `LOCK TABLE ... IN SHARE
ROW EXCLUSIVE MODE`) that a transaction-mode pooler can hand to a different
server session mid-transaction, silently breaking the lock. Startup rejects
this explicitly: `internal/storage/postgres/runtime.go`'s
`ErrCoordinatorTransactionMode` fires when the domain endpoint is
PgBouncer-pooled and the coordinator DSN resolves to that same endpoint. The
domain DSN (`POSTGRES_URI`) may continue through PgBouncer, as the Python
API/Celery processes' `DATABASE_URI` already does; the queue-control DSN
(`WORKER_DATABASE_URI`) must also be direct, for the same reason.

### ClickHouse: the Go worker needs the native port, not the HTTP port

Python's `CLICKHOUSE_URI` (via `clickhouse-connect`) speaks ClickHouse's
HTTP interface, port `8123` locally. The Go worker's `CLICKHOUSE_URI` (via
`internal/storage/clickhouse`, built on `ClickHouse/clickhouse-go/v2`)
speaks the **native wire protocol**, port `9000` locally, and eagerly
`Ping()`s at construction time. Pointing the Go worker's `CLICKHOUSE_URI` at
the HTTP port fails immediately with `ClickHouse readiness check failed`
(`internal/storage/clickhouse.ErrUnavailable`) — the two processes' env var
has the same name but must resolve to a different port.

### With both route switches off, `go-worker`'s `sync` profile fails closed at startup

This is intentional, not a bug, and it is worth expecting before you hit it:
with `POSTGRES_URI` and `WORKER_DATABASE_URI` both configured (as they must
be to do anything useful) and `DEV_HEALTH_PROFILE=sync`,
`cmd/dev-health-worker` unconditionally constructs the
`sync.team_autoimport` handler the moment the domain DSN is present
(`buildSyncCoordinatorWorker` gates only on `profile == "sync"`, not on
either route switch). Startup then requires an **exact match** between what
was actually constructed and the full job-kind/queue set the `sync`
deployment profile declares
(`cmd/dev-health-worker/dependencies.go`'s `profileReady` — the CUT-02
"exact startup validation" gate) — which also declares
`sync.provider_unit`. With `WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED` and
`WORKER_GITHUB_REPO_METADATA_ENABLED` both at their secure default of
`false`, `sync.provider_unit` is never constructed, the match fails, and the
process exits non-zero — which crash-loops under `restart: unless-stopped`.
The container never binds `:8080`, so `/readyz` never even becomes reachable
to show *why*; the reason is in the container's own log line:
`{"level":"ERROR","msg":"configure runtime dependencies","error_category":"dependency_configuration_failed"}`.

This is fail-closed by design — a worker dispatching `github`/`repo-metadata`
units into River while refusing to build the handler for them would strand
every one of those units, which is worse than not starting at all. **To get
`go-worker`'s `sync` profile to a ready state, set one of the two switches
to `true`** (for example `WORKER_GITHUB_REPO_METADATA_ENABLED=true`) in
whatever environment file feeds that compose project. The default stays
`false` in every checked-in file; this is a per-environment operator choice,
made alongside enabling the matching route in a reviewed release, never a
default flip.

### The `sync.team_autoimport` bridge needs `WORKER_OPERATIONAL_BRIDGE_*` even though `profiles.json` doesn't say so

`deploy/go-workers/profiles.json`'s `sync` process entry does not list
`WORKER_OPERATIONAL_BRIDGE_URL`/`WORKER_OPERATIONAL_BRIDGE_TOKEN` in its
`secret_env`, but `buildSyncCoordinatorWorker` constructs an HTTP bridge
(`syncdispatchruntime.NewHTTPBridge`) unconditionally for the `sync` profile
once the domain DSN is configured, and that constructor fails closed on an
empty `BaseURL` or `BearerToken`. Set `WORKER_OPERATIONAL_BRIDGE_URL` (an
HTTP(S) origin reachable from the Go worker's container — typically the
Python `api` service) and a non-empty `WORKER_OPERATIONAL_BRIDGE_TOKEN`; if
the origin is plain HTTP rather than HTTPS, also set
`WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE=true`, matching the pattern
`deploy/docker-compose/compose.go-workers.yml`'s `go-worker-heavy` service
already uses.

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

**Before you reach for `alembic upgrade head` (or the `migrate` service, which
always goes to head) to fix this: read every pending migration first, not
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
already be running for every affected queue before it commits. Running
`migrate` to head on a database with no Go workers running yet would have
silently stopped background processing for every one of those job kinds. If
a targeted `alembic upgrade <revision>` stopping short of a cutover migration
is what you need, use that instead of `head`, and confirm first that the
`api`/`migrate` container actually has your target revision's file available
(root compose mounts `./ops:/app`, so it does if you're on a branch with that
migration file; it does not against an unmodified `origin/main` checkout).

### Live landmine: a mounted `ops` checkout is one `migrate` run away from an unattended cutover

Independent of everything else in this document: if the compose project
running this stack mounts a working copy of this repository into its
`migrate` service (as the coexistence overlays and CHAOS-3142's own
repo-root wiring do, so a nested `migrate` target and other Go/Alembic
artifacts on a feature branch are visible without a rebuild), then **any
`docker compose up` that (re)starts `migrate`** — not just an explicit,
reviewed cutover — **applies every pending Alembic migration on that
checkout, including a cutover migration, unattended, the moment one exists
in the pending set.** There is no confirmation step, no dry run, and no
distinction in `migrate`'s own behavior between "add a column" and "flip 23
job kinds from Celery to River." The migration itself is the only gate, and
by the time it's pending in a checked-out branch, that gate is one ordinary
`up` away from being crossed by whoever runs it next, for any reason,
possibly without realizing a cutover migration is even in the pending set.
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

## CHAOS-3142 end-to-end proof: final report

This is the durable record of what CHAOS-3142 actually proved, against both
a real shared local stack and an isolated throwaway one, and exactly where
it stopped. Written here rather than in a session-scoped note because this
is the artifact that outlives the session.

### Proven, against a real shared local stack

- Coordinator-role provisioning on a pre-existing, pre-CHAOS-3033-split
  Postgres cluster: `go-river-provision` created the `devhealth_coordinator`
  login where none existed before.
- The exact grant/readiness asymmetry this document names above, hit for
  real: `coordinatorPosture()` requires `fixed_schedule_occurrences`
  (`0065_add_fixed_schedule_occurrences.py`), which didn't exist on this
  database (`alembic_version` was `0064`, two migrations behind head); the
  `to_regclass`-guarded grant was silently skipped;
  `coordinator_postgres` readiness failed with no stated reason until a
  **targeted** `alembic upgrade 0065` (never `head` — see above) created the
  table and a re-run of `go-worker-migrate` picked up the previously-skipped
  grant.
- `go-reconciler` reaching `/readyz` → `{"status":"ok"}` with `RestartCount
  0` against the real stack, and staying that way — `CheckCoordinatorAuthorization`
  re-queries live on every poll, so no restart was needed once the grant
  landed.
- Non-zero, real metric series from the real reconciler:
  `worker_outbox_reconciler_up 1`, `sync_dispatch_observer_up 1`, and their
  paired `..._last_success_age_seconds` gauges reporting sub-second real
  values — the "present but zero" failure mode this document elsewhere
  warns against did not occur here.

### Proven, against an isolated throwaway project

Using a separate compose project (`-p <name>`, its own network/volumes,
never the shared stack), with a hand-seeded additive
`Integration`/`IntegrationSource`/`IntegrationDataset`/`SyncRun`/
`SyncRunUnit`/`SyncRunReferenceDiscovery`/`integration_credentials` row set
for a synthetic org (github/repo-metadata), and the durable route
(`worker_job_routes.transport`) and Python producer switch
(`WORKER_GITHUB_REPO_METADATA_ENABLED`) both flipped ONLY inside that
isolated project — never on shared infrastructure:

- Producer gate (`dispatch_sync_run`) → `worker_job_outbox` row
  (`status=delivered`) → reconciler relay → River job → Go handler pickup
  and execution, all confirmed end to end.
- The Go handler reached a real `github.com` HTTP round-trip (a fake PAT, so
  a `retryable`, not `permanent`, failure — real network I/O happened, this
  wasn't a local rejection) and `worker_budget_wait_seconds{provider="github",
  cost_class="light"}` carried `count=6`, `sum=0.000241334` — a real,
  non-zero series, not the present-and-zero shape this document elsewhere
  calls out as the actual failure mode to guard against.

### Not proven

A ClickHouse `repos` row, on either stack. `worker_sync_lease_expired_total`
also never carried a non-zero series in any run: this is expected and
**not a regression signal** — that metric only increments on a claim that
itself recovered an *expired* lease
(`providerunit.Handler.observeLeaseRecovery` checks `claim.Recovered`, a
no-op for an ordinary first-attempt claim), and nothing in this proof
deliberately expired and re-claimed a lease.

**Correction (this section originally overstated the credential blocker —
see below):** the isolated project's synthetic org needed a fake PAT
because its Postgres volume was fresh and empty; that limitation does not
carry over to a stack with real data, and an earlier version of this report
wrongly transplanted it onto the real shared stack without checking. On the
real stack the blocker is routing, not credentials — see the next section.

### Not attempted against the real shared stack, and why

**Blocked on a deliberate routing decision, not on credential
availability.** A working, decryptable, correctly-shaped github credential
already exists on the real shared stack — this was checked directly rather
than assumed:

- `integrations`: 16 rows with `provider='github'`, 7 with a `credential_id`
  set.
- `integration_credentials` for `provider='github'`: 3 active rows, all
  `last_test_success = true`, most recently tested 2026-07-05.
- All three decrypt successfully today, using the app's own `decrypt_value`
  with the **current** `SETTINGS_ENCRYPTION_KEY` (run inside the
  `dev-health-api-1` container) — which also confirms that key is the one
  they were encrypted with. Two decrypt to a JSON object with keys
  `app_id`, `base_url`, `installation_id`, `org`, `private_key`, `token` —
  a real `token`, in the JSON-object shape `complete_route.go` requires,
  not a bare string (see "Provider credentials" below). The third is
  app-only: `app_id`, `installation_id`, `private_key`.

So credential availability and shape are not what stands between this stack
and a real ClickHouse `repos` row. What does, and was deliberately not
touched, is the two-key routing interlock, both halves of which were
considered and explicitly left alone:

1. **They are not independent, and the order matters.** Flipping the
   durable route (`worker_job_routes.transport` for `sync.provider_unit`,
   currently `celery`, would need `river_canary`) alone, with the Python
   producer switch (`WORKER_GITHUB_REPO_METADATA_ENABLED`) still off, is not
   a smaller, safer version of flipping both — it actively raises.
   `dispatch_sync_run` (`src/dev_health_ops/workers/sync_units.py`) consults
   `ProviderUnitRouteSwitches.is_route_ready(provider, dataset)` (matrix-only,
   unconditional) BEFORE consulting the switch. If the matrix says a pair is
   route-ready and the durable route already points at River, but the
   switch is off, that combination is treated as an explicit ownership
   fault — `dispatch_sync_run` raises
   `WorkerJobRouteError("sync provider canary capability is unavailable")`
   rather than degrading to Celery, by design ("never a reason to silently
   fall back to legacy Celery dispatch for a pair the matrix says is done").
   So the durable-route flip cannot be evaluated, or left in place, on its
   own — getting past it requires the Python switch too, immediately, for
   every org.
2. **The Python switch requires a live container recreate, not a
   reversible row.** Neither route switch is wired into `api`/`worker`/
   `beat`/`worker-heavy`/`worker-ingest` in the repo-root `compose.yml` at
   all (confirmed: `docker inspect` on the running containers, and a grep of
   the compose file, both show zero occurrences outside the `go-worker`
   service block). Setting it means recreating `dev-health-worker-1` — the
   real Celery worker actively processing real organizations' real sync
   traffic — with new environment. That is a materially different, and
   materially bigger, ask than one reversible `UPDATE` on a single
   `worker_job_routes` row, and it was not authorized. The user re-decided
   with the corrected facts above (credentials exist; the blocker is purely
   the routing interlock) and still chose to stop here — this is a
   deliberate scope decision, not a missing capability.

### Reachability: what completing the chain would actually take

With both interlock halves flipped, the chain **can** complete to a real
ClickHouse `repos` row using one of the three existing credentials above —
no GitHub PAT needs to be obtained or seeded. Whoever picks this up needs
only:

1. `UPDATE worker_job_routes SET transport='river_canary', generation=generation+1, updated_at=now() WHERE job_kind='sync.provider_unit'` — matches the already-checked-in policy in `contracts/jobs/v1/migration-state.json` (`"route": "river_canary"`, from CHAOS-3123), not a new state.
2. `WORKER_GITHUB_REPO_METADATA_ENABLED=true` on `dev-health-worker-1` (and anywhere else `dispatch_sync_run` runs), which requires recreating that container with the new environment — wire the variable into the repo-root `compose.yml`'s Python service blocks first, the same way it's already wired for `go-worker`.
3. `go-worker` running and ready (see the crash-loop section above for what that needs).

Revert both (1) and (2) back to `celery`/`false` afterward — this is a
canary capability being exercised on demand, not a standing cutover.

### Credential shape, restated for this report

See "Provider credentials the Go executor can decrypt" above for the full
detail. The two gotchas worth restating here because they are exactly what
blocked the isolated-project proof until found: the decrypted plaintext
must be a JSON object (`{"token": "..."}`), not a bare token string; and the
claim query resolves `credential_id` via `COALESCE(sync_runs.credential_id,
integrations.credential_id)`, so a credential row that exists but isn't
linked from `integrations.credential_id` is invisible to a claim even
though direct `(org_id, provider)` lookup would find it.
