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
