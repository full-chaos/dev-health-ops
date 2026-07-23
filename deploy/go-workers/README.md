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

The checked-in Compose, Swarm, Kubernetes, and Helm stacks still render the
Celery topology; none currently renders the Go `processes` entries. Static
deployment-contract tests bind the shared PgBouncer budget and one-shot
migration wiring to those real manifests until Go workload renderers land.

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
`dev-health-go-worker` (latency, sync, heavy, ops),
`dev-health-go-reconciler`, `dev-health-go-scheduler`, and
`dev-health-go-stream-runner` (external, ingest). All workload definitions
run as UID/GID `65532`, deny privilege escalation, use a read-only root
filesystem, and expose only the operator HTTP surface on port 8080:
`/healthz`, `/readyz`, and `/metrics`.

The separately deployable `sync-provider` topology starts the checked-in
`sync` runtime profile and consumes the isolated `sync.provider` River queue
when the provider-unit contract/handler release is present. It must never be
combined with the coordinator's `sync` queue: the two clients have disjoint
handlers. Both queues and all provider routes remain Celery-owned unless a
reviewed route release says otherwise.

### Coexistence canary

1. Run the one-shot migration job with the direct migration DSN and wait for a
   clean completion. A Go workload receives `POSTGRES_URI` and
   `WORKER_DATABASE_URI`, never `MIGRATION_DATABASE_URI`.
2. Verify the deployed immutable image contains the matching
   `profiles.json` and `contracts/jobs/v1/registry.json`; then deploy the
   zero-replica coexistence topology:

   ```bash
   docker compose -f deploy/docker-compose/compose.production.yml \
     -f deploy/docker-compose/compose.go-workers.yml --profile go-workers up -d
   helm upgrade --install dev-health deploy/helm/dev-health \
     -f deploy/helm/dev-health/values-go-workers-coexistence.yaml
   kubectl -n dev-health apply -f deploy/kubernetes/go-workers.yaml
   docker stack deploy -c deploy/docker-swarm/stack.yml \
     -c deploy/docker-swarm/stack.go-workers.yml dev-health
   ```

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
provider sync (`sync.provider`) contract support where applicable, and a
successful coexistence canary. The current checked-in contract fails these
conditions, so Go-only is not production-authorized.
