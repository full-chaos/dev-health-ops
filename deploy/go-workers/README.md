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
