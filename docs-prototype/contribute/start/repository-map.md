---
page_id: con-repos
summary: Route a change to the repository, package, command, contract, or deployment artifact that owns its public behavior.
content_type: concept
owner: engineering
source_of_truth:
  - docs/architecture/repo-layout.md
  - current repository roots and AGENTS.md
applicability: current
lifecycle: active
---

# Choose the correct repository and package

Dev Health spans product UI, Python services, source providers, analytics, background execution, Go worker foundations, deployment artifacts, and documentation. Start from the public behavior being changed, then follow its owning contract inward.
{: .fc-page-lede }

## `dev-health-web`

Owns Next.js routes, product navigation, UI state, charts, GraphQL client behavior, accessibility, browser interactions, OAuth browser callbacks, and current visual truth.

Use it for changes to:

- page routes and layout;
- controls, labels, and states;
- charts and product interpretation surfaces;
- browser-side OAuth callback handling;
- help links and documentation entry points.

## `dev-health-ops`: Python platform

Python code under `src/dev_health_ops/` owns:

- FastAPI and GraphQL;
- authentication, authorization, licensing, and credentials;
- providers, discovery, pagination, normalization, and sync planning;
- Customer Push and webhook ingestion;
- PostgreSQL and ClickHouse models, queries, migrations, and sinks;
- metrics, Work Graph, Investment, reports, and fixtures;
- Celery workers, Beat schedules, retries, and current production job routes;
- the `dev-hops` CLI.

New provider work belongs under `src/dev_health_ops/providers/<provider>/`. Legacy code under `connectors/` is not the destination for new provider implementations except compatibility shims.

## `dev-health-ops`: Go worker foundations

Go process entry points live under `cmd/`:

- `dev-health-worker`;
- `dev-health-scheduler`;
- `dev-health-reconciler`;
- `dev-health-stream-runner`;
- `dev-health-workerctl`;
- `worker-contractcheck`.

Shared implementation lives under `internal/`, including configuration, lifecycle, health, logging, secrets, database factories, River, job contracts, operator controls, outbox, scheduler, reconciler, and test support.

These packages are coexistence foundations. They do not own current production jobs unless the checked-in route and migration state say so.

## Versioned contracts

- `contracts/jobs/v1/` owns job envelopes, schemas, registry, capabilities, deployment profiles, and migration state.
- `contracts/sync-dispatch/v1/` owns frozen sync transport routes.
- `deploy/go-workers/profiles.json` owns the disabled Go topology and connection budget.

A job or route change that updates code without the matching contract is incomplete.

## Deployment

- `compose.yml` — local integrated development/evaluation.
- `deploy/docker-compose/` — production Compose example.
- `deploy/docker-swarm/` — Swarm example and migration ordering.
- `deploy/kubernetes/` — Kustomize resources and migration Job.
- `deploy/helm/dev-health/` — Helm chart, values, and schema.
- `deploy/go-workers/` — disabled coexistence profiles and runtime shape.
- `docker/` — Python and Go image definitions and database initialization.
- `deploy/grafana/dashboards/` — maintained dashboard examples.

## Documentation boundary

The public candidate lives under `docs-prototype/`. The non-prototype `docs/` tree remains an important source of current product, architecture, provider, deployment, and runbook facts during migration. Useful verified material should be reshaped into one canonical prototype page, not copied into multiple public destinations.

Planning records, benchmark captures, issue-specific evidence, and internal rollout notes remain source material unless they describe a durable supported contract.

Read the root and nearest `AGENTS.md` before editing. Repository guidance controls the local workflow but is not automatically public documentation.
