# Recent non-prototype documentation sync — 2026-07-23

## Scope

This review compares the documentation foundation commit `33a09d0544c2382e221802745e96a2a98476cef3` with current `main` after the incident-ingestion and Go-worker foundation work. It inventories changes under `docs/` and related checked-in contracts or deployment examples that materially affect the public `docs-prototype/` candidate.

The prototype remains organized by the approved public information architecture. New source material is merged into existing admin, operations, reference, integration, and contributor pages unless a genuinely new supported reader destination is required.

## Change groups

### PagerDuty and canonical incident ingestion

Source changes:

- `docs/user-guide/pagerduty-oauth-app-setup.md`
- `docs/architecture/pagerduty-contract.md`
- `docs/architecture/canonical-operational-model.md`
- `docs/architecture/dispatch-outbox.md`
- `docs/architecture/licensing.md`
- PagerDuty credential, service-discovery, OAuth, webhook-binding, revocation, and sync implementation

Public documentation impact:

- add a supported PagerDuty administrator setup workflow under the approved incident-response source page;
- explain organization-scoped OAuth, client-credentials fallback, exact callback path, read scopes, service discovery, repository mapping, preflight, initial backfill, disconnect, and rotation;
- update provider troubleshooting and credential lifecycle guidance;
- update environment and feature-availability reference;
- document the V3 webhook boundary only where it is useful to administrators, integrators, or operators;
- state that `canonical_incident_ingestion` is enabled by default after cutover but retains a global kill switch and optional organization override.

### Jira Service Management incidents

Source change:

- `docs/providers/jira-service-management.md`

Public documentation impact:

- do not publish a setup workflow or advertise JSM incidents as release-ready;
- record that the implementation is code/unit-contract ready but live tenant proof, merge readiness, and release readiness remain blocked;
- preserve the boundary that JSM incidents are not inferred from ordinary Jira issues, alerts, Opsgenie, labels, timestamps, or text similarity.

### Go worker and River coexistence foundation

Source changes:

- `docs/ops/workers.md`
- `docs/ops/database-connection-pooling.md`
- `docs/architecture/go-worker-runtime-trd.md`
- `docs/decisions/chaos-3034-river-compatibility.md`
- `contracts/jobs/v1/`
- `contracts/sync-dispatch/v1/`
- `deploy/go-workers/`
- Go commands and packages under `cmd/` and `internal/`

Public documentation impact:

- state unambiguously that Celery remains the production owner of all current jobs;
- describe the dormant Go worker, scheduler, reconciler, stream-runner, operator CLI, job contracts, and deployment profiles without implying production routing;
- document zero-minimum-replica coexistence and route ownership gates;
- add the direct-PostgreSQL queue-control DSN, PgBouncer-compatible domain DSN, and one-shot migration DSN/role separation;
- add Go health endpoints, readiness categories, bounded metrics, and payload-redacted operator controls;
- update repository map, architecture, storage, configuration, CLI, deployment, and operations pages.

### Deployment and operational examples

Source changes:

- `compose.yml`
- `deploy/docker-compose/`
- `deploy/docker-swarm/`
- `deploy/kubernetes/`
- `deploy/helm/dev-health/`
- `docker/go-worker.Dockerfile`
- `deploy/grafana/dashboards/go-workers.json`

Public documentation impact:

- keep current Python API/Celery deployment examples canonical;
- describe Go profiles as disabled coexistence examples, not replacement production topology;
- document migration ordering and dedicated runtime/migration database identities;
- point readers to checked-in Compose, Swarm, Kubernetes/Kustomize, Helm, and Go profile examples rather than duplicating every manifest.

### CLI, observability, configuration, and repository layout

Source changes:

- `docs/ops/cli-reference.md`
- `docs/ops/observability-tooling.md`
- `docs/configuration.md`
- `docs/architecture/repo-layout.md`
- `docs/contributing/platform-contract.md`

Public documentation impact:

- add worker operator and contract-check commands while retaining `dev-hops` as the Python CLI;
- add Go worker liveness, readiness, queue depth, oldest eligible age, execution saturation, and pool-saturation signals;
- add the new `cmd/`, `internal/`, `contracts/`, and `deploy/go-workers/` ownership boundaries;
- retain warnings about inline CLI execution where worker-backed paths are the supported operational route.

## Publication decisions

| Source material | Prototype action | Publication boundary |
| --- | --- | --- |
| PagerDuty OAuth setup | Add and navigate an administrator guide under incident-response sources | Current supported setup |
| PagerDuty backend/webhook contract | Summarize in admin, integration, operations, and reference pages | Exact backend details remain contributor/reference material |
| JSM provider contract | Add an availability warning only | Withheld as a setup workflow until live proof exists |
| Go worker TRD, PRD, plans, and evidence | Extract current runtime facts into operations and contributor docs | Planning history and benchmark evidence remain internal/source material |
| Worker and database-pooling guides | Substantially update canonical operations pages | Current runtime plus explicit coexistence state |
| Deployment manifests | Link and explain supported examples | Manifests remain source of exact topology |
| CLI and repository-layout updates | Update reference and contributor pages | Current commands and ownership boundaries |

## Updated prototype destinations

The source review is applied to:

- `/admin/data-sources/`
- `/admin/data-sources/incident-response/`
- `/admin/data-sources/credential-lifecycle/`
- `/admin/sync-and-coverage/status-and-freshness/`
- `/admin/troubleshooting/provider-connections/`
- `/integrate/webhooks/configure/`
- `/integrate/webhooks/verify-signatures/`
- `/integrate/webhooks/retries-and-replay/`
- `/operate/configure/environment-and-secrets/`
- `/operate/configure/databases-and-storage/`
- `/operate/configure/workers-and-schedules/`
- `/operate/run/workers-and-jobs/`
- `/operate/observe/health-checks/`
- `/operate/observe/metrics-and-traces/`
- `/operate/plan/capacity-and-sizing/`
- `/operate/runbooks/provider-authentication-failure/`
- `/operate/runbooks/worker-or-queue-failure/`
- `/operate/install/production/`
- `/reference/configuration/environment/`
- `/reference/configuration/feature-flags/`
- `/reference/cli/`
- `/contribute/start/repository-map/`
- `/contribute/architecture/platform/`
- `/contribute/architecture/data-and-storage/`
- `/contribute/architecture/contracts/`
- `/contribute/development/commands/`

## Explicitly withheld

- A public JSM incident setup page, because live tenant proof and release readiness are still blocked.
- A statement that Go or River owns production jobs, because all current routes remain Celery-owned and the checked-in Go profiles remain disabled.
- Internal migration plans, benchmark captures, implementation evidence, and issue-specific rollout history as public user guidance.
