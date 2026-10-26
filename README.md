# dev-health-ops

Open source analytics for developer health and team operating modes.

`dev-health-ops` is the backend of Dev Health. It ingests engineering activity
from Git providers and work trackers, stores it as persisted evidence, computes
inspectable metrics, and serves them through GraphQL and REST APIs to
[`dev-health-web`](https://github.com/full-chaos/dev-health-web) and other
consumers.

## Contents

- [Principles](#principles)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Common tasks](#common-tasks)
- [Project layout](#project-layout)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [Documentation](#documentation)

## Principles

- **Accessibility over extraction**: derive insight from data teams already own.
- **Learning, not judgment**: show operating signals, not individual rankings.
- **Trends over absolutes**: emphasize change over time and distributions.
- **Inspectable by default**: metrics trace back to schemas, queries, and evidence.

Non-goals: individual leaderboards or performance scores, HR or
performance-management workflows, and dashboards that hide definitions,
provenance, or missing data.

## Architecture

```text
Providers → Sync → Sinks → Metrics → API (REST + GraphQL) → dev-health-web
```

The runtime is Go. Every process is a subcommand of the `dho` binary
(`cmd/dho`) or a small service under `cmd/`.

| Component | Command | Role |
| --- | --- | --- |
| API | `dho api` | REST plane |
| Query API | `dho query-api` | GraphQL (`/graphql`) and query REST paths |
| Worker | `dho worker` | Runs [River](https://riverqueue.com) jobs: provider sync, metrics, reports |
| Scheduler | `dho scheduler` | Enqueues periodic jobs |
| Reconciler | `dho reconciler` | Reconciles sync state |
| Auth service | `cmd/auth-service` | Authentication |

Storage:

| Layer | Backend | Variable | Holds |
| --- | --- | --- | --- |
| Semantic | PostgreSQL | `POSTGRES_URI` | Users, organizations, settings, credentials, job queue |
| Analytics | ClickHouse | `CLICKHOUSE_URI` | Commits, PRs/MRs, work items, metrics, graph data |
| Coordination | Valkey (Redis protocol) | `REDIS_URL` | Rate limits, budgets, streams |

ClickHouse is the only supported analytics backend. Analytics rows are written
through sinks only, never through file exports or debug dumps.

Notes:

- The GraphQL schema pin is `contracts/graphql/v1/schema.graphql`. Web CI uses
  it to detect drift. REST endpoints stay next to GraphQL.
- Webhook routes exist for GitHub (`/api/v1/webhooks/github`) and PagerDuty
  only. GitLab and Jira have no webhook route and return 404.
- The Python Celery workers and the Python API are retired. The only Python
  left in the tree is docs build tooling under `scripts/`.
- LLM categorization is not part of this repository's Go code (ops #4526).
  See the [categorization contract](.github/docs-legacy/llm/categorization-contract.md).

Deep dive: [data and storage](docs/contribute/architecture/data-and-storage.md),
[repository map](docs/contribute/start/repository-map.md).

## Quick start

Prerequisites: Go (version in [`go.mod`](go.mod)), Docker with Compose.

```bash
# 1. Build the CLI
go build -o dho ./cmd/dho && ./dho help

# 2. Start the local stack (Postgres, ClickHouse, Valkey, migrations, workers, API)
docker compose up -d --build

# 3. Check readiness
curl --fail http://localhost:8000/ready
```

A router on <http://localhost:8000> sends `/graphql` and the query REST paths
to `query-api`, and every other path to `go-api`. Postgres listens on host
port 5555, ClickHouse on 8123 (HTTP) and 9000 (native), Valkey on 6379.

Local data: use `dho sync git --provider local --repo-path <path>` for a local
repository, or generate synthetic analytics rows (frozen parameter sets only,
`--seed` required):

```bash
dho fixtures generate --sink "$CLICKHOUSE_URI" --provider synthetic \
  --repo-name acme/live-e2e --repo-count 1 --days 14 --commits-per-day 6 \
  --pr-count 24 --team-count 10 --seed 20260219 --with-metrics --with-work-graph
```

Create the first user and organization with `dho admin users create` and
`dho admin orgs create`. Full walkthrough:
[get started](docs/get-started/index.md),
[evaluate locally](docs/operate/install/local-evaluation.md).

## Configuration

Set values through the environment, or `_FILE` variants where supported. Never
commit values. [`.env.example`](.env.example) lists the common names;
[`docs/reference/configuration/environment.md`](docs/reference/configuration/environment.md)
is the full reference.

| Variable | Purpose |
| --- | --- |
| `POSTGRES_URI` | Semantic database |
| `CLICKHOUSE_URI` | Analytics database |
| `REDIS_URL` | Valkey/Redis |
| `WORKER_DATABASE_URI` | Direct queue-control connection for Go workers (session mode) |
| `COORDINATOR_DATABASE_URI` | Direct connection for the River coordinator |
| `MIGRATION_DATABASE_URI` | Elevated connection for one-shot migrations only |
| `RIVER_DATABASE_SCHEMA` | River schema name |
| `GITHUB_TOKEN`, `GITLAB_TOKEN`, `LINEAR_API_KEY` | Provider credentials for CLI use |
| `JIRA_BASE_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN` | Jira credentials |
| `ATLASSIAN_JIRA_BASE_URL`, `ATLASSIAN_EMAIL`, `ATLASSIAN_API_TOKEN`, `ATLASSIAN_CLOUD_ID` | Atlassian credentials |
| `SOCIAL_GITHUB_CLIENT_ID`, `SOCIAL_GITLAB_CLIENT_ID`, `SOCIAL_GOOGLE_CLIENT_ID` (and `_CLIENT_SECRET`) | Social sign-in |
| `EMAIL_PROVIDER`, `EMAIL_FROM_ADDRESS`, `EMAIL_API_KEY`, `SMTP_HOST`, `SMTP_PORT` | Outbound email |
| `CORS_ALLOWED_ORIGINS` | Browser origins allowed by the API |
| `SYNC_BUDGET_*`, `SYNC_UNIT_*` | Provider sync budgets and leases |
| `LOG_LEVEL` | Log verbosity |

Worker processes are configured by flags first; run `dho worker --help`.
See [workers and jobs](docs/operate/run/workers-and-jobs.md).

## Common tasks

```bash
# Build the CLI
go build -o dho ./cmd/dho

# Format and vet
gofmt -l . && go vet ./...

# Tests
make go:check-fast        # fast Go checks (ci/check_go.sh fast)
make go:check             # full Go checks
make test:live-e2e        # live backend e2e (ci/run_tests.sh live-e2e)

# Migrations (required on a fresh environment)
dho migrate postgres upgrade
dho migrate clickhouse upgrade     # or: dho migrate upgrade

# Sync and backfill
dho sync git --provider local --repo-path <path>
dho backfill run --config-id <config-uuid> --backfill 30

# Metrics (operator dispatch; the scheduler runs these daily)
dho workers metrics daily-start --org <org-uuid> --day <YYYY-MM-DD> \
  --reason <code> --correlation-id <id>

# Docs
make docs:check-fast
```

A bare `dho` verb runs inline in your shell. Prefer triggering the equivalent
job so workers supply tokens. The full pre-push gate is `bash ci/local_validate.sh`
(see [`AGENTS.md`](AGENTS.md)). More commands:
[development commands](docs/contribute/development/commands.md),
[CLI reference](docs/reference/cli/index.md),
[migrations](docs/contribute/development/migrations.md).

## Project layout

| Path | Contents |
| --- | --- |
| `cmd/` | Binaries: `dho` (CLI and services), `auth-service`, `auth-migrate`, `cigate`, and tooling |
| `internal/` | Go packages: providers and sync, metrics, jobs, API, query API, scheduler, auth, storage |
| `contracts/` | Versioned contracts (GraphQL schema, jobs, auth) |
| `src/dev_health_ops/config/` | YAML config (identity, status, complexity mappings) |
| `compose.yml` | Local Docker Compose stack |
| `deploy/` | Helm chart, Go worker deployment manifest, Grafana dashboards |
| `ci/` | CI and local validation scripts |
| `docs/` | MkDocs documentation site |
| `tests/`, `scripts/`, `tools/` | Fixtures and compatibility tests, helper scripts, tooling |

## Deployment

Production runs on Kubernetes. Charts and environment configuration live in the
separate `dev-health-deploy` repository. The Go images (`dev-health-go-*`) are
published to `ghcr.io/full-chaos`. This repository holds the Compose stack for
local development and deployment manifests under [`deploy/`](deploy/).
See [`deploy/go-workers/README.md`](deploy/go-workers/README.md) and
[operate](docs/operate/index.md).

## Contributing

Read [`AGENTS.md`](AGENTS.md) first. In short:

1. Branch from `origin/main`, without upstream tracking:
   `git switch -c <branch> --no-track origin/main`.
2. Run `make install` once to set up Lefthook hooks.
3. Make the change. Update docs in the same PR when behavior changes.
4. Run `make go:check-fast` before you push.
5. Push with an explicit refspec: `git push -u origin HEAD:<branch>`.
6. Open a PR. Fill in the template, including `TEST-EVIDENCE` and `RISK-NOTES`.
7. Commits must be signed. Do not add agent attribution to commit messages.
8. Merges to `main` go through the GitHub merge queue.

See [pull requests](docs/contribute/review-and-release/pull-requests.md).

## Documentation

- [Get started](docs/get-started/index.md)
- [Use](docs/use/index.md): Investment View, reports
- [Operate](docs/operate/index.md): install, configure, run, observe
- [Integrate](docs/integrate/index.md): APIs, webhooks, Customer Push
- [Reference](docs/reference/index.md): CLI, GraphQL, configuration, data models
- [Contribute](docs/contribute/index.md): architecture, development, review

## License

See [`LICENSE.md`](LICENSE.md).
