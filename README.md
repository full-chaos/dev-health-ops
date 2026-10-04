# dev-health-ops

Open source analytics for developer health and team operating modes.

Dev Health Ops ingests engineering activity from Git providers, work trackers,
deployments, incidents, and local repositories; normalizes it into persisted
evidence; computes inspectable metrics; and serves those metrics to the
GraphQL/API layer used by `dev-health-web`.

## Why this exists

Developer health tooling often drifts into expensive, opaque scorecards that are
easy to misuse. This project is intentionally different:

- **Accessibility over extraction**: derive insight from data teams already own.
- **Learning, not judgment**: show operating signals, not individual rankings.
- **Trends over absolutes**: emphasize change over time and distributions.
- **Inspectable by default**: metrics trace back to schemas, queries, and evidence.

Non-goals:

- Individual leaderboards or performance scores
- HR/performance-management workflows
- Dashboards that hide definitions, provenance, or missing data

## Current architecture

Dev Health Ops follows a strict pipeline boundary:

```text
Providers → Processors → Sinks → Metrics → API / Visualization
```

- **Providers** fetch raw provider data from GitHub, GitLab, Jira, Linear,
  local Git, CI/CD, deployments, incidents, and synthetic/demo sources.
- **Processors** normalize provider records into internal models.
- **Sinks** persist computed outputs. Analytics persistence is ClickHouse-only.
- **Metrics jobs** compute daily rollups, DORA, complexity, risk, investment,
  AI workflow, and work graph outputs from persisted data.
- **API/GraphQL** serves persisted analytics to `dev-health-web` and other
  consumers.

Go owns the API and the GraphQL schema: `dho api` serves REST, `dho query-api`
serves GraphQL, and the schema pin is `contracts/graphql/v1/schema.graphql`.
The Go runtime lives under `cmd/` and `internal/`. Python remains the owner of
provider fetch and normalization, processors, and the job implementations the
Go fleet runs.

The primary visualization surface is now `dev-health-web`. Grafana is optional,
and this repository no longer ships the old sample dashboard gallery in this
README.

## Install

The command-line interface is `dho`, a Go binary. Build it from this
repository, or use the `dev-health-go-dho` container image:

```bash
go build -o dho ./cmd/dho
./dho help
```

For Python development from this repository:

```bash
pip install -r requirements.txt
```

## Database model

Dev Health Ops uses two databases with different responsibilities:

| Layer | Backend | Environment variable | Purpose |
| --- | --- | --- | --- |
| Semantic | PostgreSQL | `POSTGRES_URI` | Users, organizations, settings, credentials |
| Analytics | ClickHouse | `CLICKHOUSE_URI` | Commits, PRs/MRs, work items, metrics, graph data |

ClickHouse is required for analytics features. MongoDB, SQLite, and PostgreSQL
analytics sinks have been removed or deprecated; SQLite remains only for narrow
test/local fixture paths.

Start local services and run migrations:

```bash
docker compose up -d postgres clickhouse valkey

export POSTGRES_URI="postgresql://postgres:postgres@localhost:5555/postgres"
export CLICKHOUSE_URI="clickhouse://ch:ch@localhost:9000/default"
export DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1 RIVER_DATABASE_SCHEMA=river

dho migrate postgres upgrade
dho migrate clickhouse upgrade
```

See [`docs/contribute/architecture/data-and-storage.md`](docs/contribute/architecture/data-and-storage.md)
and [`docs/reference/cli/index.md`](docs/reference/cli/index.md) for details.

## Common workflows

### Sync source data

```bash
# Local git repository
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:9000/default" \
  dho sync git --provider local --repo-path /path/to/repo

# GitHub repository
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:9000/default" \
  dho sync git --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner <owner> \
  --repo <repo>

# Pull requests
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:9000/default" \
  dho sync prs --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner <owner> \
  --repo <repo>

# Work items: the native Go provider-sync route syncs them, with no manual
# per-provider command. Force a window for one sync configuration:
dho backfill run --config-id <config-uuid> --backfill 30

# Teams into the ClickHouse team catalog (ClickHouse is the system of record)
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:9000/default" \
  dho sync teams --provider github --org <org-uuid> --owner <github-org> --auth "$GITHUB_TOKEN"
```

`sync teams` takes `--provider jira|github|gitlab|linear`. It exits non-zero
when no teams are persisted; pass `--allow-empty` only for an intentional empty
sync.

Provider authentication can come from CLI flags or environment variables such as
`GITHUB_TOKEN`, `GITLAB_TOKEN`, `JIRA_*`, `ATLASSIAN_*`, and `LINEAR_API_KEY`.

### Compute metrics

The Go scheduler computes metrics every day. To dispatch a run by hand, use
the operator CLI in a worker pod (or with the worker's database settings):

```bash
# Daily analytics rollups for one organization and day range
dho workers metrics daily-start --org <org-uuid> --day 2026-08-01 --to 2026-08-30 --reason <code> --correlation-id <id>

# Complexity (and the other remaining families) for one day
dho workers metrics remaining trigger-backstop --family complexity --org <org-uuid> \
  --day 2026-08-30 --review-evidence "manual run: <why>" \
  --reason <code> --correlation-id <id>
```

### Generate demo data

`dho fixtures generate` loads a frozen parameter set, needs `--seed`, and
writes analytics rows only (no PostgreSQL users). Create the first user and
organization with `dho admin users create` and `dho admin orgs create`.

```bash
dho fixtures generate \
  --sink "clickhouse://ch:ch@localhost:9000/default" \
  --provider synthetic --repo-name acme/live-e2e --repo-count 1 \
  --days 14 --commits-per-day 6 --pr-count 24 --team-count 10 \
  --seed 20260219 \
  --with-metrics \
  --with-work-graph
```

### Run the API

The Compose stack runs the Go api. A router on <http://localhost:8000> sends
`/graphql` and the query REST paths to `query-api` and every other path to
`go-api`:

```bash
docker compose up -d --build
curl --fail http://localhost:8000/ready
```

To run the api binary yourself, use `dho api` (`--api-addr` sets the listen
address, default `:8000`; `dho api --help` lists the flags). The Go api serves
no `/docs` page.

### Run workers

**Celery is retired (CHAOS-4026, 2026-08-21): zero Python celery services run
in prod since the 2026-08-19 stop.** Go owns every periodic maintenance
cadence. `dev-hops workers start-worker`/`start-scheduler` were deleted
along with it — see [`docs/operate/run/workers-and-jobs.md`](docs/operate/run/workers-and-jobs.md)
for how to run the Go worker/scheduler/reconciler/stream-runner processes.

Go River processes use `POSTGRES_URI` for domain state and the separate,
least-privilege `WORKER_DATABASE_URI` for direct queue control. The latter is a
Go runtime setting, not a replacement Python database alias. River schema is
applied only by the one-shot migration path when `MIGRATION_DATABASE_URI` and
the two runtime role names are supplied. See [Workers](docs/operate/run/workers-and-jobs.md) for
the coexistence boundary.

## Test tiers

Canonical local test commands:

```bash
make test:unit
make test:integration
make test:e2e
make test:live-e2e
make test:ci
```

All tiers route through one entrypoint:

```bash
./ci/run_tests.sh <unit|integration|e2e|live-e2e|ci>
```

Notes:

- `integration` is token-aware and skips provider tests cleanly when credentials
  are unavailable.
- `live-e2e` starts a live backend harness, generates deterministic ClickHouse
  fixtures, waits for API readiness, and asserts `/health`, `/api/v1/meta`, and
  `/api/v1/home`.
- `ci` blocks on `flake8` and coverage-gated unit tests. `black`, `isort`, and
  `mypy` are advisory by default; set `STRICT_QUALITY_GATES=1` to make them
  blocking.
- JUnit XML paths are stable under `test-results/junit/` and can be overridden
  with `TEST_RESULTS_DIR` / `JUNIT_XML_*` variables.

## Container images

CI no longer builds or publishes the Python API image (`dev-hops-api`, from `docker/Dockerfile`; CHAOS-7674). The root compose stack does not run it (CHAOS-8361): a router on host port 8000 sends each request to the Go api or the Go query-api. The last published image stays in the registry for the bigboy test host until that stack moves to Go; the Python API source is removed. The Go images (`dev-health-go-*`) are the published images; run CLI jobs with `dho`.

## Key docs

- [`docs/get-started/index.md`](docs/get-started/index.md): setup and demo data
- [`docs/reference/cli/index.md`](docs/reference/cli/index.md): full CLI reference
- [`docs/contribute/architecture/data-and-storage.md`](docs/contribute/architecture/data-and-storage.md): PostgreSQL/ClickHouse split and provider → processor → sink boundaries
- [`docs/use/investment/index.md`](docs/use/investment/index.md): canonical Investment View
- [`docs/use/reports/index.md`](docs/use/reports/index.md): Report Center and scheduled reports

## Guardrails

- WorkUnits are evidence containers, not categories.
- Investment categorization runs at compute time and persists distributions.
- Theme rollups are deterministic from canonical subcategories.
- UX-time LLM usage is explanation-only and must not recompute categories.
- Analytics persistence goes through ClickHouse sinks, not file exports or debug
  dumps.
