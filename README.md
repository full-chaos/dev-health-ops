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

Python remains the owner of the API, GraphQL schema, provider fetch and
normalization, processors, and current Celery job implementations. The
repository also contains an additive Go worker-runtime foundation under `cmd/`
and `internal/`; adding those process shells does not move a job out of Python
or change its routing.

The primary visualization surface is now `dev-health-web`. Grafana is optional,
and this repository no longer ships the old sample dashboard gallery in this
README.

## Install

Use the package directly:

```bash
pip install dev-health-ops
dev-hops --help
```

For local development from this repository:

```bash
pip install -r requirements.txt
```

The installed command is `dev-hops`.

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

export POSTGRES_URI="postgresql+asyncpg://postgres:postgres@localhost:5555/postgres"
export CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default"

dev-hops migrate postgres
dev-hops migrate clickhouse
```

See [`docs/contribute/architecture/data-and-storage.md`](docs/contribute/architecture/data-and-storage.md)
and [`docs/reference/cli/index.md`](docs/reference/cli/index.md) for details.

## Common workflows

### Sync source data

```bash
# Local git repository
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops sync git --provider local --repo-path /path/to/repo

# GitHub repository
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops sync git --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner <owner> \
  --repo <repo>

# Pull requests
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops sync prs --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner <owner> \
  --repo <repo>

# Work items from Jira, GitHub, GitLab, Linear, synthetic data, or all providers
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops sync work-items --provider all --backfill 30

# Teams into the ClickHouse team catalog (ClickHouse is the system of record)
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops sync teams --provider config --path src/dev_health_ops/config/team_mapping.yaml --allow-empty
```

The bundled `team_mapping.yaml` is an empty onboarding sample. `sync teams`
exits non-zero when no teams are persisted; pass `--allow-empty` only for
intentional empty/no-op syncs such as validating the sample config.

Provider authentication can come from CLI flags or environment variables such as
`GITHUB_TOKEN`, `GITLAB_TOKEN`, `JIRA_*`, `ATLASSIAN_*`, and `LINEAR_API_KEY`.

### Compute metrics

```bash
# Daily analytics rollups
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops metrics daily --backfill 30

# Complexity and hotspot snapshots for a repository
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops metrics complexity --repo-path /path/to/repo --backfill 30
```

### Generate demo data

```bash
dev-hops fixtures generate \
  --sink "clickhouse://ch:ch@localhost:8123/default" \
  --days 30 \
  --with-metrics \
  --with-work-graph
```

### Run the API

```bash
POSTGRES_URI="postgresql+asyncpg://postgres:postgres@localhost:5555/postgres" \
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops api --reload
```

OpenAPI docs are available at <http://localhost:8000/docs> when the API is
running. GraphQL is served by the API for the web app.

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

The repository builds two reusable images from `docker/Dockerfile`:

| Image | Purpose |
| --- | --- |
| `dev-hops-api` | Runs `dev-hops api` on port 8000 |
| `dev-hops-runner` | Uses `dev-hops` as the entrypoint for sync, fixtures, metrics, and maintenance jobs |

Build both images:

```bash
IMAGE_REGISTRY=ghcr.io/myorg/dev-health-ops \
VERSION=$(git describe --tags --abbrev=0 2>/dev/null || echo latest) \
  ./scripts/build-images.sh
```

Run the API image:

```bash
docker run --rm -p 8000:8000 \
  -e POSTGRES_URI="postgresql+asyncpg://postgres:postgres@postgres:5432/postgres" \
  -e CLICKHOUSE_URI="clickhouse://ch:ch@clickhouse:8123/default" \
  dev-hops-api:latest
```

Run a CLI job through the runner image:

```bash
docker run --rm -it \
  --network dev-health_default \
  -v "$(pwd)":/app \
  -w /app \
  -e CLICKHOUSE_URI="clickhouse://ch:ch@clickhouse:8123/default" \
  dev-hops-runner:latest \
  metrics daily --backfill 14
```

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
