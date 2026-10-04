---
page_id: con-setup
summary: Install the Dev Health toolchain, start PostgreSQL, ClickHouse, and Valkey, run migrations, and verify the local API or documentation site.
content_type: tutorial
owner: engineering
source_of_truth:
  - pyproject.toml
  - requirements files
  - compose.yml
  - docs/getting-started.md
  - current CI workflows
applicability: current
lifecycle: active
---

# Set up a development environment

The `dev-health-ops` repository contains the Go api, query plane, workers and `dho` CLI, the Python providers, processors, metrics and sinks, migrations, and operational documentation. Local development normally uses PostgreSQL for semantic application data, ClickHouse for analytics data, and Valkey for queues and caching.
{: .fc-page-lede }

Use this guide for a source checkout. Production configuration, secrets, sizing, and hardening belong under [Install and operate](../../operate/index.md).

## Prerequisites

- Python 3.14. The project declares `requires-python = ">=3.14"` and containers ship 3.14 (`docker/Dockerfile`); older interpreters are not tested (CHAOS-3419).
- Docker with Compose.
- Git.
- A supported Node.js release only when the change touches JavaScript tooling, Wrangler, or a related frontend repository.

Read the root `AGENTS.md` and the nearest directory-level contributor guidance before editing a subsystem.

## Create the Python environment

```bash
git clone https://github.com/full-chaos/dev-health-ops.git
cd dev-health-ops

python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

On PowerShell, activate the environment with:

```powershell
.venv\Scripts\Activate.ps1
```

The editable install keeps imports connected to the source tree. The `dho` CLI is a Go binary: build it with `go build -o dho ./cmd/dho` (a Go toolchain is required).

Verify the installation:

```bash
./dho help
python -c "import dev_health_ops; print(dev_health_ops.__file__)"
```

## Start the core data services

Start the local PostgreSQL, ClickHouse, and Valkey services defined by `compose.yml`:

```bash
docker compose up -d postgres clickhouse valkey
```

The default development endpoints are:

| Service | Local endpoint | Purpose |
| --- | --- | --- |
| PostgreSQL | `localhost:5555` | Users, organizations, settings, credentials, and semantic application data |
| ClickHouse HTTP | `localhost:8123` | Synchronized facts, work items, metrics, and analytics queries |
| Valkey | `localhost:6379` | Queues, caching, and worker coordination |

Check service health:

```bash
docker compose ps
```

PostgreSQL should report healthy after `pg_isready`, ClickHouse after its `/ping` check, and Valkey after `valkey-cli ping`.

## Configure local connection values

For CLI and direct local processes, set development-only values:

```bash
export POSTGRES_URI='postgresql+asyncpg://postgres:postgres@localhost:5555/postgres'
export CLICKHOUSE_URI='clickhouse://ch:ch@localhost:8123/default'
export REDIS_URL='redis://localhost:6379/1'
```

Do not reuse production credentials or export a broad provider token into a shell history. Use a local `.env` or your normal secret manager for additional development-only values.

## Run database migrations

Apply both storage migration families before running synchronization or analytics work. `dho` needs `DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1` and `RIVER_DATABASE_SCHEMA=river` for the PostgreSQL steps, and it speaks the ClickHouse native protocol (port 9000):

```bash
export DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1 RIVER_DATABASE_SCHEMA=river
export CLICKHOUSE_URI='clickhouse://ch:ch@localhost:9000/default'
./dho migrate postgres upgrade
./dho migrate clickhouse upgrade
```

The Compose stack also defines a one-shot `migrate` service that runs `dho migrate upgrade`. Use the CLI commands above when you need to see and control each migration explicitly.

## Run the API stack

For a full Compose-managed development stack, build and start the services from the repository configuration:

```bash
docker compose up -d --build
```

The API is exposed on `http://127.0.0.1:8000`. That port is the `router` service (nginx): it sends `/graphql` and the query REST paths to `query-api` and every other path to `go-api`, as the production Ingress does. Verify readiness:

```bash
curl --fail http://127.0.0.1:8000/ready
```

The GraphQL endpoint is available at `/graphql` when the stack is running. Use the service logs to diagnose startup:

```bash
docker compose logs -f router go-api query-api migrate
```

### Create the first admin

A stack on empty volumes has no user. Create the first user and its organization with the admin verbs of the `dho` binary. Run them in the `migrate` service: its image is `dho` and it has the database login these verbs use. The password is read from standard input (the first line of a file here), never from an argument:

```bash
docker compose run --rm --no-deps -T migrate admin users create --email admin@example.com --full-name "Admin User" --password-stdin < password.txt
docker compose run --rm --no-deps -T migrate admin orgs create --name "My Organization" --owner-email admin@example.com
```

The first command prints `Created user: ...`, the second `Created organization: ...` and its owner. After the two commands `POST /api/v1/auth/login` on port 8000 answers 200 for that email and password. Remove the password file afterwards.

The [CLI reference](../../reference/cli/index.md#admin-commands) describes these two verbs, `dho admin users create` and `dho admin orgs create`, and their options.

Stop the stack without deleting data volumes:

```bash
docker compose down
```

Add `--volumes` only when you intentionally want to destroy the local PostgreSQL and ClickHouse data.

## Generate synthetic data

The fixture loader can write teams, git facts, work items, and derived metrics for a bounded local dataset:

```bash
./dho fixtures generate \
  --sink "$CLICKHOUSE_URI" \
  --provider synthetic --repo-name acme/live-e2e --repo-count 1 \
  --days 14 --commits-per-day 6 --pr-count 24 --team-count 10 \
  --seed 20260219 \
  --with-metrics \
  --with-work-graph
```

`dho fixtures generate` loads a frozen parameter set (a set that was not frozen is refused), needs `--seed`, and writes analytics rows only: it creates no PostgreSQL users, so create the first user and organization as shown above. Use synthetic data for UI and analytical development when customer or production data is unnecessary. Review `dho fixtures generate --help` before changing the target store.

## Build the documentation locally

Install the documentation dependencies into the active environment:

```bash
python -m pip install -r requirements-docs.txt
```

Run the v2 documentation with live reload:

```bash
python -m mkdocs serve \
  --strict \
  --config-file mkdocs.yml \
  --dev-addr 127.0.0.1:8001
```

Open `http://127.0.0.1:8001`. Port 8000 is the Compose `router` port, so the documentation server uses 8001. Before submitting documentation changes, use the strict build and checks described in [Preview and validate documentation](../documentation/preview-and-validate.md).

## Verify a bounded code change

Run the narrowest relevant check first, then the aggregate families that CI will execute. Common commands are listed in [Common development commands](../development/commands.md).

Before committing, confirm:

```bash
git status --short
```

Review generated files, migration output, fixtures, and local configuration deliberately. Do not leave credentials, database dumps, or unrelated generated artifacts in the change.

## Common setup failures

| Symptom | Likely cause | Check |
| --- | --- | --- |
| PostgreSQL connection refused on `5555` | Container is not healthy or the port is in use | `docker compose ps postgres` and `docker compose logs postgres` |
| ClickHouse `/ping` fails | ClickHouse is still starting or the local volume is unhealthy | `docker compose logs clickhouse` |
| API starts but queries fail | Migrations were not applied or connection values point to a different store | Run both migration commands and print the active URIs without credentials |
| Worker or queue tasks do not run | Valkey is unavailable or the expected worker service is not running | Check `valkey` and worker logs in Compose |
| Editable import points outside the checkout | Another environment or installed package is active | Inspect `which python` and `dev_health_ops.__file__` |

## Working in parallel with other agents

The Compose stack described above is shared by every agent on the machine, so
migrations, rebuilds and database writes contend. To get a private full stack
instead — your own Postgres, ClickHouse, FalkorDB, workers and API in a
dedicated Kubernetes namespace, seeded from `backups/` — see
[Lane isolation on a kiac cluster](../development/lane-isolation-kiac.md). That
page also lists which standing Compose-stack rules stop applying once your lane
is cluster-isolated.
