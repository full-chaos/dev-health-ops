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

The `dev-health-ops` repository contains the Python API, CLI, connectors, workers, analytics pipeline, migrations, and operational documentation. Local development normally uses PostgreSQL for semantic application data, ClickHouse for analytics data, and Valkey for queues and caching.
{: .fc-page-lede }

Use this guide for a source checkout. Production configuration, secrets, sizing, and hardening belong under [Install and operate](../../operate/index.md).

## Prerequisites

- Python 3.12 or newer. The project currently declares support for Python 3.12, 3.13, and 3.14.
- Docker with Compose.
- Git.
- A supported Node.js release only when the change touches JavaScript tooling, Wrangler, or a related frontend repository.

Read the root `AGENTS.md` and the nearest directory-level contributor guidance before editing a subsystem.

## Create the Python environment

```bash
git clone https://github.com/full-chaos/dev-health-ops.git
cd dev-health-ops

python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

On PowerShell, activate the environment with:

```powershell
.venv\Scripts\Activate.ps1
```

The editable install provides the `dev-hops` and `dev-health-ops` CLI entry points while keeping imports connected to the source tree.

Verify the installation:

```bash
dev-hops --help
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

Apply both storage migration families before running synchronization or analytics work:

```bash
dev-hops migrate postgres
dev-hops migrate clickhouse
```

The Compose stack also defines a one-shot `migrate` service. Use the CLI commands above when you need to see and control each migration explicitly.

## Run the API stack

For a full Compose-managed development stack, build and start the services from the repository configuration:

```bash
docker compose up -d --build
```

The API is exposed on `http://127.0.0.1:8000`. Verify readiness:

```bash
curl --fail http://127.0.0.1:8000/ready
```

The GraphQL endpoint is available at `/graphql` when the API is running. Use the service logs to diagnose startup:

```bash
docker compose logs -f api migrate
```

Stop the stack without deleting data volumes:

```bash
docker compose down
```

Add `--volumes` only when you intentionally want to destroy the local PostgreSQL and ClickHouse data.

## Generate synthetic data

The fixture generator can create teams, git facts, work items, and derived metrics for a bounded local dataset:

```bash
dev-hops fixtures generate \
  --db "$CLICKHOUSE_URI" \
  --days 30 \
  --with-metrics
```

Use synthetic data for UI and analytical development when customer or production data is unnecessary. Review `dev-hops fixtures generate --help` before increasing volume or changing the target store.

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
  --dev-addr 127.0.0.1:8000
```

Open `http://127.0.0.1:8000`. Before submitting documentation changes, use the strict build and checks described in [Preview and validate documentation](../documentation/preview-and-validate.md).

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
| Editable import points outside the checkout | Another environment or installed package is active | Inspect `which python`, `which dev-hops`, and `dev_health_ops.__file__` |
