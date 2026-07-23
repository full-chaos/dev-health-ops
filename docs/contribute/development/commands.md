---
page_id: con-commands
summary: Run the repository's Python, Go, container, documentation, migration, fixture, and validation commands from a source checkout.
content_type: task-guide
owner: engineering
source_of_truth:
  - Makefile
  - pyproject.toml
  - go.mod
  - compose.yml
  - current CI workflows
applicability: current
lifecycle: active
---

# Common development commands

Run commands from the `dev-health-ops` repository root. Start with the narrowest check that can answer your question, then run the aggregate families required for the changed contracts before opening a pull request.
{: .fc-page-lede }

## Install the Python project

```bash
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

Verify the CLI and import path:

```bash
dev-hops --help
python -c "import dev_health_ops; print(dev_health_ops.__file__)"
```

## Format, lint, and type-check Python

```bash
ruff format --check .
ruff check .
mypy
```

Apply formatting or safe lint fixes deliberately, then review the diff:

```bash
ruff format .
ruff check --fix .
git diff --check
```

## Run Python tests

```bash
make test:unit
make test:integration
make test:e2e
make test:live-e2e
make test:ci
```

Use `pytest` directly for a narrow development loop:

```bash
pytest tests/path/to/test_module.py -q
pytest tests/path/to/test_module.py::test_name -q
```

A narrow command does not replace the repository aggregate contract.

## Validate the Go worker foundation

The Go module contains coexistence foundations and versioned job contracts. Current Make targets wrap the checked-in validation scripts:

```bash
make go:fmt
make go:vet
make go:test
make go:race
make go:build
make go:contract
```

Run the faster local family while iterating:

```bash
make go:check-fast
```

Run the complete static Go family:

```bash
make go:check
```

Run integration and container checks when the change affects PostgreSQL, River compatibility, process composition, images, or deployment profiles:

```bash
make go:integration
make go:container-smoke
make go:container-reproducible
make go:container
make go:verify
```

`go:verify` includes the static, integration, and container families. Go tests must not mutate or normalize away the Celery baseline when comparing runtimes.

Useful direct commands include:

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/...
```

Use the Make/CI contract before review because it also validates checked-in job contracts, deployment profiles, and container behavior.

## Inspect job and route contracts

Versioned job envelopes and deployment profiles live under `contracts/jobs/v1/`; sync transport ownership lives under `contracts/sync-dispatch/v1/`.

Run the contract family after changing a job kind, version, route, handler, profile, or migration state:

```bash
make go:contract
```

Use the worker contract checker and operator CLI help as the exact command source for the current revision:

```bash
go run ./cmd/worker-contractcheck --help
go run ./cmd/dev-health-workerctl --help
```

Do not change a route from Celery to River without the job-specific shadow, parity, canary, and rollback evidence required by the migration contract.

## Start and inspect local services

Start core stores:

```bash
docker compose up -d postgres clickhouse valkey
```

Build and start the configured local stack:

```bash
docker compose up -d --build
```

Inspect state and logs:

```bash
docker compose ps
docker compose logs -f api migrate
docker compose logs -f clickhouse postgres valkey
```

Stop while preserving volumes:

```bash
docker compose down
```

Fresh local PostgreSQL volumes provision the development domain and River queue roles. Existing volumes may need the checked-in role-provisioning script before River migration tests.

## Run migrations

Python application and analytics migrations:

```bash
dev-hops migrate postgres
dev-hops migrate clickhouse
```

The Go coexistence foundation requires a distinct direct migration DSN and runtime roles. Use the current migration command and provisioning script documented in [Databases and storage](../../operate/configure/databases-and-storage.md). Do not run a production migration from a development shell.

## Generate fixtures

Review current options:

```bash
dev-hops fixtures generate --help
```

Generate a 30-day synthetic dataset with derived metrics:

```bash
dev-hops fixtures generate \
  --db "$CLICKHOUSE_URI" \
  --days 30 \
  --with-metrics
```

Use a disposable local store or explicitly approved fixture database.

## Build and preview the documentation

Install dependencies:

```bash
python -m pip install -r requirements-docs.txt
```

Fast authoring server:

```bash
make docs:serve
```

Cloudflare-shaped local preview:

```bash
make docs:preview
```

Full reader-critical validation:

```bash
make docs:check-v2
```

Production documentation deployment is intentionally separate:

```bash
make docs:deploy
```

`make docs:build` uses the canonical `mkdocs.yml` configuration. The Cloudflare targets add redirects, headers, preview-version upload, deployment, and rollback behavior around that same canonical build.

## Inspect CLI commands before changing state

```bash
dev-hops --help
dev-hops sync --help
dev-hops metrics --help
dev-hops migrate --help
```

Bare Python CLI commands run inline. Some provider or job paths are safer and better validated through the API and active Celery workers; check the current command `Requires:` output and [CLI reference](../../reference/cli/index.md).

## Before committing

```bash
git status --short
git diff --check
git diff
```

Confirm the diff contains no credentials, local database artifacts, unrelated generated files, benchmark captures, or accidental migration output. Then run the checks matching every affected layer and the same aggregate gate CI enforces.
