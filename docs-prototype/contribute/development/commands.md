---
page_id: con-commands
summary: Run the repository's install, lint, type, test, documentation, migration, fixture, and Compose commands from a source checkout.
content_type: task-guide
owner: engineering
source_of_truth:
  - Makefile
  - pyproject.toml
  - compose.yml
  - current CI workflows
applicability: current
lifecycle: active
---

# Common development commands

Run commands from the `dev-health-ops` repository root with the project virtual environment active. Start with the narrowest check that can answer your question, then run the aggregate families required for the change before opening a pull request.
{: .fc-page-lede }

## Install the project

```bash
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

Verify the CLI and import path:

```bash
dev-hops --help
python -c "import dev_health_ops; print(dev_health_ops.__file__)"
```

## Format and lint Python

Check formatting without changing files:

```bash
ruff format --check .
```

Format files:

```bash
ruff format .
```

Run the configured lint rules:

```bash
ruff check .
```

Apply safe automatic lint fixes, then review the diff:

```bash
ruff check --fix .
git diff --check
```

## Run static type checking

The repository's `pyproject.toml` defines the Mypy scope and overrides:

```bash
mypy
```

When working on a bounded subsystem, you can pass its paths first, but run the repository configuration before requesting review when the change affects shared contracts.

## Run tests

The Make targets call the repository's `ci/run_tests.sh` contract:

```bash
make test:unit
make test:integration
make test:e2e
make test:live-e2e
```

Run the complete CI-oriented test family when the change spans multiple layers:

```bash
make test:ci
```

Use `pytest` directly for a narrow test during development. Keep the path or node ID explicit:

```bash
pytest tests/path/to/test_module.py -q
pytest tests/path/to/test_module.py::test_name -q
```

A direct `pytest` command is a development shortcut; it does not replace the repository aggregate contract.

## Start and inspect local services

Start only the core stores:

```bash
docker compose up -d postgres clickhouse valkey
```

Build and start the full configured stack:

```bash
docker compose up -d --build
```

Inspect service state and logs:

```bash
docker compose ps
docker compose logs -f api migrate
docker compose logs -f clickhouse postgres valkey
```

Stop the stack while preserving data volumes:

```bash
docker compose down
```

## Run migrations

```bash
dev-hops migrate postgres
dev-hops migrate clickhouse
```

Run migrations before sync or analytics work when a branch changes storage contracts. Do not run a production migration from a development shell.

## Generate fixtures

Review the current options:

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

Use a disposable local store or an explicitly approved fixture database.

## Build and validate the v2 documentation

Install the documentation dependencies:

```bash
python -m pip install -r requirements-docs.txt
```

Run the authoring server:

```bash
python -m mkdocs serve \
  --strict \
  --config-file mkdocs.prototype.yml \
  --dev-addr 127.0.0.1:8000
```

Run the reader-critical gate:

```bash
mkdir -p .build
python scripts/validate_docs_v2_publication.py
python -m mkdocs build --strict --config-file mkdocs.prototype.yml
python scripts/check_built_site_links.py --site-dir .build/docs-prototype
python scripts/check_docs_candidate_search.py \
  --site-dir .build/docs-prototype \
  --queries .github/documentation-program/phase-10/search-acceptance.json
python scripts/check_docs_candidate_accessibility.py \
  --site-dir .build/docs-prototype \
  --css docs-prototype/stylesheets/extra.css
python scripts/check_docs_candidate_facts.py
```

The older `make docs:build` target still builds the legacy documentation tree. Use the v2 configuration above until the legacy tree is retired and the Make targets are promoted.

## Inspect a CLI command before changing state

Use the command's current help as the exact option source:

```bash
dev-hops --help
dev-hops sync --help
dev-hops metrics --help
dev-hops migrate --help
```

Do not copy a state-changing command from an old issue or runbook without checking the current CLI and target environment.

## Before committing

```bash
git status --short
git diff --check
git diff
```

Confirm that the diff contains no credentials, local database artifacts, unrelated generated files, or accidental migration output. Then run the checks that match the affected layers and the same aggregate gate CI will enforce.
