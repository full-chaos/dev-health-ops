# Architecture

The project follows a pipeline-style architecture that separates data collection, processing, storage, and analysis.

## Pipeline stages

1. **Connectors** (`connectors/`)
   - Fetch raw data from providers (GitHub, GitLab, Jira).
2. **Processors** (`processors/`)
   - Normalize and enrich connector payloads.
3. **Storage** (`storage.py`, `models/`)
   - Persist processed data into PostgreSQL, ClickHouse, MongoDB, or SQLite.
4. **Metrics** (`metrics/`)
   - Compute high-level metrics like throughput, cycle time, rework, and predictability.
5. **Visualization** (`grafana/`)
   - Provision dashboards for exploration and reporting.

## Storage backends

- PostgreSQL for relational storage with Alembic migrations.
- ClickHouse for analytics-heavy queries.
- MongoDB for document storage.
- SQLite for local development.

## CLI entry points

The CLI is implemented with argparse in `cli.py` and orchestrates sync and metrics workflows.

## Work unit investment payload

The Work Unit Investment API payloads include optional `work_unit_type` and `work_unit_name`
fields for UI labels. These fields are intended to be exposed through GraphQL later unchanged.

## Canonical investment view

Investment categorization is computed at job time and persisted as distributions; UX-time systems may explain but must not recompute.

- Concepts: `product/concepts.md`
- Categorization contract: `llm/categorization-contract.md`
- Investment View: `user-guide/investment-view.md`
