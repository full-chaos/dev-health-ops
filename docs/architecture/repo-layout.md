# Repo layout

## dev-health-ops

All Python source code lives under `src/dev_health_ops/`.

### Module roots

```
src/dev_health_ops/
├── cli.py                 # CLI entry point (dev-hops)
├── api/                   # FastAPI REST + Strawberry GraphQL
│   ├── graphql/           # Schema, resolvers, loaders, SQL
│   ├── auth/              # Authentication endpoints
│   ├── admin/             # Admin endpoints
│   ├── billing/           # Billing endpoints
│   ├── services/          # Business logic
│   └── queries/           # Data query helpers
├── connectors/            # Provider sync (GitHub, GitLab, etc.)
├── processors/            # Data normalization
├── storage/               # Database abstraction (ClickHouse, PostgreSQL)
├── metrics/               # Compute + sinks (daily, complexity, DORA, capacity)
│   ├── loaders/           # Metric data loaders
│   └── sinks/             # Persistence (ClickHouse primary)
├── models/                # SQLAlchemy ORM models
├── providers/             # Work-item providers (Jira, GitHub, GitLab, Linear)
├── config/                # YAML mappings (status, teams, identity, etc.)
├── work_graph/            # Investment/work graph compute
├── llm/                   # LLM categorization (OpenAI, Anthropic, Qwen, Gemini)
├── fixtures/              # Synthetic data generation
├── analytics/             # Scheduled analytics jobs
├── workers/               # Celery background workers
├── audit/                 # Data quality audits
├── credentials/           # Credential management
├── licensing/             # License management
├── core/                  # Core utilities
├── alembic/               # PostgreSQL migrations (Alembic)
└── migrations/clickhouse/ # ClickHouse DDL migrations
```

### Other top-level directories

- `tests/` — pytest test suite (120+ files)
- `docs/` — MkDocs documentation
- `docker/` — Dockerfile and init scripts
- `ci/` — CI scripts (test runner, governance gate)
- `deploy/` — Deployment configuration
- `scripts/` — Build and utility scripts

## dev-health-web

- `src/app` — Next.js pages and routes
- `src/lib/graphql` — urql client, hooks, generated types
- `src/components` — Charts and UI components
