# Repo layout

## dev-health-ops

Python application source lives under `src/dev_health_ops/`. The additive Go
worker-runtime foundation lives under `cmd/` and `internal/`; shared,
language-neutral job contracts live under `contracts/jobs/`.

### Runtime ownership

- Python continues to own FastAPI/GraphQL, providers, processors, domain
  behavior, and all currently routed Celery jobs.
- Go process foundations provide configuration, lifecycle, health, storage,
  versioned-contract support, a one-shot River migrator, and separate bounded
  domain and direct queue-control PostgreSQL pools.
- No job changes runtime because a Go command exists. Migration and routing are
  explicit per-job work with parity and rollback gates.
- River DDL runs only from the one-shot migration job when its dedicated,
  elevated DSN is configured. Long-running processes do not auto-migrate, and
  the disabled Go profiles do not change the current Celery production routes.

### Python module roots

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

### Go and cross-language roots

```text
cmd/
├── dev-health-worker-migrate/ # One-shot pinned River schema migrator
├── dev-health-worker/        # Worker-profile process shell
├── dev-health-scheduler/     # Scheduler process shell
├── dev-health-reconciler/    # Durable-repair process shell
├── dev-health-stream-runner/ # Redis Streams process shell
├── dev-health-workerctl/     # Authenticated, audited River operator CLI
└── worker-contractcheck/     # Versioned-contract validation CLI
internal/
├── platform/                 # Config, secrets, logging, lifecycle, health, version
├── storage/                  # Bounded PostgreSQL, ClickHouse, and Valkey factories
├── jobcontract/              # Go job-envelope and compatibility types
├── joboutbox/                # Transactional Python-to-River relay
├── joboperator/              # Payload-redacted operator policy and storage
├── syncdispatchcontract/     # Cross-language sync-outbox route policy
├── syncreconciler/           # Read-only bounded sync-outbox observer
└── testsupport/containers/   # Isolated pinned dependency harness
contracts/jobs/v1/            # Schemas, registry, examples, and migration state
contracts/sync-dispatch/v1/   # Sync-outbox delivery and transport routes
```

These roots are migration foundations. The command shells and route-contract
loaders do not imply a production job handler, River route, or canary
approval. Detailed runtime topology and migration gates live in the
[Go worker runtime TRD](go-worker-runtime-trd.md) and
[migration PRD](../product/go-worker-migration-prd.md).

### Other top-level directories

- `tests/` — pytest test suite (120+ files)
- `docs/` — MkDocs documentation
- `docker/` — Dockerfile and init scripts
- `ci/` — CI scripts (test runner, governance gate)
- `deploy/` — Deployment configuration
- `scripts/` — Build and utility scripts
- `contracts/` — Versioned cross-language contracts and sanitized fixtures

## dev-health-web

- `src/app` — Next.js pages and routes
- `src/lib/graphql` — urql client, hooks, generated types
- `src/components` — Charts and UI components
