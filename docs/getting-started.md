# Getting started

## Install

Use the published package:

```bash
pip install dev-health-ops
dev-hops --help
```

For source development:

```bash
pip install -r requirements.txt
```

## Docs site

```bash
pip install -r requirements-docs.txt
mkdocs serve
```

## Start the local data services

Dev Health uses PostgreSQL for semantic/application data and ClickHouse for
engineering evidence and analytics. Valkey backs queues and runtime caches;
PgBouncer is the transaction-mode PostgreSQL pooler used by the API.

```bash
docker compose up -d postgres clickhouse valkey pgbouncer

export POSTGRES_URI="postgresql+asyncpg://postgres:postgres@localhost:5555/postgres"
export CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default"

dev-hops migrate postgres
dev-hops migrate clickhouse
```

| Database | Purpose | Environment variable |
| --- | --- | --- |
| PostgreSQL | Users, organizations, settings, credentials | `POSTGRES_URI` |
| ClickHouse | Commits, PRs/MRs, work items, metrics, graph data | `CLICKHOUSE_URI` |

See [Database Architecture](architecture/database-architecture.md) for the full
storage boundary.

## Common development workflows

### Sync a local repository

```bash
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops sync git --provider local --repo-path /path/to/repo
```

### Sync work items from GitHub

```bash
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops sync work-items \
  --provider github \
  --auth "$GITHUB_TOKEN" \
  -s "org/*"
```

### Compute daily metrics

```bash
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops metrics daily
```

### Run the API

```bash
POSTGRES_URI="postgresql+asyncpg://postgres:postgres@localhost:5555/postgres" \
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default" \
  dev-hops api --reload
```

OpenAPI is available at `http://localhost:8000/docs`; GraphQL is served at
`/graphql` for `dev-health-web`.

## Test Context Fabric locally

Context Fabric/ACR is a separate private service and is not part of the default
Ops Compose project. With sibling `dev-health-{ops,acr,web}` checkouts, start the
isolated TLS fixture from this repository:

```bash
bash scripts/context-fabric-local.sh
```

The launcher builds this Ops checkout, provisions a temporary organization and
`agent_context_runtime` entitlement, seeds deterministic evidence, verifies the
ACR API and MCP sidecar, and keeps the service alive for OpenCode, Claude Code,
Codex, or Cursor testing. It prints a `client.env` path; source that path in the
shell that launches the client.

See [Test Context Fabric locally](context-fabric-local.md) for path overrides,
client setup, security boundaries, and cleanup behavior.

## Environment notes

CLI flags override environment variables.

| Variable | Status | Purpose |
| --- | --- | --- |
| `POSTGRES_URI` | Required for semantic/application operations | Users, organizations, settings, credentials |
| `CLICKHOUSE_URI` | Required for analytics operations | Sync, evidence, metrics, and work graph data |
| `DATABASE_URI` / `DATABASE_URL` | Deprecated fallback | Legacy resolver paths; keep only for compatibility |
| `GITHUB_TOKEN`, `GITLAB_TOKEN`, `JIRA_*`, `ATLASSIAN_*`, `LINEAR_API_KEY` | Optional | Provider authentication |
| `EMAIL_PROVIDER`, `EMAIL_API_KEY`, `EMAIL_FROM_ADDRESS` | Optional (`console` default) | Email delivery via [Resend](./email-setup.md) |
| `APP_BASE_URL`, `JWT_SECRET_KEY`, `SETTINGS_ENCRYPTION_KEY` | Optional in development, required in production | API callback/auth/encryption settings |
| `AUTH_AUTO_CREATE_ORG_ON_REGISTER` | Temporary compatibility flag (`true` by default) | Set `false` for identity-first onboarding through `/api/v1/auth/onboard` |
| `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET` | Required for billing | Stripe API and webhook verification |
| `STRIPE_PRICE_ID_TEAM`, `STRIPE_PRICE_ID_ENTERPRISE` | Required for billing | Stripe Price IDs for checkout |
| `TRIAL_DAYS` | Optional (`14` by default) | Team-tier free trial duration |

## Billing and free-trial setup

To enable Stripe billing locally:

1. Get Stripe test keys from the Stripe test dashboard.
2. Create Team and Enterprise products/prices.
3. Add the values to `.env`:

```bash
STRIPE_SECRET_KEY="sk_test_..."
STRIPE_PRICE_ID_TEAM="price_..."
STRIPE_PRICE_ID_ENTERPRISE="price_..."
APP_BASE_URL="http://localhost:3000"
TRIAL_DAYS=14
```

4. Forward the required events:

```bash
stripe listen --forward-to http://127.0.0.1:8000/api/v1/billing/webhooks/stripe \
  --events checkout.session.completed,customer.subscription.created,customer.subscription.updated,customer.subscription.deleted,customer.subscription.trial_will_end,invoice.paid,invoice.payment_failed
```

5. Set the webhook secret returned by the Stripe CLI:

```bash
STRIPE_WEBHOOK_SECRET="whsec_..."
```

6. Restart the relevant services.

See [Stripe Billing Runbook](ops/stripe-billing-runbook.md) for trial behavior,
abuse prevention, and email configuration.

## Demo data

Generate a complete synthetic dataset with teams, git facts, work items, derived
metrics, and work graph data:

```bash
dev-hops fixtures generate \
  --sink "clickhouse://ch:ch@localhost:8123/default" \
  --days 30 \
  --with-metrics \
  --with-work-graph
```

The fixture generator inserts synthetic teams and writes the same derived
families used by daily metrics, including complexity, hotspot, investment, and
AI workflow evidence.

## Performance tuning

Fixture generation uses batched inserts with concurrency for large datasets.

- `BATCH_SIZE` (default `1000`) controls batch size.
- `MAX_WORKERS` (default `4`) controls concurrent workers for non-SQL backends.
