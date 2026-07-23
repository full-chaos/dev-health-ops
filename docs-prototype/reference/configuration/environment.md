---
page_id: ref-config-env
summary: Current environment-variable families, process ownership, secret-file forms, and source-of-truth rules.
content_type: configuration-reference
owner: platform-operations
source_of_truth:
  - current settings modules
  - .env.example
  - deploy/ manifests and compose files
  - docs/ops/database-connection-pooling.md
  - docs/user-guide/pagerduty-oauth-app-setup.md
applicability: current
lifecycle: active
---

# Environment variables

The exact key, type, default, and validation behavior comes from the current runtime settings and checked-in deployment artifacts. This page identifies the supported families and the boundaries that must remain visible in generated reference output.
{: .fc-page-lede }

## Core application

Include environment mode, public application URLs, authentication and signing, session behavior, encryption keys, trusted proxies, GraphQL limits, logging, and feature-decision settings.

## Data stores

Document these as distinct responsibilities:

| Family | Representative settings | Boundary |
| --- | --- | --- |
| Semantic PostgreSQL | `POSTGRES_URI`, pool settings, `PGBOUNCER_TRANSACTION_MODE` | API, Celery, and Go domain state; transaction-mode pooler is supported where configured |
| River queue control | `WORKER_DATABASE_URI`, `WORKER_DATABASE_MODE`, `WORKER_DATABASE_MAX_CONNS` | Direct PostgreSQL for Go queue state; transaction mode is rejected |
| Worker domain pool | `WORKER_DOMAIN_DATABASE_MAX_CONNS` | Bounds the Go domain-state pool using `POSTGRES_URI` |
| One-shot migrations | `MIGRATION_DATABASE_URI`, role-name settings | Direct elevated connection; never injected into long-running workers |
| ClickHouse | `CLICKHOUSE_URI` and connection/query settings | Provider facts, analytics, and materializations |
| Valkey/Redis | `REDIS_URL`, `CELERY_BROKER_URL`, `CELERY_RESULT_BACKEND` | Celery, budgets, streams, and distributed coordination |

Inline values and `_FILE` forms are mutually exclusive where both are supported. Do not generate secret contents.

## Provider and integration credentials

Include provider host/account identity, authentication mode, callback URL, scopes or selected datasets, and secret ownership for GitHub, GitLab, Jira, Linear, PagerDuty, Customer Push, and webhooks.

PagerDuty runtime configuration includes:

```text
PAGER_DUTY_CLIENT_ID
PAGER_DUTY_SECRET
PAGER_DUTY_REDIRECT_URI
SETTINGS_ENCRYPTION_KEY
```

The API and sync workers need the same app client values. The redirect URI is the Dev Health Web browser callback, not an API callback. Webhook signing secrets are persisted through the binding lifecycle rather than treated as payload configuration.

## Worker and schedule settings

Generate current entries for:

- Celery broker, result backend, queue lists, concurrency, and shutdown grace;
- provider-specific and cost-class routing;
- leases, stale detection, retries, and backoff;
- synchronization windows, watermarks, provider budgets, and deferrals;
- scheduler ownership;
- Go job registry, migration state, deployment profiles, health, operator token, River schema, retention, and pool limits.

A setting that enables a route is not sufficient evidence that the corresponding worker owns production work. The checked-in route, handler coverage, profile, and migration state must agree.

## External services and telemetry

Include model providers, email, billing, Sentry-compatible error delivery, OpenTelemetry endpoints, Prometheus settings, service names, sampling, and external integration configuration.

## Reference rules

For every generated entry, include:

- key and supported aliases;
- type and validation;
- required, optional, or conditional state;
- default, if the runtime defines one;
- owning processes;
- secret classification and `_FILE` support;
- restart or reload behavior;
- feature, plan, or provider prerequisites;
- deprecation or replacement.

A deployment example is not authority for a default when runtime source disagrees. Never include secret values, DSNs with credentials, tokens, private keys, OAuth callback query parameters, or webhook payloads in generated output.
