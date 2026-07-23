---
page_id: op-env
summary: Supply API, provider, worker, database, migration, queue, signing, and telemetry configuration with explicit process ownership and rotation behavior.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - current settings modules
  - .env.example
  - deploy/ manifests and environment templates
  - docs/user-guide/pagerduty-oauth-app-setup.md
  - docs/ops/database-connection-pooling.md
applicability: current
lifecycle: active
---

# Environment and secrets

Dev Health configuration is process-specific. An API, Celery worker, dormant Go worker, migration job, scheduler, and operator command may require different subsets of the same deployment settings. Do not copy one large environment block into every process: assign each process only the ordinary configuration and secrets it owns.
{: .fc-page-lede }

## Separate configuration from secrets

Ordinary configuration includes feature switches, concurrency, queue names, timeouts, limits, hosts without credentials, and telemetry settings. Secrets include database passwords, provider tokens, OAuth client secrets, signing keys, encryption keys, webhook secrets, billing keys, and service-operator tokens.

Store secret values in the approved secret manager and inject them at runtime. Where a setting supports a `_FILE` form, use either the inline value or the file form—not both. Never place production values in repository files, container images, screenshots, support tickets, or documentation examples.

## Assign settings to the correct process

| Process | Typical owned configuration |
| --- | --- |
| API | Public host, authentication, encryption, provider app configuration, PostgreSQL and ClickHouse access, trusted proxies, GraphQL limits |
| Celery workers | Provider credentials needed for sync, queue/broker settings, ClickHouse/PostgreSQL access, model credentials, worker concurrency and routing |
| Celery scheduler | Broker access and schedule configuration; exactly one active scheduler unless the deployment contract says otherwise |
| Go worker foundations | Domain PostgreSQL DSN, direct River queue-control DSN, job registry/profile settings, health and telemetry configuration |
| One-shot migration job | Direct elevated migration DSN and runtime role names; never long-running worker credentials only |
| Worker operator CLI | Payload-redacted operator token plus the domain and queue-control database roles required for the requested read or mutation |

## Database and worker DSNs

The current Go coexistence foundation deliberately separates three PostgreSQL responsibilities:

| Responsibility | Setting | Endpoint |
| --- | --- | --- |
| Domain/semantic state | `POSTGRES_URI` | Transaction-mode PgBouncer is supported |
| River queue control | `WORKER_DATABASE_URI` | Direct PostgreSQL; transaction mode is rejected |
| One-shot application and River migrations | `MIGRATION_DATABASE_URI` | Direct PostgreSQL with the dedicated migration role |

Do not collapse these into one connection string. Long-running workers must not receive `MIGRATION_DATABASE_URI` and never apply migrations. The usernames in the runtime DSNs must match the declared domain and queue role names; mismatches fail closed.

## PagerDuty app configuration

The API and every worker that can synchronize PagerDuty need the same OAuth app values:

```dotenv
PAGER_DUTY_CLIENT_ID="<client-id>"
PAGER_DUTY_SECRET="<client-secret>"
PAGER_DUTY_REDIRECT_URI="https://YOUR_HOST/org/admin/integrations/pagerduty/callback"
SETTINGS_ENCRYPTION_KEY="<stable-encryption-key>"
```

The callback URI is a browser route on Dev Health Web. Do not expose the client secret to the browser. Keep `SETTINGS_ENCRYPTION_KEY` stable anywhere encrypted provider credentials are read; changing it without a coordinated credential migration or reconnect can break token refresh.

## Queue and routing settings

Routing configuration must match deployed consumers. Enabling provider-specific or cost-class queues before workers consume them can strand jobs. Likewise, checked-in Go deployment profiles remain disabled with zero minimum replicas and Celery route ownership; configuration alone does not transfer a job to River.

Review together:

- broker and result backend URLs;
- provider and cost-class routing switches;
- worker concurrency and heavy-worker capacity;
- lease, stale detection, retry, and backoff settings;
- sync budget limits and deferral windows;
- job-contract and deployment-profile versions;
- scheduler ownership.

## Rotation and restart behavior

For every secret, record:

- owning process and secret manager location;
- whether rotation requires API, worker, scheduler, or migration-job restart;
- whether the provider grant or webhook binding must be recreated;
- how to verify the replacement with a bounded request or synchronization;
- how to revoke the old authority after recovery is confirmed.

A configuration rollout is complete only after all required processes use the same intended revision and the relevant health, permission, and bounded-work checks pass.
