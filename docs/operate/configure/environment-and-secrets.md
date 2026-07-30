---
page_id: op-env
summary: Supply API, provider, worker, database, migration, queue, signing, and telemetry configuration with explicit process ownership and rotation behavior.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - current settings modules
  - .env.example
  - deploy/ manifests and environment templates
  - docs/admin/data-sources/incident-response.md
  - docs/operate/configure/databases-and-storage.md
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

## Ask Dev model credentials

Ask Dev reuses the existing source-bound LLM credential bundles: workspace BYO
key and base URL stay together, and platform key and base URL stay together.
Treat the source label separately from the provider family label. A connection
may be `platform` or `byo` regardless of whether the endpoint family is
OpenAI-compatible, local, Ollama, LM Studio, or another supported server.

Never combine a platform key with a workspace endpoint or a workspace key with a
platform endpoint. OpenAI-compatible BYO base URLs pass the existing
public-HTTPS and SSRF validation before they can be certified.

Enabling Ask Dev is not sufficient to admit model traffic. Admission also
requires a current readiness result for the exact provider/model fingerprint.
The platform connection may use an OpenAI-compatible endpoint, including a local
one, but Ask Dev only admits that connection after readiness succeeds for that
exact fingerprint. Global LLM disable, the Ask Dev emergency disable, and
provider/model deny rules must remain available during rollout.

Supported platform OpenAI-compatible bundles include:

```dotenv
# Generic/local OpenAI-compatible server (for example LM Studio or vLLM)
LLM_PROVIDER=local
LOCAL_LLM_BASE_URL=http://host.docker.internal:1234/v1
LOCAL_LLM_MODEL=your-model-id

# LM Studio provider aliases
LLM_PROVIDER=lmstudio
LMSTUDIO_BASE_URL=http://host.docker.internal:1234/v1
LMSTUDIO_MODEL=your-model-id

# Local Ollama or authenticated Ollama Cloud-compatible endpoint
LLM_PROVIDER=ollama
OLLAMA_BASE_URL=http://host.docker.internal:11434/v1
OLLAMA_MODEL=your-model-id
OLLAMA_API_KEY=optional-cloud-key
```

`OLLAMA_API_KEY` is the provider-specific platform alias; `LLM_API_KEY` remains
supported. Omit the key for an unauthenticated local host. These environment
bundles are platform configuration, never organization BYO settings.

Workspace-to-platform fallback defaults to platform after a configured org BYO
is evaluated; an explicit organization fail_closed choice opts out of that
fallback. Source-tagged accounting keeps platform-managed usage and BYO usage
separate.

Organization policy is stored through the dedicated Ask Dev administrator API,
not environment variables or the generic settings endpoint. A malformed stored
emergency-disable value fails closed. Global feature disable and explicit
entitlement denial continue to take precedence over every organization setting.

The API owns two non-secret maxima for platform-managed Ask Dev usage:

| Setting | Default | Hard range | Purpose |
| --- | ---: | ---: | --- |
| `ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX` | `1000` | `100`–`5000` | Maximum accepted platform runs per organization and UTC calendar month |
| `ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD` | `100000000` ($100) | `10000000`–`500000000` ($10–$500) | Maximum estimated platform provider cost per organization and UTC calendar month |

Organization administrators may lower their provisioned values through the Ask
Dev controls, but cannot raise them above these operator maxima. Raising an
operator maximum above the default does not silently raise an unconfigured
organization's default. Limits reset at 00:00 UTC on the first day of each
month and do not roll over.

Readiness failures expose only these stable classes to users and durable state:
disabled, provider not configured, model not supported, provider unavailable,
invalid response, timeout, or cancelled. Inspect provider logs through the
approved secret-redacting path; do not copy raw provider bodies into settings or
support records.

The readiness provider-call timeout is 30 seconds, matching the Ask Dev runtime
contract. The synthetic exchange accepts either an OpenAI-native `tool_calls`
decision or the normalized JSON decision envelope, then verifies tool-result
continuation and a strict final response. A timeout, authentication failure,
temporarily unavailable endpoint, and invalid agent response remain distinct
safe administrative outcomes.
