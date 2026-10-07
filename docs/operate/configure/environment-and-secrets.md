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

Dev Health configuration is process-specific. An API, Go worker, migration job, scheduler, and operator command may require different subsets of the same deployment settings. Do not copy one large environment block into every process: assign each process only the ordinary configuration and secrets it owns.
{: .fc-page-lede }

## Separate configuration from secrets

Ordinary configuration includes feature switches, concurrency, queue names, timeouts, limits, hosts without credentials, and telemetry settings. Secrets include database passwords, provider tokens, OAuth client secrets, signing keys, encryption keys, webhook secrets, billing keys, and service-operator tokens.

Store secret values in the approved secret manager and inject them at runtime. Where a setting supports a `_FILE` form, use either the inline value or the file form, not both. Never place production values in repository files, container images, screenshots, support tickets, or documentation examples.

## Assign settings to the correct process

| Process | Typical owned configuration |
| --- | --- |
| API | Public host, authentication, encryption, provider app configuration, PostgreSQL and ClickHouse access, trusted proxies, GraphQL limits |
| Go workers | Domain PostgreSQL DSN, direct River queue-control DSN, provider credentials needed for sync, ClickHouse access, model credentials, job registry/profile settings, worker concurrency and routing, health and telemetry configuration |
| Scheduler | Queue-control access and schedule configuration; exactly one active scheduler unless the deployment contract says otherwise |
| One-shot migration job | Direct elevated migration DSN and runtime role names; never long-running worker credentials only |
| Worker operator CLI | Payload-redacted operator token plus the domain and queue-control database roles required for the requested read or mutation |

## Database and worker DSNs

The Go worker fleet deliberately separates three PostgreSQL responsibilities:

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

Routing configuration must match deployed consumers. Enabling provider-specific or cost-class queues before workers consume them can strand jobs. Checked-in Go deployment groups remain disabled with zero minimum replicas even though the sync-dispatch contract targets River; deploy the consumers and apply the durable routes before sending work.

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

### TypeSafe decision backend

The `typesafe` kind is a decision backend (TypeSafe System One, model family
`jev`), not a text completer. It is never selected by `LLM_PROVIDER`, never by
auto-detection, and never serves a user-visible categorization. Only code that
builds the TypeSafe client reads these names:

| Variable | Meaning | Default |
| --- | --- | --- |
| `TYPESAFE_API_KEY` | Bearer token. Secret; read through the named-secret mechanism, so it is redacted in logs and errors. A key shorter than 12 bytes is refused. | none (client is not built) |
| `TYPESAFE_BASE_URL` | Must be `https://api.typesafe.ai`. Any other value is refused, so the key cannot be sent to another host. | `https://api.typesafe.ai` |
| `TYPESAFE_MODEL` | A versioned id such as `jev-1.13.0`. The moving aliases `jev-latest` and `jev-preview` are refused. | `jev-1.13.0` |

The generic `LLM_API_KEY`, `LLM_BASE_URL` and `LLM_MODEL` overrides do not apply
to this client. Give `TYPESAFE_API_KEY` only to the worker group that runs
`investment.materialize`.

How to give it to that group only:

- **Helm:** every worker group loads the shared ConfigMap and the shared
  Secret, so a key placed there reaches all groups. Set the key, and the
  `INVESTMENT_SHADOW_*` or `INVESTMENT_SERVED_DECISION_ORG_IDS` switch, in `goWorkers.groups[].extraEnv` of the `heavy`
  group, with the key as a `secretKeyRef` to a separate Secret that holds only
  that key. Nothing is rendered when `extraEnv` is not set.
- **Docker Compose:** the root `compose.yml` and the bigboy worker overlay
  (`ci/bigboy/compose.bigboy.workers.yml`) pass `TYPESAFE_*` and
  `INVESTMENT_SHADOW_*` and `INVESTMENT_SERVED_DECISION_ORG_IDS` from the shell or `--env-file` of the compose command to
  `go-worker-heavy` only. Do not put them in `ops/.env`: every worker reads it.
  Unset values are empty, which means off.

### Investment categorization served by the decision backend

`INVESTMENT_SERVED_DECISION_ORG_IDS` selects, for each organization, which
backend writes the investment categorization that users see. It is **off by
default**: with the variable empty no TypeSafe client is built for the served
path and the configured LLM provider categorizes every organization.

| Variable | Meaning | Default |
| --- | --- | --- |
| `INVESTMENT_SERVED_DECISION_ORG_IDS` | Comma-separated organization ids whose categorization is written by the TypeSafe decision backend; `*` means every organization. | empty (off) |

For an organization on the list:

- Each work unit with enough text gets one request to the decision backend.
  There is no repair request and no second provider.
- The rows carry a `categorization_model_version` that starts with
  `provider=typesafe;api=systemone;`, so they are told apart from the rows of
  an LLM provider. Each work unit is categorized again one time after the
  switch, because the version is new.
- A work unit for which the backend finds no clear category gets its most
  probable category with a low `evidence_quality` (0.3 or less) and no evidence
  quote. Such a unit is asked again by the next run.
- A work unit whose request fails for a reason that can pass (a timeout, a
  rate limit, a server error) gets no new row in that run: its last row stays,
  and the next run asks again. A work unit that never had a row has none until
  a later run answers it.
- A rejected key or an unknown model ends the run with an error, like the
  same failure of an LLM provider.
- The run's usage is recorded under provider `typesafe` and the configured
  model, so the LLM spend view shows it as its own line.
- The shadow phase (below) is off for that organization.

The switch needs `TYPESAFE_API_KEY` (above). An organization on the list with a
missing key, or with a `TYPESAFE_*` value that cannot be used, fails its
`investment.materialize` run with an error: it is never categorized by the LLM
provider in silence. To go back to the LLM provider, take the organization off
the list.

### Investment shadow categorization

The shadow phase is a trial switch for the worker group that runs
`investment.materialize`. When it is on, the worker asks the TypeSafe decision
backend to categorize the same work units a second time, after the normal
results are written, and stores those answers in two trial tables
(`work_unit_investment_shadow`, `llm_categorization_attempts`). No product
view reads them, and the categorization that users see does not change. It is
**off by default**: with `INVESTMENT_SHADOW_PROVIDER` unset no TypeSafe client
is built and no request is sent.

| Variable | Meaning | Default |
| --- | --- | --- |
| `INVESTMENT_SHADOW_PROVIDER` | `typesafe` turns the shadow phase on for the process. Empty means off. Any other value keeps it off and logs a warning. | empty (off) |
| `INVESTMENT_SHADOW_ORG_IDS` | Comma-separated organization ids that are asked; `*` means every organization. Empty with the provider set means off (it fails closed). | empty (off) |
| `INVESTMENT_SHADOW_SAMPLE_PERCENT` | 0 to 100: the share of work units that are asked. A work unit is always in the sample or always out of it. | `100` |
| `INVESTMENT_SHADOW_CONCURRENCY` | Parallel shadow requests. Values above 32 are lowered to 32. | `4` |
| `INVESTMENT_SHADOW_MAX_SECONDS` | Time budget of the shadow phase of one run, in whole seconds. Values above 120 are lowered to 120 in code. Units that are not reached are asked by a later run. | `60` |
| `INVESTMENT_SHADOW_MAX_USD_PER_RUN` | Spend cap of the shadow phase of one run, in US dollars. A request is not sent when its estimated cost would pass the cap. Values above 100 are lowered to 100. | `0.50` |

The phase also needs `TYPESAFE_API_KEY` (above). A setting that cannot be used,
or a missing key, keeps the phase off and logs one warning; it never fails the
materialize run. The phase runs inside the materialize job: it delays the end
of that job by up to its time budget plus the time of its writes, so keep the
budget well below the worker's stop grace period. Apply ClickHouse migrations
109 and 110 before a worker runs with the phase on; with a table missing the
phase stops and says so.

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
