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

## Authentication configuration

The [auth control plane](../architecture/auth-control-plane.md) separates browser sessions, user access tokens,
workload identity and database logins. Use the [credential reference](../../reference/auth/credential-reference.md)
for issuers, consumers, storage and expiry.

| Process | Settings and purpose |
| --- | --- |
| auth-service | `AUTH_DATABASE_URI` or `AUTH_DATABASE_URI_FILE` for the unprivileged runtime login; `AUTH_SIGNING_KEY_FILE` or a key set for Ed25519 signing; `AUTH_TOKEN_ISSUER_URL` for the HTTPS issuer. |
| go-api and query-api | `AUTH_ACCESS_TOKEN_ISSUER` must match the issuer exactly. They verify public JWKS keys. `AUTH_PROXY_SESSIONS=true` enables the auth-service session path. |
| go-api auth-service client | `AUTH_PROXY_URL`, `AUTH_WORKLOAD_ISSUER`, and exactly one of `AUTH_WORKLOAD_SUBJECT_FILE` or `AUTH_WORKLOAD_BOOTSTRAP_FILE`. The file supplies a workload-exchange subject, not a user token. |
| Web | `AUTH_SECRET` or `NEXTAUTH_SECRET` protects its Auth.js session cookie. Keep it separate from backend signing and link secrets. |
| Auth migration command | `AUTH_MIGRATION_DATABASE_URI` or `AUTH_MIGRATION_DATABASE_URI_FILE`. Never inject it into a long-running service. |

`AUTH_ACCESS_TOKEN_CLASS` defaults to `eddsa`. The retired `hs256_compat` value is a startup error.
The former HS256 edge secret and `JWT_SECRET_KEY` signing paths are retired.
auth-service ignores the retired `AUTH_JWT_SECRET_KEY`, `AUTH_JWT_SECRET_KEY_FILE`, `AUTH_JWT_ISSUER` and
`AUTH_LEGACY_JWT_AUDIENCE` settings and warns when they are set. They are not installation requirements.
The former go-api principal envelope is also retired; query-api accepts no envelope bearer.

Session routes require `AUTH_SESSION_SUCCESSOR_KEY` or its `_FILE` form: 32 bytes encoded as base64url without
padding. Local password login also requires `AUTH_LOGIN_LOCKOUT_KEY` in the same encoding and size.
`AUTH_RATE_LIMIT_KEY` is optional, with at least 32 characters when supplied. Without it, the rate limiter
uses a key derived from the successor key. It does not fall back to an HS256 access-token secret.

Use independent `SSO_STATE_SECRET`, `GITHUB_APP_STATE_SECRET` and `LINK_TOKEN_SECRET` values in go-api.
The two state secrets require at least 32 characters. Missing state keys disable the corresponding SSO or
GitHub App installation routes with `503 config_missing`. A missing link secret makes go-api's signed-link
routes return `503 config_missing`. `LINK_TOKEN_SECRET_PREVIOUS` accepts older signed links during rotation;
new links use only the current secret. Neither `JWT_SECRET_KEY` nor `SETTINGS_ENCRYPTION_KEY` signs these links.
auth-service verification and reset routes instead consume purpose-bound opaque link credentials.

Direct `AUTH_SIGNING_KEY` values are refused. Mount the private signing key as a protected regular file.
Only auth-service receives it. See [signing-key custody](auth-signing-key-custody.md) and the
[auth configuration reference](../../reference/auth/configuration.md) for file permissions, setting bounds
and the workload-exchange switches.

## PagerDuty app configuration

The API and every worker that can synchronize PagerDuty need the same OAuth app values:

```dotenv
PAGER_DUTY_CLIENT_ID="<client-id>"
PAGER_DUTY_SECRET="<redacted>"
PAGER_DUTY_REDIRECT_URI="https://YOUR_HOST/org/admin/integrations/pagerduty/callback"
SETTINGS_ENCRYPTION_KEY="<redacted>"
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

## Platform LLM provider variables

These variables configure the platform LLM provider that the investment categorization and explanation jobs use. They are not Ask Dev settings: Ask Dev runs in the ACR service, which reads none of them. For which model Ask Dev uses, see [Turn on Ask Dev](../../admin/ask-dev/index.md).

Supported platform OpenAI-compatible bundles include:

```dotenv
# Generic/local OpenAI-compatible server (for example LM Studio or vLLM)
LLM_PROVIDER=local
LOCAL_LLM_BASE_URL=http://host.docker.internal:1234/v1
LOCAL_LLM_MODEL=your-model-id

# Local Ollama or authenticated Ollama Cloud-compatible endpoint
LLM_PROVIDER=ollama
OLLAMA_BASE_URL=http://host.docker.internal:11434/v1
OLLAMA_MODEL=your-model-id
OLLAMA_API_KEY=<redacted>
```

`OLLAMA_API_KEY` is the provider-specific platform alias; `LLM_API_KEY` remains
supported. Omit the key for an unauthenticated local host. These environment
bundles are platform configuration, never organization BYO settings.

Per-purpose platform model. `LLM_MODEL_EXPLAIN` names the model the investment
and work unit explanations ask the platform provider for. It wins over
`LLM_MODEL` and the provider-specific names, and never over an explicit request
model or an organization's own BYO model; an organization's own provider is
never given it. Categorization does not read it and keeps using `LLM_MODEL`.
Set it to a model the platform provider serves.

Hosted provider bundles. The model is resolved in one order for every route
(categorize, both explain routes and the budget status): a model set on the
request or in the organization's BYO settings, then `LLM_MODEL_<PROVIDER>` (or
the provider's own name such as `GEMINI_MODEL`), then the generic `LLM_MODEL`
for the platform provider only, then the provider's default. The platform
provider is `LLM_PROVIDER`, or the provider the keys auto-detect when it is
unset. The generic `LLM_MODEL` never names the model of another provider. An
empty base URL is the provider's public endpoint and an empty model is the
provider's default (`claude-haiku-5-5`, `gemini-3.8-flash`, `qwen-plus`). Set
`ANTHROPIC_BASE_URL` without a trailing `/v1`: the Anthropic client appends
`/v1/messages` itself, so `https://api.anthropic.com/v1` posts to
`/v1/v1/messages` and the call fails. The OpenAI-compatible providers (OpenAI,
Gemini, Qwen, local) append only `/chat/completions`, so their base URLs keep
their `/v1` (or `/ai/v1`, `/compatible-mode/v1`) segment, as in the examples
below.

```dotenv
# Anthropic
LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=<redacted>
LLM_MODEL_ANTHROPIC=claude-haiku-5-5   # optional; ANTHROPIC_BASE_URL optional

# Google Gemini
LLM_PROVIDER=gemini
GEMINI_API_KEY=<redacted>
GEMINI_MODEL=gemini-3.8-flash          # optional; LLM_MODEL_GEMINI also works; GEMINI_BASE_URL optional

# Qwen on DashScope
LLM_PROVIDER=qwen
DASHSCOPE_API_KEY=<redacted>             # or QWEN_API_KEY
QWEN_MODEL=qwen-plus                   # optional; LLM_MODEL_QWEN also works
DASHSCOPE_BASE_URL=https://dashscope-intl.aliyuncs.com/compatible-mode/v1   # optional; default is the Beijing endpoint

# Qwen on Cloudflare Workers AI (OpenAI-compatible route)
LLM_PROVIDER=qwen
QWEN_API_KEY=<redacted>
QWEN_BASE_URL=https://api.cloudflare.com/client/v4/accounts/<account-id>/ai/v1
QWEN_MODEL=@cf/qwen/qwen3-30b-a3b-fp8
```

An organization's BYO settings take the same provider names, with its key,
base URL and model stored in the settings, and its base URL passes the same
public-HTTPS and SSRF validation. A BYO monetary budget prices a call only on
the provider's public endpoint at its published list price; a gateway, a
self-hosted server or an unlisted model is unpriced and is refused while a
budget is configured.

### TypeSafe decision backend

The `typesafe` kind is a decision backend (TypeSafe System One, model family
`jev`), not a text completer. It is never selected by auto-detection: a present
`TYPESAFE_API_KEY` alone selects nothing. `LLM_PROVIDER=typesafe` selects it for
investment categorization only (see "Investment categorization served by the
decision backend" below). Only code that builds the TypeSafe client reads these
names:

| Variable | Meaning | Default |
| --- | --- | --- |
| `TYPESAFE_API_KEY` | Bearer token. Secret; read through the named-secret mechanism, so it is redacted in logs and errors. A key shorter than 12 bytes is refused. | none (client is not built) |
| `TYPESAFE_BASE_URL` | Must be `https://api.typesafe.ai`. Any other value is refused, so the key cannot be sent to another host. | `https://api.typesafe.ai` |
| `TYPESAFE_MODEL` | A versioned id such as `jev-1.13.0`. The moving aliases `jev-latest` and `jev-preview` are refused. | `jev-1.13.0` |

The generic `LLM_API_KEY`, `LLM_BASE_URL` and `LLM_MODEL` overrides do not apply
to this client. Give `TYPESAFE_API_KEY` only to the worker group that runs
`investment.materialize`.

An organization that opts in to categorization on its own model can also choose
TypeSafe with its own key: the categorize override of the LLM settings with
provider `typesafe`, a model from the closed list (`jev-1.13.0`; `jev-latest`,
`jev-preview` and other ids are refused) and the org's TypeSafe key. That
backend does not read the `TYPESAFE_*` variables. It takes no base URL, because
the endpoint `https://api.typesafe.ai/v1/systemone` is fixed in code, and a
saved base URL is refused. Only the categorize override can name `typesafe`.
The default row and the investigate and explain overrides refuse it. The org's
calls are priced at USD 0.042 per million input tokens (output is not billed).
The rows carry the provider `byo-typesafe` and the credential source `org`. A
refused or failed call is never served by the platform model.

How to give it to that group only:

- **Helm:** every worker group loads the shared ConfigMap and the shared
  Secret, so a key placed there reaches all groups. Set the key, and the
  `INVESTMENT_SHADOW_*` switch, in `goWorkers.groups[].extraEnv` of the `heavy`
  group, with the key as a `secretKeyRef` to a separate Secret that holds only
  that key. Nothing is rendered when `extraEnv` is not set.
- **Docker Compose:** the root `compose.yml` and the CI worker overlay
  compose file pass `TYPESAFE_*` and
  `INVESTMENT_SHADOW_*` from the shell or `--env-file` of the compose command to
  `go-worker-heavy` only. Do not put them in `ops/.env`: every worker reads it.
  Unset values are empty, which means off.

### Investment categorization served by the decision backend

`LLM_PROVIDER=typesafe` makes the worker that runs `investment.materialize`
write the investment categorization that users see with the TypeSafe decision
backend. There is no other switch: with any other value, or unset, nothing
changes. To go back, set `LLM_PROVIDER` to its earlier value.

```dotenv
LLM_PROVIDER=typesafe
TYPESAFE_API_KEY=...   # to the investment worker group only (see above)
```

`LLM_PROVIDER` is one value for every process that reads it, and only
investment categorization can use the decision backend:

- **Explanations** (the investment mix and work unit explanations of
  query-api) treat `typesafe` as "not a text provider": with no organization
  BYO provider they select the text provider by key, as with `LLM_PROVIDER`
  unset (`OPENAI_API_KEY` first, with `LLM_MODEL`). Keep that key and
  `LLM_MODEL` set, or explanations answer "no LLM provider is configured".
- **Organizations with BYO LLM settings:** the worker does not read
  organization LLM settings, so such an organization's categorization is also
  written by the decision backend, while its explanations keep its BYO
  provider.
- **Usage:** each run records one `llm_token_usage` row under provider
  `typesafe` and the TypeSafe model (`TYPESAFE_MODEL`, default `jev-1.13.0`),
  so the LLM spend view shows it as its own line. Explanation usage keeps its
  own provider.
- **Shadow phase:** it does not run when the decision backend serves the run.
- **Where to look:** the worker metric
  `dev_health_investment_served_outcomes_total{provider, model, outcome}`
  counts every classification of the served mode. `ok`, `zero_support` and
  `evidence_none` write a row from the backend's answer; `invalid_answer` and
  `adapter_defect` write the `invalid_llm_output` prior row; `timeout`,
  `refused` (the connection was refused, nothing was sent), `server_error`,
  `rate_limited`, `rejected` (a rejected key or an unknown model: the run ends)
  `transport_other`, `budget_accounting` (the budget guard refused the call
  before it was sent: nothing was sent) and `budget_settle_failed` (the call was
  sent and answered, and its accounting could not be settled) are failed
  requests, and those units keep their last row. Every series is reported, at 0 when the mode is off. As a reference, in
  the first shadow run on real data `zero_support` and `evidence_none` together
  were about 1.3% of the classifications (41 of 3,214), and no request failed;
  a share far above that, or a rising count of failed requests, needs a look.
  The run log line `investment served decision complete` holds the same counts
  for one run.

What a categorization is, for each work unit with enough text (one request,
no repair request, no second provider):

- The rows carry a `categorization_model_version` that starts with
  `provider=typesafe;api=systemone;`, so each work unit is categorized again
  one time after the switch, because the version is new.
- A work unit for which the backend finds no clear category gets its most
  probable category with a low `evidence_quality` (0.3 or less) and no
  evidence quote. The next run asks it again.
- A work unit whose request fails for a reason that can pass (a timeout, a
  rate limit, a server error) gets no new row in that run: its last row stays,
  and the next run asks again. A work unit that never had a row has none until
  a later run answers it.
- A rejected key or an unknown model ends the run with an error, like the same
  failure of an LLM provider. A missing `TYPESAFE_API_KEY`, or a `TYPESAFE_*`
  value that cannot be used, fails the run with an error: it is never
  categorized by another provider in silence.

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

### Investment categorization

Investment categorization always runs on the platform provider. An
organization's own provider configuration (BYO LLM) never applies to it, even
when the provider is `auto`.

### Investment provider batch mode

The worker group that runs `investment.materialize` can send the categorization
requests of a run as one provider batch job instead of one request per work
unit. A provider batch costs about half as much and is answered within 24 hours
instead of at once. Only the `openai` provider has a batch mode (the OpenAI
Batch API). It is **off by default**: with the setting unset every request is
sent one by one, as before.

| Variable | Meaning | Default |
| --- | --- | --- |
| `INVESTMENT_LLM_BATCH_MODE` | Off by default (`sync`). A batch trades latency (up to the timeout below; the provider window is 24 hours) for about half the generative price. `sync`: one request per work unit. `auto`: a provider batch when the provider has one and the run has at least 25 work units to categorize, else `sync`. `provider_batch`: always a provider batch; a run whose provider has none fails with an error before it reads data. Any other value fails the run. | `sync` |
| `INVESTMENT_LLM_BATCH_TIMEOUT_SECONDS` | How long a run waits for its batch, in seconds, up to 86400. A value that cannot be used falls back to the default with a warning. | `3000` |

The run waits for the batch inside the job and polls it every 30 seconds. Keep
the timeout below the job timeout of `investment.materialize` (7200 seconds).
When the timeout passes, or the job stops (a lost lease, a worker shutdown), the
run cancels the provider batch; its work units are written as for a failed
request (the fallback row with status `llm_task_failed`), and a later run asks
them again. A cancelled batch can still bill the requests the provider already
answered; the run counts their tokens in its `llm_token_usage` row when the
provider reports them. A rejected key, an unknown model or an exhausted quota
ends the run with an error, as in `sync` mode. A batch is not resumed after a
worker restart: the next run sends its own batch. The log line
`investment llm batch submitted` names the provider batch job as soon as it is
created (look it up there to cancel a batch a stopped worker left running), and
`investment llm batch complete` ends each batch: a warning unless the outcome is
`completed`. A timeout counts as `batch_timeout` in the run's failure counts.
