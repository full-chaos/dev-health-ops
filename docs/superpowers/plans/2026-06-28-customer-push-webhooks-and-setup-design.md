# Customer Push Ingestion: Webhooks and Setup Design Details

## Context

This is an addendum to `2026-06-26-external-customer-push-ingestion-api.md`.

The missing product and implementation work is not just API shape. Customer push needs:

- developer and user documentation,
- customer authorization and credential lifecycle,
- concrete CI/CD examples,
- product screens for source setup and credential management,
- an explicit webhook position.

## Current repo grounding

### Backend: existing webhook path

`dev-health-ops` already mounts `webhooks_router` in the FastAPI app alongside GraphQL, admin, telemetry, product telemetry, ingest, and org routes.

Existing webhook router shape:

- `/api/v1/webhooks/github`
- `/api/v1/webhooks/gitlab`
- `/api/v1/webhooks/jira`
- `/api/v1/webhooks/health`

Existing router behavior:

- validates provider-specific signature/token,
- parses provider headers or body event type,
- creates a canonical `WebhookEvent`,
- dispatches a Celery task,
- returns an accepted response immediately.

Important limitation: current webhook dispatch is best-effort. If Celery is unavailable, the current helper logs and continues. That behavior is acceptable for low-stakes webhook hints but is not acceptable for customer-push ingestion because customers need durable status, retry visibility, and rejected-record diagnostics.

Existing webhook model support:

- providers: GitHub, GitLab, Jira,
- event types: push, pull request, merge request, issue created/updated/closed/deleted, pipeline, deployment, check run, installation, marketplace purchase, unknown,
- delivery ID field exists and should be reused as part of webhook event idempotency.

Existing auth support:

- GitHub: HMAC-SHA256 via `X-Hub-Signature-256`,
- GitLab: shared token via `X-Gitlab-Token`,
- Jira: shared secret validation.

Gap: secrets are environment-wide. Customer push needs org/source-scoped secrets and token scopes, not global provider secrets.

### Frontend: existing setup surfaces

`dev-health-web` is a Next.js 16 app using React Server Components, TypeScript, Tailwind CSS v4, and custom components. Authenticated app routes are under `(app)`, admin routes require admin/owner access, and backend `/api/v1/*` plus `/graphql` are proxied with authorization headers.

Existing admin integration routes:

- `/org/admin/integrations`
- `/org/admin/integrations/[provider]`
- `/org/admin/sync`

Existing integration UX:

- integration cards for GitHub, GitLab, Jira, Linear, and LaunchDarkly,
- saved credential list,
- add/edit credential form,
- provider-specific credential fields,
- first-time setup auto-opens the form when no credentials exist.

Customer-push onboarding should extend this admin integration area instead of creating a disconnected setup surface.

## Webhook exploration

### Recommendation

Use webhooks as **optional acceleration and hinting**, not as the v1 source of truth.

V1 source of truth should remain REST batch ingestion:

```text
customer source / runner / relay
→ external ingest batch
→ durable stream
→ status store
→ normalized records
→ sinks
→ bounded recomputation
```

Webhook-assisted ingestion should feed the same external ingest schemas and status path:

```text
provider webhook
→ customer-owned relay or FullChaos webhook endpoint
→ external ingest records
→ /api/v1/external-ingest/batches
```

Do not let provider webhook handlers write metrics, raw facts, or final tables directly.

### Why webhooks are not enough alone

Provider webhooks are event notifications, not complete state synchronization:

- payloads can be capped or truncated,
- delivery can fail,
- missed events require reconciliation,
- some fields are only available for specific actions,
- historical backfill still needs API/runner support,
- webhook event semantics differ by provider.

Therefore, webhook mode must include periodic reconciliation/backfill through `dev-hops push batch` or provider export helpers.

## Provider feasibility

### GitHub

Recommended v1 webhook role: **useful for acceleration, not source of truth**.

Events to support first:

- `push` → commit hints, branch updates, repo activity window
- `pull_request` → PR lifecycle changes
- `pull_request_review` → review lifecycle
- `pull_request_review_comment` → review interaction hints
- `check_run` / `check_suite` → CI status hints
- `deployment` / `deployment_status` → deployment lifecycle hints
- `issues` / `issue_comment` → work-item hints only when GitHub Issues are enabled

Design notes:

- GitHub sends `X-GitHub-Event` and `X-GitHub-Delivery` headers.
- `X-GitHub-Delivery` should become the provider delivery idempotency key.
- GitHub signs webhook payloads with `X-Hub-Signature-256` when a webhook secret is configured.
- GitHub payloads are capped at 25 MB, so large push/create cases can require follow-up reconciliation.

Customer setup options:

1. Customer-owned relay receives GitHub webhook, normalizes/batches, then calls FullChaos external ingest.
2. FullChaos-hosted webhook endpoint receives GitHub webhook only after source-scoped secret and org mapping exist.
3. GitHub Actions scheduled job remains the simpler v1 recommended path.

### GitLab

Recommended v1 webhook role: **good for merge request and pipeline acceleration; push needs reconciliation**.

Events to support first:

- Push Hook → commit hints
- Merge Request Hook → MR lifecycle, review state, merge state
- Pipeline Hook → CI status and duration hints
- Job Hook → optional later if test/build phase detail is needed

Design notes:

- GitLab push event payloads include newest 20 commits only when a push contains more than 20 commits, with `total_commits_count` carrying the actual count. This makes push webhooks insufficient as the only source for commit ingestion.
- GitLab merge request webhooks provide `object_attributes.action`, `changes`, reviewer arrays, approval actions, merge action, and merge timing fields. This is useful for review and lifecycle hints.
- GitLab pipeline hooks fire when pipeline status changes.
- GitLab job hooks fire when job status changes, but they are more verbose and likely not v1.

Customer setup options:

1. GitLab runner scheduled job using `dev-hops push batch`.
2. GitLab project/group webhook to a customer relay.
3. Optional FullChaos-hosted webhook later after source-scoped verification exists.

### Jira

Recommended v1 webhook role: **useful for work-item status transition hints, but backfill remains required**.

Events to support first:

- `jira:issue_created`
- `jira:issue_updated`
- `jira:issue_deleted`

Design notes:

- Jira issue-related webhook callbacks include `webhookEvent`, timestamp, user, issue, and for issue updates a `changelog` array.
- The changelog is available for `jira:issue_updated`, which makes Jira webhooks useful for transition/event hints.
- Jira webhook registration supports project/JQL filtering for issue-related events, but sprint/version event filtering has limitations.
- Jira webhooks larger than 25 MB are not delivered, and some known webhook gaps exist. Do not rely on webhook-only ingestion.

Customer setup options:

1. Jira scheduled export through `dev-hops push batch` remains v1 default.
2. Jira webhook relay can accelerate issue transition ingestion.
3. Sprint and version reconciliation should stay pull/batch based.

### Linear

Recommended v1 webhook role: **defer or experimental**.

Reasoning:

- `dev-health-ops` already has a Linear provider based on Linear API ingestion.
- The current generic webhook router does not include Linear as a webhook provider.
- Linear webhook support should not be added until it can reuse the same external ingest schema path and tenant-scoped source registration.

Potential later scope:

- issue created/updated/deleted,
- comment created,
- project/cycle changes,
- customer relay sample.

## Webhook architecture options

### Option A: Customer-owned relay, recommended for v1 beta

```text
GitHub/GitLab/Jira webhook
→ customer relay
→ normalize to external-ingest.v1 records
→ POST /api/v1/external-ingest/batches
→ status/rejections visible in FullChaos
```

Pros:

- FullChaos does not receive provider secrets or raw provider webhooks unless the customer chooses to send them.
- Tenant/source authorization stays simple: relay uses an ingest token.
- Customers retain control over provider webhook security.
- Same REST ingest contract handles webhooks, scheduled jobs, and CI runners.

Cons:

- Requires customer-operated relay.
- Needs sample relay code.
- Needs docs per provider.

### Option B: FullChaos-hosted provider webhook endpoint, not v1 default

```text
GitHub/GitLab/Jira webhook
→ /api/v1/webhooks/customer/{source_id}/{provider}
→ verify source-scoped secret
→ normalize to external-ingest records
→ durable status path
```

Pros:

- Lower customer infrastructure burden.
- Real-time path can be productized.

Cons:

- Requires source-scoped secret storage.
- Requires provider-to-org/source mapping before parsing is trusted.
- Requires replay protection and durable delivery diagnostics.
- Higher security and support burden.

Use only after customer-push auth, source registration, and status diagnostics are implemented.

### Option C: Existing `/api/v1/webhooks/{provider}` endpoints, not sufficient

Current provider webhooks should not be repurposed directly for customer-push ingestion.

Reasons:

- secrets are environment-global, not org/source-scoped,
- dispatch is best-effort,
- no customer-visible ingest status,
- no rejected-record diagnostics,
- no external ingest schema validation,
- no one-active-owner source policy.

## Required backend design details

### Source-scoped webhook secrets

Add source-level webhook secret metadata:

```json
{
  "sourceId": "uuid",
  "orgId": "uuid",
  "system": "github",
  "instance": "github.com/acme",
  "mode": "customer_push",
  "webhookSecretId": "uuid",
  "webhookMode": "customer_relay | fullchaos_hosted | disabled"
}
```

### Webhook delivery idempotency

Use provider delivery identifiers where available:

- GitHub: `X-GitHub-Delivery`
- GitLab: no universal GUID in all payloads, derive from event header, object kind, object id, action, updated timestamp, and project id
- Jira: derive from `webhookEvent`, issue key/id, timestamp, and changelog id when present

Webhook events should be deduped before they create external ingest batches.

### Reconciliation policy

Every webhook-enabled source needs a reconciliation schedule:

- GitHub: daily or hourly API/runner batch for commits/PRs/reviews depending on customer scale
- GitLab: daily or hourly batch because push payloads can include only the newest 20 commits for large pushes
- Jira: daily work-item and transition reconciliation
- Linear: defer until webhook support is explicitly scoped

### Webhook status model

Surface webhook-assisted ingestion through the same status model as batches:

- received
- verified
- normalized
- enqueued
- processed
- partial
- failed
- ignored unsupported event

Do not show provider webhook status only in logs.

## Required web product design details

Customer-push setup belongs inside existing admin integration surfaces:

- `/org/admin/integrations`
- `/org/admin/integrations/[provider]`
- add customer-push mode selection under provider detail
- add source registration and ingest credential management under provider detail

### Screen 1: Provider detail mode choice

Purpose: choose between FullChaos-managed sync and customer push.

Content:

- managed sync card: FullChaos connects to provider, stores provider credential, runs syncs
- customer push card: customer sends normalized data with an ingest token, provider credentials stay outside FullChaos
- webhook-assisted badge: optional/experimental unless relay mode is configured

CTA:

- Set up managed sync
- Set up customer push

### Screen 2: Customer-push source registration

Fields:

- provider: GitHub, GitLab, Jira, Linear, Custom
- source instance: `github.com/acme`, `gitlab.com/group/project`, Jira cloud URL, Linear workspace slug
- source display name
- ingestion mode: customer push
- reconciliation schedule recommendation

Validation:

- reject duplicate active source instance
- show one-active-owner policy if managed sync already exists

### Screen 3: Ingest credential creation

Fields/states:

- credential name
- scope checkboxes: schema read, batch write, batch status read
- provider/source binding
- optional expiration
- create token
- one-time token display
- copy token

Warnings:

- token is shown once
- rotate/revoke anytime
- store in GitHub Actions secrets or GitLab CI variables

### Screen 4: Setup examples

Tabs:

- GitHub Actions
- GitLab Runner
- Generic Docker
- cURL
- Webhook relay

Each tab includes:

- secret names
- minimal workflow/job file
- validate step
- batch push step
- status polling step

### Screen 5: Validate first payload

Modes:

- paste JSON
- upload file
- use sample payload
- call validation endpoint

Display:

- valid/invalid summary
- accepted/rejected counts
- table of errors with record index, kind, external id, field path, message

### Screen 6: Ingest status

Filters:

- source
- status
- time window
- producer type: CLI, CI, relay, API

Columns:

- ingestion id
- source
- window
- status
- items received
- accepted
- rejected
- created at
- completed at

Drilldown:

- rejected records
- recompute status
- linked runner example / producer

### Screen 7: Credential management

Rows:

- credential name
- scopes
- source binding
- created at
- last used
- last result
- rotate
- revoke

States:

- active
- rotated
- revoked
- expired
- never used

## CI/CD examples to implement

### GitHub Actions

```yaml
name: Push Dev Health Data
on:
  schedule:
    - cron: "*/30 * * * *"
  workflow_dispatch:

jobs:
  push-dev-health:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Generate payload
        run: dev-hops push export github --repo "$GITHUB_REPOSITORY" --since "$SINCE" --until "$UNTIL" > payload.json
      - name: Validate payload
        run: dev-hops push validate payload.json --schema external-ingest.v1
      - name: Push payload
        run: dev-hops push batch payload.json --api-url "$FULLCHAOS_API_URL" --token "$FULLCHAOS_INGEST_TOKEN" --org "$FULLCHAOS_ORG_ID" --poll
        env:
          FULLCHAOS_API_URL: ${{ vars.FULLCHAOS_API_URL }}
          FULLCHAOS_ORG_ID: ${{ vars.FULLCHAOS_ORG_ID }}
          FULLCHAOS_INGEST_TOKEN: ${{ secrets.FULLCHAOS_INGEST_TOKEN }}
```

### GitLab Runner

```yaml
push_dev_health:
  image: ghcr.io/full-chaos/dev-hops:latest
  script:
    - dev-hops push export gitlab --project "$CI_PROJECT_PATH" --since "$SINCE" --until "$UNTIL" > payload.json
    - dev-hops push validate payload.json --schema external-ingest.v1
    - dev-hops push batch payload.json --api-url "$FULLCHAOS_API_URL" --token "$FULLCHAOS_INGEST_TOKEN" --org "$FULLCHAOS_ORG_ID" --poll
  rules:
    - if: $CI_PIPELINE_SOURCE == "schedule"
```

### Generic cURL

```bash
curl -sS -X POST "$FULLCHAOS_API_URL/api/v1/external-ingest/batches" \
  -H "Authorization: Bearer $FULLCHAOS_INGEST_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  --data-binary @payload.json
```

## Issue updates recommended

### CHAOS-2714 Web setup screens

Add explicit route target:

- build under `/org/admin/integrations/[provider]`, not a standalone wizard first,
- introduce a customer-push mode card beside managed sync,
- add source registration, credential creation, setup examples, validation, status, and credential management screens.

### CHAOS-2715 Webhook-assisted ingestion

Update recommendation:

- v1: customer-owned relay as beta/experimental,
- do not use existing `/api/v1/webhooks/{provider}` endpoints as customer-push source of truth,
- hosted provider webhooks require source-scoped secrets, source mapping, durable status, and rejected-record diagnostics.

### CHAOS-2712 Authorization

Add source-scoped webhook secrets and delivery idempotency to the auth model.

### CHAOS-2713 CI/CD examples

Add GitHub Actions, GitLab Runner, Generic Docker, cURL, and webhook relay tabs to the product UI, not just docs.

## Final recommendation

Implement customer push in this order:

1. REST batch ingestion and status.
2. Source registration and ingest credentials.
3. Web setup surfaces under existing admin integrations.
4. CI/CD examples and docs.
5. Customer-owned webhook relay examples as beta.
6. FullChaos-hosted provider webhook ingestion only after source-scoped secrets and durable status exist.
