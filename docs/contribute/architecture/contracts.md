---
page_id: con-contracts
summary: Rules for canonical provider, identity, API, schema, job, route, metric, taxonomy, feature, and documentation contracts.
content_type: architecture
owner: engineering
source_of_truth:
  - docs/contributing/platform-contract.md
  - contracts/jobs/v1/
  - contracts/sync-dispatch/v1/
  - current ADRs and canonical registries
applicability: current
lifecycle: active
---

# Stable contracts and source-of-truth rules

A stable contract has one authoritative source, explicit ownership, and defined compatibility behavior. Code, deployment, tests, documentation, and runtime admission must agree before a new provider, job, route, or schema becomes supported.
{: .fc-page-lede }

## Provider and canonical model contracts

- Provider clients own authentication, fetching, pagination, retry, discovery, and provider-specific limits.
- Normalizers map provider records into canonical models; they do not redefine product meaning.
- Canonical IDs include organization and provider/source authority wherever collisions are possible.
- Missing provider transitions or bounded-query absence are unknown unless an authoritative delete/tombstone event exists.
- A provider-native field or relationship is not canonical without an explicit normalization and provenance rule.

PagerDuty REST reads and Webhooks V3 are separate interfaces with distinct authentication and replay behavior, but both use the shared canonical incident identity and ordering model. The webhook payload cannot choose organization, source, credential, or signing authority.

Jira Service Management incident code is not a supported public capability until live tenant proof satisfies its release contract. Do not convert ordinary Jira issues or Opsgenie alerts into incidents by inference.

## Public API and schema contracts

- Public REST, GraphQL, CLI, webhook, and Customer Push schemas are generated or verified from code.
- Incompatible public changes require an explicit version, compatibility bridge, or deprecation path.
- Nullability, pagination, rate limits, errors, and authorization are part of the contract, not implementation details.
- A frontend label is not a backend enum unless the public mapping says so.

Ask Dev v1 follows a cross-repository generated-contract chain: canonical
Pydantic models in ops produce Draft 2020-12 schemas and golden fixtures; the
web repository vendors the pinned artifacts and mechanically produces strict
TypeScript declarations. Both repositories run no-drift checks. The canonical
contracts contain server-issued IDs, generic source metadata, and typed bounded
payloads; provider credentials, provider-specific tool fields, arbitrary JSON,
and unbounded collections are outside the contract.

Cross-object invariants are part of the contract even when JSON Schema cannot
express them alone. In particular, an answer cannot cite evidence or metrics
that are absent from the same answer, a `complete` answer cannot carry a stale
or unavailable required source, and a stream has exactly one terminal answer
or error immediately followed by `done`.

Wave 3.1 (CHAOS-3294) adds a parallel `dev_answer.v2` contract chain --
server-owned intent, per-mention resolution, investigation planning, a
canonical answer frame, and an optional presentation narrative -- alongside
the unmodified v1 contracts described above. See
[Ask Dev v2 contracts](ask-dev-contracts-v2.md) for the full contract map,
the append-only resolution ledger, the five semantic validators, and the
single v2-to-v1 compatibility projector.

Contextual Ask Dev handoffs use a server-owned, provider-neutral V1 allowlist
of route IDs and permitted entity types. A request is rejected before
persistence when its surface route is unknown or deferred, carries arbitrary
metadata, or disagrees with the request's direct scope. The browser supplies
only an untrusted proposal: every accepted run re-resolves the matching
canonical IDs through the authorized entity catalog before model decisions or
tool execution. `surface_context: null` remains valid for the permanent window
and `/dev` on pages without an approved contextual entrypoint.

The permanent Ask Dev window and `/dev` read the same retained transcript from
`GET /api/v1/dev/conversations/{conversation_id}/transcript`. The response is a
bounded chronological page of paired persisted questions and validated
answers; internal rendered content, tool calls, and provider payloads are not
wire fields. A retry submits a new idempotency key and an optional
`retry_of_run_id`; the server accepts only a terminal run owned by the same user,
organization, and conversation, and persists a distinct linked run. Closing a
browser stream signals cancellation and waits for the recorder to persist the
terminal `cancelled` state.

The production runtime is request-scoped. FastAPI dependency cleanup closes it
exactly once after a new stream, an idempotent replay, or an early validation or
persistence response; replay never executes the provider a second time.

Ask Dev model decisions use `AgentLLMProvider`, not the completion-oriented
`LLMProvider.complete` contract. Canonical messages, tools, structured final
answers, disambiguation, refusals, usage, latency, cancellation, and safe errors
remain provider-neutral. Adapters alone translate native wire tool calls. The
OpenAI-compatible and deterministic scripted adapters therefore exercise the
same decision seam in unit and offline tests; scripted mode is not an
orchestrator bypass and is not a selectable product provider family.

Provider selection is a separate fail-closed policy. A current certified BYO
connection wins, followed by a certified platform connection only when no BYO
is selected or platform fallback was explicitly allowed. Disable controls and
deny rules win before readiness. Deterministic full-stack acceptance instead
boots a scripted OpenAI Chat Completions wire service behind the explicit
acceptance-only environment gate. The production adapter still discloses that
endpoint as provider family `openai` and source `platform`; public model source
remains `platform | byo`. Partial acceptance configuration fails closed, and
ordinary platform and BYO base URLs retain the public-HTTPS SSRF policy.

The acceptance endpoint is admitted only when all of these operator-owned
values are present together: `ASK_DEV_LIVE_ACCEPTANCE=1`,
`ENVIRONMENT=acceptance`, `LLM_PROVIDER=openai`,
`ASK_DEV_ACCEPTANCE_OPENAI_BASE_URL`, and
`ASK_DEV_ACCEPTANCE_OPENAI_API_KEY`. The base URL must use `/v1` over HTTP on
the exact host `127.0.0.1`, `localhost`, or the checked-in Compose service name
`ask-dev-scripted-openai`; suffix matches and other private or metadata targets
are rejected. The acceptance model and internal disclosure key are fixed as
`ask-dev-scripted-v1` and `ask_dev_scripted_acceptance`. A partial activation
after `ASK_DEV_LIVE_ACCEPTANCE=1` is asserted does not fall back to ordinary
platform credentials.

The reusable Compose boundary is
`tests/acceptance/compose.ask-dev.yml`; it publishes no provider host port and
places `ask-dev-scripted-openai` only on an internal acceptance network. Run it
through `scripts/acceptance/run_ask_dev_compose.sh --web-root <web-checkout>`.
The launcher owns deterministic graph, metric, and evidence seeding, the
canonical organization and user, only the Ask Dev and contextual-entrypoint
entitlement overrides, the real
organization-admin readiness action, the Compose-built Web service, and Web's
fixed authenticated REST/SSE browser oracle. The provider must call
`query_metric.v1`, `search_evidence.v1`, and `data_health.v1` in that order; the
final answer fails the versioned oracle unless metric, evidence, and
provider-health results are all non-empty. The oracle fixture owns the browser
question and exact expected summary independently of the provider
implementation. Missing services, credentials, preparation, or substantive
browser execution are failures, not skips.

Agent Context Runtime remains independently gated and is not an Ask Dev
prerequisite. The acceptance oracle requires signed Ask Dev evidence resolution
while `agent_context_runtime` remains disabled.

## Job contracts

Versioned Go job contracts live under `contracts/jobs/v1/` and define:

- envelope and argument schema;
- stable kind and version;
- registry entry;
- handler capability;
- deployment profile;
- migration and route state;
- compatibility and admission evidence.

A running binary does not admit a job. The route, contract version, compiled handler, schema, deployment profile, and runtime capability must all agree.

## Route ownership

Sync transport routes live under `contracts/sync-dispatch/v1/`; generic job migration state lives with the job contract. Current routes remain Celery-owned.

A route migration must define:

1. source runtime and target runtime;
2. shadow/parity behavior without mutating the baseline;
3. canary admission;
4. idempotency and duplicate prevention;
5. in-flight classification and drain behavior;
6. domain completion evidence;
7. rollback ordering;
8. mixed-version and schema compatibility.

Do not infer route ownership from deployed replicas, health endpoints, queue names, or feature flags alone.

## Storage and outbox contracts

- PostgreSQL semantic state, River queue state, ClickHouse facts, and Valkey/Redis coordination have separate authority.
- Producer-owned outbox intent and relay-owned delivery state use different database privileges.
- Runtime roles cannot inherit migration or cross-domain authority.
- Outbox and replay identifiers preserve tenant and source scope.
- An ambiguous commit outcome is not safe to retry without inspection.

## Metric and taxonomy contracts

Metrics and taxonomies have one registry or computation source. Units, scope, time, weighting, aggregation, evidence quality, and missing-state semantics are part of the public contract.

An unavailable sample is not zero. A model-derived estimate is not a factual category assignment or causal conclusion.

## Feature and deployment contracts

Feature availability comes from the current feature decision and entitlement source. Deployment manifests define process composition and secret ownership but do not override runtime validation or route contracts.

A feature-off path must block new work at every producer, scheduler, reconciler, API, and webhook boundary while preserving necessary inspection and cleanup controls.

## Documentation contracts

The documentation IA owns one canonical public page and URL per reader outcome. Source documents, ADRs, plans, evidence captures, and issue histories feed that page but do not create competing public truth.

ADRs can explain a durable decision. Benchmark captures, migration planning, and rollout evidence stay internal unless a supported reader needs them to operate the current system.
