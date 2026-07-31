---
page_id: con-release
summary: Build an immutable release, apply migrations in the approved order, verify health and data progress, and retain rollback evidence.
content_type: task-guide
owner: engineering
applicability: current
lifecycle: active
---

# Release and rollback expectations

1. Select the reviewed immutable revision and artifact.
2. Record schema, configuration, API, provider, feature, and documentation compatibility changes.
3. Back up required data and preserve the prior artifact.
4. Apply migrations through one controlled release path.
5. Deploy and verify API, workers, queues, stores, source progress, and representative product tasks.
6. Monitor the stabilization window.
7. Roll back only when schema and data compatibility permit it.

Release notes must distinguish new, changed, deprecated, removed, fixed, and known-limited behavior.
## Ask Dev contract baseline

The Ask Dev v1 contract release is additive and dark. Migration `0067` registers
the `ask_dev` entitlement through the existing explicit-enable path, so every
tier remains disabled by default. It does not change `byo_llm` or
`agent_context_runtime`. The release also adds provider-neutral schemas,
positive and negative fixtures, version compatibility rules, and generated web
types. Wave 1 adds the authorized scope-search and bounded metric GraphQL fields
behind the same canonical entitlement decision. It does not ship orchestration,
conversation persistence, or the user interaction UI.

Before release, run the ops contract check and migration tests and the web
generated-type check. Rollback may leave the additive feature row in place; it
remains effectively disabled unless explicitly granted, and the global feature
decision is still the emergency kill switch. Future inclusion in paid plans is
a separate product change and is not part of this V1 release.

## Ask Dev Wave 1 release note

- **New:** additive PostgreSQL conversation, message, run, safe tool-audit,
  feedback, and minimal deletion-tombstone storage behind the disabled Ask Dev
  feature.
- **Changed:** the retention job contract adds version 3 and the
  table-scoped `ask_dev_conversations` policy; versions 1 and 2 remain accepted
  with their original frozen policy domains.
- **Known limited:** this storage release does not enable Ask Dev or replace
  existing LLM workflows. UI/API/orchestrator owners must use the canonical
  persistence service and validated `dev_answer.v1` seam before rollout. The
  ops consumer accepts retention v3, but the active producer remains v2 until
  live capability reports prove every consumer is compatible.
- **Rollback:** disable Ask Dev normally. Downgrade migration `0068` only in a
  pre-release environment after new binaries are stopped because it deletes
  all Ask Dev tables and their contents. Disabling the feature never disables
  retention for conversations already stored.

## Ask Dev Wave 2 provider foundation release note

- **New:** a provider-neutral multi-turn decision contract, OpenAI-compatible
  agent adapter, deterministic scripted provider, current-readiness seam, safe
  provider errors, and `use_case=ask_dev` token telemetry.
- **Changed:** none of the existing `LLMProvider.complete` callers or fallback
  behavior changes. Ask Dev has an independent fail-closed resolver: workspace
  BYO cannot silently fall through to platform credentials.
- **Known limited:** this foundation does not expose the Ask Dev REST or stream
  routes and does not enable the feature. Provider/model construction and the
  orchestration loop land in their owning Wave 2 integration lanes.
- **Rollback:** disable Ask Dev, roll back the binary, then remove the additive
  ClickHouse `use_case` column only if older readers require it. Existing rows
  default to `legacy`; no customer prompt or response content is introduced.

## Ask Dev Wave 2 runtime integration release note

- **New:** the authenticated `/api/v1/dev` surface now submits idempotent
  conversation messages over bounded SSE, runs the exact nine server-owned
  tools through the certified provider, and reauthorizes answer-bound evidence
  expansion through `dev_evidence_expansion.v1`.
- **Changed:** capabilities reflect the live provider policy and current
  readiness record. Workspace BYO remains first and platform fallback remains
  explicit; missing certification, signing authority, or analytics services
  fails closed before model or tool work starts.
- **Limits:** admission enforces one active run per user, five per organization,
  20 user requests per 15 minutes, and 100 organization requests per hour.
  Runtime ceilings remain four model rounds, six tool calls, 45 seconds total,
  64 KiB per tool result, and 256 KiB across tool results.
- **Known limited:** this Ops delivery does not enable Ask Dev or add the `/dev`
  web experience. ACR remains an optional evidence source; native authorized
  evidence continues to work without it.
- **Rollback:** disable Ask Dev first, then roll back the application binary.
  Conversation retention and tombstone policy continue to apply to already
  admitted runs; no database downgrade is required.

## Ask Dev Wave 3 administrator controls release note

- **New:** organization administrators can read the safe effective provider and
  readiness state, run deterministic certification, select the exact 0/30-day
  retention policy, explicitly approve or deny platform fallback, inspect
  content-free Ask Dev usage, and emergency-disable both Ask Dev surfaces.
- **Security:** settings and readiness mutations are denied during
  impersonation. Responses never include credentials, base URLs, prompts,
  evidence, packets, tool payloads, or conversation content.
- **Compatibility:** the app-shell chat window and `/dev` use the same
  entitlement, readiness, emergency-disable, retention, conversation, and run
  policy. Existing BYO settings and Context Fabric Validation are unchanged.
- **Rollback:** remove the administrator UI or roll back the binary. Stored
  policy rows are inert to older binaries; retained conversations continue to
  follow their persisted lifecycle policy.

## Ask Dev Wave 3 shared conversation release note

- **New:** the permanent Ask Dev window and `/dev` can load one canonical,
  cursor-paginated persisted transcript through
  `dev_conversation_transcript.v1`. Only bounded user questions/scopes,
  validated `dev_answer.v1` values, and safe run linkage are returned.
- **Changed:** `dev_message_request.v1` adds optional `retry_of_run_id`. A retry
  creates a distinct run linked to a terminal run in the same owned
  conversation; existing history and idempotent replays remain immutable.
- **Security:** retention-zero, deleted, expired, cross-tenant, and cross-user
  transcript reads fail with the same not-found response. Tool payloads,
  provider content, prompts, credentials, and rendered storage content are not
  transcript fields.
- **Reliability:** closing the browser stream signals cancellation and waits for
  a durable `cancelled` terminal state before the stream task is released.

## Ask Dev Wave 3 full-page workspace and shared UI core release note

- **New:** `dev-health-web` adds the full-page `/dev` investigation workspace
  and a shared conversational UI core (conversation client, SSE parser/state
  machine, transcript, structured `dev_answer.v1` renderer, citation/evidence
  and metric components, history/retention UI) consumed by both `/dev` and the
  permanent app-shell Ask Dev window through one `AskDevProvider` instance
  mounted in the authenticated app layout. Same-origin, server-only
  `/api/v1/dev/**` route handlers proxy every browser call; access tokens,
  provider credentials, and raw upstream error bodies never reach the browser.
- **Changed:** none of the existing platform-admin Context Fabric Validation
  console (`/superadmin/context-fabric/validation`) changes; it remains an
  independent, LLM-independent diagnostic surface not reachable from `/dev` or
  the Ask Dev window.
- **Known limited:** the composer's per-conversation retention selector shown
  in an earlier build was removed — retention is organization-admin policy
  only (0-day or 30-day) and was never honored per-conversation server-side.
  Live verification against a real OpenAI-backed organization surfaced a
  release-blocking defect in the Ops-side tool-call adapter (dotted
  `*.v1`-suffixed tool names are rejected by the OpenAI Chat Completions API),
  tracked separately as CHAOS-3286; it blocks the real-provider answer path
  for both platform and BYO OpenAI connections until fixed and does not
  originate in this release's web code.
- **Reliability/accessibility hardening:** a follow-up hardening pass (tracked
  under CHAOS-3215) fixes a cross-tenant conversation-state leak on
  organization switch, a request-race that could route a question to the
  wrong conversation, screen-reader announcement noise during streaming,
  reduced-motion support, and several keyboard/focus gaps found during
  adversarial review; see the linked pull request for the exact fix list.
- **Rollback:** disable Ask Dev or roll back the `dev-health-web` binary; no
  database migration is introduced by the web-side workspace or shared UI
  core.
