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
