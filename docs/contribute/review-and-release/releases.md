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
types. It does not ship orchestration, persistence, API routes, or UI.

Before release, run the ops contract check and migration tests and the web
generated-type check. Rollback may leave the additive feature row in place; it
remains effectively disabled unless explicitly granted, and the global feature
decision is still the emergency kill switch.

## Ask Dev Wave 1 release note

- **New:** additive PostgreSQL conversation, message, run, safe tool-audit,
  feedback, and minimal deletion-tombstone storage behind the disabled Ask Dev
  feature.
- **Changed:** the retention job contract adds version 3 and the
  table-scoped `ask_dev_conversations` policy; versions 1 and 2 remain accepted
  with their original frozen policy domains.
- **Known limited:** this storage release does not enable Ask Dev or replace
  existing LLM workflows. UI/API/orchestrator owners must use the canonical
  persistence service and validated `dev_answer.v1` seam before rollout.
- **Rollback:** disable Ask Dev normally. Downgrade migration `0068` only in a
  pre-release environment after new binaries are stopped because it deletes
  all Ask Dev tables and their contents.
