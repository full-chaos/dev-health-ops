# CHAOS-3024 Work Plan v9 — Frozen Scope and Convergent Review

## Status

**Planning outcome:** replace Work Plan v8.  
**Implementation approval state:** `awaiting-domain-review`.  
**Release state:** default off; no customer enablement is authorized by this plan.

This plan changes planning and review governance only. It does not enable PagerDuty, GitLab native incidents, Atlassian operational incidents, External Push operational records, or canonical incident consumers.

## Outcome

Ship a default-off organization feature, `canonical_incident_ingestion`, that safely contains the new canonical operational ingestion paths while the existing provider contracts are corrected:

1. PagerDuty uses one backend-owned operational target, correct credentials/sync separation, and the already-selected V1 event contract.
2. GitLab ingests native incidents by provider type (`issue_type=incident`), not by label.
3. GitHub is excluded from the native incident-provider model; ordinary GitHub issues remain work items.
4. Atlassian/JSM Operations and Opsgenie are enabled only for capabilities proven by the current client and live evidence.
5. External Push operational records are gated and proven through the existing canonical identity, ownership, and sink paths.
6. Canonical consumers switch per organization and fall back to established legacy reads when the flag is off.
7. Final controlled enablement is owned by CHAOS-3031 and remains separate from implementation merge approval.

## Why v8 is replaced

V8 expanded a release-gating remediation into a new distributed transaction, source lifecycle, lease, write-intent, receipt, and cross-repository release system. That duplicated existing platform primitives and introduced internal contradictions.

V9 deliberately reuses:

- `FeatureFlag`, `OrgFeatureOverride`, and the existing entitlement response;
- `Integration`, `IntegrationSource`, and `IntegrationDataset`;
- existing `SyncRunUnit` lease/retry fields and dispatch outbox claims;
- deterministic canonical operational IDs;
- `ReplacingMergeTree(source_version_at)` ordering;
- the existing `IngestionSink.insert_operational_batch` / `write_operational_batch` boundary;
- provider-specific idempotency already present in REST sync, webhooks, and External Push;
- normal repository CI, GitHub Actions artifacts, PR evidence, and Linear issue relationships.

V9 does **not** introduce:

- `SourceState` lifecycle/mode/generation tables;
- a second canonical incident work-lease system;
- intent columns or payload digests on all twelve ClickHouse tables;
- a universal commit/readback/quarantine transaction protocol;
- a bespoke receipt/manifest/aggregate package;
- a new Celery queue;
- one immutable run ID spanning every implementation attempt;
- automatic provider enablement;
- a GitHub native incident source.

## Frozen product decisions

### Source matrix

| Source | Product status | Selection contract | Release requirement |
| --- | --- | --- | --- |
| PagerDuty | Native incident-response provider | PagerDuty REST plus selected V3 webhook events | GO required |
| GitLab | Native incident source | GitLab issue/work-item type `incident` | GO required |
| Atlassian JSM Operations | Native only where current source APIs expose operational entities | Capability inventory decides GO / NO_GO | GO or authoritative NO_GO |
| Opsgenie | Native only where current source APIs expose operational entities | Capability inventory decides GO / NO_GO | GO or authoritative NO_GO |
| External Push | Customer-provided canonical operational facts | Existing versioned operational record kinds | GO required |
| GitHub | Not a native incident provider | Ordinary issues remain work items | EXCLUDED; legacy proxy inventoried separately |

### Feature behavior

`canonical_incident_ingestion` is an explicit-purchase-style organization feature:

- it is present in the canonical feature registry;
- it is globally available for an explicit organization override;
- it evaluates to false for every tier by default;
- no existing or new organization is enabled by migration;
- `OrgFeatureOverride.config` is unchanged;
- removing the override is the rollback mechanism.

### Feature-off guarantees

With the feature off:

- PagerDuty cannot be newly connected or added to a new sync configuration;
- PagerDuty status, disconnect, deletion, and secret cleanup remain available for pre-existing records;
- PagerDuty webhook requests do not enqueue canonical work;
- GitLab native-incident canonical planning and writes are disabled, but ordinary GitLab work-item/code ingestion continues;
- GitHub cannot write or backfill canonical incidents;
- External Push does not advertise or accept operational record kinds;
- Atlassian/JSM canonical dual-writes and canonical consumer cutovers are disabled;
- canonical consumers use established legacy reads;
- stored canonical data is not deleted.

### Rollback semantics

This plan does not promise an instantaneous distributed kill of provider calls already in flight.

The supported rollback contract is:

1. API, scheduler, and planner checks prevent new gated work.
2. Workers check the organization feature before provider execution.
3. The canonical operational write boundary checks the feature immediately before a gated batch write.
4. Removing the override prevents later canonical writes and returns consumers to legacy reads.
5. Previously stored canonical rows remain for audit and later reconciliation.

Existing `SyncRunUnit` lease/retry behavior owns worker crash and duplicate-delivery handling. Provider-specific event IDs, ingestion IDs, deterministic entity IDs, and `source_version_at` own idempotency and ordering. A new cross-database transaction protocol is out of scope.

## Review convergence contract

This section is normative. Reviewers must review against it rather than inventing a new rubric each cycle.

### Review domains

- **Momus:** product scope, source semantics, issue traceability, UX/config separation, and absence of scope expansion.
- **Oracle:** technical correctness, tenant isolation, data-loss risk, rollback behavior, and test sufficiency.
- A reviewer may raise a finding outside its domain only for a concrete security, cross-tenant, irreversible data-loss, or credential-exposure risk.

### Finding ledger

All findings live in `.omo/drafts/chaos-3024.md` and have:

- stable ID (`M-###`, `O-###`, or `A-###` for adjudication);
- plan SHA and reviewed section;
- domain and severity (`blocker` or `non-blocking`);
- repository or provider evidence;
- the minimal closure condition;
- status: `open`, `resolved`, `accepted-risk`, or `follow-up`.

A plan revision may change only:

1. text tied to an open finding ID;
2. mechanically affected traceability/DAG text;
3. factual corrections supported by primary provider documentation or merged repository code.

Unrelated rewrites are prohibited during closure.

### Review sequence

1. **One baseline full review** by each reviewer against this frozen rubric.
2. **One delta review** limited to changed lines and closure of existing finding IDs.
3. **One adjudication pass** only if the two reviewers disagree about an open blocker.

No repeated “fresh unconditional PASS” loop is required.

### Late finding rule

A new blocker against unchanged text after the baseline review is invalid unless it demonstrates:

- credential or secret exposure;
- cross-tenant access;
- irreversible or silent data loss;
- an execution path that bypasses the default-off gate;
- a provider semantic claim contradicted by current primary documentation.

Other late findings become non-blocking follow-ups.

### Pass condition

Implementation planning passes when:

- every baseline finding has a terminal status;
- zero blocker IDs remain open;
- any accepted risk names an owner and Linear issue;
- each reviewer signs only its domain;
- conflicts have one recorded adjudication result.

Implementation merge does not require a live customer canary. Controlled enablement and owner approval belong to CHAOS-3031.

## Implementation tasks

### Task 1 — Organization feature gate and consumer fallback

**Issues:** CHAOS-3025, parent CHAOS-3024.

Add `canonical_incident_ingestion` to the canonical feature registry and seed its `FeatureFlag` row so it is globally available but false without an explicit org override.

Create one shared org-feature evaluator used by async API code and synchronous worker code. Its semantics must match the entitlement response, including unexpired `OrgFeatureOverride`; do not rely on the current decorator fallback if it does not evaluate org overrides consistently.

Enforce the gate at:

- provider/catalog and sync-target discovery;
- PagerDuty credential creation/authorization;
- sync-config create/update/trigger/backfill;
- scheduler and planner;
- worker entry and the canonical operational batch-write boundary;
- PagerDuty webhook receive and worker processing;
- External Push schema discovery, validation, and acceptance;
- canonical incident consumer selection.

Keep status/inspect/disconnect/delete paths available for pre-existing PagerDuty records.

**Acceptance:**

- default false for existing and new orgs;
- explicit test-org override enables only gated paths;
- removing the override blocks the next plan/dispatch/write;
- ordinary GitHub/GitLab/Jira work-item and code ingestion is unchanged;
- canonical consumers revert to legacy reads without deleting data;
- API, planner, worker, webhook, External Push, and consumer tests cover feature off/on/rollback.

**Verification:**

- targeted feature/licensing tests plus new gate tests;
- existing planner, sync-unit, webhook, and consumer regression tests;
- full Ops CI before merge.

### Task 2 — PagerDuty backend contract

**Issues:** CHAOS-3026, CHAOS-3018, relevant backend portion of CHAOS-3027.

Make the backend provider/dataset registry the sole PagerDuty target contract.

For V1 expose one target, `operational`, expanding to exactly these eleven dataset keys:

1. `services`
2. `business-services`
3. `escalation-policies`
4. `schedules`
5. `on-calls`
6. `users`
7. `teams`
8. `incidents`
9. `incident-alerts`
10. `incident-log-entries`
11. `incident-notes`

The target must include parent `incidents`; child enrichment datasets cannot be materialized without it.

OAuth requests the full read-only V1 scope bundle derived from this target. API-token and client-credentials setup performs a real secret-safe provider validation before becoming usable. Stored state, last live test, and permission readiness are separate API facts.

Preserve the existing selected V1 PagerDuty webhook event allowlist. Do not expand scope merely because PagerDuty documents additional events. Validate the current event-to-entity mapping, treat the official subscription ping as a no-write test, and reject/ignore unsupported events safely according to the typed API contract.

Bind canonical webhook writes to an organization-scoped stored connection/subscription and `IntegrationSource`. An environment-only secret may validate a request but must not create canonical state without that binding.

Use the existing `webhooks` queue and current Redis-stream/Celery handoff. Do not create `system-webhook` or another queue.

**Acceptance:**

- API and web consume the same target registry;
- `operational` creates all eleven dataset rows including `incidents`;
- OAuth scopes cover all eleven datasets through the existing eight scope families;
- invalid manual credentials cannot be marked connected/ready;
- partial permission results are dataset-specific;
- all selected V1 webhook events, ping, duplicate, invalid signature, replay, out-of-order, and subscription isolation tests pass;
- feature-off requests fail closed while cleanup remains available.

**Verification:**

Use existing PagerDuty client, OAuth, webhook, worker, dataset-adapter, and sync tests plus focused new contract tests. Live PagerDuty evidence is env-gated and separate from hermetic scratch infrastructure.

### Task 3 — PagerDuty credential/sync UX separation

**Issue:** CHAOS-3027.

Restore the shared information architecture:

**Providers → PagerDuty**

- credential inventory rows;
- account/subdomain, region, auth method, stored state, last live test;
- dependent sync-config count;
- explicit Test, Reconnect/Rotate, and Disconnect/Delete actions;
- hosted OAuth, private client credentials, and API-token fallback in a focused add flow.

**Sync Status → New/Edit Sync Config**

- select an existing PagerDuty credential;
- select the backend-owned `operational` target;
- configure service/repository mappings;
- history depth and schedule;
- REST-only versus webhook-enabled mode once the backend contract is present;
- review and initial sync.

Remove dataset/scope controls and mapping/schedule controls from credential setup. Remove the existing-name silent-upsert selector. OAuth callback completion hands off to New Sync Config with PagerDuty and the new credential preselected.

When the feature is off, hide new setup and render pre-existing records in manage/cleanup-only state.

**Acceptance:**

- no credential page contains sync dataset controls;
- no sync page contains secret-entry controls;
- existing credentials are rows, not overwrite choices;
- dependency warnings use real sync-config counts;
- keyboard, mobile, callback, retry, stale-response, and feature-off tests pass;
- current web/current Ops and independent-deploy compatibility are proven in normal CI.

### Task 4 — GitLab native incident selection and reconciliation

**Issues:** CHAOS-3028 and CHAOS-2966.

Extend the GitLab client’s project-issues query with the native `issue_type` filter and ingest native incidents using `issue_type=incident`.

Labels are metadata only. An ordinary issue with an incident-like label is not a native incident.

Preserve:

- provider instance;
- project/repository context;
- global issue ID and IID;
- URL and raw issue type;
- severity;
- source state and timestamps;
- assignees and any incident-specific fields actually exposed by the supported API.

Do not infer acknowledgement, responders, alerts, timeline, or on-call semantics from ordinary issue fields. If a field requires an additional GitLab capability that is not implemented, record that limitation in the capability matrix rather than inventing it.

Reconciliation runs only after a complete successful paginated snapshot. It deactivates native incidents that disappear, become inaccessible within the authoritative source scope, or convert away from the incident type. Label removal alone does not deactivate a still-native incident. Partial/permission/rate-limit-degraded snapshots do not create absence tombstones.

Inventory rows previously captured by the label path. Deduplicate genuine native incidents under stable canonical identities and classify ordinary label-matched rows for compatibility cleanup.

**Acceptance:**

- unlabeled `issue_type=incident` is ingested;
- `issue_type=issue` plus an `incident` label is excluded from the native path;
- native incident plus legacy label is written once;
- severity and supported lifecycle fields persist;
- close/reopen, disappearance, deletion, and type conversion reconcile deterministically;
- partial snapshots do not tombstone;
- ordinary GitLab work items and all non-incident datasets are unchanged;
- feature-off/on/rollback and cross-org/project/instance isolation pass.

### Task 5 — GitHub native-incident exclusion and legacy inventory

**Issue:** CHAOS-3032.

GitHub is absent from native incident provider catalogs and canonical incident target/cutover logic.

Prevent new canonical incident dual-writes and canonical backfills from ordinary GitHub issues. Keep ordinary issue/work-item ingestion unchanged.

For existing configurations and historical rows:

- inventory organizations and consumers using the legacy label proxy;
- prevent new configs from adding the misleading native `incidents` capability;
- retain rollback-compatible legacy reads only until dependencies have an explicit migration/deprecation decision;
- correct provenance so historical proxy rows are not presented as provider-native incidents.

Retiring a dependency discovered by the inventory may require a follow-up issue; it must not expand the native incident release scope.

**Acceptance:**

- no provider registry, UI, API, source matrix, or docs call GitHub a native incident source;
- new configs cannot enter the canonical incident path;
- no canonical dual-write/backfill from GitHub issues;
- normal GitHub repositories, PRs, deployments, CI/CD, security, and work items are unaffected;
- legacy dependencies and their disposition are documented.

### Task 6 — Atlassian/JSM and Opsgenie capability decision

**Issue:** CHAOS-3029.

First produce a checked-in capability matrix from merged code and source API behavior. For each source family classify each entity as:

- `GO`: implemented and proven;
- `NO_GO`: authoritative source/client evidence says it is unsupported or intentionally not shipped;
- `BLOCKED`: expected but unimplemented or unproven.

The matrix covers incidents, alerts, services, schedules/on-call, users/teams/responders, timeline/notes, lifecycle, severity/priority, identities, deletion/tombstones, permissions, and consumer parity.

Only `GO` capabilities get producer/cutover work. `NO_GO` capabilities remain absent from catalogs and do not run producer tests. Any `BLOCKED` capability keeps CHAOS-3029 open.

**Acceptance:**

- ordinary Jira issues remain work items;
- native JSM/Opsgenie operational records are distinguished from Jira issues;
- GO rows have fixture, idempotency, ordering, tombstone, isolation, rollback, and live/provider evidence where credentials exist;
- NO_GO rows cite authoritative evidence and prove catalog absence;
- feature-off retains established behavior;
- consumer cutover is per org and reversible.

Declared NO_GO branches are not test skips. They have deterministic catalog-absence tests. Unexpected test skips fail; expected environment-gated live tests are reported separately and cannot satisfy a required GO proof.

### Task 7 — External Push operational gate and proof

**Issues:** CHAOS-3030 and CHAOS-2965.

Gate the existing twelve operational record kinds as one family. For a disabled organization:

- schema discovery does not advertise them as enabled;
- validation returns a typed feature-disabled result;
- batch acceptance rejects them before facts are written;
- existing non-operational record kinds are unchanged.

Reuse current canonical identities, entity-family ownership, normalization, and sinks. Do not add a universal intent protocol.

Complete live scratch proof for:

- all twelve kinds;
- HTTP acceptance and worker processing on `external-ingest`;
- replay idempotency;
- active/newer/tombstone ordering;
- mixed native/push ownership rejection;
- org/source isolation;
- mapping-driven correlation;
- feature disable/reenable behavior.

Scratch means isolated local Postgres, ClickHouse, and Redis/Valkey. It does not mean a live third-party provider call.

### Task 8 — Final release audit and controlled enablement

**Issue:** CHAOS-3031.

Run after implementation PRs merge. Pin exact Ops and Web SHAs and record standard GitHub Actions run/artifact URLs.

Required source verdicts:

- PagerDuty: `GO`;
- GitLab native: `GO`;
- External Push: `GO`;
- JSM: `GO` or authoritative `NO_GO`;
- Opsgenie: `GO` or authoritative `NO_GO`;
- GitHub: `EXCLUDED`.

Run:

1. feature-off E2E for an ordinary org;
2. feature-on E2E for an explicit test-org override;
3. removal-of-override rollback E2E;
4. current Web/current Ops;
5. independent-deploy compatibility for the changed API surface;
6. source-specific live proofs required for each GO verdict.

Use normal JUnit/Playwright output and GitHub Actions artifacts. Reports must have tests greater than zero and no failures/errors. Unexpected skips fail. Declared provider-live tests without credentials are not counted as GO evidence.

Produce:

- source coverage matrix;
- parity/rollback report;
- auth/rate-limit/queue/mapping/webhook runbook;
- explicit go/no-go decision;
- owner-approved controlled-enable record.

Implementation merges leave the feature default off. No customer org is enabled by deployment.

## Repository and PR strategy

Use separate implementation PRs:

1. **Ops gate + provider contracts** — Tasks 1, 2, 4, 5, 6, and 7 may be split by issue when review size warrants.
2. **Web UX** — Task 3.
3. **Release audit evidence** — Task 8 after implementation merges.

Do not build a local cross-repository receipt framework. Cross-repository compatibility is a final CI workflow that pins the two repository SHAs.

Each implementation PR must:

- link its Linear issue(s);
- state which frozen plan task it implements;
- include targeted test commands and standard CI results;
- contain no source enablement or customer override;
- avoid unrelated refactors;
- record follow-ups for non-blocking findings.

## DAG and traceability

```text
Task 1 (gate)
├── Task 2 (PagerDuty backend) ── Task 3 (PagerDuty web)
├── Task 4 (GitLab native + reconciliation)
├── Task 5 (GitHub exclusion)
├── Task 6 (Atlassian capability decision)
└── Task 7 (External Push)
Tasks 2–7 ── Task 8 (final release audit)
```

| Linear issue | Plan task |
| --- | --- |
| CHAOS-3025 | Task 1 |
| CHAOS-3026 | Task 2 |
| CHAOS-3018 | Tasks 2–3 |
| CHAOS-3027 | Task 3 |
| CHAOS-3028 | Task 4 |
| CHAOS-2966 | Task 4 |
| CHAOS-3032 | Task 5 |
| CHAOS-3029 | Task 6 |
| CHAOS-3030 | Task 7 |
| CHAOS-2965 | Task 7 |
| CHAOS-3031 | Task 8 |
| CHAOS-3024 | Parent outcome |

## Plan success criteria

The plan is implementation-ready when:

- the frozen source matrix and feature behavior are accepted;
- Momus and Oracle each complete one baseline review in their assigned domain;
- the finding ledger has zero open blockers;
- no reviewer requires the removed SourceState/lease/intent/receipt architecture without a separate approved issue;
- each Linear blocker maps to exactly one implementation task;
- all enablement remains default off;
- controlled release remains owned by CHAOS-3031.
