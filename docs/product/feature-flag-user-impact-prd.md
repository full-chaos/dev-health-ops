# Feature Flag and User Impact Mapping PRD

_Last updated: 2026-02-27_

> **Document Status**: Reviewed — corrections applied from gap analysis (see `feature-flag-prd-review.md`)

## One sentence
Add a privacy-safe analytics layer that links releases, feature-flag rollouts, and aggregated user-friction signals to existing Dev Health work metrics and work graph evidence.

## Purpose
Dev Health already maps engineering activity to effort and flow. This PRD adds a new lens: how shipped changes and rollout decisions appear to affect user experience at team/release scope.

This document scopes:
- data model additions for flags, releases, and telemetry signal buckets
- candidate metrics to evaluate release and rollout impact
- prototype experiments to measure signal-to-noise before broad productization

## What This Is Not
- Individual performance scoring
- Session replay analytics product
- Causal proof engine
- Taxonomy replacement for Investment View

## Guardrails (Non-negotiable)
- No person-to-person comparisons; team/repo/release scopes only
- WorkUnits remain evidence containers, not categories
- Existing canonical investment taxonomy stays fixed
- LLM remains compute-time categorization only; no UX-time recategorization
- Persistence only through sinks and analytics tables; no ad hoc file outputs
- User telemetry stored as aggregated signals only by default (no raw replay payloads)
- Every computed metric must be traceable to source events and window definitions

## Problem Statement
Teams can currently answer "what work happened" and "where effort went," but cannot reliably answer:
- Which releases or rollout changes coincided with user friction shifts?
- Which flags were active when a specific issue spike appeared?
- Whether a release likely improved or worsened user-facing outcomes, with confidence bounds.

## Users and User Stories
- Engineering manager: "Show me which recent releases had elevated friction and whether the rollout path used flags."
- Tech lead: "From an incident or spike in issues, trace back to deployment, related PRs/issues, and active flags."
- Product manager: "Compare impact between partial rollout and full rollout for similar features."
- Platform/data team: "Assess whether the signal is reliable enough for dashboard exposure."

## Scope
### In scope (MVP)
- Ingest and normalize feature-flag lifecycle events (create/update/toggle/rule/rollout)
- Ingest and normalize aggregated user telemetry signals (errors, friction, adoption counters)
- Link releases/deployments to PRs/issues using existing join keys
- Compute release and flag impact metrics at team/repo/release level
- Add inspectable confidence and coverage fields to impact outputs

### Out of scope (MVP)
- Raw session replay storage and playback
- Individual-level telemetry attribution
- Automated causal claims in UI
- Experiment platform replacement

## Current Platform Anchors
Existing assets this plan builds on:
- Daily metrics schema and record contracts (`repo_metrics_daily`, `work_item_metrics_daily`, `deploy_metrics_daily`, `incident_metrics_daily`)
- Work graph edges with `provenance` and `confidence`
- Deployment linkage via `pull_request_number` and merge/deploy timestamps
- Ingest API pattern for batched async stream-backed payloads

## Proposed Canonical Entities

### New event entities

- `feature_flag` (raw dimension — provider-synced flag registry)
  - `org_id`, `provider`, `flag_key`, `project_key`, `repo_id`, `environment`, `flag_type`, `created_at`, `archived_at`, `last_synced`

- `feature_flag_event` (raw event — flag lifecycle changes)
  - `org_id`, `event_type`, `flag_key`, `environment`, `repo_id`, `actor_type`, `prev_state`, `next_state`, `event_ts`, `ingested_at`, `source_event_id`, `dedupe_key`

- `feature_flag_link` (raw linkage — flag-to-entity relationships)
  - `org_id`, `flag_key`, `target_type` (`issue`/`pr`/`release`), `target_id`, `provider`, `link_source`, `link_type` (`code_reference`/`configuration`/`issue_tag`/`rollout_issue`), `evidence_type`, `confidence`, `valid_from`, `valid_to`, `last_synced`

- `telemetry_signal_bucket` (raw event — aggregated user impact counters)
  - `org_id`, `signal_type`, `signal_count`, `session_count`, `unique_pseudonymous_count` (nullable, k-anonymity gated), `endpoint_group`, `environment`, `repo_id`, `release_ref`, `bucket_start`, `bucket_end`, `ingested_at`, `is_sampled`, `schema_version`, `dedupe_key`

### New derived entity

- `release_impact_daily` (derived metric — computed from raw events)
  - `org_id`, `release_ref`, `environment`, `repo_id`, release and flag impact rollups with confidence, coverage, `data_completeness`, and `concurrent_deploy_count` (append-only with `computed_at`)

### Storage contracts

All tables use ClickHouse as the analytics backend. `org_id` is the **first** element in every `ORDER BY` clause per migration 027.

| Entity | Classification | Engine | PARTITION BY | ORDER BY |
|--------|---------------|--------|-------------|----------|
| `feature_flag` | Raw dimension | `ReplacingMergeTree(last_synced)` | — | `(org_id, provider, flag_key)` |
| `feature_flag_event` | Raw event | `MergeTree()` | `toYYYYMM(event_ts)` | `(org_id, flag_key, environment, event_ts)` |
| `feature_flag_link` | Raw linkage | `ReplacingMergeTree(last_synced)` | — | `(org_id, flag_key, target_type, target_id)` |
| `telemetry_signal_bucket` | Raw event | `MergeTree()` | `toYYYYMM(bucket_start)` | `(org_id, environment, repo_id, release_ref, bucket_start)` |
| `release_impact_daily` | Derived metric | `MergeTree()` | `toYYYYMM(day)` | `(org_id, release_ref, environment, day)` |

**Deduplication strategy:**
- `ReplacingMergeTree` tables: last-write-wins via `last_synced` version column; `FINAL` or `argMax` at query time.
- `MergeTree` event tables: dedup via `dedupe_key` at application level (insert-skip or idempotent upsert). For `release_impact_daily`, use `argMax(..., computed_at)` pattern for latest values.

**Postgres semantic layer:** Only `feature_flag` registry metadata (for UX navigation and access control) requires an Alembic migration in Postgres. All event streams and computed metrics are ClickHouse-only.

**Migration approach:** New tables use explicit DDL migrations in `migrations/clickhouse/`, consistent with existing patterns (e.g., `000_raw_tables.sql`, `014_work_graph.sql`). Not auto-created.

### Sink interface extensions (Phase 0 deliverable)

The following methods must be added to `BaseMetricsSink` and implemented in `ClickHouseMetricsSink`:

- `write_feature_flags(records: list[FeatureFlagRecord])` — upsert flag registry
- `write_feature_flag_events(records: list[FeatureFlagEventRecord])` — append flag lifecycle events
- `write_feature_flag_links(records: list[FeatureFlagLinkRecord])` — upsert flag-entity links
- `write_telemetry_signal_buckets(records: list[TelemetrySignalBucketRecord])` — append telemetry buckets
- `write_release_impact_daily(records: list[ReleaseImpactDailyRecord])` — append derived impact rollups

Corresponding record dataclasses must be added to `metrics/schemas.py`.

## Measurement Contract
### Event taxonomy (MVP)
- `feature_flag.change` (management-plane: create/update/toggle/rule/rollout)
- `feature_flag.exposure` (data-plane: bucketed exposure counts)
- `telemetry.signal` (bucketed user-impact counters: friction/error/adoption)
- `release.deployment` (deployment and release markers)

### Required fields
- temporal: `event_ts` (UTC), `ingested_at`, `bucket_start`, `bucket_end`
- join keys: `org_id`, `provider`, `environment`, `repo_id` or `work_scope_id`, `release_ref`, `flag_key`
- quality: `source_event_id`, `schema_version`, `dedupe_key`, `is_sampled`

### Identity and privacy strategy
- keep developer identities and end-user telemetry identities disjoint
- telemetry dimensions default to aggregate-only keys; no raw personal identifiers in analytics tables
- if deduplication requires identity, use irreversible pseudonymous IDs only
- k-anonymity threshold: default `k=5`, enforced at query layer before any segmentable view is returned; configurable per org

### Missingness and instrumentation drift
- store `coverage_ratio` and `missing_required_fields_count` per computed output
- include `instrumentation_change_flag` when schema version/volume shifts exceed threshold
- suppress derived metrics when required fields drop below threshold

### Release ref canonical format

`release_ref` is the canonical release identifier used to join deployments to telemetry. Format varies by provider:

| Provider | `release_ref` format | Source |
|----------|---------------------|--------|
| GitHub | Git tag (e.g., `v1.2.3`) or `deployment_id` fallback | Releases API / Deployments API |
| GitLab | Git tag or environment-scoped `deployment_iid` | Releases API / Deployments API |
| Generic | `deployment_id` (opaque string) | Ingest API payload |

**Enrichment strategy:** A new processor step maps `deployment_id` + provider metadata to `release_ref` during ingestion. When no tag/version is available, `deployment_id` is used as fallback with confidence downgrade (heuristic provenance, confidence 0.3).

**When `release_ref` is unavailable:** Impact metrics are suppressed for that deployment (coverage ratio reflects the gap). The deployment still appears in work graph edges but with no telemetry linkage.

### Late data and recomputation contract

Telemetry and flag events may arrive late (mobile clients, batch exports, retries).

- **Recomputation window:** `release_impact_daily` is recomputed for the last 7 days on every metrics run. Older rows are considered stable unless an explicit backfill is triggered.
- **Stability SLA:** Data older than 14 days is treated as stable for dashboard display. A `data_completeness` field (0.0–1.0) is stored per day/release to indicate ingestion completeness.
- **Dual timestamps:** All raw events store both `event_ts` (when it happened) and `ingested_at` (when we received it). Metric queries use `event_ts` for windowing; `ingested_at` is used to detect late arrivals.
- **UI contract:** When `data_completeness < 0.80` for a displayed period, UI must show a "data still arriving" indicator.

## Proposed Work Graph Extensions

### Node types (proposed)
- `release`
- `feature_flag`

### Edge types (proposed)
- `introduced_by` (release <- PR): evidence = provider release/deployment API linking PR to release
- `config_changed_by` (feature_flag <- flag_event): evidence = provider audit log event ID; only created when explicit provider evidence links a flag change to a release or PR (e.g., GitLab `introduced_by_url`). **Not created from time-window heuristics alone.**
- `guards` (feature_flag -> issue/epic/scope): evidence = provider `rollout_issue_url` or explicit tag/label
- `impacts` (release/feature_flag -> telemetry_signal_bucket): evidence = `release_ref` + environment match; note: `telemetry_signal_bucket` is joined at query-time, not stored as a first-class work graph node

> **Design decision:** The originally proposed `rolls_out` edge (release -> feature_flag) is removed. Flag rollouts are config changes that happen independently of code releases. The `config_changed_by` edge captures the narrower, evidence-backed relationship. Reuse existing `REFERENCES` edge type for "PR/commit mentions flag key" relationships detected via code search.

### Provenance and confidence model
- Keep current provenance model (`native`, `explicit_text`, `heuristic`)
- Extend evidence to include provider event IDs and normalized source references; evidence remains a JSON-encoded string within the existing `WorkGraphEdge.evidence: str` field (no schema change)
- Confidence scoring bands:
  - `1.0`: direct ID/key linkage from provider-native payload
  - `0.8-0.9`: deterministic text/key mapping (documented pattern)
  - `0.3`: time-window heuristic only (aligned with existing platform heuristic confidence)
- Store `coverage_ratio` for each derived metric (matched_events / eligible_events)

### Builder and API extensions (Phase 2 deliverable)

The following changes are required to support new graph nodes/edges:

**`work_graph/models.py`:**
- Add `RELEASE`, `FEATURE_FLAG` to `NodeType` enum
- Add `INTRODUCED_BY`, `CONFIG_CHANGED_BY`, `GUARDS`, `IMPACTS` to `EdgeType` enum

**`work_graph/ids.py`:**
- Add `generate_release_id(org_id, provider, release_ref)` function
- Add `generate_feature_flag_id(org_id, provider, flag_key)` function

**`work_graph/builder.py`:**
- Add `_build_release_edges()` method (source: deployment/release data)
- Add `_build_feature_flag_edges()` method (source: flag events + links)
- Update `build()` orchestrator to include new discovery steps

**`api/graphql/models/inputs.py` and `outputs.py`:**
- Add new values to `WorkGraphNodeTypeInput`, `WorkGraphEdgeTypeInput`, `WorkGraphNodeType`, `WorkGraphEdgeType` strawberry enums

## Join Strategy (Canonical)
Primary spine:
1. Issue -> PR via work graph issue/PR links
2. PR -> Deployment via `pull_request_number`
3. Deployment -> Release ref via provider-specific mapping (see "Release ref canonical format" above)
4. Release/Flag -> telemetry signal buckets via `release_ref` + `environment` + time window

Secondary joins (lower confidence):
- Time-window-only attribution when explicit linkage is missing (confidence = 0.3)
- Branch/tag fallback mapping where release ID absent

**Coverage metrics (shipped alongside impact):**
- `%PRs linked to deployments` — join completeness step 1→2
- `%deployments with release_ref` — join completeness step 2→3
- `%releases with telemetry coverage` — join completeness step 3→4

## Metric Catalog (Candidate)
All metrics are team/repo/release scoped unless explicitly stated.

### Release impact metrics

| Metric Key | Unit | Formula | Window | Min Sample | Notes |
|---|---|---|---|---|---|
| `release_user_friction_delta` | ratio | `(mean(friction_signals / session_count, post_window) - mean(friction_signals / session_count, baseline_window)) / mean(friction_signals / session_count, baseline_window)` | baseline: 7d pre-deploy; post: 24-72h post-deploy | 300 sessions in both windows | Numerator: `signal_type IN ('friction.*')`. Denominator: `session_count`. Environment-filtered. Do not label causal. Companion absolute: `release_post_friction_rate`. |
| `release_error_rate_delta` | ratio | `(mean(error_signals / session_count, post_window) - mean(error_signals / session_count, baseline_window)) / mean(error_signals / session_count, baseline_window)` | baseline: 7d pre-deploy; post: 24-72h post-deploy | 1000 events across both windows | Numerator: `signal_type IN ('error.*')`. Denominator: `session_count`. Environment-segmented. Companion absolute: `release_post_error_rate`. |
| `time_to_first_user_issue_after_release` | hours | `min(issue.created_at) - deployment.completed_at` where issue is linked via work graph to same release | post: 72h | 1 linked issue | P50/P90 at repo aggregate. "User issue" = work item with `signal_type = 'user_reported'` tag or label-based heuristic. |
| `release_impact_confidence_score` | 0..1 | `w1 * linkage_quality + w2 * coverage_ratio + w3 * sample_sufficiency` (weights TBD in Phase 2) | per release | n/a | Not a business KPI. Weights calibrated during prototype. |
| `release_impact_coverage_ratio` | 0..1 | `count(telemetry_buckets matched to release) / count(telemetry_buckets in environment window)` | per release | n/a | Display with every impact metric. |

### Flag metrics

| Metric Key | Unit | Formula | Window | Min Sample | Notes |
|---|---|---|---|---|---|
| `flag_exposure_rate` | ratio | `exposed_sessions / eligible_sessions` | session window to 24h | 200 eligible sessions | MVP uses session-based denominator (not users). `eligible_sessions` = sessions in environment where flag is evaluated. If `unique_pseudonymous_count` is available and meets k>=5, user-based variant can be computed as Phase 2+ metric. |
| `flag_activation_rate` | ratio | `activated_sessions / exposed_sessions` | session window to 24h | 100 exposed sessions | "Activated" = session with at least one defined success action after flag exposure. Action contract defined per flag in `feature_flag_link` config. MVP uses session-based denominator. |
| `flag_reliability_guardrail` | ratio | `error_free_sessions / total_sessions` for exposed cohort proxy | session to 24h | 300 sessions | Guardrail metric, not success KPI. Numerator: sessions with zero `signal_type IN ('error.*', 'crash.*')`. |
| `flag_friction_delta` | ratio | Same formula as `release_user_friction_delta` but scoped to flag exposure window | baseline: 7d pre first rollout event; post: 24-72h | 200 sessions | Requires session-level flag evaluation signal. Exclude contaminated sessions (multiple concurrent flags). |
| `flag_rollout_half_life` | hours | `event_ts(50% exposure marker) - event_ts(first rollout event)` | rollout period | 2 rollout events | Provider-specific: LaunchDarkly percentage rollout events; GitLab incremental strategy steps. |
| `flag_churn_rate` | count/week | `count(flag_events WHERE event_type IN ('toggle', 'rule_change')) / weeks_in_window` | rolling 28d | n/a | Operational volatility indicator. |

### Data quality / linkage metrics

| Metric Key | Unit | Formula | Window | Min Sample | Notes |
|---|---|---|---|---|---|
| `issue_to_release_impact_link_rate` | 0..1 | `count(completed_work_items with release_impact_coverage >= 0.5) / count(completed_work_items)` | rolling 30d | 50 work items | "Completed" per existing work-item completion semantics (`completed_at IS NOT NULL`). Data quality signal. |
| `rollback_or_disable_after_impact_spike` | count | `count(flag_events WHERE event_type IN ('toggle_off', 'rollback', 'disable') AND event_ts BETWEEN release_deploy_ts AND release_deploy_ts + 72h)` | post 72h from deploy | n/a | Stability response marker. No dependency on undefined alert mechanism. |

## Prototype Metrics Use-Cases
### Use-case A: Release regression detector
- Input: deployment events + telemetry friction/error buckets
- Output: ranked release windows by normalized impact delta and confidence
- Success: high-confidence windows align with known incidents/issue spikes in retrospective review

### Use-case B: Flag rollout safety monitor
- Input: flag events + rollout progression + telemetry buckets
- Output: rollout stages with friction deltas, disable/rollback correlation
- Success: can distinguish stable rollout from rollback-prone rollout with low false alarms

### Use-case C: Work-to-user impact traceability
- Input: issue->PR->deployment edges + release impact rollups
- Output: % of completed work with measurable post-release signal signature
- Success: usable coverage without introducing person-level attribution

## Signal-to-Noise Validation Plan
### Hypotheses
- H1: high-confidence release mappings produce materially lower false-positive impact alerts than heuristic-only mappings.
- H2: adding flag events improves explanatory power for post-release anomalies.
- H3: coverage and confidence thresholds reduce over-interpretation risk.

### Validation dataset and windows
- Backfill 90 days for 3-5 active repos/environments
- Compare baseline windows (7d pre) against post windows (24h, 48h, 72h)
- Include incident and issue timelines as retrospective labels

### Prototype evaluation metrics
- precision/recall of impact alerts against incident windows
- false-positive rate by confidence band
- lift in explanatory coverage when flag events are included
- percentage of releases meeting minimum sample thresholds

### Acceptance thresholds (prototype gate)
- >= 0.65 precision for high-confidence alerts on retrospective incident windows
- <= 0.20 false-positive rate in high-confidence band
- >= 0.60 release coverage for target repos
- No violations of privacy guardrails in sampled payload audits

## Confidence and Coverage Gates
### Show/warn/suppress rules
- show: coverage >= 0.70 and minimum sample threshold met
- warn: coverage 0.50-0.69 or instrumentation drift flag present
- suppress: coverage < 0.50 or denominator below minimum sample

### Overlap and contamination rules
- when users/sessions can belong to multiple concurrent rollouts, mark contaminated cohorts
- exclude contaminated cohorts from MVP impact deltas unless explicit precedence rule is configured
- always publish contaminated-cohort percentage with impact outputs

## Confounders and Bias Controls
- concurrent releases in same environment
- seasonality/day-of-week traffic changes
- incident remediation activity independent of release
- telemetry collector outages and sampling changes
- survivorship bias: only instrumented surfaces generate telemetry; publish observability scope fields and missingness by scope

Mitigations:
- store concurrent-deploy count and confounder flags with each impact result
- require minimum denominator/sample thresholds before metric publication
- expose confidence and coverage alongside all deltas
- default UI language to "appears/suggests" and ban deterministic causal wording
- compute per-segment first, then aggregate with explicit weights (avoid Simpson's paradox in rollups); require stratified outputs by repo and environment for any headline metric

## Interpretation and Claims Policy
- Allowed language: "appears", "suggests", "leans", "is consistent with"
- Forbidden language (MVP): "caused", "proved", "determined"
- All release/flag impact views must display: attribution window, confidence, coverage, and confounder context
- No metric may be presented as an individual accountability signal
- Frame all impact as observational and conditioned on coverage; require "unattributed due to missing linkage" bucket when join chain is incomplete (distinct from "unknown" categorization, which remains forbidden)

## Data Privacy and Security
- Aggregated telemetry by default, no raw session payload persistence in analytics tables
- Pseudonymous identifiers only if needed for deduplication
- allowlist-only telemetry fields; drop free-form text by default
- k-anonymity threshold: default `k=5`, enforced at query layer; configurable per org with minimum floor of `k=3`
- retention policy: raw ingest buffers 30 days, derived aggregates 365 days (configurable per org)

## Operational Constraints

### Volume estimates (per repo/environment/day)
- Feature flag events: ~10-500 events (management-plane changes are infrequent)
- Telemetry signal buckets: ~100-10,000 buckets (depends on bucket granularity and endpoint count)
- Release impact daily: ~1-50 rows (one per release per environment per day)

### Default bucket granularity
- Telemetry buckets: 1-hour windows (configurable down to 15 minutes for high-traffic environments)
- Exposure buckets: 1-hour windows

### Sampling policy
- When `is_sampled = true`, store `sample_rate` field; all rate metrics must adjust by `1 / sample_rate`
- Default: no sampling (ingest all). Sampling opt-in per org when volume exceeds threshold.

### Retention / TTL
- Raw event tables (`feature_flag_event`, `telemetry_signal_bucket`): 90 days default
- Dimension tables (`feature_flag`, `feature_flag_link`): no TTL (retain until archived)
- Derived tables (`release_impact_daily`): 365 days default
- All TTLs configurable per org via `org_settings`

## Delivery Phases

### Phase 0: Schema and contract design
- finalize canonical entities, join keys, windows, confidence/coverage fields
- define ClickHouse migration DDL for all 5 tables (explicit migrations in `migrations/clickhouse/`)
- define Alembic migration for `feature_flag` registry in Postgres semantic layer
- implement sink interface extensions (5 new `write_*` methods in `BaseMetricsSink` + `ClickHouseMetricsSink`)
- add record dataclasses to `metrics/schemas.py`

### Phase 1: Ingestion and normalization MVP
- **MVP providers:** GitHub (releases/deployments — already integrated), LaunchDarkly (feature flags — richest audit log + data export API), generic ingest API (telemetry signals — provider-agnostic)
- add provider connectors/processors for flag events and telemetry buckets
- persist canonical events to ClickHouse through sinks
- implement `release_ref` enrichment processor (deployment → release ref mapping)

### Phase 2: Derived metrics and graph edges
- compute `release_impact_daily` and related rollups with recomputation window
- add proposed graph nodes/edges with provenance/confidence
- implement builder extensions (`_build_release_edges`, `_build_feature_flag_edges`)
- update GraphQL enums and ID generation functions
- calibrate `release_impact_confidence_score` weights

### Phase 3: Prototype dashboards and interpretation guidance
- add internal-only views for release/flag impact validation
- iterate thresholds and copy based on prototype results

### Phase 4: Productization decision
- promote selected metrics to stable registry only if acceptance thresholds hold
- document rejected metrics and reasons (low signal/high misuse risk)

## Engineering Test Requirements

### Unit tests
- Join logic: Issue→PR→Deployment→Release ref mapping for each provider
- Dedup-key enforcement: verify `dedupe_key` prevents duplicate inserts for all raw event types
- Metric formulas: verify numerator/denominator computation for each candidate metric with known inputs
- Confidence scoring: verify band assignment for native, explicit_text, and heuristic provenance

### Integration tests
- Drift gate behavior: verify `instrumentation_change_flag` triggers when schema version or volume shifts
- Coverage suppression: verify metrics are suppressed when coverage < 0.50
- Late data: verify recomputation window correctly updates `release_impact_daily` for last 7 days
- Sink round-trip: write records via sink, read back via query, verify field integrity

### Smoke tests
- Backfill job writes append-only with `computed_at` (no overwrites)
- `org_id` isolation: verify cross-org queries return no results
- Environment normalization: verify environment strings match between deployments and telemetry

## Success Criteria
- New release/flag impact metrics are inspectable and reproducible from persisted events
- Coverage and confidence are included for all impact outputs
- Prototype evaluation meets acceptance thresholds for signal quality
- No person-level ranking surfaces are created
- Work graph can trace issue -> release/flag -> user impact buckets for supported providers
- All 5 new tables have explicit ClickHouse migrations with `org_id` first in ORDER BY

## Risks and Mitigations
| Risk | Impact | Mitigation |
|---|---|---|
| Identity conflation (developer vs end-user) | High | hard schema separation; no developer dimension in telemetry impact tables |
| Causal over-claim in UI | High | enforced wording guidelines; confidence + confounder display |
| Low coverage from missing release refs | Medium | dual linkage methods; explicit coverage metric; confidence downgrade for fallback |
| Telemetry volume/cost explosion | Medium | bucketed ingestion (1h default), sampling opt-in, retention TTLs (90d raw / 365d derived) |
| Provider API variability | Medium | provider-specific mappers behind canonical event contract |
| Out-of-order / late telemetry | Medium | 7-day recomputation window; dual timestamps; `data_completeness` field in UI |
| Survivorship bias (instrumented surfaces only) | Medium | observability scope fields; missingness/coverage published per scope |

## Decision Gates
- Gate A (end Phase 0): approve schema, storage contracts, sink interfaces, and privacy contract
- Gate B (end Phase 2): approve prototype metric set for internal validation; confirm builder/API extensions
- Gate C (end Phase 4): promote only metrics that pass signal thresholds

## Open Questions for Iteration
- Which telemetry signal families are required for MVP: friction, error, adoption, or all three?
- What should be the default post-release attribution window by environment type?
- Should rollout half-life rely on provider exposure events or a deterministic proxy?
- Which teams/repos are best pilot candidates for representative signal validation?
- What is the right `release_impact_confidence_score` weight distribution (w1/w2/w3)?

## References
- `docs/product/prd.md`
- `docs/product/concepts.md`
- `docs/product/feature-flag-prd-review.md` (gap analysis)
- `docs/metrics.md`
- `docs/user-guide/work-graph.md`
- `src/dev_health_ops/work_graph/models.py`
- `src/dev_health_ops/metrics/schemas.py`
- `src/dev_health_ops/metrics/sinks/base.py`
