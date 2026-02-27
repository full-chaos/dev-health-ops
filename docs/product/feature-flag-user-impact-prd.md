# Feature Flag and User Impact Mapping PRD

_Last updated: 2026-02-27_

> **Document Status**: Draft for iteration

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
- `feature_flag`
  - `provider`, `flag_key`, `project_key`, `environment`, `flag_type`, `created_at`, `archived_at`
- `feature_flag_event`
  - `event_type`, `flag_key`, `environment`, `actor_type`, `prev_state`, `next_state`, `event_ts`
- `feature_flag_link`
  - `flag_key`, `target_type` (`issue`/`pr`/`release`), `target_id`, `provider`, `link_source`
- `telemetry_signal_bucket`
  - `signal_type`, `signal_count`, `session_count`, `endpoint_group`, `environment`, `release_ref`, `bucket_start`, `bucket_end`

### New derived entity
- `release_impact_daily`
  - release and flag impact rollups with confidence and coverage (append-only with `computed_at`)

## Measurement Contract
### Event taxonomy (MVP)
- `feature_flag.change` (management-plane: create/update/toggle/rule/rollout)
- `feature_flag.exposure` (data-plane: bucketed exposure counts)
- `telemetry.signal` (bucketed user-impact counters: friction/error/adoption)
- `release.deployment` (deployment and release markers)

### Required fields
- temporal: `event_ts` (UTC), `ingested_at`, `bucket_start`, `bucket_end`
- join keys: `provider`, `environment`, `repo_id` or `work_scope_id`, `release_ref`, `flag_key`
- quality: `source_event_id`, `schema_version`, `dedupe_key`, `is_sampled`

### Identity and privacy strategy
- keep developer identities and end-user telemetry identities disjoint
- telemetry dimensions default to aggregate-only keys; no raw personal identifiers in analytics tables
- if deduplication requires identity, use irreversible pseudonymous IDs only

### Missingness and instrumentation drift
- store `coverage_ratio` and `missing_required_fields_count` per computed output
- include `instrumentation_change_flag` when schema version/volume shifts exceed threshold
- suppress derived metrics when required fields drop below threshold

## Proposed Work Graph Extensions
### Node types (proposed)
- `release`
- `feature_flag`

### Edge types (proposed)
- `introduced_by` (release <- PR)
- `rolls_out` (release -> feature_flag)
- `guards` (feature_flag -> issue/epic/scope)
- `impacts` (release/feature_flag -> telemetry_signal_bucket)

### Provenance and confidence model
- Keep current provenance model (`native`, `explicit_text`, `heuristic`)
- Extend evidence to include provider event IDs and normalized source references
- Confidence scoring bands:
  - `1.0`: direct ID/key linkage from provider-native payload
  - `0.8-0.9`: deterministic text/key mapping (documented pattern)
  - `0.4-0.7`: time-window heuristic only
- Store `coverage_ratio` for each derived metric (matched_events / eligible_events)

## Join Strategy (Canonical)
Primary spine:
1. Issue -> PR via work graph issue/PR links
2. PR -> Deployment via `pull_request_number`
3. Deployment -> Release key (`deployment_id`/tag/version)
4. Release/Flag -> telemetry signal buckets via `release_ref` and environment windows

Secondary joins (lower confidence):
- Time-window-only attribution when explicit linkage is missing
- Branch/tag fallback mapping where release ID absent

## Metric Catalog (Candidate)
All metrics are team/repo/release scoped unless explicitly stated.

| Metric Key | Unit | Definition | Window | Minimum Sample | Notes |
|---|---|---|---|---|---|
| `release_user_friction_delta` | ratio | `(post_friction_rate - baseline_friction_rate) / baseline` | baseline 7d pre, post 24-72h | 300 sessions | Do not label causal |
| `release_error_rate_delta` | ratio | Relative change in error signals after release | baseline 7d pre, post 24-72h | 1000 events | Environment-segmented |
| `flag_exposure_rate` | ratio | `exposed_users / eligible_users` for a rollout window | session to 24h | 200 eligible users | Required denominator audit |
| `flag_activation_rate` | ratio | `activated_users / exposed_users` for one meaningful action | session to 24h | 100 exposed users | Define action contract per feature |
| `flag_reliability_guardrail` | ratio | crash-free or error-free session ratio for exposed cohort proxy | session to 24h | 300 sessions | Guardrail metric, not success KPI |
| `time_to_first_user_issue_after_release` | hours | Time from deployment to first user-impact issue signal | post 72h | 1 issue | P50/P90 at aggregate |
| `flag_rollout_half_life` | hours | Time from first rollout event to 50% exposure marker | rollout period | 2 rollout events | Provider-specific semantics |
| `flag_friction_delta` | ratio | Friction rate delta for flagged cohort proxy | baseline 7d pre, post 24-72h | 200 sessions | Requires cohort-safe approximation |
| `flag_churn_rate` | count/week | Number of flag rule/toggle changes per week | rolling 28d | n/a | Operational volatility indicator |
| `release_impact_confidence_score` | 0..1 | Weighted score from linkage quality + coverage + sample sufficiency | per release | n/a | Not a business KPI |
| `release_impact_coverage_ratio` | 0..1 | Share of telemetry buckets reliably mapped to release/flag | per release | n/a | Display with every impact metric |
| `issue_to_release_impact_link_rate` | 0..1 | Fraction of completed work items linked to measurable release impact | rolling 30d | 50 work items | Data quality signal |
| `rollback_or_disable_after_impact_spike` | count | Number of rollback/flag-disable events within impact alert window | post 72h | n/a | Stability response marker |

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

Mitigations:
- store concurrent-deploy count and confounder flags with each impact result
- require minimum denominator/sample thresholds before metric publication
- expose confidence and coverage alongside all deltas
- default UI language to "appears/suggests" and ban deterministic causal wording

## Interpretation and Claims Policy
- Allowed language: "appears", "suggests", "leans", "is consistent with"
- Forbidden language (MVP): "caused", "proved", "determined"
- All release/flag impact views must display: attribution window, confidence, coverage, and confounder context
- No metric may be presented as an individual accountability signal

## Data Privacy and Security
- Aggregated telemetry by default, no raw session payload persistence in analytics tables
- Pseudonymous identifiers only if needed for deduplication
- allowlist-only telemetry fields; drop free-form text by default
- k-anonymity thresholds for any segmentable view
- retention policy for raw ingest buffers shorter than derived aggregates

## Delivery Phases
### Phase 0: Schema and contract design
- finalize canonical entities, join keys, windows, confidence/coverage fields
- define migration plan and sink contracts

### Phase 1: Ingestion and normalization MVP
- add provider connectors/processors for flag events and telemetry buckets
- persist canonical events to ClickHouse through sinks

### Phase 2: Derived metrics and graph edges
- compute `release_impact_daily` and related rollups
- add proposed graph nodes/edges with provenance/confidence

### Phase 3: Prototype dashboards and interpretation guidance
- add internal-only views for release/flag impact validation
- iterate thresholds and copy based on prototype results

### Phase 4: Productization decision
- promote selected metrics to stable registry only if acceptance thresholds hold
- document rejected metrics and reasons (low signal/high misuse risk)

## Success Criteria
- New release/flag impact metrics are inspectable and reproducible from persisted events
- Coverage and confidence are included for all impact outputs
- Prototype evaluation meets acceptance thresholds for signal quality
- No person-level ranking surfaces are created
- Work graph can trace issue -> release/flag -> user impact buckets for supported providers

## Risks and Mitigations
| Risk | Impact | Mitigation |
|---|---|---|
| Identity conflation (developer vs end-user) | High | hard schema separation; no developer dimension in telemetry impact tables |
| Causal over-claim in UI | High | enforced wording guidelines; confidence + confounder display |
| Low coverage from missing release refs | Medium | dual linkage methods; explicit coverage metric |
| Telemetry volume/cost explosion | Medium | bucketed ingestion, sampling, and retention controls |
| Provider API variability | Medium | provider-specific mappers behind canonical event contract |

## Decision Gates
- Gate A (end Phase 0): approve schema and privacy contract
- Gate B (end Phase 2): approve prototype metric set for internal validation
- Gate C (end Phase 4): promote only metrics that pass signal thresholds

## Open Questions for Iteration
- Which telemetry signal families are required for MVP: friction, error, adoption, or all three?
- What should be the default post-release attribution window by environment type?
- Should rollout half-life rely on provider exposure events or a deterministic proxy?
- Which teams/repos are best pilot candidates for representative signal validation?

## References
- `docs/product/prd.md`
- `docs/product/concepts.md`
- `docs/metrics.md`
- `src/dev_health_ops/work_graph/models.py`
- `src/dev_health_ops/metrics/schemas.py`
