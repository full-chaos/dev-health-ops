# Feature Flag + User Impact PRD — Gap Analysis & Review

_Review date: 2026-02-27_
_PRD under review: `docs/product/feature-flag-user-impact-prd.md`_
_Reviewers: Momus (plan critic), Oracle (architecture), 2x Explore agents (ClickHouse/sink compat, work graph compat)_

> **Status**: All 4 reviewers completed. Findings synthesized below. Apply corrections before converting PRD to implementation tickets.

---

## Reviewer Consensus

All reviewers confirmed the PRD is **directionally sound** and aligned with platform philosophy (no rankings, inspectable, team-scoped, sink-only persistence). The main structural issues are:

1. Storage contracts are underspecified (cannot implement sinks/migrations as written)
2. Several metrics are not computable from the proposed aggregate-only telemetry model
3. Work graph extensions need tighter semantic alignment with existing edge types
4. Multi-tenancy (`org_id`) is missing from all proposed entities

---

## Critical (Must Fix Before Implementation)

### C1: All proposed entities missing `org_id`

| Field | Value |
|-------|-------|
| **PRD lines** | 68–79 (entity field lists) |
| **Severity** | Critical |
| **Sources** | Momus, Oracle, Explore (ClickHouse) |
| **Issue** | None of the 5 proposed entities include `org_id`. Migration 027 requires `org_id String DEFAULT 'default'` as the **first** element in every ClickHouse `ORDER BY` clause. Without it, multi-tenant query pruning breaks and cross-org data leaks become possible. |
| **Fix** | Add `org_id` as the first field in every entity definition. Note: `ClickHouseMetricsSink._insert_rows` already auto-injects `org_id` from sink context, so the schema addition will "just work" for persistence. |

### C2: Storage contracts unspecified — cannot implement sinks or migrations

| Field | Value |
|-------|-------|
| **PRD lines** | 66–79 (entity definitions), 229–232 (Phase 0) |
| **Severity** | Critical |
| **Sources** | Momus, Oracle, Explore (ClickHouse) |
| **Issue** | Entities are named and fields listed, but there is no specification of: (a) whether each entity is a **raw fact table** (connector writes) or **derived metrics table** (metrics job writes); (b) ClickHouse engine type (`MergeTree` vs `ReplacingMergeTree`); (c) `PARTITION BY` and `ORDER BY` keys; (d) deduplication strategy; (e) target backend (ClickHouse-only vs also Postgres semantic layer). Without these, implementers cannot write migrations or sink methods. |
| **Fix** | Add a "Storage Contract" subsection per entity. Recommended engines: |

| Entity | Classification | Engine | ORDER BY (recommended) |
|--------|---------------|--------|----------------------|
| `feature_flag` | Raw dimension | `ReplacingMergeTree(last_synced)` | `(org_id, provider, flag_key)` |
| `feature_flag_event` | Raw event | `MergeTree()` | `(org_id, flag_key, environment, event_ts)` |
| `feature_flag_link` | Raw linkage | `ReplacingMergeTree(last_synced)` | `(org_id, flag_key, target_type, target_id)` |
| `telemetry_signal_bucket` | Raw event | `MergeTree()` | `(org_id, environment, release_ref, bucket_start)` |
| `release_impact_daily` | Derived metric | `MergeTree()` | `(org_id, release_ref, day)` |

### C3: Cohort-based metrics use "users" as denominator but PRD mandates aggregate-only telemetry

| Field | Value |
|-------|-------|
| **PRD lines** | 30 (aggregate-only guardrail), 74–75 (telemetry bucket fields: `signal_count`, `session_count` — no `user_count`), 141–142 (metrics using `exposed_users / eligible_users`, `activated_users / exposed_users`) |
| **Severity** | Critical |
| **Sources** | Momus, Oracle |
| **Issue** | `flag_exposure_rate` and `flag_activation_rate` are defined with "users" as both numerator and denominator, but the `telemetry_signal_bucket` entity only has `signal_count` and `session_count` — no user count field. The PRD guardrail at line 30 says "aggregated signals only by default." These metrics are **not computable** as written. |
| **Fix** | Either: (a) redefine metrics using `sessions` instead of `users` (e.g., `exposed_sessions / eligible_sessions`), clearly defining eligibility; or (b) add an optional `unique_pseudonymous_count` field to `telemetry_signal_bucket` with k-anonymity enforcement, and gate these metrics on its availability. Mark user-based variants as Phase 2+ if aggregate-only is the MVP contract. |

---

## Major (Must Fix Before Phase 1)

### M1: Heuristic confidence band 0.4–0.7 conflicts with existing platform value of 0.3

| Field | Value |
|-------|-------|
| **PRD lines** | 117–120 (proposed bands) |
| **Reference** | `docs/user-guide/work-graph.md:37-41` (heuristic = 0.3) |
| **Severity** | Major |
| **Source** | Momus |
| **Issue** | Existing platform defines heuristic confidence as `0.3`. PRD proposes `0.4-0.7` for "time-window heuristic only." This creates an inconsistency that will confuse consumers comparing confidence values across edge types. |
| **Fix** | Either (a) align with existing 0.3 for pure time-window heuristic edges, or (b) explicitly propose a platform-wide confidence recalibration with a list of all impacted consumers/docs. Option (a) is recommended for MVP. |

### M2: `release_ref` is undefined — no canonical format, deployments table lacks release identifier

| Field | Value |
|-------|-------|
| **PRD lines** | 74–75 (`release_ref` in telemetry bucket), 127 (join step 3: "Deployment → Release key") |
| **Reference** | `migrations/clickhouse/000_raw_tables.sql` — `deployments` has `deployment_id`, `environment`, `pull_request_number` but NO `release_ref`/`tag`/`version` |
| **Severity** | Major |
| **Sources** | Momus, Oracle, Explore (ClickHouse) |
| **Issue** | The join strategy assumes step 3 maps deployments to release keys, but the existing `deployments` table stores no release identifier. `release_ref` appears in entity definitions but has no canonical format specification (tag? SHA? semver? deployment_id?). |
| **Fix** | (a) Define `release_ref` format per provider (e.g., GitHub tag, GitLab release, deployment_id fallback). (b) Add `release_ref` enrichment to deployment processing (new field or lookup table). (c) Document what happens when `release_ref` is unavailable (confidence downgrade? suppress?). |

### M3: `rolls_out` edge is semantically incorrect — flags change independently of releases

| Field | Value |
|-------|-------|
| **PRD lines** | 110 (`rolls_out`: release -> feature_flag) |
| **Severity** | Major |
| **Source** | Oracle |
| **Issue** | Most real-world flag rollouts are config changes (LaunchDarkly dashboard toggle, API call) that happen **independently** of code deployments. Forcing a `release → feature_flag` edge as the primary relationship will be wrong more often than right, poisoning downstream impact attribution. |
| **Fix** | (a) Model flag config changes as their own event stream (already proposed as `feature_flag_event`). (b) Replace `rolls_out` with a confidence-bearing evidence-based edge that only exists when explicit provider evidence links a flag change to a release (e.g., GitLab `introduced_by_url`). (c) Consider reusing existing `REFERENCES` edge type for "PR/commit mentions flag key" relationships. Oracle recommends introducing **at most one** new edge type with strict evidence requirements. |

### M4: `BaseMetricsSink` interface needs ~5 new `write_*` methods — not mentioned

| Field | Value |
|-------|-------|
| **PRD lines** | 229–232 (Phase 0 mentions "sink contracts" generically) |
| **Reference** | `src/dev_health_ops/metrics/sinks/base.py` (explicit `write_*` methods per record type) |
| **Severity** | Major |
| **Sources** | Momus, Explore (ClickHouse) |
| **Issue** | The sink interface requires explicit write methods for each new record type. The PRD introduces 5 entities but doesn't mention the ~5 corresponding `write_feature_flag`, `write_feature_flag_event`, `write_feature_flag_link`, `write_telemetry_signal_bucket`, `write_release_impact_daily` methods needed in `BaseMetricsSink` and all implementations (`ClickHouseMetricsSink`, etc.). |
| **Fix** | Add a "Sink Interface Extensions" subsection to Phase 0 listing the required methods. Also add corresponding record dataclasses to `src/dev_health_ops/metrics/schemas.py`. |

### M5: `WorkGraphBuilder` needs new builder methods — not mentioned

| Field | Value |
|-------|-------|
| **PRD lines** | 103–112 (proposed graph extensions) |
| **Reference** | `src/dev_health_ops/work_graph/builder.py` — orchestrates graph construction with `_build_*_edges` methods |
| **Severity** | Major |
| **Sources** | Momus, Explore (work graph) |
| **Issue** | `WorkGraphBuilder.build()` currently handles issue/PR/commit/file nodes. New `release` and `feature_flag` node types require: (a) `_build_release_edges` and `_build_feature_flag_edges` methods; (b) ID generation functions in `src/dev_health_ops/work_graph/ids.py`; (c) GraphQL enum updates in `src/dev_health_ops/api/graphql/models/inputs.py` and `outputs.py`. |
| **Fix** | Add "Builder and API Extensions" subsection to Phase 2 listing: new builder methods, new ID generators, GraphQL enum additions. |

### M6: Incomplete metric formulas — missing denominators, baselines, window aggregation rules

| Field | Value |
|-------|-------|
| **PRD lines** | 137–151 (metric catalog table) |
| **Severity** | Major |
| **Sources** | Momus, Oracle |
| **Specific issues** | |

- **Line 139** (`release_user_friction_delta`): Formula uses `/ baseline` but doesn't define what `baseline_friction_rate` is computed from (which signals? which sessions? how is the 7d window aggregated — mean? median?).
- **Line 140** (`release_error_rate_delta`): Says "Relative change" but no explicit formula. What counts as "error signals"? Which `signal_type` values?
- **Line 144** (`time_to_first_user_issue_after_release`): Depends on "user-impact issue signal" which is not defined as a telemetry signal type or work-item type.
- **Line 151** (`rollback_or_disable_after_impact_spike`): Depends on undefined "impact alert" mechanism. What constitutes a "spike"? What's the alert window?

| **Fix** | For every metric, add: (a) numerator definition with exact signal type filters; (b) denominator definition with eligibility criteria; (c) aggregation method (mean/median/sum/max); (d) inclusion/exclusion criteria (environment filters, sampling handling); (e) companion absolute-count metric for every ratio. |

### M7: No test/verification strategy for joins, dedup, and drift gates

| Field | Value |
|-------|-------|
| **PRD lines** | 169–190 (validation plan covers signal quality but not engineering tests) |
| **Severity** | Major |
| **Source** | Momus |
| **Issue** | PRD has prototype evaluation metrics but no engineering test strategy. Platform requires tests for behavior changes. |
| **Fix** | Add "Engineering Test Requirements" section covering: (a) unit tests for join logic (Issue→PR→Deployment→Release); (b) dedup-key enforcement tests; (c) drift gate behavior tests (coverage suppression, `instrumentation_change_flag` triggering); (d) smoke test for append-only writes with `computed_at`. |

### M8: Out-of-order arrivals and late data unaddressed

| Field | Value |
|-------|-------|
| **PRD lines** | 89 (temporal fields mentioned), 79 (`computed_at` mentioned for `release_impact_daily` only) |
| **Severity** | Major |
| **Source** | Oracle |
| **Issue** | Telemetry and flag events often arrive late (mobile/offline, batching, retries). If `release_impact_daily` is computed once and never revised, results lock in wrong values. No recomputation window, stability SLA, or late-data handling is defined. |
| **Fix** | Add "Late Data and Recomputation" subsection: (a) define recomputation window (e.g., "always recompute last N days"); (b) define stability SLA ("older than X days is stable unless backfill triggered"); (c) require dual timestamps (`event_time`, `ingested_at`); (d) add `data_completeness` field per day/release for UI surfacing. |

### M9: Phase 1 doesn't name a starting provider — can't build connectors

| Field | Value |
|-------|-------|
| **PRD lines** | 233–236 (Phase 1 description) |
| **Severity** | Major |
| **Source** | Momus |
| **Issue** | Phase 1 says "add provider connectors/processors" but doesn't pick which provider(s) to start with. Implementers cannot build or validate schemas without a concrete target. |
| **Fix** | Explicitly name MVP provider(s). Recommendation: GitHub (already integrated) for releases/deployments, LaunchDarkly for feature flags (richest API), and a generic telemetry ingest API for user signals. |

### M10: Cost controls not operationalized

| Field | Value |
|-------|-------|
| **PRD lines** | 262–263 (risk: "Telemetry volume/cost explosion"), 226–227 (retention mention) |
| **Severity** | Major |
| **Sources** | Momus, Oracle |
| **Issue** | Risk table mentions volume/cost but provides no operational constraints. No expected event volume per repo/day, no default bucket granularity, no retention/TTL for raw vs aggregated data, no per-environment storage policy. |
| **Fix** | Add "Operational Constraints" subsection: (a) expected volume estimates (events/day/repo); (b) default bucket granularity (1h? 15m?); (c) retention TTL for raw ingest buffers vs derived aggregates; (d) sampling policy defaults. |

---

## Minor (Fix During Implementation)

### m1: k-anonymity threshold not specified

| Field | Value |
|-------|-------|
| **PRD line** | 225 |
| **Source** | Momus |
| **Fix** | Define default `k` value (e.g., k=5) and where enforcement happens (query layer vs sink). |

### m2: Edge evidence model mismatch (single string today vs structured)

| Field | Value |
|-------|-------|
| **PRD lines** | 114–116 |
| **Reference** | `src/dev_health_ops/work_graph/models.py` — `WorkGraphEdge.evidence` is `str` |
| **Source** | Momus |
| **Fix** | Clarify whether evidence remains a JSON-encoded string or whether the edge schema changes to structured fields (which requires storage + API changes). |

### m3: `feature_flag_link` is ambiguous — needs link_type + evidence contract

| Field | Value |
|-------|-------|
| **PRD line** | 72–73 |
| **Source** | Oracle |
| **Fix** | Add `link_type` enum (e.g., `code_reference`, `configuration`, `issue_tag`), `evidence_type`, `confidence`, and `valid_time_range` fields. Without these, the link table becomes a dumping ground with no semantic consistency. |

### m4: ClickHouse migration execution ownership unclear

| Field | Value |
|-------|-------|
| **PRD lines** | 229–232 (Phase 0) |
| **Source** | Momus |
| **Fix** | Document whether new tables are auto-created on first write (like some metrics tables) or require explicit DDL migrations. Recommend explicit migrations for raw tables, consistent with existing patterns in `migrations/clickhouse/`. |

---

## Positive Findings (No Action Needed)

| Finding | Source |
|---------|--------|
| ClickHouse `work_graph_edges` uses `String` columns (not `Enum8`) — new node/edge types need only Python + GraphQL enum updates, no CH migration | Explore (work graph) |
| `ClickHouseMetricsSink._insert_rows` auto-injects `org_id` from sink context — adding `org_id` to schemas will "just work" | Explore (ClickHouse) |
| ID generation (`generate_edge_id`) is generic — automatically supports new types | Explore (work graph) |
| PRD philosophy aligns with platform guardrails (no rankings, inspectable, team-scoped) | All reviewers |
| Provenance model (`native`, `explicit_text`, `heuristic`) is extensible for new edge types | Explore (work graph) |

---

## Recommended Fix Priority

### Before converting to tickets (do now)
1. **C1**: Add `org_id` to all entity definitions
2. **C2**: Add storage contract table (engine, keys, classification) per entity
3. **C3**: Redefine cohort metrics to session-based or gate on optional user count field
4. **M1**: Align heuristic confidence to 0.3
5. **M2**: Define `release_ref` canonical format and enrichment strategy
6. **M3**: Replace `rolls_out` with evidence-gated edge, reduce new edge types

### Before Phase 0 implementation
7. **M4**: Document sink interface extensions
8. **M5**: Document builder and API extensions
9. **M6**: Complete all metric formulas
10. **M7**: Add engineering test requirements
11. **M8**: Add late data/recomputation contract
12. **M9**: Name MVP providers
13. **M10**: Add operational constraints

### During implementation
14. **m1–m4**: Address during relevant phase

---

## Summary Statistics

| Severity | Count | Status |
|----------|-------|--------|
| Critical | 3 | Must fix before tickets |
| Major | 10 | Must fix before Phase 1 |
| Minor | 4 | Fix during implementation |
| Positive | 5 | No action needed |
| **Total findings** | **17** | |

---

## Appendix: Reviewer Coverage Matrix

| Finding | Momus | Oracle | Explore (CH) | Explore (WG) |
|---------|:-----:|:------:|:------------:|:------------:|
| C1 (org_id) | x | x | x | |
| C2 (storage contracts) | x | x | x | |
| C3 (user denominators) | x | x | | |
| M1 (confidence bands) | x | | | |
| M2 (release_ref) | x | x | x | |
| M3 (rolls_out edge) | | x | | |
| M4 (sink methods) | x | | x | |
| M5 (builder methods) | x | | | x |
| M6 (metric formulas) | x | x | | |
| M7 (test strategy) | x | | | |
| M8 (late data) | | x | | |
| M9 (MVP provider) | x | | | |
| M10 (cost controls) | x | x | | |
| m1 (k-anonymity) | x | | | |
| m2 (evidence model) | x | | | |
| m3 (link ambiguity) | | x | | |
| m4 (migration ownership) | x | | | |
