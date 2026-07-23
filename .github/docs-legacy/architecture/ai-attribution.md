# AI Attribution Architecture

> **Canonical contract for CHAOS-1579 (storage) and CHAOS-1580 (ingestion).**

## Overview

AI attribution tracks which engineering artifacts (PRs, commits, issues, reviews, workflow runs)
had AI involvement, what kind, how confident, and which signal produced that conclusion.

Attribution is **evidence, not verdict** — every detected signal is preserved raw.  Resolution
(which signal "wins" for a given subject) happens at read time via the `ai_attribution_resolved`
view, not at write time.

---

## Data Model

### Enumerations

#### `AIAttributionSource` — where the signal came from

| Value              | Description                           |
| ------------------ | ------------------------------------- |
| `pr_label`         | Explicit label on the PR              |
| `bot_author`       | GitHub App / known bot user           |
| `commit_trailer`   | `AI-Assisted-By` / `Co-authored-by` AI bot trailer |
| `branch_name`      | Weak heuristic from branch name       |
| `pr_body`          | Weak heuristic from PR body text      |
| `ci_annotation`    | CI workflow step annotation           |
| `manual`           | User override — always wins           |

#### `AIAttributionKind` — type of involvement

| Value           | Description                                    |
| --------------- | ---------------------------------------------- |
| `ai_assisted`   | Human-authored with AI assistance              |
| `agent_created` | Autonomous agent produced this artifact        |
| `ai_review`     | AI performed the review                        |
| `unknown`       | Signal detected but kind unclear — do not guess |

### `AIAttributionSignal`

Lightweight detection output from `providers/_ai_detection.py`.  Does not carry
org/subject context — the normalization caller attaches context and promotes to
`AIAttributionRecord` via `AIAttributionRecord.from_signal(...)`.

### `AIAttributionRecord`

Full persisted record.  One record per detected signal per subject.

| Field           | Type                | Notes                                     |
| --------------- | ------------------- | ----------------------------------------- |
| `record_id`     | `UUID`              | Surrogate ID; used for supersession links |
| `org_id`        | `UUID`              | Organization scope                        |
| `provider`      | `str`               | `github` / `gitlab` / `jira` / `local`   |
| `subject_type`  | `SubjectType`       | `pull_request` / `commit` / etc.          |
| `subject_id`    | `str`               | Provider-native ID                        |
| `repo_id`       | `UUID \| None`      | Repository scope (optional)               |
| `kind`          | `AIAttributionKind` | Type of AI involvement                    |
| `source`        | `AIAttributionSource` | Signal origin                           |
| `confidence`    | `float`             | `[0.0, 1.0]`                             |
| `actor`         | `str \| None`       | Bot name / agent name / `"human"`         |
| `evidence`      | `dict[str, object]` | Raw signal payload (serialized to JSON)   |
| `observed_at`   | `datetime`          | When signal was emitted by provider       |
| `ingested_at`   | `datetime`          | When record was created (auto-set)        |
| `superseded_by` | `UUID \| None`      | `record_id` of the MANUAL override        |

---

## Source Precedence

```
MANUAL > pr_label > bot_author > commit_trailer > ci_annotation > branch_name > pr_body
```

Precedence is applied at **read time** by the `ai_attribution_resolved` view.
Lower precedence integer = higher authority.

| Source           | Priority |
| ---------------- | -------- |
| `manual`         | 1        |
| `pr_label`       | 2        |
| `bot_author`     | 3        |
| `commit_trailer` | 4        |
| `ci_annotation`  | 5        |
| `branch_name`    | 6        |
| `pr_body`        | 7        |

---

## Storage Architecture

### Database selection

AI attribution is **pure analytics data** — it has no semantic-layer identity, is not
joined to Postgres tables, and carries no billing or access-control implications.

> **All AI attribution reads and writes go through ClickHouse only.**

No SQLAlchemy mixin is defined.  If future requirements surface a Postgres-backed
attribution presence (e.g., user-managed overrides in the settings API), introduce an
Alembic migration at that point.

### ClickHouse table: `ai_attribution`

```sql
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, provider, subject_type, subject_id, source)
```

- **Deduplication key**: `(org_id, provider, subject_type, subject_id, source)`
- **Latest wins**: re-inserting the same key with a newer `computed_at` supersedes the row
- Multiple sources for the same subject all have distinct keys and are all persisted

### ClickHouse view: `ai_attribution_resolved`

A **plain `VIEW`** (not an incremental `MATERIALIZED VIEW`) that picks the
highest-precedence, non-superseded record per `(org_id, subject_type, subject_id)`.

```sql
SELECT ...
FROM (
    SELECT *,
        multiIf(source = 'manual', 1, source = 'pr_label', 2, ...) AS _source_priority
    FROM ai_attribution FINAL
    WHERE superseded_by IS NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY org_id, subject_type, subject_id
    ORDER BY _source_priority ASC, confidence DESC
) = 1;
```

**Why not a true incremental MV?**
Incremental ClickHouse MVs process newly inserted rows only.  Selecting the globally
highest-precedence record per subject requires visibility across all rows for that
subject, which incremental aggregation cannot provide correctly.  The base table's
`ReplacingMergeTree` handles write-time deduplication per source; this plain VIEW
handles read-time cross-source precedence resolution.

**Performance note**: `FINAL` on a `ReplacingMergeTree` can be slow at scale.
Consider:
- Running `OPTIMIZE TABLE ai_attribution FINAL` periodically to compact segments
- Querying with `max_threads` and partition pruning on `observed_at`

---

## Write Path

```
Normalization (providers/_ai_detection.py)
    → AIAttributionSignal(s)
    → AIAttributionRecord.from_signal(...)
    → AIAttributionMixin.write_ai_attribution([...])
    → ClickHouse: ai_attribution (ReplacingMergeTree)
```

Key invariants:
- **Every detected signal is persisted** — do not collapse signals before writing
- **Sink only** — no file exports, no debug dumps
- **Idempotent** — same `(org_id, provider, subject_type, subject_id, source)` re-inserted is safe

## Read Path

```
ClickHouse: SELECT * FROM ai_attribution_resolved
    WHERE org_id = ? AND subject_type = ? AND subject_id = ?
```

Returns: one row per subject with the effective attribution (highest precedence, non-superseded).

### GraphQL: `aiAttributionOverview` (CHAOS-2744)

The dedicated `/ai/attribution` page reads `ai_attribution_resolved` directly through
`AIAttributionClickHouseLoader` (`metrics/loaders/ai_attribution.py`) and the
`aiAttributionOverview` resolver (`api/graphql/resolvers/ai.py`). It returns two
projections of the same org-scoped window, never fabricated or recomputed client-side:

- `mix: [AIAttributionMixRow!]!` — `GROUP BY kind` counts + share over the resolved
  view. There is intentionally **no synthesized `human` bucket** here: this view only
  ever contains subjects with a detected signal, so a human count would require the
  full PR population — that inference already lives in `ai_impact_metrics_daily` /
  `aiImpactSummary` and must not be duplicated with a second, undocumented method.
- `rows: [AIAttributionEvidenceRow!]!` — one row per resolved record with
  `source`, `confidence`, `evidence`, and `actor` always populated (non-nullable on
  the base table), plus `teamId` resolved the same way as `aiAttributedPrs`
  (`RepoPatternTeamResolver` over `teams.repo_patterns`, never a SQL join on UUID).

This is additive: `aiImpactSummary`, `aiGovernanceSummary`, and `aiAttributedPrs` are
unchanged and remain the source for impact/governance/drilldown-selector use cases.

---

---

## Supersession (MANUAL overrides)

When a user manually overrides attribution:

1. Write a `MANUAL` source record (highest precedence — automatically wins in the resolved view)
2. Optionally mark the previous record's `superseded_by = <manual_record.record_id>` to make the
   override explicit in the audit trail (excluded by `WHERE superseded_by IS NULL`)

Because `MANUAL` has precedence=1, step 2 is strictly optional for correctness — the resolved
view will pick the MANUAL record regardless.  The `superseded_by` field exists for auditability.

---

## Migration

File: `src/dev_health_ops/migrations/clickhouse/035_ai_attribution.sql`

Applied via:
```bash
dev-hops migrate clickhouse
```

All DDL uses `CREATE TABLE IF NOT EXISTS` / `CREATE VIEW IF NOT EXISTS` — fully idempotent.

---

## Files

| File | Role |
| ---- | ---- |
| `src/dev_health_ops/models/ai_attribution.py` | Canonical Python models + enums |
| `src/dev_health_ops/migrations/clickhouse/035_ai_attribution.sql` | DDL migration |
| `src/dev_health_ops/metrics/sinks/clickhouse/ai_attribution.py` | CH sink mixin |
| `src/dev_health_ops/storage/mixins/ai_attribution.py` | Postgres decision doc |
| `tests/storage/test_ai_attribution.py` | Unit tests |
| `docs/architecture/ai-attribution.md` | This document |
