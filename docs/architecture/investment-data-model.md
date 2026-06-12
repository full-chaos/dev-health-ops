# Investment Data Model

Where investment categorization is persisted, and how to read it correctly.

Investment analytics are **ClickHouse-only**. PostgreSQL holds none of this data — it is
the semantic/admin layer only. Tables are written by the materializer (see
[Investment Categorization Pipeline](investment-categorization-pipeline.md)) and read,
**effort-weighted**, by the API (see [Investment API](../api/investment-api.md)).

Migrations live in `src/dev_health_ops/migrations/clickhouse/`.

---

## Tables

### `work_unit_investments` (canonical)

One row per WorkUnit per materialization run. Created in
`017_investment_materialize_tables.sql`; label columns added in
`019_work_unit_investment_labels.sql`; `org_id` added in `024_add_org_id.sql`.

| Column | Type | Notes |
| ------ | ---- | ----- |
| `work_unit_id` | `String` | Stable SHA-256 of the component's sorted nodes |
| `work_unit_type` | `Nullable(String)` | Label (added in 019) |
| `work_unit_name` | `Nullable(String)` | Label (added in 019) |
| `from_ts` / `to_ts` | `DateTime64(3,'UTC')` | Component time bounds (min/max node times) |
| `repo_id` | `Nullable(UUID)` | Null → surfaces as `unassigned` scope in the API |
| `provider` | `Nullable(String)` | Source provider |
| `effort_metric` | `String` | **Runtime values: `churn_loc` or `active_hours`** |
| `effort_value` | `Float64` | Weight used by the API (see below) |
| `theme_distribution_json` | `Map(String, Float64)` | 5 themes → probability (~sums to 1) |
| `subcategory_distribution_json` | `Map(String, Float64)` | 15 subcategories → probability |
| `structural_evidence_json` | `String` | Serialized structural signals |
| `evidence_quality` | `Float64` | `0.0–1.0` |
| `evidence_quality_band` | `String` | **Runtime values: `high` / `moderate` / `low` / `very_low`** |
| `categorization_status` | `String` | **Runtime values: `ok`, `repaired`, `invalid_llm_output`, `insufficient_evidence`, `no_text_sources`, `llm_task_failed`** |
| `categorization_errors_json` | `String` | Serialized validation errors (if any) |
| `categorization_model_version` | `String` | Model id (or provider name) |
| `categorization_input_hash` | `String` | SHA-256 of the serialized evidence bundle |
| `categorization_run_id` | `String` | Per-run UUID |
| `computed_at` | `DateTime64(3,'UTC')` | Run timestamp; **the ReplacingMergeTree version** |
| `org_id` | `String` | Tenant (added in 024) |

> **Heads-up: the SQL comments are stale.** The DDL comments name example values that
> the code no longer emits (e.g. `effort_metric` comment says `'fte_days', 'story_points'`;
> `evidence_quality_band` says `'high','medium','low'`; `categorization_status` says
> `'success','error','partial'`). The **runtime** values are the ones in the table above.
> The header comment also says work units are "(PR/Issue)" — in fact a WorkUnit can also
> contain **commit** nodes.

### `work_unit_investment_quotes` (evidence quotes)

Extractive evidence quotes, one row per quote. Created in
`017_investment_materialize_tables.sql`.

| Column | Type | Notes |
| ------ | ---- | ----- |
| `work_unit_id` | `String` | FK to the WorkUnit |
| `quote` | `String` | A literal substring of the source text |
| `source_type` | `String` | **Runtime values: `issue`, `pr`, `commit`** (the DDL comment is stale) |
| `source_id` | `String` | Source item id |
| `computed_at` | `DateTime64(3,'UTC')` | ReplacingMergeTree version |
| `categorization_run_id` | `String` | Run UUID |
| `org_id` | `String` | Tenant (added in 024) |

> **Quotes are written by default for materialization runs.** The CLI and worker
> materializer default to persisting validated extractive quotes; `--no-persist-evidence-snippets`
> can deliberately skip them for storage-constrained backfills. UX should still tolerate
> missing quotes because fallback WorkUnits and historical runs may have none. See
> [Investment Materialization](../ops/investment-materialization.md).

### `investment_explanations` (UX-time cache)

Caches AI-generated **explanations** of already-persisted categorizations. Created in
`018_investment_explanations.sql`. Explanations are read-only narrative; they **never**
alter persisted distributions (see the
[LLM Categorization Contract](../llm/categorization-contract.md)).

| Column | Type | Notes |
| ------ | ---- | ----- |
| `cache_key` | `String` | Explanation cache key |
| `explanation_json` | `String` | Serialized explanation |
| `llm_provider` / `llm_model` | `String` / `Nullable(String)` | Provider used |
| `computed_at` | `DateTime64(3,'UTC')` | ReplacingMergeTree version |
| `org_id` | `String` | Tenant (added in 024) |

### Legacy daily tables (not the canonical path)

`007_complexity_investment_issues.sql` defines `investment_classifications_daily`,
`investment_metrics_daily`, and `issue_type_metrics_daily`. These predate the WorkUnit
model and are **not** the canonical distribution path. Prefer `work_unit_investments`
for all new work.

---

## Read semantics — important

The three canonical investment tables (`work_unit_investments`, `work_unit_investment_quotes`, `investment_explanations`) use `ENGINE = ReplacingMergeTree(computed_at)`. ClickHouse replaces rows
with the same sort key **eventually**, during background merges — not immediately.

- `work_unit_investments` is `ORDER BY (work_unit_id)`, so re-materializing a WorkUnit
  produces a new row that *eventually* replaces the old one.
- The investment API queries currently `sum(...)` directly **without** `FINAL` or
  `argMax(..., computed_at)`. Between a re-materialization and the next merge, duplicate
  rows for the same `work_unit_id` can therefore be double-counted in user-visible
  totals.

> This is a known correctness caveat, tracked as an engineering issue (review API
> latest-row semantics). Until resolved, prefer querying with explicit latest-row logic
> (`argMax`/`FINAL`/a latest subquery) when exact totals matter. See
> [Investment API](../api/investment-api.md).

---

## Related

- [Investment Categorization Pipeline](investment-categorization-pipeline.md) — how rows are produced
- [Investment API](../api/investment-api.md) — how rows are aggregated and weighted
- [Investment Materialization](../ops/investment-materialization.md) — the CLI that writes these tables
- [Database Architecture](database-architecture.md) — dual-database (semantic vs analytics) contract
