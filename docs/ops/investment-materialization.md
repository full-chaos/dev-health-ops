# Investment Materialization (CLI)

How to build the Work Graph and materialize investment categorization from the command
line. These commands write to ClickHouse only.

- Pipeline internals: [Investment Categorization Pipeline](../architecture/investment-categorization-pipeline.md)
- Tables produced: [Investment Data Model](../architecture/investment-data-model.md)
- LLM schema/behavior: [LLM Categorization Contract](../llm/categorization-contract.md)

Commands are registered in `src/dev_health_ops/work_graph/runner.py`.

---

## Prerequisites

1. ClickHouse migrations applied: `dev-hops migrate clickhouse`.
2. Raw provider data already synced (git, PRs, work items) — see
   [Getting Started](../getting-started.md).
3. An LLM provider configured (or use `--llm-provider mock` for deterministic output).
   See the provider table in the
   [LLM Categorization Contract](../llm/categorization-contract.md#llm-provider-options).

The two commands run in order: **`work-graph build`** produces the edges that
**`investment materialize`** clusters into WorkUnits.

---

## Step 1 — Build the Work Graph

```bash
dev-hops work-graph build \
  --db "$CLICKHOUSE_URI" \
  --from 2026-05-01 \
  --to 2026-06-01
```

| Flag | Default | Purpose |
| ---- | ------- | ------- |
| `--db` | *(required)* | ClickHouse DSN (`clickhouse://user:pass@host:port/db`) |
| `--from` | 30 days ago | Start date `YYYY-MM-DD` |
| `--to` | today | End date `YYYY-MM-DD` |
| `--repo-id` | all | Restrict to a repository UUID |
| `--heuristic-window` | `7` | Days window for heuristic issue→PR matching |
| `--heuristic-confidence` | `0.3` | Confidence score assigned to heuristic matches |
| `--allow-degenerate` | off | Allow a single connected-component graph (otherwise fail) |
| `--check-components` | on | Perform component analysis |

This writes typed rows to `work_graph_edges` (and helper tables). The investment step
reads these edges.

## Step 2 — Materialize investments

```bash
dev-hops investment materialize \
  --db "$CLICKHOUSE_URI" \
  --from 2026-05-01 \
  --to 2026-06-01 \
  --persist-evidence-snippets
```

| Flag | Default | Purpose |
| ---- | ------- | ------- |
| `--db` | `$CLICKHOUSE_URI` | ClickHouse DSN (falls back to env var) |
| `--from` | `--window-days` before `--to` | Start date `YYYY-MM-DD` |
| `--to` | now | End date `YYYY-MM-DD` |
| `--window-days` | `30` | Window size when `--from` is omitted |
| `--repo-id` | all | Repository UUID; repeatable |
| `--team-id` | all | Team identifier; repeatable |
| `--llm-provider` | `auto` | LLM provider (`openai`, `anthropic`, `mock`, …) |
| `--model` | provider default | Override the model |
| `--persist-evidence-snippets` | **off** | Persist extractive evidence quotes to `work_unit_investment_quotes` |
| `--force` | off | Force re-materialization |

> **`--persist-evidence-snippets` is off by default.** Without it, no rows land in
> `work_unit_investment_quotes`, so evidence drill-downs are empty: validated LLM quotes
> are not persisted, and fallback WorkUnits have no quotes to begin with. Enable it for any
> run whose evidence you want to surface in the UI. (Whether this default should change is
> tracked as an engineering issue.)

### Time window behavior

`--from`/`--to` bound which WorkUnits are **materialized** (by their node time-bounds),
but they do **not** bound how components are **formed** — components are built from the
full edge set first, then filtered. See the
[pipeline doc](../architecture/investment-categorization-pipeline.md#step-1-form-the-workunit).

### Output

On success the command logs `Components=… Records=… Quotes=…` and returns exit code `0`.
`Records` is the number of `work_unit_investments` rows written; `Quotes` is `0` unless
`--persist-evidence-snippets` was set.

---

## Fixtures (mock LLM)

Synthetic/demo data uses the mock provider and forces evidence persistence on, via
`materialize_fixture_investments` in `runner.py`. Use the fixtures flow
(`dev-hops fixtures …`) rather than calling this directly for demo environments.

## Re-running

Re-materializing the same window writes new rows (new `categorization_run_id` /
`computed_at`). Because the tables are `ReplacingMergeTree(computed_at)`, old rows are
replaced only after a background merge — see the read-semantics note in the
[Investment Data Model](../architecture/investment-data-model.md#read-semantics-important).
