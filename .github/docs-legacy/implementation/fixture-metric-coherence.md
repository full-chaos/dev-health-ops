# Fixture Metric Coherence (ops-side)

> **Related front-end contract:** `web/docs/metric-coherence.md` — the same
> Rule 1 principle governs both sides.  This document is the ops-backend
> mirror: it records which invariants the synthetic fixture generators enforce
> and how to validate them programmatically.

Implemented as part of **CHAOS-2040**.

---

## Why this exists

The synthetic seed (`dev-hops fixtures generate`) populates the live-backend
demo with plausible data.  Before CHAOS-2040, the generators produced
independent random values, which meant the seeded database could contain
figures that silently contradict each other — for example, coverage snapshots
where branch coverage exceeded line coverage, or work-item metrics where
"unassigned completions" exceeded "total completions".

A viewer of the live demo who noticed such a contradiction would reasonably
assume it was a data bug, not an intended signal.  That erodes trust in the
product at exactly the moment we want to build it.

The fix is **enforce coherence at generation time**, so invariant-violating
rows can never be produced in the first place, and add a **validation pass**
that raises loudly if any violation slips through (e.g., via a future
refactor).

---

## Invariants by domain

### Coverage snapshots

| Invariant | Rationale |
|---|---|
| `branch_coverage_pct ≤ line_coverage_pct` | A branch is only covered if its containing line is covered — branch coverage is a strict subset. |
| `lines_covered ≤ lines_total` | Covered count cannot exceed the universe. |
| `branches_covered ≤ branches_total` | Same constraint for branches. |

### Test suite results

| Invariant | Rationale |
|---|---|
| `passed + failed + skipped + error_count ≤ total_count` | Sub-counts are shares of the total; they may be *less than* total (quarantined tests appear in no sub-bucket), but never *more than* total. |

Note: the sub-counts share the denominator `total_count`.  A viewer seeing
"80 passed, 10 failed, 5 skipped, 5 errors out of 100 total" can reconcile
the numbers (80+10+5+5 = 100).  The remaining capacity is quarantined /
pending tests, which the UI labels explicitly.

### Work-item metrics (daily)

| Invariant | Rationale |
|---|---|
| `items_completed_unassigned ≤ items_completed` | Unassigned completions are a subset of all completions. |
| `items_started_unassigned ≤ items_started` | Unassigned starts are a subset of all starts. |
| `wip_unassigned_end_of_day ≤ wip_count_end_of_day` | Unassigned WIP is a subset of total WIP. |
| `cycle_time_p50_hours ≤ cycle_time_p90_hours` | Percentiles are non-decreasing by definition. |
| `lead_time_p50_hours ≤ lead_time_p90_hours` | Percentiles are non-decreasing. |
| `cycle_time_p50_hours ≤ lead_time_p50_hours` | Lead time = queue time + cycle time.  Lead time is always ≥ cycle time. |
| `wip_age_p50_hours ≤ wip_age_p90_hours` | Percentiles are non-decreasing. |

### Commit stats

| Invariant | Rationale |
|---|---|
| `deletions ≤ additions` (per file per commit) | The synthetic generator models organic change: files that are edited have more added/rewritten content than deleted content. |

### CI pipeline runs

| Invariant | Rationale |
|---|---|
| `status` is a known value | Each run belongs to exactly one status bucket (`success`, `failure`/`failed`, `cancelled`/`canceled`, `timeout`, `running`, `queued`, `skipped`). The denominator Rule 1 note applies: `success_rate + failure_rate` need not total 100 % because some runs are `cancelled` — the frontend copy already explains this. The ops contract ensures no run uses an unrecognised status that the UI cannot categorise. |
| `queued_at ≤ started_at ≤ finished_at` | Timeline order is required for duration and queue-wait calculations to be non-negative. Violated ordering would produce negative latency values on the CI metrics views. |

Note: each pipeline run is allocated a **single** status by construction in the generator, so there is no `sum of buckets = total` check needed. The denominator reconciles trivially — the check validates status validity and time ordering only.

### Already-enforced invariants (pre-CHAOS-2040)

These were correct before this work; they are documented here for completeness.

| Domain | Invariant |
|---|---|
| Coverage | `branch_coverage_pct ≤ line_coverage_pct` (enforced via `min(branch_coverage, line_coverage - 2.0)` in the random walk) |
| Complexity | `very_high_complexity_functions ≤ high_complexity_functions ≤ functions_count` |
| Pipeline timing | `queued_at ≤ started_at ≤ finished_at` (derived sequentially) |
| Deployment timing | `started_at ≤ finished_at ≤ deployed_at` (derived sequentially) |
| User metrics | `loc_deleted ≤ loc_added` per author-day |

---

## Validation API

```python
from dev_health_ops.fixtures.coherence import (
    FixtureBundle,
    validate_all,
    CoherenceError,
)

bundle = FixtureBundle(
    coverage_snapshots=my_snapshots,      # list[dict]
    test_suite_results=my_suite_rows,     # list[dict]
    work_item_metrics=my_wi_rows,         # list[dict]
    commit_stats=my_stat_rows,            # list[dict]
)

try:
    validate_all(bundle)                  # raises CoherenceError if any violation
except CoherenceError as exc:
    for v in exc.violations:
        print(v)
```

`validate_all` **collects every violation** before raising, so callers get the
full picture in one pass rather than stopping at the first problem.

Individual check functions (`check_pipeline_runs`, `check_coverage_snapshots`,
`check_test_suite_results`, `check_work_item_metrics`, `check_commit_stats`)
are also exported for targeted use.

### CLI escape hatch

`dev-hops fixtures generate` runs `validate_all` by default after generating
each repo's data.  To bypass validation (e.g., during profiling or bulk
historical backfills where you trust the generator):

```bash
dev-hops fixtures generate --sink $CLICKHOUSE_URI --skip-coherence-validation
```

The flag is intentionally off by default; it should only be needed when
generation performance is the bottleneck and you have independent confidence
in the generator's correctness.

---

## Testing

Unit tests live in `tests/fixtures/test_metric_coherence.py`.  They cover:

1. **Happy path** — valid rows pass `validate_all`.
2. **Violation detection** — each invariant category has at least one test
   that confirms a deliberately-broken row triggers the correct error.
3. **Generator regression** — `SyntheticDataGenerator` output passes
   `validate_all` for seeds `[0, 1, 7, 42, 99, 137, 255, 1024]`.

4. **Runner integration** — `run_fixtures_generation` propagates
   `CoherenceError` when the validator fires; `--skip-coherence-validation`
   is verified to bypass the gate without calling the validator.

Run with:

```bash
cd ops/
uv run pytest tests/fixtures/test_metric_coherence.py -v
```

> **Same-seed determinism note:** the `seed` parameter controls the
> numeric shape of generated values (counts, percentiles, pass rates) but
> does not fix wall-clock timestamps — generators call `datetime.now()`
> internally.  Two runs with the same seed produce coherent data with
> identical statistical profiles, but with different absolute timestamps.
> The coherence invariants are timestamp-order invariants, not absolute
> value invariants, so this does not affect the validation guarantees.

---

## Denominator note (Rule 1 alignment)

The front-end contract (Rule 1 in `web/docs/metric-coherence.md`) says:

> If two figures *look* like they should add up but don't, the page MUST
> either make them reconcile **or explain the denominator**.

The ops generators take the "make them reconcile" path: the values are
constrained at write time so the page never needs a caveat.  Where a caveat
*is* appropriate (e.g., success rate + failure rate < 100% because some runs
are cancelled), the existing frontend copy already explains the denominator —
the ops seed now produces data that is consistent with that explanation rather
than contradicting it.
