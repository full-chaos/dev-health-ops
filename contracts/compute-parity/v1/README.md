# Compute-parity harness (CHAOS-3092 P0)

How a CUT-20 compute-port slice proves its port matches Python. Read this
before writing a parity claim of your own — the whole point of P0 is that
lanes do not each invent one.

## One comparison vocabulary, three boundaries

There is exactly one way rows are encoded and compared in this repo:
`internal/testsupport/oraclecompare`. Everything below sits on top of it.

| Boundary | Where | Question |
|---|---|---|
| Row ↔ row | `internal/providersync` oracle pairs | Does the Go row builder produce what Python's does? |
| Write ↔ readback | `internal/providersync/oracle_readback_integration_test.go` | Does the row survive ReplacingMergeTree resolution? |
| **Store ↔ store** | **`internal/testsupport/computeparity`** | **Python ran a job against one store, Go against another — do the two stores hold the same table?** |

If you find yourself writing a comparison rule — how to encode a value, when
two values are equal, how an exclusion is declared — stop. It already exists.
Adding a second one is the defect P0 was created to remove.

## The two claims, never merged

| Claim | Tool | Verdicts |
|---|---|---|
| Product-row parity | `internal/testsupport/computeparity` (Go) | divergence messages, or none |
| Operational health | `scripts/worker/compare_runtime_observations.py` | `WITHIN_ENVELOPE` / `OUTSIDE_ENVELOPE` / `UNPROVEN` |

Neither implies the other. A port can write byte-identical rows and burn four
times the memory; it can behave impeccably at runtime and compute the wrong
numbers. The vocabularies are disjoint so a reader cannot mistake one for the
other, and nothing compares product rows in Python.

`compare_runtime_observations.py` returns `UNPROVEN` for every input today,
because `ci/evidence/go-worker-migration/v3-canary-release-proof/parity-thresholds.json`
carries `review.approved: false`. That is the correct answer, not a defect —
threshold approval is a deliberate decision that rides with the R1 pilot.

## Adding a kind

### 1. Declare the row type, not a column list

```go
type doraMetricsDailyRow struct {
    OrgID      string    `json:"org_id" ch:"org_id"`
    RepoID     string    `json:"repo_id" ch:"repo_id"`
    Day        time.Time `json:"day" ch:"day"`
    MetricName string    `json:"metric_name" ch:"metric_name"`
    Value      float64   `json:"value" ch:"value"`
    ComputedAt time.Time `json:"computed_at" ch:"computed_at"`
}
```

`computeparity.Query[T]` derives the `SELECT` from this struct and
`oraclecompare.TypedEncode` reflects every field of it exhaustively. So adding
a column to the table means adding one field here — the query and the diff both
follow. There is deliberately **no** column list to maintain beside it: a hand
list that falls behind the struct means the column it forgot is simply never
compared, and the comparison still reports a pass.

### 2. Declare the table

```go
computeparity.Table{
    Name:        "dora_metrics_daily",
    OrderBy:     "org_id, repo_id, day, metric_name",
    SemanticKey: []string{"org_id", "repo_id", "day", "metric_name"},
    Exclusions: map[string]string{
        "computed_at": "job_dora stamps datetime.now(UTC) once per job run; it " +
            "carries no product meaning and differs on every execution",
    },
    Repeat: computeparity.AppendDuplicates,
}
```

Every exclusion needs a **written reason**, and
`oraclecompare.CheckExclusionIntegrity` additionally fails any exclusion that
never matches a field actually present — an exclusion cannot outlive the thing
it excused.

### 3. Declare the replay policy honestly

| Policy | A second execution over the same input |
|---|---|
| `Idempotent` | leaves the table unchanged |
| `AppendDuplicates` | keeps the key set, accumulates rows |
| `ReplaceWindow` | keeps the key set and count, values may move |

`metrics.dora` is `AppendDuplicates`: `dora_metrics_daily` is a plain
`MergeTree` and `job_dora` never deletes, so a replay appends a second copy.
That is the kind's real behaviour. A port that quietly became idempotent is a
difference worth failing on, not a tidier implementation to wave through.

### 4. Seed from producers, never by hand

`scripts/worker/compute_parity_fixtures.py`:

```bash
compute_parity_fixtures.py provision --dsn "$LEFT"  --reset
compute_parity_fixtures.py provision --dsn "$RIGHT" --reset
compute_parity_fixtures.py seed  --kind metrics.dora --dsn "$LEFT" --as-of 2026-08-22T00:00:00+00:00
compute_parity_fixtures.py clone --kind metrics.dora --from-dsn "$LEFT" --to-dsn "$RIGHT"
compute_parity_fixtures.py produce --kind metrics.dora --dsn "$LEFT"  --as-of ...   # Python reference
# ... and your native Go executor against $RIGHT
```

`provision` applies the **real checked-in migration chain** through the same
`dev-hops` entrypoint the CLI uses. No DDL is authored in a test: a hand-typed
schema is a second, unversioned copy of one, and a comparison over it only ever
confirms what the test itself declared.

`seed` builds rows with the production fixture generators and writes them
through the production writers, then rebases the generated window onto a
declared anchor so the fixture is reproducible.

`clone` copies the declared input tables to the other store and then **proves**
the copy: it refuses a non-empty destination, and compares row counts plus an
order-independent SHA256 over the sorted multiset of every row on both sides.
That is what makes "both sides consumed identical input" a fact rather than an
assumption — and the premise the entire parity claim rests on.

An earlier version used `groupBitXor(cityHash64(*))`, which is commutative but
also self-cancelling: a duplicated row contributes zero. Measured on this
repo's ClickHouse, `('r1','r2','r2')` and `('r1','r3','r3')` produce the
*identical* XOR aggregate — an equality proof that cannot see two completely
different tables. The SHA256 form distinguishes them and stays
order-independent.

To add a kind, add a `seed_<kind>` function and register it in `KINDS` with the
input tables it reads.

### 5. Make it a port proof, not a self-test

`computeparity.RunProducer` executes each side and records what actually ran —
the resolved binary and entry point. `RequirePortProof` then refuses a pair
whose two sides have the same observed identity:

```go
left  := computeparity.RunProducer(t, "python", root, env, python, script, "produce", ...)
right := computeparity.RunProducer(t, "go", root, env, goBinary, "metrics.dora", ...)
computeparity.RequirePortProof(t, left, right)
```

Identity is **observed, not declared**. An earlier version took an
implementation string from the caller, which meant a port test could keep
invoking the Python reference on both sides, call one of them `"go"`, and
satisfy the guard while proving nothing — the exact degradation the guard
exists to prevent, re-entering through its own input. Calling a Python run
"go" now changes nothing.

`dora_table_parity_integration_test.go` is a comparator **self-test** and says
so in its name: both sides run Python because the Go DORA executor is slice R1.
It asserts the guard REFUSES that pair.

Symlinks are resolved, so two names for the same file are one implementation.
The guard stops there: it does **not** hash executable contents or detect a
wrapper that re-execs the reference producer under another name. That needs
execution provenance this repo has no plane for, and it is a decoy someone
would have to build deliberately — the realistic mistake, forgetting to point
one side at the native executor, is caught.

### 6. Prove it fails

A comparator that has not been shown to fail has not been shown to work.
`internal/testsupport/computeparity` ships the three controls the slice was
accepted on — a mutated row, a dropped row, and a float nudged by one ULP —
both as unit tests and against real rows in
`dora_table_parity_integration_test.go`. Port them for your kind.

Two of those controls failed on first run for reasons that were bugs in the
*controls*: one formatted the `{"t","v"}` envelope instead of its leaf, and one
removed a single row after a replay had already appended a second copy of every
key, which the comparator correctly reported as a multiplicity difference
rather than an absent key. Both are worth knowing before you write yours.

## Safety

`provision` runs `DROP DATABASE`, so it is guarded four ways and none is a
blacklist:

1. **Shape** — a name that is not a plain identifier is refused before any
   statement runs, not quoted and hoped.
2. **Allowlist** — the database must start with `parity` or `ci_local_validate`.
   Refusing only `default` was never a boundary: a production DSN such as
   `.../devhealth` passed it.
3. **Ownership** — every database this tool creates gets a
   `compute_parity_scratch_marker` table, written *before* the migrations so a
   database left behind by a failed migration stays reclaimable. No existing
   database is dropped without it: a name can be typed by mistake, the marker
   cannot be there by mistake.
4. **Intent** — dropping an existing database still needs an explicit `--reset`.

## Known limitation

Runtime observations are **self-declared**. Scope digests and build identity
are checked for shape and internal consistency; they are not recomputed from an
independent source and the build is not resolved against an artifact registry,
because no attestation plane exists in this repo. Every runtime report records
`attestation: "self_declared"` for that reason. That gap belongs with the
outstanding v3 threshold approval, not with a single slice.

## Related

- `CHAOS-4111` — `map_issue_incidents` leaves `valid_from` unset, and the
  canonical incident projection filters `valid_from <= {as_of}`; `NULL <= x` is
  `NULL`, so DORA silently loses its incident-derived metric on fixture paths.
  The seeder sets it explicitly and says so in-code.
- These manifests are **not** a runtime contract tree — `dev_health_ops` never
  reads them, only the harness does, from a checkout. They are deliberately
  absent from `[tool.setuptools.data-files]`.
