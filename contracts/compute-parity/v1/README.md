# Compute-parity manifests v1

The shared oracle every CUT-20 compute-port slice uses to make a parity claim
(CHAOS-3092 P0; also CHAOS-3090 clause 4). Before this existed, each lane was
free to invent its own definition of "the Go port matches Python", and none of
them was comparable with any other.

## The two claims, and why they never share a report

| Claim | Command | Evidence | Says |
|---|---|---|---|
| `algorithm_row_parity` | `compare_compute_outputs.py rows` | Two isolated scratch destinations built from the same producer-derived fixture | The two implementations write the same product rows |
| `operational_health` | `compare_compute_outputs.py runtime` | The pinned `v0-celery-baseline` capture and `v3-canary-release-proof` thresholds | The Go runtime stays inside the recorded Celery operational envelope |

Neither implies the other. A port can be byte-identical on rows and burn four
times the memory; it can be perfectly well-behaved operationally and compute the
wrong numbers. Every report carries a `claim` field and the two verdict
vocabularies are disjoint (`EQUAL`/`DIFFERENT`/`INDETERMINATE` versus
`WITHIN_ENVELOPE`/`OUTSIDE_ENVELOPE`/`UNPROVEN`), so a reader cannot mistake one
for the other and a reviewer cannot be handed one when they asked for the other.

`runtime` currently returns `UNPROVEN` for every input, because
`v3-canary-release-proof/parity-thresholds.json` carries `review.approved:
false`. That is the correct answer, not a defect: the thresholds have never been
approved. It reuses `canary_release_proof.py`'s pinning and threshold rules
rather than re-deciding them.

## Where things live

| Thing | Path |
|---|---|
| Comparator | `scripts/worker/compare_compute_outputs.py` |
| Fixture seeding / reference execution | `scripts/worker/compute_parity_fixtures.py` |
| Manifest schema | `contracts/compute-parity/v1/manifest.schema.json` |
| Manifests | `contracts/compute-parity/v1/<kind>.json` |
| Unit coverage (runs in the standard gate) | `tests/scripts/test_compare_compute_outputs.py` |
| Live end-to-end proof (`-m clickhouse`, opt-in) | `tests/scripts/test_compare_compute_outputs_live.py` |

The wired reference kind is `metrics.dora` — the R1 pilot's smallest
deterministic output: one table, six columns, one volatile column, no JSON, no
generated identifiers.

## Running a comparison

```bash
L=clickhouse://ch:ch@localhost:8123/parity_left
R=clickhouse://ch:ch@localhost:8123/parity_right

.venv/bin/python scripts/worker/compute_parity_fixtures.py provision --dsn "$L"
.venv/bin/python scripts/worker/compute_parity_fixtures.py provision --dsn "$R"
.venv/bin/python scripts/worker/compute_parity_fixtures.py seed  --kind metrics.dora --dsn "$L"
.venv/bin/python scripts/worker/compute_parity_fixtures.py clone --kind metrics.dora \
  --from-dsn "$L" --to-dsn "$R"

.venv/bin/python scripts/worker/compare_compute_outputs.py rows \
  --manifest contracts/compute-parity/v1/metrics.dora.json \
  --left-dsn "$L" --right-dsn "$R" \
  --left-label python --right-label go \
  --right-exec "<the native Go executor, reading PARITY_DSN>" \
  --repeat 2 --as-of 2026-08-22T00:00:00Z --out /tmp/parity.json
```

Exit codes: `0` EQUAL / WITHIN_ENVELOPE, `1` DIFFERENT / OUTSIDE_ENVELOPE, `2`
the comparison could not be made at all, `3` INDETERMINATE / UNPROVEN. `2` and
`3` are not softer versions of `0`.

Producers receive `PARITY_DSN`, `PARITY_SIDE`, `PARITY_RUN_INDEX` and
`PARITY_AS_OF` in the environment. `--no-exec` compares two destinations exactly
as they stand.

## Adding a kind

1. **Copy `metrics.dora.json`** and set `kind` to the registered job kind. The
   file name must equal the kind (a guard test asserts it).
2. **Declare the fixture.** Add a `seed_<kind>` function to
   `compute_parity_fixtures.py` that builds rows with the production fixture
   generators and writes them through the production writers, then register it
   in `KINDS` with the input tables the kind reads. Never hand-author rows and
   never hand-write DDL: the schema comes from the checked-in migrations, which
   `provision` applies through `dev-hops`.
3. **Declare every source of run-to-run variation** under `determinism`
   (`clock`, `seed`, `id_source`, plus `model`/`prompt` for LLM kinds). Each
   entry needs a written `notes`; a guard test fails an empty one. Anything you
   do not declare here and do not canonicalize in `outputs[]` shows up as a
   false difference in every future run.
4. **Declare the inputs.** The comparator digests them on both sides *before*
   comparing any output. A mismatch makes the run `INDETERMINATE`, never
   `DIFFERENT` — an output comparison over different inputs is not a parity
   claim.
5. **Declare the outputs.** For each table: the `select`, the `semantic_key`,
   every selected column under `fields`, and the `repeat_policy`. The
   comparator refuses a snapshot whose columns disagree with `fields`, so a
   `select` cannot quietly drift away from the declaration.
6. **Canonicalize the volatile columns**, not the meaningful ones:
   - `drop` — no product meaning and differs every run (a `computed_at` stamp).
   - `placeholder` — only null-ness is comparable.
   - `utc_normalize` — a real timestamp; normalized to UTC then compared exactly.
   - `ordinal` — a generated identifier whose *ordering* among the run's own
     values is meaningful even though the value is not.
   A key column may not be canonicalized away; the loader refuses it, because a
   collapsed key hides a missing row behind an extra one.
7. **Set the numeric policy per field.** `exact` is the posture. A tolerance is
   declared on the one field that needs it and must carry a written `reason`
   naming the persisted column precision that forces it — the loader refuses a
   tolerance without one, and the schema has no place for a global tolerance.
   A tolerance-compared field is excluded from the canonical row digest and the
   report names it under `digest_excluded_fields`, so the reduction in what was
   proven is visible rather than implied.
8. **Add the Go producer** under `producers.go` once the port exists. Nothing
   else in the manifest changes when a kind is ported — that is the point.
9. **Prove it.** Add the kind to the live test's coverage and run the
   self-test (same implementation both sides → `EQUAL`) plus the three negative
   controls (mutate a row, drop a row, nudge a float past its policy → each
   reported precisely). A comparator that has not been shown to fail has not
   been shown to work.

## Repeat runs

`--repeat 2` executes each side twice and checks the observed replay behaviour
against the declared `repeat_policy`, **per side**:

| Policy | Observed as |
|---|---|
| `idempotent` | count and canonical row digest both unchanged |
| `append_duplicates` | key set unchanged, count grew |
| `replace_window` | key set and count unchanged, rows may differ |
| `tombstone` | as `replace_window`/`idempotent`, with `tombstone_predicate` naming the marker rows |

`metrics.dora` declares `append_duplicates`: `dora_metrics_daily` is a plain
`MergeTree` and `job_dora` never deletes, so a replay appends a second copy.
That is the kind's real behaviour, and a port that quietly became idempotent
would be reported as a repeat-policy violation rather than passing because it
"looks cleaner".

## Safety

Both sides must be scratch databases. The comparator refuses `default`
outright — it holds real dev data (see `ops/AGENTS.md`, "Safety rule") and
`--left-exec`/`--right-exec` write to whatever they are pointed at. It also
refuses two sides that resolve to the same database.
