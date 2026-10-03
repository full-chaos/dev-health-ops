---
name: go-checks
description: Run the Go gate (ci/check_go.sh) correctly — required for any Go change, since ci/local_validate.sh does NOT cover Go
disable-model-invocation: true
---

# Go checks

`ci/local_validate.sh` does **not** validate Go. Its Go stage is commented out
(`ci/local_validate.sh:1189`, `# run_stage "go: format + vet + test" go_fast gate_go_fast`),
so a `GATE PASSED. [8/8 ...]` line says nothing about a Go diff. Any change under
`internal/`, `cmd/` or `contracts/` needs this gate as well.

## Invocation

```bash
cd <your ops worktree root>
bash ci/check_go.sh all
```

Expect `rc=0`. The `test`/`race` verbs alone report ~118 `ok` package lines;
`all` additionally runs the FULL integration suite for real (every
non-denylisted package, unsharded) plus `multi-replica-workers`, both against
real containers, so expect several more minutes and **Docker running
locally** — `all` has required Docker unconditionally since `multi-replica-workers`
was added, `check_integration` is not a new prerequisite, only new time.

Three verbs, in ascending cost:
- `fast` — format + vet + unit + build + contract +
  multi-replica-workers + an integration shard **plan** only (no execution,
  no race detector). The quick local-iteration mode.
- `ci` — `fast` plus the race detector. Byte-for-byte what `all` ran before
  CHAOS-3948. This is what `go.yml`'s `go-quality` step runs — CI's real
  integration signal comes from the separate, already-sharded
  `go-storage-integration-plan`/`-shard` jobs, not from this step, so
  duplicating a slow unsharded run here would only blow that job's 20-minute
  budget for no new coverage.
- `all` (default) — `ci` plus the FULL integration suite executed for real
  (measured ~24m alone). The honest full local pre-push signal; use this
  before pushing a Go change, not `ci`.

For fast local iteration on one package without paying for the whole suite:

```bash
GOWORK=off go test -mod=readonly -tags=integration -count=1 ./internal/<pkg>/...
```

## No Python for the Go gate

The live-Python oracles (Go tests that started the Python implementation and
compared answers) were deleted (CHAOS-7308): Go is the implementation of
record. The recorded answers stay as plain Go regression tests (frozen
goldens) and need no interpreter, no `.venv` and no `PYTHON`. A Go test that
needs Python is a defect; do not add one.

## Hard rules

- **`ok` does not mean "ran".** Env-gated suites skip and the package still
  prints `ok`. Count before believing a green:
  ```bash
  go test ./internal/<pkg>/... -v 2>&1 | grep -cE '^\s*--- (PASS|SKIP|FAIL)'
  ```
  A suite that skipped everything is not evidence.
- **Integration tests are behind a build tag** and are opt-out, not optional:
  `ci/check_go.sh all` runs them for real (CHAOS-3948 — it used to only PLAN
  the shards and never execute them, which is exactly the "ok doesn't mean
  ran" trap above one level up: `rc=0` read as complete while the shards
  never ran). `fast` and `ci` both still only plan — run `all` or
  `ci/check_go.sh integration` when you need the real signal.
  ```bash
  go test -tags=integration ./internal/<pkg>/... -count=1
  ```
- `TestFenceAgainstMigratedPostgres` moved to
  `internal/syncroute/fence_integration_test.go` (CHAOS-3948, part of the
  CHAOS-3448 never-run-live-Postgres family). It used to skip unconditionally
  — nothing in `check_go.sh` or `go.yml` ever set the env var it was gated
  on, so it had never run in the automated pipeline. Now integration-tagged
  and wired to `containers.StartPostgres`, the same pattern this package's
  `control_integration_test.go` already used; it runs for real under `all`
  or `integration` now.
- **A red check means no merge.** No diagnosed exclusions.

## Frozen goldens

A behaviour that once existed in both Python and Go is held by a frozen golden
(a recorded Python answer). Recording stops: do not re-record. A golden test
that fails means the Go side drifted; fix Go. Confirm a golden test is
comparing what you changed rather than passing vacuously: perturb the Go side,
watch it FAIL, and check that the diff names the recorded value you expect.

## Constants that must move in lockstep

`internal/synccoverage/types.go`'s `projectionVersion` and
`SYNC_COVERAGE_PROJECTION_VERSION` in
`src/dev_health_ops/api/services/sync_coverage.py` are read by different
processes against the same table. The API filters projections on its value, so
a mismatch makes every projection unreadable and coverage returns 503
indefinitely. Bump both in one changeset, and remember the Python side is
bind-mounted into the API container (live on save) while the Go side ships in a
built image — they do **not** go live at the same moment locally.
