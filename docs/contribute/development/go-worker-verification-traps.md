---
page_id: con-go-worker-traps
summary: Verification traps in the Go worker codebase where a broken check reports success, and the control that distinguishes each one.
content_type: reference
owner: engineering
source_of_truth:
  - ci/check_go.sh (Go gate stages)
  - internal/storage/postgres/domain_authorization.go (role postures)
applicability: current
lifecycle: active
---

# Verification traps in the Go worker runtime

Every entry below is a case where a check reported success while the thing it
checked was broken. They are collected because they share one shape: the
failure biases toward *fine*, so nothing prompts a second look.
{: .fc-page-lede }

Read this alongside
[Go worker runtime architecture](../architecture/go-worker-runtime.md), which
describes the boundaries these checks are supposed to protect.

## A superuser test proves nothing about roles

The Go fleet runs under three separate PostgreSQL login roles with exact-match
postures. A test that connects as the database superuser holds every privilege,
so a statement issued on the wrong pool succeeds — and the same statement
returns SQLSTATE 42501 the moment it meets the production role split.

That is exactly how the CHAOS-4005 unreclaimable sweep shipped dead: it read a
coordinator-exclusive table on the domain pool, passed its tests, and returned
permission denied on every pass in production (CHAOS-4035).

**Control.** An integration test for any pool-sensitive path must connect as the
real, separately granted role, with each role holding exactly its declared
posture. Do not widen a posture to make a test pass — that inverts the
assertion, because `CheckRolePosture` fails on an *extra* grant as readily as on
a missing one.

## A shared error string hides the fault

`internal/syncreconciler` returns one sentinel, `ErrUnavailable`, from 61 lines
across 8 files, and its text names the observer
(`internal/syncreconciler/observer.go:41`). A permission failure in the sweep
therefore logs a line naming a healthy, uninvolved component and no SQLSTATE.
The same shape exists in `internal/jobruntime`, where
`errIdempotencyUnavailable` covers at least nine conditions and surfaces as
`dev-health job failed [idempotency]`.

Both discard the underlying `pgx` error rather than wrapping it. Keeping
connection material out of a caller-facing error is right; discarding the
SQLSTATE and the statement identity is what makes a provisioning defect
indistinguishable from an outage — and those need opposite responses.

**Control.** A caller-facing sentinel and an operator-facing log line are
different audiences and can coexist. Log the underlying error where it occurs,
naming the component and the statement, and keep returning the sentinel.
`internal/storage/postgres/posture_diagnostics.go` is the precedent for naming a
privilege gap without leaking a DSN. Tracked as CHAOS-4036 and CHAOS-4028.

## A start-only probe cannot see a dependency that leaves

`preclaim-readiness` (`cmd/dev-health-worker/dependencies.go:309`) is evaluated
once, at startup, and it works: it refused to admit four workers whose pools
were unreachable and exited fail-closed. But it is never re-evaluated. When a
pooler was recreated seventeen seconds after admission, every worker reported
healthy for two hours while completing no job at all (CHAOS-4029).

The probe answered "were my dependencies reachable when I booted?" while the
question being asked was "can this worker do work right now?"

**Control.** When adding a health signal, state which of those two questions it
answers. A dependency check that runs once belongs to admission, not to
liveness, and should not be read as either.

## A pipe replaces the exit code you meant to read

In a shell pipeline the status of the last command wins, so `script | tail`
reports `tail`'s success regardless of what `script` did. A gate stage was
recorded as `rc=0` this way and had to be re-run unpiped.

**Control.** Run gate commands unpiped and read the real exit code. If you need
the output filtered, capture to a file first and filter the file. Do not infer a
verdict from output shape either: `ci/check_go.sh` prints no verdict line at
all, so a log that simply ends is what a *successful* run looks like — only
`ci/local_validate.sh` emits `GATE PASSED` and `GATE_STAGE_MANIFEST`.

## A skipped Go suite reads as `ok`

`go test` prints `ok` for a package whose tests all skipped. Env-gated
integration suites therefore look identical to suites that ran and passed, in
both the terminal and any harness that greps for `FAIL`.

**Control.** A harness must count what executed, not what failed to fail. Assert
the expected number of tests ran, or assert the gating variable was set, rather
than reading absence-of-`FAIL` as coverage.

## A zero result is unproven, not clean

Several of the checks above return an empty result on both "nothing is wrong"
and "the check never ran". Queue depth has the same property from the operator
side: when jobs are minted and discarded at the same rate, depth stays at zero
and the system reads as idle rather than as destroying work.

**Control.** Pre-seed counters so that absence is distinguishable from "never
ran", following the `worker_daily_metrics_lease_total{stage,result}` contract.
Count refusals rather than filtering them: a guard whose refusals are invisible
cannot be told apart from one that never fires.

## A test can pass for a reason the mechanism does not uniquely produce

A blank-value test asserted that a blank flag resolves to the default, with no
environment value present. Both "blank is ignored" and "blank overrides with
nothing" satisfy that assertion, so it passed while the second was true and a
blank flag was shadowing a non-blank environment value.

**Control.** Construct the case that distinguishes the two behaviours — here, a
non-blank environment value behind the blank flag. Then remove the fix and
confirm the test fails. A test that has never been observed failing has not been
shown to bind.

## A hand-written test schema proves the code against a database that does not exist

Integration-test DDL must be **derived from the alembic migrations**, never
hand-authored: a suite that invents a column proves the query against a schema
production does not have, and stays green while the shipped SQL fails to parse
in production (CHAOS-4041 — the strand repair's test DDL invented
`daily_metrics_partitions.org_id` and the merged query crash-looped the prod
reconciler on `column partition.org_id does not exist`).

## Related

- [Testing layers and local validation](testing.md)
- [Debug and observe development environments](debugging.md)
- [Go worker runtime architecture](../architecture/go-worker-runtime.md)
