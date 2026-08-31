---
page_id: con-integration-suite-targets
summary: Which Go and Python integration suite runs where — host Testcontainers, kiac in-cluster datastores by env DSN, or GitHub Actions — and the ClickHouse engine versions that decide it.
content_type: reference
owner: engineering
source_of_truth:
  - internal/testsupport/containers/harness.go (the single helper every Go integration suite obtains PostgreSQL/ClickHouse/Valkey from)
  - internal/testsupport/containers/remote.go (the env-DSN path and scratch-database lifecycle)
  - ci/go_integration_shards.tsv (the authoritative package list and CI weights)
  - .github/workflows/test.yml (the ClickHouse and PostgreSQL service versions CI runs)
applicability: current
lifecycle: active
---

# Integration suite targets: Testcontainers, kiac, or CI

Every Go integration suite in dev-health-ops needs a real datastore. There are
three places it can come from, and picking the wrong one produces a green run
that proves nothing.

Tracked by CHAOS-4428. The companion runbook,
[Lane isolation on a kiac cluster](lane-isolation-kiac.md), covers how a lane
cluster is built; this page covers only **where a given suite should run**.

## The three targets

| Target | What it is | What it costs |
| --- | --- | --- |
| **Host Testcontainers** | `StartPostgres`/`StartClickHouse`/`StartValkey` start a container per caller through Docker. The default. | Docker CPU on the developer's Mac — the allocation CHAOS-4428 exists to reduce. |
| **kiac in-cluster** | The same helpers, pointed at a cluster's existing datastores by env DSN. Every caller **that goes through the harness** gets its own scratch database; a resource created out-of-band is outside that ownership model — see below. | Near zero extra CPU: the datastores are already running. |
| **GitHub Actions CI** | The suite is not run locally at all; the `go-storage-integration-*` jobs are the signal. | No local cost, but see the version table — CI does not run what production runs. |

## The rule

**Start from the engine versions, not from convenience.** They are not the same
across the three targets, and that is what decides the routing:

| Where | ClickHouse | PostgreSQL | How this was established |
| --- | --- | --- | --- |
| Host Testcontainers (this repo) | **26.6.1.1193** | 18-alpine | The image is digest-pinned with no version in the source; resolved from the registry config blob label `com.clickhouse.build.version` (built 2026-06-26). Independently corroborated in-repo: `internal/providersync/linear_stale_project_ownership_cleanup.go:75-82` records the same resolution, reached by running against the digest directly. |
| GitHub Actions CI services | **25.1** | 18-alpine | `.github/workflows/test.yml:165,335`; `.github/workflows/live-e2e.yml:82,176`. |
| kiac in-cluster (`acr-local`) | **26.7.3.19** | **18.6** | Live readback, `SELECT version()` / `SHOW server_version`, 2026-08-31. |
| acr's own Testcontainers | 26.7.5.10 | 18-alpine | Pinned deliberately by CHAOS-4549; floor `26.7` in `acr/internal/chfixture`. |
| Production | 26.7.x (floating) | — | CHAOS-4519: prod's exact patch drifts, so the floor is `major.minor`. |

Three consequences follow, and they are the whole decision procedure:

1. **CI cannot prove ClickHouse engine behaviour.** It runs 25.1 against a
   production line of 26.7. CHAOS-4549 is the precedent for why the gap
   matters rather than being cosmetic: a multi-arm `JOIN ... ON (... OR ...)`
   is accepted on 26.7 and rejected on 24.8 with
   `Code: 403 Unsupported JOIN ON conditions`, under every analyzer setting.
   A version gap changes *what SQL is accepted at all*.
2. **The ops host Testcontainers cannot prove it either.** They run
   26.6.1, one minor **below** the 26.7 floor acr ruled for exactly this
   reason — so an ops host container would fail acr's own
   `AtLeastVersionFloor` check. This is the caveat that is easiest to miss:
   "I ran it against a real ClickHouse locally" is not the same claim as "I
   ran it against production's engine".
3. **kiac is the only target in dev-health-ops that matches production's
   ClickHouse line.** So moving ClickHouse-touching suites there is a
   correctness upgrade, not only a Docker saving.

> **Open:** the ops ClickHouse pin is an opaque digest set under CHAOS-3033 and
> never revisited, and it disagrees with acr's ruled 26.7 floor. Reconciling
> the two is tracked separately — it changes what every ops ClickHouse suite
> runs against and needs its own red-first change, so it is deliberately not
> folded in here.

## Decision flow

Routing is **per store**, not per suite: each store is decided on its own, and
one package can get a different answer for each of its three.

> **The table below is the only place this page asserts where anything runs.**
> Everything else — this flow, and every paragraph — explains *why* a store is
> blocked, never *what* routes where, and makes no universal statement about a
> class of suite. That rule exists because the prose kept drifting from the
> table: four separate corrections traced to sentences generalising about
> "Valkey suites" or "role suites", every one of them wrong about
> `cmd/dev-health-worker` and `internal/syncdispatchruntime`, which are blocked
> by two stores at once. A second source of truth for routing is a second thing
> that can be wrong; read the row.

```mermaid
flowchart TD
    A[Integration suite] --> P{Is every Start* result<br/>eventually Closed?}
    P -- no --> PF[Fix that FIRST: a discarded Instance<br/>leaks a database on every run,<br/>including passing ones]
    P -- yes --> Q[Ask these of EACH store<br/>independently]

    Q --> O{Is anything created<br/>behind the harness?}
    O -- yes --> O1[Nothing namespaces or drops it,<br/>so lanes collide]

    Q --> R{Does it create cluster-scoped objects?<br/>CREATE ROLE, tablespace,<br/>event trigger}
    R -- yes --> R1[A scratch database does not isolate<br/>these, and two lanes collide on a<br/>fixed name even if one drops it first]

    Q --> V{Does it use Valkey?}
    V -- yes --> V1[Valkey has no CREATE DATABASE<br/>to isolate parallel callers]

    Q --> C{Does it touch ClickHouse?}
    C -- yes --> C1[CI runs 25.1 against a 26.7<br/>production line, so it cannot<br/>prove engine behaviour]

    O1 --> T[Read that store's cell<br/>in the table below]
    R1 --> T
    V1 --> T
    C1 --> T
```

The flow asks the questions; **the table answers them**. A `yes` to any of these
is a reason a store may be blocked, not a routing decision — the row is the
routing decision.

Note what is **not** a fork here. "Does it assert a pristine database?" used to
be one, and should not be: every caller gets a freshly created scratch database,
so that requirement is satisfied on either target. The distinction that matters
for those suites is scratch vs. *seeded*, which is a different question and is
covered below.

## Per-package matrix — Go, dev-health-ops

Weights are the declared CI shard weights from `ci/go_integration_shards.tsv`;
they are the best available proxy for what each package costs. "Stores" is
derived from which harness entry points the package's files call — including
files that reach the harness through a package-local helper without importing
`testcontainers` themselves, which is the majority.

**Routing is per store, and the table says so per store.** An earlier version
of this page gave each package a single target, which cannot express a package
that needs Valkey local for one reason and PostgreSQL local for another — and
it silently lost the second reason wherever both applied. A column per store
removes that failure by construction.

Alongside it, one separate question: **is CI sufficient proof?** — whether the
`go-storage-integration-*` jobs can stand in for a local run at all. That is
where the version table bites.

**The bar is two lanes running concurrently** (chris, 2026-08-31), which is
CHAOS-4428's own scope item 5. That is what makes a fixed name disqualifying:
a suite creating `CREATE ROLE reindex_domain_runtime` is re-runnable on its own
and still unsafe beside a second lane doing the same.

PostgreSQL-only packages get **yes**: CI's PostgreSQL is the same 18 line as
kiac (18.6) and production, so nothing about the engine differs. ClickHouse-
touching packages get **no** unless every construct they exercise is engine-
neutral — CI is 25.1 against a 26.7 production line.

Engine-sensitivity below is per-package and evidenced. The recurring sensitive
constructs in this codebase are `FINAL` on `ReplacingMergeTree`, `argMax`
tie-breaks, window functions, and insert-block dedup `SETTINGS` — all of them
semantics that a version change can move.

| Package | Weight | PG | CH | Valkey | CI proof? | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `internal/providersync` | 1166s | kiac | kiac | — | **no** | Sensitive. `FINAL`/`argMax`/`ReplacingMergeTree` in 53 of 55 CH-touching files, plus a dedup-window `SETTINGS` test. Largest single cost in the repo. Role name parameterised by CHAOS-4661 for both re-run- and concurrency-safety; self-cleans via `containers.DropRole` (see "Executed evidence" below). |
| `internal/scheduler/fixed` | 143s | kiac | — | — | yes | Self-seeding PostgreSQL. |
| `internal/streamhandlers` | 113s | — | kiac | — | **no** | Sensitive. `argMax` tie-break over `(occurred_at, event_id)`; migration 077 states outright that a tie lets ClickHouse "return either key". Weight is *almost entirely container startup* (six tests, fresh container each) — the biggest per-package saving. |
| `internal/storage/postgres` | 91s | kiac | — | — | yes | Role names parameterised by CHAOS-4661 (7 creation sites across this package -- 6 that literally `CREATE ROLE`, plus `provision_script_integration_test.go`'s `TestProvisionScriptGrantsNoTablePrivileges`, whose roles come from an external `psql --file=provision_river_roles.sql` invocation -- each self-cleaning via `containers.DropRole`; plus 3 same-package pure consumers of `domain_grant_reconciliation_integration_test.go`'s shared `startGrantHarness` fixture a `CREATE ROLE` grep could not see -- `coordinator_statement_privileges`, `posture_diagnostics`, `fixed_engine_statement_privileges`, and `provision_script`'s other test, `TestProvisionScriptNeverWipesMigrateGrants`). Otherwise pure PostgreSQL. |
| `internal/testsupport/computeparity` | 50s | — | **host** | — | **no** | Creates FIXED-name databases (`parity_left`, `parity_right`, `parity_capacity_*`) through a fixture tool outside the harness, and provisions them with `--reset`. On a shared cluster one lane drops another's live database. Also sensitive: `capacity_table_parity` uses `FINAL` on `ReplacingMergeTree(computed_at)`. Out of CHAOS-4661 scope; tracked as CHAOS-4677. |
| `internal/jobs/report` | 33s | kiac | kiac | — | **no** | Mixed: most files use only `LIMIT 1 BY`/`uniqExact`, but `team_metrics_daily_ratio` uses `countIf(...) OVER (PARTITION BY ...)`. |
| `internal/scheduler/sync` | 32s | kiac | — | — | yes | Pure PostgreSQL. |
| `cmd/dev-health-worker` | 24s | kiac | kiac | **host** | **no** | Sensitive via `dora_refusal_boot`, which classifies ordering contracts from `system.tables.sorting_key`. Role names parameterised by CHAOS-4661; still host-bound overall for Valkey -- moving PostgreSQL alone does not move the package until Valkey is also resolved (CHAOS-4666). |
| `internal/syncreconciler` | 16s | kiac | — | — | yes | Role names parameterised by CHAOS-4661: `unreclaimable_sweep_role_split_integration_test.go` plus 3 same-package consumers of `kernel_integration_test.go`'s shared role fixture (`active_active_integration_test.go`, `terminal_delivery_repair_integration_test.go` × 3 call sites) that a `CREATE ROLE` grep could not see. |
| `internal/externalrecompute` | 15s | kiac | — | **host** | yes | Uses Valkey. |
| `cmd/dev-health-workerctl` | 13s | kiac | — | — | yes | Role names parameterised by CHAOS-4661. |
| `internal/joboperator` | 13s | kiac | — | — | yes | Role names parameterised by CHAOS-4661 (2 test functions sharing 1 setup helper). |
| `internal/synccoverage` | 13s | kiac | — | — | yes | Pure PostgreSQL. |
| `internal/testsupport/containers` | 13s | **host** | **host** | **host** | yes | Engine-neutral (boot/open/close only) — but it is the harness's own self-test, so it must keep exercising the container path. |
| `internal/joboutbox` | 12s | kiac | — | — | yes | Role names parameterised by CHAOS-4661 (`relay_integration_test.go`, `strand_repair_integration_test.go`). |
| `internal/jobs/system` | 12s | kiac | — | — | yes | Pure PostgreSQL. |
| `internal/providerfoundation` | 12s | kiac | kiac | **host** | **no** | Sensitive: asserts insert-block dedup under `SETTINGS non_replicated_deduplication_window=100`. |
| `cmd/dev-health-reconciler` | 10s | kiac | — | — | yes | Pure PostgreSQL. |
| `internal/jobs/pagerduty` | 9s | kiac | — | — | yes | Pure PostgreSQL. |
| `internal/storage/river` | 9s | kiac | — | — | yes | Role names parameterised by CHAOS-4661 across `migrate_integration_test.go` and `telemetry_integration_test.go` -- both independently call `containers.RoleName` and create their own roles (not a shared-fixture consumer pair, despite the similar name); each self-cleans via `containers.DropRole`. **Dual-path backup/restore**: its round-trip test dispatches on `instance.Container` -- non-nil (host Testcontainers, including CI) runs `pg_dump`/`createdb`/`pg_restore` via `instance.Container.Exec`, reusing the `postgres:18-alpine` image's own bundled PostgreSQL 18 client tools with zero host dependency (this is the original, pre-CHAOS-4661 mechanism, restored); nil (the kiac remote path, no container to exec into) runs the same three binaries as host processes instead. **Host-tools requirement is remote-path only**: `assertPostgresClientToolsAvailable` (PATH + PostgreSQL 18+ major-version check, failing with an install instruction rather than an opaque exec error) gates only the host-process branch -- CI, which runs the container path, carries no such dependency and is unaffected by what PostgreSQL client tools its runner happens to have. |
| `internal/jobs/workgraph` | 7s | kiac | — | — | yes | Pure PostgreSQL. |
| `internal/jobroute` | 6s | kiac | — | — | yes | **Demonstrated** — see below. |
| `internal/jobs/metrics/daily` | 6s | kiac | kiac | — | **no** | Role name parameterised by CHAOS-4661 -- this is the package the original collision was found on (`finalize_redrive_test_domain`, SQLSTATE 42710). Also sensitive: `argMax` tie-break, `DateTime64(6)` precision, `INNER JOIN ... FINAL`. **Demonstrated** — see below. |
| `internal/jobs/metrics/remaining` | 6s | kiac | kiac | — | **no** | Mixed: the capacity schema guard reads `system.tables` as strings (neutral), but `dora_ordering_contract` tests `FINAL` vs `LIMIT 1 BY` divergence directly. |
| `internal/syncdispatchruntime` | 6s | kiac | kiac | **host** | **no** | Sensitive: `argMax(id, updated_at)` dedup readback. Role names parameterised by CHAOS-4661 across 3 files (7 test functions sharing 1 setup helper, plus 2 standalone files); still host-bound overall for Valkey, same as `cmd/dev-health-worker`. Also fixed a cross-*package* literal collision: this package and `internal/storage/postgres`'s grant-reconciliation suite hard-coded the identical role name `grant_domain_runtime`, which ordinary `go test ./...` parallelism could already collide on, independent of any lane/cluster concept. |
| `cmd/query-api/internal/routeswitch` | 5s | kiac | — | — | yes | Pure PostgreSQL. |
| `internal/jobrescue` | 5s | kiac | — | — | yes | Pure PostgreSQL. |
| `internal/jobruntime` | 5s | kiac | — | — | yes | Pure PostgreSQL. |
| `internal/syncroute` | 3s | kiac | — | — | yes | Pure PostgreSQL. |
| `internal/cacheinvalidation` | 2s | — | — | **host** | yes | Valkey only. |
| `internal/streamrunner` | 2s | — | — | **host** | yes | Valkey only. |

**Nine packages carry engine-sensitive ClickHouse SQL and cannot be proven by
CI at all** — 1416s, 76.5% of the total integration weight. Three of them
(`computeparity`, `jobs/report`, `jobs/metrics/remaining`) are sensitive only
in *some* files; splitting those files into their own packages would let the
neutral remainder defer to CI, which is a worthwhile follow-up but is not done
here.

Two classifications were left explicitly uncertain rather than guessed:
`jobs/metrics/daily/repo_user_commit_org_scope` (queries `FROM work_items FINAL`
but never inserts into that table, so `FINAL`'s effect is not outcome-asserted)
and `cmd/dev-health-worker/multi_family_boot` (may transitively hit the
ordering guard but asserts nothing about it). Both sit inside packages already
routed to kiac, so the uncertainty changes no routing decision today.

### The second blocker: PostgreSQL roles are cluster-scoped — RESOLVED by CHAOS-4661

A scratch **database** isolates tables. It does **not** isolate roles, tablespaces
or event triggers — those live in the cluster, which is shared.

19 test files across 10 packages ran `CREATE ROLE`, and **17 of them did not drop
the role first**; they relied on the container being a brand-new cluster. Point
one of those at a shared server and the first run passed, left the role
behind, and the second run failed:

```
ERROR: role "finalize_redrive_test_domain" already exists (SQLSTATE 42710)
```

Found by running `internal/jobs/metrics/daily` twice against kiac, and
independently reproduced by CHAOS-4661 on a fresh detached worktree at the
untouched parent commit before any fix: run 1 passed, run 2 failed with the
same SQLSTATE. It was a re-runnability failure, not a correctness one, and it
was invisible on the first run — which is exactly what made it worth writing
down.

Two packages (`internal/providersync`, `internal/storage/river`) already
`DROP ROLE`d before creating, so they were safe to **re-run**. They were still
not safe to run **concurrently**: two lanes would race on the same fixed role
name, and the one that dropped it would do so out from under the other.

**Under the concurrent bar that is the deciding test, so a fixed role name
blocked its package's PostgreSQL — including where the package dropped the
role first.** This was the single largest constraint on this page:
`internal/providersync` alone is 1166s, and it was in this set.

**CHAOS-4661 fixed this in the tests, not the harness** — the harness cannot
rename a role a test hard-codes, and having it drop unknown roles on a shared
server would be far more dangerous than the problem it solves. Every role name
in the 19 files (plus same-package and cross-file consumers a literal
`CREATE ROLE` grep could not see — shared setup helpers, an external
`provision_river_roles.sql` invocation, and a cross-*package* literal
collision between two packages) is now suffixed with
`containers.RoleSuffix(instance)`: the scratch database's own crypto-random
suffix (`internal/testsupport/containers/rolename.go`), so two successive runs
and two concurrent lanes on the same kiac cluster never collide. The same
literal-hardcoding defect existed one level down for the DATABASE name — about
a dozen files `GRANT`/`REVOKE`d against a hard-coded `worker_test`, which only
exists on the host container path and does not exist on kiac at all — fixed
the same way, via `containers.DatabaseName(instance.URI)`. All ten packages'
PostgreSQL now routes to kiac in the table above.

### What that adds up to

31 packages, **1852s (30.9 min)** of declared integration weight.

**Before CHAOS-4661:**

| Class | Weight | Share |
| --- | --- | --- |
| Movable to kiac | 402s | **21.7%** |
| Blocked by unparameterised role names | 1356s | 73.2% |
| Blocked by out-of-band fixed-name databases | 50s | 2.7% |
| Blocked by Valkey | 31s | 1.7% |
| The harness's own self-test | 13s | 0.7% |

Exactly, 402/1852 = **21.71%**, rounded to 21.7% above.

**That number was small because of one package.** `internal/providersync` alone
is 1166s — 63% of all integration weight — and it created a fixed role name. It
dropped it first, so it was fine run on its own, and it was unsafe beside a
concurrent lane. Under the concurrent bar it counted as blocked.

**After CHAOS-4661 (current state, this page):**

| Class | Weight | Share |
| --- | --- | --- |
| Movable to kiac | 1728s | **93.3%** |
| Blocked by out-of-band fixed-name databases | 50s | 2.7% |
| Blocked by Valkey | 31s | 1.7% |
| Blocked by roles **and** Valkey (`cmd/dev-health-worker`, `internal/syncdispatchruntime`) — Valkey is now the ONLY blocker, but it still blocks | 30s | 1.6% |
| The harness's own self-test | 13s | 0.7% |

1728/1852 = **93.30%**, rounded to 93.3% above — re-derived by script from
`ci/go_integration_shards.tsv` against the exact set of packages this page
marks role-blocked, not typed: `movable_today (402) + role_blocked (1356) -
still_blocked_by_valkey_too (30) = 1728`.

**A package counts as movable only when EVERY blocking store is resolved.**
That distinction is not pedantry — it is the correction that produced this
number. Adding the blocked buckets together over-counts, because
`cmd/dev-health-worker` and `internal/syncdispatchruntime` were blocked by
roles **and** Valkey, so CHAOS-4661 moved their PostgreSQL and left them
host-bound anyway (CHAOS-4666 tracks the Valkey side). The same 30s pair had
already caused two earlier arithmetic errors on this page while it was still a
projection.

**CHAOS-4661 was the priority unlock**: it moved 72 percentage points on its
own (21.7% → 93.3%), far more than anything else on this page. CHAOS-4677 would
add another 2.7 (93.3% → 96.0%) by namespacing the compute-parity databases.
The residual **124s (6.7%)** is Valkey (61s: the 31s Valkey-only packages plus
the 30s pair now blocked by Valkey alone), the out-of-band databases (50s), and
the harness's own self-test (13s), and it does not shrink further without
CHAOS-4677 and CHAOS-4666.

An earlier version of this page published 85.2%, and before that 87.9%, then
21.7% with a 93.3%/96.0% projection. The first two were computed with a single
target per package, which hid role-creating packages behind another reason,
and measured re-runnability rather than concurrency. 21.7% was the last
pre-CHAOS-4661 measurement. 93.3% is no longer a projection against a
hypothetical future fix: it is the routing this page's table now asserts,
because the code shipping in the same change makes it true for all ten
role-blocked packages by the same mechanism (`containers.RoleName`). The
figures above are derived from the table by script rather than by hand, and
every bucket can be recomputed from it. **Executed twice-in-succession proof
for every individual package is tracked in "Executed evidence: CHAOS-4661"
below — do not treat the percentage as a substitute for that table, and check
it for any row still pending rather than assuming green.**

The Valkey and self-test rows will not shrink. Valkey has no `CREATE DATABASE`
to carve a private namespace out of a shared server, and its container is the
cheapest of the three anyway; `internal/testsupport/containers` is separated out
deliberately rather than folded in with it, because it is the harness's own
self-test and must keep exercising the container path — that path is the thing
under test. For what each of those packages actually does with its other
stores, read its row: two of them are blocked by a second store as well.

### Before moving a suite: check it actually closes its Instance

**This is the precondition that is easiest to miss, because the container path
hid it.** Discarding an `Instance` and keeping only a connection was harmless
against a container — the daemon reaps the container when the run ends. Against
a shared server the `Instance` holds the *only* closure that drops the scratch
database, so a suite that discards it leaks a database on **every run,
including passing ones**.

`internal/jobs/metrics/remaining` did exactly this: its `migratedClickHouse`
helper cached the `driver.Conn` package-wide and dropped the `Instance` on the
floor. It now keeps the instances and releases them from `TestMain`, which is
the only scope matching the cache's lifetime — `t.Cleanup` would drop the store
while other tests still read through the cached conn.

So before routing a suite here, confirm every `StartPostgres` / `StartClickHouse`
result is eventually `Close`d. A suite that caches across tests needs a
`TestMain`, not a `t.Cleanup`.

### And check nothing creates a datastore behind the harness's back

The check above is necessary but not sufficient, because it only sees resources
the harness created. `internal/testsupport/computeparity` passes it — both its
callers close their instance correctly — and is still unsafe on a shared
cluster, because it creates **fixed-name** databases (`parity_left`,
`parity_right`, `parity_capacity_*`) through a separate fixture tool and
provisions them with `--reset`. Two lanes then collide on the same names and one
drops the other's live database. The tool's ownership marker does not help: it
is the same table name in every lane, so it distinguishes a fixture database
from a real one but not one lane's from another's.

**So the second question is: does anything here create a datastore the harness
does not own?** Any creator that is not the harness is a potential collision and
a potential orphan, because nothing will namespace it and nothing will drop it.
Swept at the time of writing: no Go test issues `CREATE DATABASE` directly, and
`scripts/worker/compute_parity_fixtures.py` is the only out-of-band creator in
the repository, used by four call sites in that one package.

That package is therefore **host-only until its names are namespaced** — which
leaves it in an uncomfortable spot worth stating plainly: it is
engine-sensitive *and* host-only, so today it can be proven neither by CI (25.1)
nor by kiac (destructive), and its host containers run 26.6.1 rather than
production's 26.7 line.

### Orphans

A run that fails hard can also leave its scratch database behind, which is what
the per-lane prefix and the create/drop log exist for. A failed `DROP`
additionally logs `ORPHANED`, because callers routinely discard
`Instance.Close`'s error and would otherwise never see it.

## Why Valkey stays on Testcontainers

It is the one deliberate exception and it is not an oversight — a test pins it.

PostgreSQL and ClickHouse both have `CREATE DATABASE`, so a shared server can
hand each caller a private database. Valkey has no equivalent: only 16 fixed
numeric slots, which cannot cover packages running in parallel, and a suite
that issues `FLUSHDB` would silently destroy a neighbour's state. Eleven of the
164 files touch Valkey and its container is by far the lightest of the three,
so accepting that isolation risk would buy back almost no CPU.

## Suites that must never point at a shared database

Independent of the target, some suites assert a **pristine** database and break
against anything holding prior state:

- Migration suites that assert an exact applied-version sequence.
- Suites asserting exact row counts starting from zero.

The scratch-database-per-caller design satisfies these — a scratch database is
empty by construction — but pointing such a suite at a *seeded* lane database
does not. CHAOS-4457 recorded this concretely: two `tests/test_dispatch_outbox.py`
tests failed with `IndexError` against a seeded lane whose `worker_job_outbox`
already held 32 pending rows, because they claim with `limit=1` and expect
their own seeded row back. Against a scratch database both pass unchanged (see
the executed evidence below). **The distinction that matters is scratch vs.
seeded, not container vs. cluster.**

## How to run a suite against kiac

```bash
export KUBECONFIG=<repo>/acr/.tmp/kiac/acr-local/kubeconfig
eval "$(bash acr/deploy/local/trial-data.sh dsn --env | sed -E 's/^/export /')"

export DEV_HEALTH_TEST_POSTGRES_DSN="postgres://${ACR_TEST_TRIAL_PG_USER}:${ACR_TEST_TRIAL_PG_PASSWORD}@${ACR_TEST_TRIAL_PG_HOST}:${ACR_TEST_TRIAL_PG_PORT}/${ACR_TEST_TRIAL_PG_DB}?sslmode=disable"
export DEV_HEALTH_TEST_CLICKHOUSE_DSN="clickhouse://${ACR_TEST_TRIAL_CH_USER}:${ACR_TEST_TRIAL_CH_PASSWORD}@${ACR_TEST_TRIAL_CH_HOST}:${ACR_TEST_TRIAL_CH_PORT}/${ACR_TEST_TRIAL_CH_DB}"
export DEV_HEALTH_TEST_CLICKHOUSE_HTTP_DSN="clickhouse://${ACR_TEST_TRIAL_CH_USER}:${ACR_TEST_TRIAL_CH_PASSWORD}@${ACR_TEST_TRIAL_CH_HOST}:${ACR_TEST_TRIAL_CH_HTTP_PORT}/${ACR_TEST_TRIAL_CH_DB}"

# Namespaces this lane's scratch databases so an orphan from a killed run can
# be swept without matching a concurrent lane's live ones.
export DEV_HEALTH_TEST_SCRATCH_PREFIX=lane_<ticket>

# ClickHouse suites that apply the real migration chain shell out to python3
# (internal/testsupport/chschema). The ambient interpreter is not enough --
# without the repo venv on PATH they fail with ModuleNotFoundError:
# typing_extensions, which reads as a ClickHouse problem and is not one.
export PATH="<repo>/.venv/bin:$PATH"

go test -tags=integration ./internal/jobroute/ -count=1
```

Set neither DSN and the harness starts containers exactly as before — the
default is unchanged, so nothing that works today stops working.

**Each DSN must name exactly one host, literally, in the DSN itself.** Anything
else is refused with a loud error before a connection is opened.

The reason is that a scratch database is created on one connection and dropped
on a later, separate one, and the drop must reach the server that created it.
Otherwise `DROP ... IF EXISTS` succeeds as a no-op, the harness logs `dropped`,
and the database survives with nothing naming it — the one failure mode that
makes the orphan log lie, which the sweep below depends on it not doing.

Two ways a DSN can break that, both refused:

- **More than one host.** Either driver may pick a different one per connection.
- **No host at all.** `postgres://u:p@/acr` has its host resolved from `$PGHOST`
  at *every* parse — the same string yields a different server as the variable
  changes, and a local Unix socket when it is unset. In a test harness, where
  `t.Setenv` is routine, that is a live way for the drop to go elsewhere.

So the requirement is on the DSN *string* rather than on what it happens to
resolve to.

**Known limit of that guard, stated rather than implied.** It refuses a DSN that
names no host, or that names more than one *hostname*. It does **not** catch two
endpoints that share a hostname, nor a port supplied from outside the DSN:

```
postgres://u:p@pg-gateway:5432,pg-gateway:15432/acr   1 hostname, 2 endpoints -> accepted
postgres://u:p@pg-gateway/acr  with PGPORT=15432      port comes from the environment
```

Either could in principle put the `CREATE` and the `DROP` on different servers,
by the same mechanism as the cases that *are* refused. This is documented as a
**harness constraint rather than fixed**, deliberately: it is the sixth instance
of this class found in review, each previous fix having closed the cases it was
shown and missed the next one, and the decision was taken that the class is
better bounded by a stated requirement than by another guard.

**So: point the harness at a single, fully-specified `host:port` endpoint.** The
recipe above does. A DSN that reaches a pooler or a failover front-end with more
than one backend is outside what this harness is built for.

`DEV_HEALTH_TEST_CLICKHOUSE_HTTP_DSN` is needed alongside the native DSN
whenever a suite reaches ClickHouse over HTTP (the Python migration runner
does). A remote instance has no container to ask for a mapped port, so the HTTP
address cannot be derived; omitting it produces an error that names the
variable rather than a connection failure.

### Sweeping orphans

Each scratch database is announced before it is created, confirmed, and logged
again when dropped:

```
containers: creating scratch postgres database "lane_4428_8a2c4feb5e391ab5"
containers: created  scratch postgres database "lane_4428_8a2c4feb5e391ab5"
containers: dropped  scratch postgres database "lane_4428_8a2c4feb5e391ab5"
```

The first line exists because a server can commit `CREATE DATABASE` and the
client still lose the response to a timeout or a dropped connection. Logging
only on success would leave that database with no name anywhere. **The sweep
rule is therefore: any `creating` with no matching `dropped` is a candidate
orphan** — not any `created`.

That race cannot be closed, only made findable: there is no way to create a
database and register its cleanup atomically across a network.

A run killed at any point leaves the database behind. Find them by prefix:

```sql
SELECT datname FROM pg_database WHERE datname LIKE 'lane_<ticket>%';
```

## Executed evidence (2026-08-31)

Against `acr-local`'s in-cluster data plane — ClickHouse 26.7.3.19,
PostgreSQL 18.6 — with the Docker daemon left untouched. Running-container
count was **22 before and 22 after every run**.

| Suite | Language | Stores | Result |
| --- | --- | --- | --- |
| `internal/jobroute` | Go | PostgreSQL | **PASS** — run twice, 0.85s / 0.94s |
| `internal/jobs/report` | Go | PostgreSQL + ClickHouse | **PASS** — run twice, 1.04s / 1.10s |
| `tests/test_dispatch_outbox.py` (the two `test_real_postgres_*` cases) | Python | PostgreSQL | **PASS**, 6.68s |

Each Go suite was run **twice** deliberately. A single green run against a
shared server proves less than it looks: the role collision described above
passes on the first run and only fails on the second.

The Python pair is the CHAOS-4457 sub-item (b) result: both are recorded there
as failing against a seeded lane database, and both pass against a scratch one
with no change to the tests. The DSN used the `postgresql+asyncpg://` dialect,
which is sub-item (a)'s correction.

Isolation was verified by comparing the full `pg_database` and
`system.databases` listings before and after: both are byte-identical to their
baselines, and a `LIKE 'lane_4428%'` sweep returns 0. The shared trial
ClickHouse `default` database and the real-data `dh_0830` database were never
written to.

## Executed evidence: CHAOS-4661 (2026-08-31)

Against `acr-local`'s in-cluster data plane — PostgreSQL 18.6 — with the Docker
daemon left untouched.

**Red-on-baseline**, independently reproduced (not only cited from the
finding): a detached worktree at the untouched parent commit, running
`internal/jobs/metrics/daily`'s
`TestReconcileOrphanedFinalizeRedriveRunsClosesAnEventWhenRiverDiscardedTheJob`
twice against kiac — run 1 **PASS**, run 2 **FAIL**
`ERROR: role "finalize_redrive_test_domain" already exists (SQLSTATE 42710)`.

**Mutation-proof**: on the fixed tree, restoring one role name to its old
hard-coded literal reproduced the same twice-in-succession failure (run 1
PASS, run 2 FAIL, same SQLSTATE); the tree was restored from the pre-mutation
content (sha256 digest match), never via `git checkout`.

**Twice-in-succession, every affected package, green both times:**

| Package | Result |
| --- | --- |
| `internal/storage/postgres` | **PASS** — full package, both runs |
| `internal/joboutbox` | **PASS** — full package, both runs |
| `internal/joboperator` | **PASS** — full package, both runs |
| `internal/syncreconciler` | **PASS** — full package, both runs |
| `cmd/dev-health-workerctl` | **PASS** — full package, both runs |
| `cmd/dev-health-worker` | **PASS** — the role-creating test (`TestRiverWorkerClientRunsReindexerWithoutPermissionErrors`); the package's other tests are Valkey/ClickHouse suites this ticket does not touch and stay host-bound regardless (see the matrix) |
| `internal/syncdispatchruntime` | **PASS** — the 9 role-creating tests across its 3 files; same host-Valkey caveat as above |
| `internal/jobs/metrics/daily` | **PASS** — see red-on-baseline and mutation-proof above |
| `internal/storage/river` | **PASS** — full package, both runs, including the rewritten backup/restore round-trip |
| `internal/providersync` | **PASS/PASS** — RUN 1 963.826s (16m5.834s), RUN 2 899.627s (15m1.354s), fresh pair, `go test -tags=integration -timeout=30m ./internal/providersync/...` |

**Concurrent-pair proof**: `internal/providersync` and `internal/storage/postgres` run
SIMULTANEOUSLY against the same shared `acr-local` cluster (distinct
`DEV_HEALTH_TEST_SCRATCH_PREFIX` each) — both green (`exit=0`), 19:31:46Z→19:46:56Z,
proving the role-name parameterisation holds under real concurrency, not only
across successive runs.

**Role cleanup — every role-creating test is now self-cleaning.** Unique-per-call
role names (the fix above) silently disabled the old collision-avoidance side
effect that used to bound a role's lifetime: two calls never share a name
anymore, so the leading `DROP ROLE IF EXISTS` never fires for real, and
without an explicit cleanup every role a test creates became a PERMANENT,
unbounded addition to the shared cluster — one per test per run, forever, not
the bounded/self-healing accumulation the fixed-name era had. This was found
mid-review (both fresh, clean runs above still left roles behind) and fixed
across all 22 role-creating files in scope: `containers.DropRole(pool, role,
logf)` (`internal/testsupport/containers/rolename.go`) runs `DROP OWNED BY
<role>` then `DROP ROLE IF EXISTS <role>` from a `defer`/`t.Cleanup`
registered after the admin pool's own close-cleanup (so it fires while the
pool is still open) — `DROP OWNED BY` first because a plain `DROP ROLE` fails
closed against the role's own `GRANT`s ("some objects depend on it"),
verified against a live cluster; it works even while a separate connection is
still logged in as that role. 4 unit tests in `rolename_test.go` cover
statement order, non-vacuity (both statements still attempt even if the first
fails) and unsafe-name refusal. **Proof, no manual sweep**:
`internal/joboperator`'s role count (`operator_domain/queue/coordinator_runtime`)
held at 12 before → after RUN 1 → after RUN 2, twice-in-succession, both green,
zero growth. **Mutation-spot-check (non-vacuity)**: removing one `defer
containers.DropRole(...)` line and re-running once moved the domain-role count
4→5 (a real leak, caught); the line was restored (verified byte-identical via
`git diff`) and the single leaked role dropped.

**River backup/restore: CI regression found and fixed.** The first version of
`assertBackupRestore` ran `pg_dump`/`createdb`/`pg_restore` as host processes
unconditionally, which is required on the remote/kiac path but broke CI: CI
runs the host-Testcontainers path, where the test previously reused the
`postgres:18-alpine` container's own bundled v18 client tools via
`instance.Container.Exec`, and the CI runner's own `pg_dump` is major version
16 (below this suite's PostgreSQL 18 floor) -- red on every push once the
unconditional rewrite landed, confirmed identical failure on this branch's
CI going back to before this session, `main` unaffected (doesn't have the
test yet). Fixed with a dual-path dispatch on `instance.Container` (see the
matrix row above). **Proof**: both paths run green locally -- container path
(no `DEV_HEALTH_TEST_POSTGRES_DSN` set) PASS in 3.04s reusing the container's
tools; kiac remote path PASS/PASS twice-in-succession (2.02s, 3.19s, full
package, 21 tests). **Mutation-spot-check (non-vacuity)**: inverting the
dispatch condition and running on the remote path (where `instance.Container`
is nil) forced the container-exec branch, which panicked immediately with a
nil-pointer dereference at the mutated line -- loud, not silent -- confirming
the dispatch is load-bearing; the panic's `defer`s still ran (Go's unwind
semantics), leaving zero leaked roles/databases even from the crashed run.
The condition was restored and re-verified (`git diff` byte-identical,
build/vet/gofmt clean, both paths re-run green).

Isolation verified after every run: no leaked scratch databases (`LIKE
'lane_4661%'` sweep returns 0 throughout). Roles are a separate story from
scratch databases: a **pre-existing backlog of 477 roles** (matching every
prefix these test files use — `grant_*`, `operator_*`, `kernel_*`, `sweep_*`,
`workerctl_*`, etc.) was found on the shared cluster on 2026-08-31, accumulated
from runs across this ticket's own verification history before the cleanup fix
above existed. It was deliberately **not** bulk-swept as part of this change —
enumerating and dropping roles by pattern on a server other lanes are actively
using is exactly the harness-level danger "why not a harness change" (above)
warns against; a blind sweep could drop a role a concurrent lane's live test
still owns. Every role created by code in THIS PR from here forward
self-cleans (proof above); the historical backlog is a separate, explicitly
flagged cleanup decision for whoever owns a quiet window on the shared
cluster, not a defect in this fix.

## acr's Go suites

acr is **not** covered by the plumbing above and needs its own change, tracked
separately.

It has no single chokepoint. `acr/internal/chfixture` looks like one but starts
no containers — it holds the image pin, the version floor, and the static
JOIN-portability linter that replaced the old-engine fixture gate. The
container starts instead live in roughly twenty per-package helpers
(`newXTestDatabase`, `newXIntegrationClient`, `newLiveAdapter`), so remote-DSN
support is about twenty small edits rather than one.

acr already has the convention to adopt — `ACR_TEST_TRIAL_{PG,CH,FALKOR}_*`,
`ACR_CLICKHOUSE_INTEGRATION_DSN`, `ACR_CHAOS4645_KIAC_DSN` — and several suites
already skip unless a live DSN is set. Its highest-risk suites to move are
under `migrations/postgres`, which take a fresh container per test and assert
exact migration sequences; those need a scratch database, never a shared one.
