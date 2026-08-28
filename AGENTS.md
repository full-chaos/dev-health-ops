# AGENTS — dev-health-ops

Backend: ingest → metrics → API → jobs. The versioned platform contract (Work Graph, Investment taxonomy, sink-only, ClickHouse-only, hard bans, and documentation delivery) lives in [`docs/contributing/platform-contract.md`](docs/contributing/platform-contract.md) and is **not repeated here**. Deep dives: MkDocs site under [`docs/`](docs/index.md).

## Read-first

| Need | Source |
| --- | --- |
| Product intent / guardrails | [`docs/product/prd.md`](docs/product/prd.md), [`docs/product/concepts.md`](docs/product/concepts.md) |
| Repo layout & boundaries | [`docs/architecture/repo-layout.md`](docs/architecture/repo-layout.md), [`docs/architecture/data-pipeline.md`](docs/architecture/data-pipeline.md) |
| Dual DB (semantic vs analytics) | [`docs/architecture/database-architecture.md`](docs/architecture/database-architecture.md) |
| Provider pattern | [`docs/architecture/adr-001-canonical-provider-pattern.md`](docs/architecture/adr-001-canonical-provider-pattern.md) |
| LLM categorization contract | [`docs/llm/categorization-contract.md`](docs/llm/categorization-contract.md) |
| API surface | [`docs/api/graphql-overview.md`](docs/api/graphql-overview.md), [`docs/api/view-mapping.md`](docs/api/view-mapping.md) |
| CLI & workers | [`docs/reference/cli/index.md`](docs/reference/cli/index.md), [`docs/operate/run/workers-and-jobs.md`](docs/operate/run/workers-and-jobs.md) |
| How to run / test tiers | [`README.md`](README.md), [`ci/run_tests.sh`](ci/run_tests.sh) |

## Source layout (`src/dev_health_ops/`)

| Dir | Role |
| --- | --- |
| `providers/<provider>/` | **Canonical** fetch + normalize. New integrations go here. github, gitlab, jira, linear. Contracts in `providers/base.py`; async REST helpers in `providers/_base.py`. |
| `connectors/` | **Legacy & frozen.** No new code — compatibility aliases only. |
| `processors/` | Orchestrate provider calls + persistence. No raw fetch / no provider normalization. |
| `metrics/` + `metrics/sinks/` | Compute rollups; **sinks are the only persistence path** (`sinks/clickhouse/`). |
| `api/` | FastAPI app (`api/main.py`) + Strawberry GraphQL (`api/graphql/`), admin, auth, billing, webhooks, ingest. |
| `workers/` | Celery (`celery_app.py`): sync, metrics, reports, team auto-import, schedulers. |
| `work_graph/`, `llm/`, `licensing/`, `reports/`, `backfill/`, `sync/` | Investment categorization, LLM calls, billing/licensing, AI reports, backfills, sync orchestration. |

## North Star (read before changing Ask Dev / Context Fabric behavior)

Canonical intent documents (Linear, Dev Health Ops project):
- **Ask Dev / Dev Health Ops North Star Summary**: https://linear.app/fullchaos/document/ask-dev-dev-health-ops-north-star-summary-eb80e9e132a4 — the standard future implementation decisions are measured against.
- **Dev Health Ops Purpose and Contract**: https://linear.app/fullchaos/document/dev-health-ops-purpose-and-contract-2026-08-28-112058-pdt-d6e5c6c391b9 — question contracts, answer schema, boundaries.
- **Review of North Star Summary**: https://linear.app/fullchaos/document/review-of-north-star-summary-2026-08-28-97180bc4b921

One-line definition: *Ask Dev turns a natural engineering question into a bounded investigation across canonical facts, organizational context, graph evidence, and derived findings, then returns the strongest defensible explanation, evidence, uncertainty, and next action in the form best suited to the user's intent.*

Design checks every Ask Dev / acr / ops-metrics change must pass — checks 1–15 are North Star §22 principles; checks 16–18 are repository operating rules from the review doc above:
1. Intent determines evidence — never answer the nearest measurable question.
2. Retrieval is part of reasoning — correct intent with failed retrieval is a failed answer.
3. Preserve semantic distinctions — status ≠ completion ≠ readiness ≠ health ≠ pressure ≠ investment ≠ burden ≠ productivity.
4. Evidence rules constrain claims; they do not refuse the conversation.
5. Make partial truth useful.
6. Memory carries investigation context (subject, cohort, scope, window, comparison basis persist across turns).
7. Graph edges establish relevance, not cause.
8. Scores help prioritize; drivers explain — never a bare score.
9. Investment explains tradeoffs.
10. Rich views (tables, landscapes, treemap/sunburst/sankey, burndown, Monte Carlo) are conditional on intent, never default.
11. The answer contract is richer than the prose (completeness/terminal state/coverage are public contract fields).
12. Missing is not healthy — unknown/stale/sparse/not-applicable/zero are distinct.
13. Small-org reality (3–8 teams, thin history) must work.
14. Prefer investigation over diagnosis-by-metric.
15. Ask Dev is the product surface over Context Fabric; metrics/graph/landscape/investment views are supporting surfaces.
16. Diagnoses need an executed repro; label code-argued findings as such.
17. Append-only daily tables + argMax readers; never zero-fill missing days.
18. Authorization is re-checked live every turn — never carried as conversation memory.

Contract rule: any acr contract widening ⇒ ask-dev pin bump before any live proof. Team authorization is ownership-derived (`team_repo_ownership` rows), so team answers are impossible until ownership is synced.

Data vocabulary: "local" = the admin@test.com org (`70d529e0`, REAL synced data on the compose stack); `dev-hops fixtures generate` = contrived CI data; "prod" = read-only post-deploy readback. Team = project/repo OWNERSHIP only (never person→membership→team); ownership is sync-derived, provider-agnostic; no manual TEAM mappings (ClickHouse `manual_fallback` records remain valid for work-item/PR attribution — never for team authorization).

Checks that bite hardest here: 3, 9, 12, 13, 17 — plus: team is OWNERSHIP only, never person→membership→team; metrics readers use append-only daily tables + `argMax`, never zero-fill missing days.

## Ops-specific rules (beyond root contracts)

- **Provider boundary:** raw fetch/auth/pagination/retry/rate-limit + normalization stay inside the provider; processors only orchestrate. Hard ban: new code under `connectors/`.
- **Backend selection:** semantic DB via `--db`/`POSTGRES_URI` (legacy `DATABASE_URI`); analytics via `--analytics-db`/`CLICKHOUSE_URI`; secondary sink `SECONDARY_DATABASE_URI` for `sink='both'`.
- **aiosqlite** is allowed for test fixtures / local ephemeral dev only — never a production semantic DB, never analytics, never CI long-runs. URL-normalization helpers in `db.py` / `metrics/db_utils.py` are compatibility, not permission.
- **LLM compute-time:** strict JSON per `work_graph/investment/llm_schema.py`, canonical subcategory keys, extractive evidence quotes, one repair attempt then deterministic fallback, audit fields persisted.
- **Grafana queries:** table format + stable ordering; normalize null/empty `team_id`; use `WITH … AS` aliasing (not `WITH name = expr`). Don't replatform dashboards incidentally.
- **Atlassian AGG:** Jira issue listing is REST/JQL; GraphQL is fetch-by-key + worklog/ops-team enrichment. Gate with `ATLASSIAN_GQL_ENABLED`, `JIRA_FETCH_WORKLOGS`, `JIRA_USE_PROVIDER`.
- **Team attribution source of truth (CHAOS-2600 — governing contract, rolling out CS1–CS7):** ClickHouse is the **only** system of record for teams **and** identity→team membership; there is no Postgres `TeamMapping`/`IdentityMapping` — those models, services, and tables were **deleted in CHAOS-2600 CS6 (CHAOS-2607)** (Alembic `0020` drops the tables), along with the bridge `team_bridge.py` + `team_reconcile.py` (removed CS5), the `sync-team-drift`/`reconcile-team-members` tasks, and the Postgres-backed drift engine (the four admin drift-review endpoints currently stand as HTTP 501 stubs and are being **rebuilt natively on ClickHouse — not deleted — under CHAOS-2622**; the earlier "removed in CS7 with the web caller" intent is superseded and must not be executed). Admin team/identity CRUD goes through `ClickHouseTeamAdminService` / `ClickHouseIdentityStore` (CH `teams` + `identities`); identity membership is edited surgically by facet so Auto Import members survive. Manual mappings are ClickHouse fallback records (`source = manual_fallback`) — never overrides, never outranking WTI-native facts. PR/MR attribution comes from an **actual linked issue donor row**, not an issue-key prefix. Staged precedence + decision tree + off-the-rails matrix: [`docs/contribute/architecture/team-attribution.md`](docs/contribute/architecture/team-attribution.md) §0.
- **Documentation freshness:** when you change attribution behavior (precedence, WTI normalization, PR/MR issue linking, manual fallback, ClickHouse attribution tables, API provenance), update the matching docs **in the same PR** and make tests assert the documented precedence. The legacy Postgres-path docs (`database-architecture.md`, `cli-reference.md`) are rewritten in the CS that removes that behavior (CS5/CS6). If docs and code disagree, the implementation is incomplete.
- **Provider coverage (provider-agnostic contract):** attribution must be tested across the full **provider × entity matrix** — `{jira, gitlab, github, linear} × {teams, projects, members, issues}`. Changes must keep the matrix green; **never add Linear-only coverage** (jira/github/gitlab work items have `native_team_key=None`, so non-Linear attribution rides entirely on the autoimport team/project/member dimension). Live matrix + open gaps: [`docs/contribute/architecture/team-attribution.md`](docs/contribute/architecture/team-attribution.md) §0.4 (gaps tracked in CHAOS-2609).

## CLI quickref (full reference: [`docs/ops/cli-reference.md`](docs/ops/cli-reference.md))

```bash
dev-hops migrate postgres && dev-hops migrate clickhouse          # required on fresh envs
CLICKHOUSE_URI=… dev-hops sync git --provider local --repo-path PATH
CLICKHOUSE_URI=… dev-hops sync work-items --provider <jira|github|gitlab|all> -s "org/*"
CLICKHOUSE_URI=… dev-hops fixtures generate --sink "$CLICKHOUSE_URI" --days 30
CLICKHOUSE_URI=… dev-hops metrics daily
```

**Interim (CHAOS-2475):** bare CLI runs inline and skips credential preflight. Prefer triggering the equivalent job (sync-config/backfill endpoints, `triggerReport` mutation) so workers supply tokens/LLM/Stripe keys. **Celery is retired (CHAOS-4026, 2026-08-21):** `dev-hops workers start-worker`/`start-scheduler` no longer exist; see [`docs/operate/run/workers-and-jobs.md`](docs/operate/run/workers-and-jobs.md) for running the Go worker/scheduler processes.

## Tests & hooks

- API endpoint tests follow [`tests/api/auth/test_invite_flow.py`](tests/api/auth/test_invite_flow.py) (aiosqlite in-memory, `dependency_overrides`, `httpx.ASGITransport`). Journey: [`tests/api/test_new_user_journey.py`](tests/api/test_new_user_journey.py). Admin CRUD: `tests/api/admin/`.
- GraphQL schema export `api/graphql/export_schema.py` is consumed by web CI for drift detection.
- **Lefthook** (`make install` once — `core.hooksPath` is shared across worktrees): `commit-msg` strips agent attribution; `pre-commit` ruff format+fix then `mypy` gate; `pre-push` `ruff format --check` + `ruff check` + `mypy`. Fix code, don't add ignores/config exclusions.
- **Mutation testing has no shared harness — do not write one.** `scripts/mutation_harness.py`, its ~8k lines of meta-tests, and its 98 checked-in plan JSONs were removed under CHAOS-3875: no GitHub workflow ever ran them, only `ci/local_validate.sh`'s `verify` stage touched the harness at all, and the plans had to be re-anchored by hand on every refactor of the files they pinned. The durable lesson from the 2026-07-26 incident survives the tool: three ad-hoc per-lane harnesses produced false results, all the same shape — the harness could not detect its own failure. One left `if false && (guard)` on disk while reporting a restore it had "verified" with `go build`; one used `git checkout` to restore and silently reverted unrelated uncommitted edits; one waited on `pgrep -qf "m22.sh"`, matched its own command line, and hung. **Never verify a restore with a build or a git check** (`go build`/`go vet` pass on `if false && …`; `git diff` calls an *untracked* file clean whatever it contains), and **mutate compound predicates clause by clause** — a wholesale mutation reported KILLED on a condition holding a wrong, unasserted clause. If you need a one-off kill proof, run it by hand, restore by content digest, and keep it out of the tree.

## Worktrees and branches

Agents keep creating branches that track `origin/main`, then a bare `git push` pushes straight to `main`. The canonical recipe, every time:

```bash
git worktree add --no-track -b <branch> <path> origin/main
cd <path>
UV_CACHE_DIR="$(git rev-parse --show-toplevel)/.uv-cache" SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 \
  uv sync --all-extras --dev --no-install-project
# or, without a worktree: git switch -c <branch> --no-track origin/main
git push -u origin HEAD:<branch>
```

- `--no-track` is required. A branch created without it silently tracks `origin/main`, and a bare `git push` from that branch pushes to `main`, not your branch.
- Push with an explicit refspec, `HEAD:<branch>` (or `-u origin HEAD:<branch>` the first time) — never a bare `git push`.
- Verify before the *first* push: `git for-each-ref --format='%(refname:short) -> %(upstream:short)' refs/heads/<branch>` must print nothing after `->`. After `git push -u` sets the upstream, the same command legitimately prints `origin/<branch>` — the check that matters from then on is that it never prints `origin/main`.
- **`dev-health-go` trap:** that repo sets `push.default=upstream`. A tracked branch plus a bare `git push` pushes `main` there even faster than elsewhere — the same `--no-track` + explicit-refspec recipe is mandatory, no exceptions.
- **Always set `UV_CACHE_DIR` per worktree (CHAOS-4411).** uv's default cache is `~/.cache/uv`, shared machine-wide; concurrent `uv sync` runs from different worktrees serialize on `~/.cache/uv/.lock` (and the editable sdist build lock under it), which has cost lanes 30+ minute waits. Each worktree already gets its own `.venv` — give it its own cache dir too. `.uv-cache/` is gitignored. Cost: no cross-worktree cache sharing, more disk per worktree.
- **`--no-install-project` is required too (CHAOS-4181 / CHAOS-4407).** setuptools_scm's git file finder shells out to `git check-attr -z --stdin export-ignore` while building the editable install of `dev-health-ops` itself; the pipe deadlocks forever (0% CPU, not slow) once this repo's file list fills the 64KB buffer — every linked worktree hits it, `SETUPTOOLS_SCM_PRETEND_VERSION` does NOT avoid it (the file-listing call runs regardless of the version env var). `--no-install-project` skips that build entirely — the 158 third-party deps still install, and `pytest.ini`'s `pythonpath = src` resolves test imports without the local package being installed. Cost: no `.venv/bin/dev-hops` console script, so `ci/local_validate.sh`'s ClickHouse-dependent stages can't run in this venv locally — run those via CI, or `SKIP_CLICKHOUSE=1 bash ci/local_validate.sh` for the pure-Python stages only.

### Cluster-isolated lanes (CHAOS-4428)

Several standing rules in this file exist only because every lane shares ONE
Docker Compose stack. If your lane runs in its own namespace on the local kiac
cluster, they do not apply to you:

- **Unmerged-migration downgrade.** You do not have to downgrade an unmerged
  Alembic revision to unblock a sibling lane. `alembic_version` is per database
  and each namespace has its own Postgres — two namespaces have been held at
  0115 and 0109 simultaneously with both stacks healthy.
- **Compose heads-up before a rebuild.** `helm upgrade` touches one namespace,
  so no cross-domain heads-up is needed.
- **The host-wide `local_validate` lock.** Not required for a gate pointed at
  your own namespace's DSNs. It is still required for anything run against the
  Compose stack.
- **"Never write to the shared Postgres/ClickHouse."** Write freely inside your
  namespace; the blast radius stops at its boundary.

What does NOT change: never touch another lane's namespace, never
`kiac down`/`delete`/prune a cluster you did not create, and never run
host-wide `container system` verbs — they stop every kiac VM on the machine.

Recipe, cost, and the testcontainers-per-suite policy:
[docs/contribute/development/lane-isolation-kiac.md](docs/contribute/development/lane-isolation-kiac.md).

## Landing the plane

- **Push early and incrementally.** “Work is not done until `git push` succeeds” is not only end-of-run cleanup. One CHAOS-3033 lane ran 57 minutes, compacted with “commit, push, PR” still in its plan, and delivered code that was never pushed. Push after the first commit; an unpushed worktree is one crash from nothing.
- **Split landing from long autonomous work.** Commit, push, and PR is short, mechanical, and high-certainty. Exploratory porting is long and compaction-prone. Dispatch landing as its own short task.
- **Give long-running lanes a durable brief and a todo pointer.** Write the brief to `<project>/.remember/lanes/<lane-id>/BRIEF.md`. The `dev-health` root is not a git repository, so this does not pollute `git status --porcelain`; unlike `/tmp`, it does not evaporate. Forty review artifacts were lost in `/tmp` (CHAOS-3169). The todo list is load-bearing because the harness re-injects it after compaction: its first item must be “re-read BRIEF.md” and its last must be “land per BRIEF.md”. Require the lane to quote a `MARKER` line from the brief in its final report; otherwise “I re-read it” is unverified. Briefs are operational scaffolding for one run. Decisions and plans still belong in Linear, never only in `.remember/`.
- **Do not let worker lanes delegate to sub-agents.** One port lane blocked on a delegated result and was killed after 30 minutes of inactivity without producing anything. Two sibling lanes completed the same task without delegating. A “30 minutes of inactivity” timeout is itself a fallback masquerade: an outstanding call made the lane look alive until the clock showed it was dead.
- **A cited constructor is not proof of capability.** The constructor must be reachable with only its own switch enabled. A port satisfied the `file:line` acceptance bar while `cmd/dev-health-worker/provider_sync.go` returned an empty worker family because its config switch was missing from the activation condition. Registry ownership did not make the binary construct it. Strengthen the bar: cite the constructor and prove reachability with a table-driven test that enables only each pair's switch.

## Compute-port parity (CUT-20)

There is ONE row-comparison vocabulary in this repo:
[`internal/testsupport/oraclecompare`](internal/testsupport/oraclecompare)
(type-tagged leaves, exhaustive reflection over the concrete row type,
exclusions that need a written reason and must actually match something). Three
boundaries sit on it: row-vs-row oracle pairs in `internal/providersync`,
write-vs-readback in that package's readback pairs, and store-vs-store whole
TABLE parity in
[`internal/testsupport/computeparity`](internal/testsupport/computeparity).

If you are about to write a comparison rule -- how to encode a value, when two
values are equal, how an exclusion is declared -- it already exists. A second
one is the defect CHAOS-3092 P0 was created to remove.

Field completeness comes from the row TYPE, never a hand-written column list:
the SELECT is derived by reflection, so adding a column to the struct adds it to
the query and the diff in one edit. Fixtures are producer-derived and stores are
built from the real migration chain
([`scripts/worker/compute_parity_fixtures.py`](scripts/worker/compute_parity_fixtures.py)).
Operational health is a SEPARATE claim with disjoint verdicts
([`scripts/worker/compare_runtime_observations.py`](scripts/worker/compare_runtime_observations.py));
nothing compares product rows in Python. How-to:
[`contracts/compute-parity/v1/README.md`](contracts/compute-parity/v1/README.md).

## PR and CI conventions

- The `governance` check requires `TEST-EVIDENCE` and `RISK-NOTES` markers in the PR body when a change touches `src/dev_health_ops/workers/provider_unit_route.py`. This is a PR-body requirement, not a code requirement. Missing markers cost two lanes a CI round.

## Pre-push validation gate (REQUIRED for every ops change)

Before pushing ANY change to `dev-health-ops`, run the standing local gate from the
worktree root using the worktree's `.venv`:

```bash
bash ci/local_validate.sh
```

It mirrors the PR-time CI gates of the ops repo and MUST be green before `git push`:

1. `ruff format --check .` and `ruff check .` (== lint.yml)
2. `mypy --install-types --non-interactive .` (== typecheck.yml)
3. The **FULL** unit suite, byte-for-byte as `ci/run_tests.sh unit_tests()` runs it
   (`pytest tests -m "not benchmark and not clickhouse" --ignore=… -n 4 --dist loadscope`,
   matching CI's `PYTEST_XDIST_WORKERS=4` — the worker count changes the test→worker
   distribution and a different count surfaces order-dependent pollution CI never hits),
   with the local socks5h proxy neutralized. **Run the whole `tests/` dir — never a
   hand-picked subset of files.** Many CI-blocking guards are unmarked pure-Python
   tests that glob/parse `src/` (migration-splitter semicolon guard, RMT `org_id`
   sorting-key contract, dataclass/sink `org_id` parity, pyformat-`%%` safety); a
   per-file run passes locally while these fail in CI. This is exactly how CHAOS-2604
   broke: a push after running only 2 test files missed
   `tests/test_clickhouse_migration_splitter.py::test_no_committed_migration_comment_line_contains_semicolon`.
   A few unmarked API tests (`tests/api/admin/test_org_deletion.py`) also call
   `get_clickhouse_uri()` and need a reachable, migrated ClickHouse — the gate provisions
   an isolated **scratch db**, migrates it, and points `CLICKHOUSE_URI` at it before
   running the suite, exactly as CI provides one (a locked dev `default` user makes them
   false-red otherwise). Without docker AND without `SKIP_CLICKHOUSE=1`, the gate now
   hard-fails before the unit suite ever runs at all (see CHAOS-3571 below) — the
   module-deselect behavior only fires under the explicit `SKIP_CLICKHOUSE=1` opt-out,
   where the pure-Python guards still run but that one CH-dependent module is skipped.
4. A **live-ClickHouse argMax proof** that CI's unit/ci tiers never run: after migrating
   the scratch db, it builds a real `ClickHouseDataLoader` and `await`s
   `load_team_attribution_context`, forcing the real engine to parse + EXECUTE every
   `argMax(…, (updated_at, valid_from))` / `GROUP BY` block. The mock-based unit test
   only string-matches `argMax`; only a live engine catches a tuple-arg / column /
   unescaped-`%` mistake. (The broader seeded `pytest -m clickhouse` suite —
   flow-matrix-live, recommendations, resolver EXPLAIN — needs `dev-hops fixtures
   generate` and is a separate opt-in run, not part of this gate; CI does not run it either.)
5. A **host-wide single-flight lock** (CHAOS-3403): the gate serializes across every
   worktree on the host, since all worktrees share one ClickHouse container and one
   host's CPU/RAM. A run that finds the lock already held blocks, waiting up to
   `LOCK_WAIT_SECS` (default 1800s = 30 minutes), then fails with an actionable
   message naming the holder's PID and cwd. `LOCK_WAIT_SECS=0` fails fast instead
   of waiting.

### Safety rule (NON-NEGOTIABLE)

The local container `dev-health-clickhouse-1` db `default` holds **real dev data**.
The gate **MUST NOT** create/drop/alter tables in `default`. It isolates everything to
a scratch db (default `ci_local_validate`) via `CLICKHOUSE_URI=…/ci_local_validate`,
and **drops that scratch db on exit (trap cleanup)**. `CLICKHOUSE_URI` must never
default to `…/default` for any `-m clickhouse` run, migrate, or `ensure_schema(force=True)`
call. If you must run CH tests by hand, always export the scratch DSN first and unset
it after.

**CHAOS-3571 (stage manifest — reversed a prior statement in this doc):** without
`SKIP_CLICKHOUSE=1`, the gate no longer "cleanly SKIPs" when docker / the CH
container is unavailable — that exact behavior let a FAILED docker probe read as
"container not running" and silently drop 3 of 8 stages while still printing
`GATE PASSED`. Every reason the CH stages might not run (docker missing, the probe
itself failing/timing out, the container confirmed absent, a missing dev-hops CLI)
is now a **hard failure** with a message naming the true mechanism. The **only**
sanctioned way to run without the CH-dependent stages is the explicit, logged
`SKIP_CLICKHOUSE=1` opt-out — a caller decision made up front, not a runtime probe
result the gate trusts on its own. A machine-readable `GATE_STAGE_MANIFEST …` log
line carries the literal `declared=<N> executed=<N>` counts plus the exact stage
ids that ran, and the verdict line carries the same information formatted as
`[executed/declared: ids]` (`GATE PASSED. [8/8: lint_format,…,ch_argmax_proof] safe
to push.`, or `[4/4: …]` under `SKIP_CLICKHOUSE=1`), and a self-check fails the
gate on any declared-but-not-executed gap even if every stage that did run passed.
See the "STAGE MANIFEST" header comment in `ci/local_validate.sh` and
`tests/tooling/test_local_validate_stage_manifest.py`.

Do not push if `ci/local_validate.sh` prints `GATE FAILED`.
