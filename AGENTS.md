# AGENTS — dev-health-ops

Backend: ingest → metrics → API → jobs. Platform-wide contracts (Work Graph, Investment taxonomy, sink-only, ClickHouse-only, hard bans) live in [`../AGENTS.md`](../AGENTS.md) and are **not repeated here**. Deep dives: MkDocs site under [`docs/`](docs/index.md).

## Read-first

| Need | Source |
| --- | --- |
| Product intent / guardrails | [`docs/product/prd.md`](docs/product/prd.md), [`docs/product/concepts.md`](docs/product/concepts.md) |
| Repo layout & boundaries | [`docs/architecture/repo-layout.md`](docs/architecture/repo-layout.md), [`docs/architecture/data-pipeline.md`](docs/architecture/data-pipeline.md) |
| Dual DB (semantic vs analytics) | [`docs/architecture/database-architecture.md`](docs/architecture/database-architecture.md) |
| Provider pattern | [`docs/architecture/adr-001-canonical-provider-pattern.md`](docs/architecture/adr-001-canonical-provider-pattern.md) |
| LLM categorization contract | [`docs/llm/categorization-contract.md`](docs/llm/categorization-contract.md) |
| API surface | [`docs/api/graphql-overview.md`](docs/api/graphql-overview.md), [`docs/api/view-mapping.md`](docs/api/view-mapping.md) |
| CLI & workers | [`docs/ops/cli-reference.md`](docs/ops/cli-reference.md), [`docs/ops/workers.md`](docs/ops/workers.md) |
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

## Ops-specific rules (beyond root contracts)

- **Provider boundary:** raw fetch/auth/pagination/retry/rate-limit + normalization stay inside the provider; processors only orchestrate. Hard ban: new code under `connectors/`.
- **Backend selection:** semantic DB via `--db`/`POSTGRES_URI` (legacy `DATABASE_URI`); analytics via `--analytics-db`/`CLICKHOUSE_URI`; secondary sink `SECONDARY_DATABASE_URI` for `sink='both'`.
- **aiosqlite** is allowed for test fixtures / local ephemeral dev only — never a production semantic DB, never analytics, never CI long-runs. URL-normalization helpers in `db.py` / `metrics/db_utils.py` are compatibility, not permission.
- **LLM compute-time:** strict JSON per `work_graph/investment/llm_schema.py`, canonical subcategory keys, extractive evidence quotes, one repair attempt then deterministic fallback, audit fields persisted.
- **Grafana queries:** table format + stable ordering; normalize null/empty `team_id`; use `WITH … AS` aliasing (not `WITH name = expr`). Don't replatform dashboards incidentally.
- **Atlassian AGG:** Jira issue listing is REST/JQL; GraphQL is fetch-by-key + worklog/ops-team enrichment. Gate with `ATLASSIAN_GQL_ENABLED`, `JIRA_FETCH_WORKLOGS`, `JIRA_USE_PROVIDER`.
- **Team attribution source of truth (CHAOS-2600 — governing contract, rolling out CS1–CS7):** ClickHouse is the **target** source of truth for analytics team / project / member / repo / manual attribution. **Do not add or extend Postgres-based team attribution** — the legacy `team_bridge.py` / `team_mappings` path is frozen and removed in CS5/CS6 (which rewrite `docs/architecture/database-architecture.md` + `docs/ops/cli-reference.md`). Manual mappings are ClickHouse fallback records (`source = manual_fallback`) — never overrides, never outranking WTI-native facts. PR/MR attribution comes from an **actual linked issue donor row**, not an issue-key prefix. Staged precedence + decision tree + off-the-rails matrix: [`docs/architecture/team-attribution.md`](docs/architecture/team-attribution.md) §0.
- **Documentation freshness:** when you change attribution behavior (precedence, WTI normalization, PR/MR issue linking, manual fallback, ClickHouse attribution tables, API provenance), update the matching docs **in the same PR** and make tests assert the documented precedence. The legacy Postgres-path docs (`database-architecture.md`, `cli-reference.md`) are rewritten in the CS that removes that behavior (CS5/CS6). If docs and code disagree, the implementation is incomplete.
- **Provider coverage (provider-agnostic contract):** attribution must be tested across the full **provider × entity matrix** — `{jira, gitlab, github, linear} × {teams, projects, members, issues}`. Changes must keep the matrix green; **never add Linear-only coverage** (jira/github/gitlab work items have `native_team_key=None`, so non-Linear attribution rides entirely on the autoimport team/project/member dimension). Live matrix + open gaps: [`docs/architecture/team-attribution.md`](docs/architecture/team-attribution.md) §0.4 (gaps tracked in CHAOS-2609).

## CLI quickref (full reference: [`docs/ops/cli-reference.md`](docs/ops/cli-reference.md))

```bash
dev-hops migrate postgres && dev-hops migrate clickhouse          # required on fresh envs
CLICKHOUSE_URI=… dev-hops sync git --provider local --repo-path PATH
CLICKHOUSE_URI=… dev-hops sync work-items --provider <jira|github|gitlab|all> -s "org/*"
CLICKHOUSE_URI=… dev-hops fixtures generate --sink "$CLICKHOUSE_URI" --days 30
CLICKHOUSE_URI=… dev-hops metrics daily
```

**Interim (CHAOS-2475):** bare CLI runs inline and skips credential preflight. Prefer triggering the equivalent Celery job (sync-config/backfill endpoints, `triggerReport` mutation) so workers supply tokens/LLM/Stripe keys. Worker: `dev-hops workers start-worker --queues default metrics sync reports`. See [`docs/ops/workers.md`](docs/ops/workers.md).

## Tests & hooks

- API endpoint tests follow [`tests/api/auth/test_invite_flow.py`](tests/api/auth/test_invite_flow.py) (aiosqlite in-memory, `dependency_overrides`, `httpx.ASGITransport`). Journey: [`tests/api/test_new_user_journey.py`](tests/api/test_new_user_journey.py). Admin CRUD: `tests/api/admin/`.
- GraphQL schema export `api/graphql/export_schema.py` is consumed by web CI for drift detection.
- **Lefthook** (`make install` once — `core.hooksPath` is shared across worktrees): `commit-msg` strips agent attribution; `pre-commit` ruff format+fix then `mypy` gate; `pre-push` `ruff format --check` + `ruff check` + `mypy`. Fix code, don't add ignores/config exclusions.

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
   false-red otherwise). Without docker, the gate deselects that one module so the
   pure-Python guards still run.
4. A **live-ClickHouse argMax proof** that CI's unit/ci tiers never run: after migrating
   the scratch db, it builds a real `ClickHouseDataLoader` and `await`s
   `load_team_attribution_context`, forcing the real engine to parse + EXECUTE every
   `argMax(…, (updated_at, valid_from))` / `GROUP BY` block. The mock-based unit test
   only string-matches `argMax`; only a live engine catches a tuple-arg / column /
   unescaped-`%` mistake. (The broader seeded `pytest -m clickhouse` suite —
   flow-matrix-live, recommendations, resolver EXPLAIN — needs `dev-hops fixtures
   generate` and is a separate opt-in run, not part of this gate; CI does not run it either.)

### Safety rule (NON-NEGOTIABLE)

The local container `dev-health-clickhouse-1` db `default` holds **real dev data**.
The gate **MUST NOT** create/drop/alter tables in `default`. It isolates everything to
a scratch db (default `ci_local_validate`) via `CLICKHOUSE_URI=…/ci_local_validate`,
and **drops that scratch db on exit (trap cleanup)**. `CLICKHOUSE_URI` must never
default to `…/default` for any `-m clickhouse` run, migrate, or `ensure_schema(force=True)`
call. If you must run CH tests by hand, always export the scratch DSN first and unset
it after. When docker / the CH container is unavailable, the CH stage cleanly SKIPs
(or pass `SKIP_CLICKHOUSE=1`) — but the pure-Python gates 1–3 are always required.

Do not push if `ci/local_validate.sh` prints `GATE FAILED`.