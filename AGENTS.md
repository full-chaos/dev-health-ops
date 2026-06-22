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
