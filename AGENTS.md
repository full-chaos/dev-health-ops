# AGENTS — dev-health-ops

If available locally:

**Canonical Reference:** See [`/AGENTS.md`](../AGENTS.md) for the unified Dev Health platform agent briefing.
**Deep Dives:** See the MkDocs site under [`docs/`](docs/index.md) — e.g. the [Investment Categorization Pipeline](docs/architecture/investment-categorization-pipeline.md), [Investment Data Model](docs/architecture/investment-data-model.md), and [LLM Categorization Contract](docs/llm/categorization-contract.md).


# AGENTS — Briefing and pointers (dev-health-ops)
This document contains **dev-health-ops specific** guidance. For cross-cutting concerns (Investment View, Work Graph contract, visualization rules), refer to the canonical AGENTS.md.

This file is intentionally short. The canonical instructions live in the MkDocs site under `docs/`.

## Read-first (in order)
1. **Product intent and guardrails**: `docs/product/prd.md`, `docs/product/concepts.md`
2. **Pipeline boundaries**: `docs/architecture/data-pipeline.md`
3. **Dual database contract (semantic vs analytics)**: `docs/architecture/database-architecture.md`, `docs/ops/cli-reference.md`
4. **Investment model (canonical)**: `docs/user-guide/investment-view.md`, `docs/product/investment-taxonomy.md`
5. **LLM contract (compute-time only)**: `docs/llm/categorization-contract.md`
6. **Views and interpretation**: `docs/user-guide/views-index.md`, `docs/visualizations/patterns.md`
7. **API surface**: `docs/api/graphql-overview.md`, `docs/api/view-mapping.md`
8. **How to run it**: `README.md` (test tiers), `ci/run_tests.sh`
9. **Test patterns**: `tests/api/auth/test_invite_flow.py` (gold-standard fixture), `tests/api/auth/test_register.py`, `tests/api/admin/` (admin CRUD)

## Non-negotiables (summary)
- **WorkUnits are evidence containers, not categories.**
- Categorization is **compute-time only** and persisted as distributions.
- Theme roll-up is deterministic from subcategories (taxonomy is fixed).
- UX-time LLM is **explanation only** and must not recompute categories/edges/weights.
- Persistence goes through **sinks** only (no file exports, no debug dumps).

## GitHub PR workflow override

- Use the `github-gh` skill for GitHub operations.
- **Push the branch first, then create the PR with `--head <branch>`.** In non-interactive contexts (agents, CI) `gh pr create` will NOT push the branch for you. `--head` does NOT auto-push either — per the gh docs, it "explicitly skip[s] any forking or pushing behavior." The error message `you must first push the current branch to a remote, or use the --head flag` is misleading; `--head` only suppresses the interactive push prompt and still requires the branch to be on remote.
- Canonical sequence:

  ```bash
  GIT_MASTER=1 git push origin <branch>:<branch>
  gh pr create --head <branch> --title "<title>" --body "<body>" --base main
  ```

  - `--fill` (derives title/body from commits) is independent of pushing — still push first: `git push origin <branch>:<branch> && gh pr create --head <branch> --fill --base main`.
- Prefer `gh pr create ...` over the `gh pct` alias in agent contexts. `pct` resolves to `gh pr create -t`, which then collides on argument parsing when you also pass `--head` or `--title` after it.
- Never use a bare `git push` from a worktree; worktree upstreams can point at `main` and trigger branch-protection failures. The explicit refspec form (`git push origin <branch>:<branch>`) bypasses this.

## Provider boundary
- New provider integrations live under `src/dev_health_ops/providers/<provider>/`.
- Use the canonical provider contracts in `src/dev_health_ops/providers/base.py`;
  async REST/TestOps providers use the shared base helpers in
  `src/dev_health_ops/providers/_base.py`.
- Raw fetch, pagination, auth, transport setup, retry, and rate-limit handling
  belong in provider clients/adapters.
- Normalization lives next to the provider (`providers/<provider>/normalize.py`
  or provider-local mapping helpers) and returns normalized domain rows/models.
- Processors orchestrate provider calls and persistence only; they must not own
  raw provider fetch logic or provider-specific normalization.
- `src/dev_health_ops/connectors/` is legacy and frozen. **Hard ban:** do not add
  new provider code under `connectors/`; only compatibility aliases for existing
  callers are allowed.

## Change discipline (agents)
- Identify which layer you are changing: connector, processor, metrics, sink, API, UI.
- Make the smallest possible change that achieves the outcome.
- If behavior changes, add/adjust tests.
- Do not blur responsibilities across layers.

## Atlassian AGG integration notes
- Jira issue listing remains REST (JQL). GraphQL currently supports **fetch-by-key** only.
- AGG GraphQL is used for enrichment (worklogs) and ops team mappings.
- Enable worklog GraphQL enrichment with `ATLASSIAN_GQL_ENABLED=true` and `JIRA_FETCH_WORKLOGS=true`.
- Use `JIRA_USE_PROVIDER=true` to route work-items ingestion through `JiraProvider`.
- Ops teams are synced via `dev-hops sync teams --provider jira-ops` using AGG project queries.

## Deprecated repo-root agent docs
The following repo-root files were historical duplicates and are no longer authoritative:
- `AGENTS-INVESTMENT.md`
- `AGENTS-INVESTMENT-CATEGORY.md`
- `AGENTS-WG.md`

They have been moved under `docs/appendix/legacy/agents/` for reference only.
---

## 0) Read-first (order matters)

1. `cli.py`
2. `processors/local.py`
3. `connectors/__init__.py`

Goal: understand **boundaries** (ingest → normalize → persist → metricize → visualize) before touching anything.

---

## 1) Mission and product intent

`dev-health-ops` is an OSS analytics platform for **team operating modes** and developer health, backed by provider data (GitHub, GitLab, Jira, local Git) and computed metrics stored in DB sinks.

### Investment View is canonical

* **Signals is retired** (POC only). Do not extend it.
* The platform answers: **“Where is human effort actually being invested, and what is the cost to people when certain work dominates?”**

---

## 2) System architecture (pipeline)

### 2.1 Data flow

1. **Connectors (`connectors/`)**

   * Fetch raw provider data.
   * Network I/O should be async and batch-friendly.

2. **Processors (`processors/`)**

   * Normalize/transform connector outputs into internal models.

3. **Storage / Sinks (`metrics/sinks/`)**

   * Persist computed outputs.
   * **No file exports. No debug dumps. No JSON/YAML output paths.**

4. **Metrics (`metrics/`)**

   * Compute higher-level rollups (daily/backfills/etc.) from persisted data.

5. **Visualization (Grafana + dev-health-web)**

   * UI renders **persisted** data.
   * `dev-health-web` is visualization-only: it must not become the source of truth.

### 2.2 Storage backends

* PostgreSQL (SQLAlchemy + Alembic) — semantic layer only
* ClickHouse (analytics store) — **required for all analytics features**
* MongoDB — deprecated for analytics, will be removed
* SQLite — deprecated for analytics

> **ClickHouse is the only supported analytics backend.** MongoDB, PostgreSQL, and SQLite support for analytics is deprecated.

#### aiosqlite scope

`aiosqlite` is intentionally kept as a SQLite driver alias for narrow, non-production use only.

Allowed:

* Test fixtures under `tests/`, including in-memory API endpoint tests that use `httpx.ASGITransport`.
* Local-only ephemeral development with `sqlite:///path`.

Forbidden:

* Any production semantic database; use PostgreSQL.
* Any analytics backend; use ClickHouse.
* CI long-run pipelines or durable environments.

The SQLite URL normalization paths in `src/dev_health_ops/db.py` and `metrics/db_utils.py` are intentional compatibility helpers, not permission to use SQLite beyond the scopes above.

Backend selection:

* Semantic DB: CLI `--db` or `POSTGRES_URI` (legacy fallback: `DATABASE_URI`).
* Analytics DB: CLI `--analytics-db` or `CLICKHOUSE_URI`.
* Secondary sink: `SECONDARY_DATABASE_URI` when using `sink='both'`.

---

## 3) The non-negotiable Work Graph + Investment contract

### 3.1 Core contract

* **WorkUnits are evidence containers, not categories.**
* LLM decides **subcategory distributions** at **compute time only**.
* **Theme roll-up is deterministic** from subcategories.
* UX renders **only persisted distributions and edges**.
* LLM explanations may run on-demand but **must not alter persisted decisions**.
* **Sinks only** for persistence (no output files).

### 3.2 Canonical taxonomy

#### Themes (fixed)

* Feature Delivery
* Operational / Support
* Maintenance / Tech Debt
* Quality / Reliability
* Risk / Security

Rules:

* No synonyms.
* No overrides.
* No per-team configuration.
* Provider-native labels/types are **inputs only** and must be normalized away.

#### Subcategories (fixed per theme)

* There is a small curated set per theme.
* Subcategories provide resolution without fragmenting language.
* Subcategory probabilities roll up to theme probabilities.

### 3.3 Data model guarantees

For every WorkUnit (evidence container):

* Theme probabilities sum to ~1.0
* Subcategory probabilities sum to theme probabilities
* Evidence arrays exist (may be empty)
* Evidence quality always emitted
* Categorization never returns “unknown”

---

## 4) LLM usage rules

### 4.1 Compute-time categorization (required)

* Output must be **strict JSON** matching `work_graph/investment/llm_schema.py`.
* Keys must be from canonical subcategory registry.
* Probabilities must be normalized and valid.
* Evidence quotes must be **extractive substrings** from provided inputs.
* Retry policy: **one repair attempt** only.
* On failure: mark invalid + apply deterministic fallback.
* Persist audit fields for every run.

### 4.2 UX-time explanations (allowed, constrained)

* Explanations can only use **persisted distributions and stored evidence**.
* Explanations must not recompute categories/edges/weights.
* All explanation output must be labeled AI-generated.

Language constraints:

* Allowed: *appears, leans, suggests*
* Forbidden: *is, was, detected, determined*

Explanation format:

* SUMMARY (max 3 sentences)
* REASONS (specific evidence)
* UNCERTAINTY (limits + evidence quality)

---

## 5) Visualization rules (Grafana + web)

### 5.1 Investment views

* Treemap: **Theme-level by default**
* Sunburst: Theme → (optional) scope/team → (optional) clusters
* Sankey: Theme → scope/team pressure flows

### 5.2 Drill-down contract

* Default: Theme-only (leadership readable)
* Drill: Theme → Subcategory → Evidence (WorkUnits)
* Never show WorkUnits as peers to themes/subcategories.

### 5.3 Grafana query conventions (when touching dashboards)

* Prefer table format and stable time ordering where required.
* Handle legacy `team_id` null/empty normalization.
* Avoid ClickHouse `WITH name = expr` syntax; use `WITH ... AS` aliasing.

(Only modify Grafana provisioning when needed; do not replatform dashboards incidentally.)

---

## 6) Developer workflow (CLI)

### 6.1 Run migrations (required for fresh environments)

* `dev-hops migrate postgres` — PostgreSQL schema (Alembic)
* `dev-hops migrate clickhouse` — ClickHouse analytics tables

### 6.2 Sync data

* Git data (local):

  * `CLICKHOUSE_URI=clickhouse://... dev-hops sync git --provider local --repo-path /path/to/repo`

* Work items:

  * `CLICKHOUSE_URI=clickhouse://... dev-hops sync work-items --provider <jira|github|gitlab|synthetic|all> -s "org/*"`

* Teams:

  * `POSTGRES_URI=postgresql+asyncpg://... dev-hops sync teams --provider <config|jira|jira-ops|synthetic|ms-teams|github|gitlab>`

### 6.3 Generate synthetic data

* `CLICKHOUSE_URI=clickhouse://... dev-hops fixtures generate --sink "$CLICKHOUSE_URI" --days 30`

### 6.4 Compute metrics

* Daily rollups:

  * `CLICKHOUSE_URI=clickhouse://... dev-hops metrics daily`
### 6.5 Interim Workaround: Trigger via Celery Jobs (CHAOS-2475)

Bare CLI commands run inline. The CLI preflight does not enforce credentials like provider tokens, LLM keys, or Stripe keys. As an interim workaround, trigger the equivalent Celery job instead of running bare CLI commands. Workers carry the credentials that the bare CLI does not enforce.

Start the worker process using:
```bash
dev-hops workers start-worker --queues default metrics sync reports
```

Triggered jobs run in the worker environment. Trigger them using:
- The sync-config trigger or backfill endpoints for data syncs.
- The `triggerReport` mutation for reports.

This serves as a temporary workaround, not a permanent contract change. For details on worker setup and queues, see the runbook at [docs/ops/workers.md](docs/ops/workers.md). Tracked in CHAOS-2475 and CHAOS-2482.


---

## 7) Engineering rules for agents

### 7.1 Change discipline

* **NEVER commit directly to main** — Always create a feature branch first:
  ```bash
  git checkout -b <type>/<descriptive-name>  # e.g., fix/password-hashing, feat/oauth-sso
  ```
* **Use git worktrees for parallel work** — When starting a new feature or unrelated task, create the worktree under this repo's `.worktrees/` directory:
  ```bash
  git worktree add .worktrees/<branch-or-task-name> <branch-name>
  ```
  Do not create ops worktrees as siblings of `ops/` or at the platform root.
  This keeps each task isolated, preventing cross-contamination of changes.
* Prefer **minimal, surgical** changes.
* Keep surrounding style; use targeted edits.
* Add/adjust tests under `tests/` for behavior changes.
* If DB models change: include Alembic migrations (Postgres).

### 7.1a Visual evidence for cross-repo frontend impact

If a `dev-health-ops` change affects `dev-health-web` rendering (e.g., API shape changes, new/modified GraphQL fields, metric schema changes), screenshot evidence from the web frontend **must** be included in the PR.

* Use the **Playwright MCP** (`playwright` skill) in the `dev-health-web` repo to capture affected pages after the dev server is running.
* Attach screenshots to the GitHub PR body using a GitHub-hosted release asset URL, then attach the same URL to Linear:

  ```bash
  # 1) Host local PNG(s) in the web repo with a stable GitHub URL.
  gh release create gh-attach-assets --repo full-chaos/dev-health-web --title "PR screenshot assets" --notes "Long-lived release for agent-uploaded PR and Linear screenshot evidence." 2>/dev/null || true
  gh release upload gh-attach-assets "/path/to/screenshot.png" --repo full-chaos/dev-health-web --clobber

  # 2) Embed the hosted image URL in the PR body/comment.
  IMAGE_URL="https://github.com/full-chaos/dev-health-web/releases/download/gh-attach-assets/screenshot.png"
  gh pr edit <PR> --repo full-chaos/dev-health-web --body-file <updated-body.md>

  # 3) Attach the same hosted URL to Linear (linear-cli does not upload local files).
  linear-cli attachments create --title "Screenshot: <description>" --url "$IMAGE_URL" <ISSUE>
  ```
* **When to skip:** Changes that have no impact on rendered frontend output (add `SCREENSHOT-WAIVER: <reason>` to PR body).
### 7.2 Correct boundaries

* Connectors fetch. Processors normalize. Metrics compute. Sinks persist.
* Do not collapse responsibilities into one layer.
* Do not add “helpful” outputs like file dumps. Persistence goes through sinks only.

### 7.3 Test patterns

* **API endpoint tests** follow `tests/api/auth/test_invite_flow.py` — SQLite in-memory via `aiosqlite`, `monkeypatch.setattr` for `get_postgres_session`, `dependency_overrides` for admin guards, `httpx.ASGITransport` client.
* Auth tests: `tests/api/auth/` (registration, password reset, email verification, invite flow, SSO).
* Admin CRUD tests: `tests/api/admin/` (sync configs, teams, identities, IP allowlist, retention policies, impersonation).
* Journey integration test: `tests/api/test_journey.py` chains register → login → onboard → admin CRUD → sync trigger.
* GraphQL schema export: `src/dev_health_ops/api/graphql/export_schema.py` is used by `dev-health-web` CI to detect schema drift.

### 7.4 Performance and reliability

* Async/batching for network I/O.
* Respect any existing rate limit/backoff mechanisms.
* Close SQLAlchemy engines in tests to avoid event-loop teardown warnings.

---

## 8) What to do when you start a task

1. **Create a branch** if this is a new task unrelated to previous work on the current branch.
   ```bash
   git checkout -b <descriptive-branch-name>  # e.g., feat/add-metrics-validation
   ```
2. Identify which layer you're changing (connector, processor, metric, sink, viz).
3. Re-state the relevant non-negotiables (WorkUnits are evidence; themes/subcats canonical; sinks only).
4. Make the smallest possible change that achieves the outcome.
5. Add a test (or update an existing one) for the new behavior.
6. Ensure no new outputs bypass sinks.

---

## 9) Quick reference

### Hard bans

* Treating WorkUnits as categories
* User-configurable categories/subcategories
* “Unknown” categorization output
* LLM recomputation at UX-time
* Any persistence path outside `metrics/sinks/*`

### Allowed references to dev-health-web

* Visualization implementation details only (charts, drill-down UX, rendering).
* Not allowed: redefining taxonomy, recomputing categories, or becoming data source.

---

## 10. Task Tracking (Linear)

> **Canonical Reference:** See [`/AGENTS.md`](../AGENTS.md#11-task-tracking-github-or-linear) for full documentation.

**Tracker:** Linear (default team: **CHAOS**).

### Quick Reference

```bash
linear-cli issues create "Task title" --team CHAOS --priority high
linear-cli issues list
linear-cli issues get CHAOS-123
linear-cli issues update CHAOS-123 --state "In Progress"
linear-cli issues update CHAOS-123 --state "Done"
```

---

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

---

## 11. Pre-commit + pre-push hooks (lefthook + ruff + mypy)

Lefthook drives three hooks:

- **commit-msg** strips agent-attribution trailers from every commit message
  before the commit lands. Backs the repo-root AGENTS.md rule
  "Never add contribution attribution for agents in commits." Removes
  `Ultraworked with [...]`, `Co-authored-by: Sisyphus`,
  `Co-authored-by: Claude`, and `🤖 Generated with [Claude Code]` lines.
  Implementation: `scripts/strip_agent_attribution.py`. Tests:
  `tests/scripts/test_strip_agent_attribution.py`.
- **pre-commit** auto-fixes formatting and lint on staged `.py` files and
  re-stages the fixes (`stage_fixed: true`), then runs `mypy` as a non-fixable
  gate over the whole project. The resulting commit is clean and type-checked.
- **pre-push** is a final gate: `ruff format --check` + `ruff check` on the
  files being pushed, plus `mypy` over the whole project. No auto-fix here —
  pre-push cannot modify the commits it's gating, so blocking with an
  instruction is the only correct shape.

### One-time install

```bash
# From the ops/ repo root (or any worktree):
make install
# Equivalent long form:
pip install -r requirements.txt
lefthook install
```

`lefthook install` writes hooks into `.git/hooks/pre-commit` and
`.git/hooks/pre-push`.

### Worktree note

This repo sets `core.hooksPath` to the main repo's `.git/hooks/` directory.
All worktrees share the same hooks path, so installing lefthook **once** covers
the main checkout and every worktree:

```bash
# From ops/ main checkout OR any worktree — installs for all:
lefthook install --force
```

If `lefthook install` (without `--force`) warns about `core.hooksPath`, add `--force`.

### Hook behaviour

**commit-msg** (on every `git commit`, edits the commit message file in place):

| Step | Command | Behaviour |
|------|---------|-----------|
| 1 | `python3 scripts/strip_agent_attribution.py {1}` | Removes agent-attribution trailers (`Ultraworked with`, `Co-authored-by: Sisyphus`, `Co-authored-by: Claude`, `🤖 Generated with`). Idempotent. Preserves real human `Co-authored-by`, `Signed-off-by`, `Refs`, `Closes`. |

**pre-commit** (on every `git commit`, for staged `.py` files):

| Step | Command | Behaviour |
|------|---------|-----------|
| 1 | `ruff format {staged_files}` | Auto-formats + re-stages |
| 2 | `ruff check --fix {staged_files}` | Auto-fixes lint issues + re-stages |
| 3 | `mypy` | **Gate**: blocks commit if type errors remain (whole-project per `[tool.mypy] files`, not just staged files) |

**pre-push** (on every `git push`, for `.py` files in commits being pushed):

| Step | Command | Behaviour |
|------|---------|-----------|
| 1 | `ruff format --check {push_files}` | **Gate**: blocks if formatting issues remain |
| 2 | `ruff check {push_files}` | **Gate**: blocks if lint issues remain |
| 3 | `mypy` | **Gate**: blocks push if type errors remain (whole-project) |

If pre-push blocks you, the failure message tells you exactly what to run
(`ruff format .`, `ruff check --fix .`, or `mypy` to surface the type errors),
then fix, commit, and re-push.

### Escape hatch

```bash
git commit --no-verify   # skip pre-commit (use sparingly, e.g. WIP)
git push --no-verify     # skip pre-push (emergency pushes)
```

### No ruff/mypy config changes

The hooks use the existing `[tool.ruff]` and `[tool.mypy]` settings from
`pyproject.toml`. Do not add new rules, exclusions, or `# type: ignore`
suppressions to satisfy the hook — fix the code instead.
