# AGENTS — dev-health-ops

If available locally:

**Canonical Reference:** See [`/AGENTS.md`](../AGENTS.md) for the unified Dev Health platform agent briefing.
**Deep Dives:** See [`/docs/agent-instructions/`](../docs/agent-instructions/) for detailed topic documentation.


# AGENTS — Briefing and pointers (dev-health-ops)
This document contains **dev-health-ops specific** guidance. For cross-cutting concerns (Investment View, Work Graph contract, visualization rules), refer to the canonical AGENTS.md.

This file is intentionally short. The canonical instructions live in the MkDocs site under `docs/`.

## Read-first (in order)
1. **Product intent and guardrails**: `docs/product/prd.md`, `docs/product/concepts.md`
2. **Pipeline boundaries**: `docs/architecture/data-pipeline.md`
3. **Investment model (canonical)**: `docs/user-guide/investment-view.md`, `docs/product/investment-taxonomy.md`
4. **LLM contract (compute-time only)**: `docs/llm/categorization-contract.md`
5. **Views and interpretation**: `docs/user-guide/views-index.md`, `docs/visualizations/patterns.md`
6. **API surface**: `docs/api/graphql-overview.md`, `docs/api/view-mapping.md`
7. **How to run it**: `docs/ops/cli-reference.md`

## Non-negotiables (summary)
- **WorkUnits are evidence containers, not categories.**
- Categorization is **compute-time only** and persisted as distributions.
- Theme roll-up is deterministic from subcategories (taxonomy is fixed).
- UX-time LLM is **explanation only** and must not recompute categories/edges/weights.
- Persistence goes through **sinks** only (no file exports, no debug dumps).

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
- Ops teams are synced via `python cli.py sync teams --provider jira-ops` using AGG project queries.

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

### 2.2 Storage backends (supported)

* PostgreSQL (SQLAlchemy + Alembic)
* ClickHouse (analytics store)
* MongoDB
* SQLite

Backend selection:

* CLI `--db` flag or `DATABASE_URI`
* Secondary sink via `SECONDARY_DATABASE_URI` when using `sink='both'`.

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

### 6.1 Sync data

* Git data (local):

  * `python cli.py sync git --provider local --db "$DATABASE_URI" --repo-path /path/to/repo`

* Work items:

  * `python cli.py sync work-items --provider <jira|github|gitlab|synthetic|all> -s "org/*" --db "$DATABASE_URI"`

* Teams:

  * `python cli.py sync teams --provider <config|jira|synthetic> --db "$DATABASE_URI"`

### 6.2 Generate synthetic data

* `python cli.py fixtures generate --db "$DATABASE_URI" --days 30`

### 6.3 Compute metrics

* Daily rollups:

  * `python cli.py metrics daily --db "$DATABASE_URI"`

---

## 7) Engineering rules for agents

### 7.1 Change discipline

* Prefer **minimal, surgical** changes.
* Keep surrounding style; use targeted edits.
* Add/adjust tests under `tests/` for behavior changes.
* If DB models change: include Alembic migrations (Postgres).

### 7.2 Correct boundaries

* Connectors fetch. Processors normalize. Metrics compute. Sinks persist.
* Do not collapse responsibilities into one layer.
* Do not add “helpful” outputs like file dumps. Persistence goes through sinks only.

### 7.3 Performance and reliability

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

## 10. Task Tracking (bd + GitHub)

> **Canonical Reference:** See [`/AGENTS.md`](../AGENTS.md#11-task-tracking-bd--github) for full documentation.

**Project Board:** `https://github.com/orgs/full-chaos/projects/1`

### Quick Reference

```bash
# bd (local task tracking)
bd create "Task title" --priority P2 --external-ref gh-123
bd list --status open
bd status <id> in-progress
bd status <id> done
bd dep add <child-id> <parent-id> --type parent-child
bd sync

# GitHub issues (use labels, not --type)
gh issue create --title "Title" --body "Description" --label task
gh issue edit NNN --add-project "https://github.com/orgs/full-chaos/projects/1"
```

### Workflow

1. Create bd issue with `--external-ref gh-NNN` to link to GitHub
2. Update bd status during work
3. Run `bd sync` before `git push`
4. Close GitHub issue when complete

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
   bd sync
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
