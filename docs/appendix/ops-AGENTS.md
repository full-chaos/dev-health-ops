# AGENTS — Single Source of Truth (dev-health-ops)

This document is the **canonical** agent briefing for this repo: how it works, what not to break, and the minimum rules to move fast without causing architectural regressions. It consolidates prior agent guidance into one set of enforceable constraints.

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

1. Identify which layer you’re changing (connector, processor, metric, sink, viz).
2. Re-state the relevant non-negotiables (WorkUnits are evidence; themes/subcats canonical; sinks only).
3. Make the smallest possible change that achieves the outcome.
4. Add a test (or update an existing one) for the new behavior.
5. Ensure no new outputs bypass sinks.

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
