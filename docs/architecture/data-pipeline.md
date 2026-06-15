# Data Pipeline Architecture (dev-health-ops)

## Pipeline Overview

The dev-health-ops backend follows a strict unidirectional pipeline:

```
Connectors → Processors → Sinks → Metrics → Visualization
```

Each stage has clear responsibilities. Do not collapse layers or bypass stages.

---

All paths below are relative to `src/dev_health_ops/`.

## 1. Connectors (`connectors/`)

**Purpose:** Fetch raw data from external providers.

### Supported Providers

| Provider | Module | Sync Targets |
|----------|--------|--------------|
| Local Git | `connectors/local.py` | git, blame |
| GitHub | `connectors/github.py` | git, prs, cicd, deployments, incidents, work-items |
| GitLab | `connectors/gitlab.py` | git, prs, cicd, deployments, incidents, work-items |
| Jira | `connectors/jira.py` | work-items |
| Synthetic | `connectors/synthetic.py` | fixtures generation |

### Rules

- Network I/O should be async and batch-friendly
- Respect rate limits and backoff mechanisms
- Return raw provider data (minimal transformation)
- Handle pagination completely (never assume single page)

---

## 2. Processors (`processors/`)

**Purpose:** Normalize and transform connector outputs into internal models.

### Key Processor

- `processors/local.py` — Primary processor for local git data

### Responsibilities

- Map provider-specific fields to unified models
- Normalize timestamps to UTC
- Resolve identities across providers
- Enrich with computed fields (e.g., commit size buckets)

### Rules

- No network I/O
- No persistence logic
- Transform only, no business decisions
- Output must match models in `models/`

---

## 3. Storage / Sinks (`metrics/sinks/`)

**Purpose:** Persist processed data to storage backends.

### Supported Backends

| Backend | Connection | Use Case |
|---------|------------|----------|
| PostgreSQL | `postgresql+asyncpg://` | Relational, migrations |
| ClickHouse | `clickhouse://` | Analytics queries |
| MongoDB | `mongodb://` | Document storage |
| SQLite | `sqlite+aiosqlite://` | Local dev/test |

### Rules

- **No file exports. No debug dumps. No JSON/YAML output paths.**
- All persistence goes through sink modules
- Backend selection via `--db` flag or `DATABASE_URI`
- Secondary sink via `SECONDARY_DATABASE_URI` with `sink='both'`

### Sink Interface

```python
async def write_batch(records: List[Model], session: AsyncSession) -> int:
    """Write a batch of records. Returns count written."""
```

---

## 4. Metrics (`metrics/`)

**Purpose:** Compute higher-level rollups and aggregates from persisted data.

### Key Metric Tables

| Table | Key | Content |
|-------|-----|---------|
| `repo_metrics_daily` | `(repo_id, day)` | Commits, LOC, PR cycle time |
| `user_metrics_daily` | `(repo_id, author_email, day)` | User activity |
| `work_item_metrics_daily` | `(day, provider, work_scope_id, team_id)` | Throughput, WIP, cycle time |
| `team_metrics_daily` | `(team_id, day)` | After-hours, weekend ratios |

### Computation Model

- Metrics are **append-only** with `computed_at` versioning
- Use `argMax(<metric>, computed_at)` to get latest value
- Re-computation is safe (idempotent via compound keys)

### Work-item team attribution

Every work item is stamped with a `team_id` at compute time
(`metrics/compute_work_items.py`). Resolution is a fallback cascade
(`resolve_base_team` + one inheritance tier), first match wins:

1. **Scope key** — `ProjectKeyTeamResolver.resolve(work_scope_id)`: the Jira
   project key, GitHub/GitLab repo path, or Linear project name.
2. **Project key** — retry with `WorkItem.project_key` (Linear's TEAM key,
   which differs from the project name when an issue sits in a project).
3. **Assignee membership** — `TeamResolver` maps the primary assignee's
   canonical identity to a team via `IdentityMapping.team_ids`.
4. **Linked-issue inheritance** — `LinkedIssueTeamResolver`: an item that
   still resolved to no team borrows the team of an issue it links to via
   `work_item_dependencies`. This is **provider-agnostic** — a GitHub/GitLab
   PR inherits the team of the Linear/Jira issue it closes — and is what lets
   PRs (which match none of tiers 1–3) share a team dimension with the issue
   trackers in the investment allocation-coverage and team-exchange views.
5. **`unassigned`** — the normalized sentinel when every tier misses.

Cross-provider links are captured during sync as provider-neutral
`extkey:KEY` dependency edges (GitHub: PR body magic-words + head branch;
GitLab: issue/MR description magic-words; Jira: native `issuelinks`). The
key is resolved to the real `linear:`/`jira:` work item at inheritance time,
so over-capturing is harmless — a key with no matching issue never resolves,
and a key that exists in **both** Linear and Jira is treated as ambiguous and
dropped rather than guessed.
Only **inheritance-safe relationship types** (`relates_to`, `relates`,
`duplicates`, `external_issue_key`) transfer a team; blocking links
(`blocks`/`blocked_by`), which routinely span teams, are ignored. When
several donors match one source, the lexicographically smallest canonical
target wins (a stable tiebreak, since ClickHouse rows are unordered).

The resolver (`build_linked_issue_team_resolver`) is built **once per run**
and applied to *every* work-item metric family — cycle-times
(`compute_work_item_metrics_daily`), state-durations
(`compute_work_item_state_durations_daily`), and the issue-type / investment
rollups (via `_get_team`) — so a PR reads with the same team in every table
and cross-table joins stay consistent.

It must see a **donor-complete** set, not just the active window — but the
read is **bounded to the linked surface** (the work items actually referenced
by a dependency edge), not the tenant's whole history, and is best-effort (a
failed donor read degrades to no inheritance rather than aborting the run):

- `job_daily` reads dependency edges
  (`ClickHouseDataLoader.load_work_item_dependencies`, `FINAL`) **bounded to
  the source ids evaluated this run** (the run-window work items), collects the
  referenced target ids / external keys, and loads only those donor items via
  `load_work_item_dependencies_donors`. This still spans repos and time (a PR
  can close an issue completed long before the metrics day, or a repo-less
  Linear/Jira issue) without scanning the full graph.
- `job_work_items` (the sync) treats the **freshly-extracted edges as
  authoritative** for the items it synced — they are the current
  source-of-truth, so a link removed from a PR is simply absent and stops
  granting inheritance — and loads only the referenced donor *items* from
  ClickHouse (bounded), so an incremental run that re-fetches only the PR still
  finds a donor synced earlier.

All donor reads are **tenant-scoped**: the org-wide donor/edge queries run
only under an explicit `org_id`, so a PR can never inherit a team from another
organization's issue. An unscoped (dev/CLI) run skips inheritance rather than
reading across tenants.

Because the edges are written during a **work-items sync**, a sync (not just a
metrics recompute) is required for newly-captured links to take effect.

> Note: branch-name capture trusts the head branch (the Linear convention),
> so a contributor could in principle name a branch to force a team
> inheritance. This is an analytics-attribution signal, not an authorization
> boundary, and it is bounded to the contributor's own org (tenant-scoped
> donors) and to inheritance-safe relationship types — the worst case is a
> self-inflicted mis-attribution of one PR's team within the org — so it is
> accepted rather than gated.

> Known limitation: `work_item_dependencies` is append-only and has no
> tombstone, so a *removed* link is not deleted from the table. The **sync**
> path is unaffected — it re-extracts the PR's current edges, so a removed link
> stops inheriting immediately. But a standalone **`job_daily` recompute** reads
> persisted edges and can keep honoring a removed link until the next sync
> re-stamps the source. A general link-lifecycle/tombstone for
> `work_item_dependencies` (which also affects the work-graph) is tracked as a
> follow-up.

**Purpose:** Render persisted data for exploration.

### dev-health-web

- **Visualization-only** — Must not become source of truth
- Consumes data via GraphQL API from dev-health-ops
- No category recomputation at UX time

---

## Backfill Pipeline

Historical backfill reuses the same data pipeline (Connectors -> Processors -> Sinks) but operates differently from incremental sync:

### How It Works

1. **Date range splitting** -- The `BackfillChunker` divides the requested date range into 7-day windows
2. **Sequential processing** -- Each chunk runs through the standard sync pipeline independently
3. **Progress tracking** -- A `BackfillJob` record in PostgreSQL tracks chunk completion and overall progress

### Key Differences from Incremental Sync

| Aspect | Incremental Sync | Backfill |
|--------|-----------------|----------|
| Trigger | Scheduled / manual | Manual or API-triggered |
| Date range | From watermark to now | Explicit `--since` / `--before` |
| Watermarks | Updates SyncWatermarks | **Never** updates watermarks |
| Chunking | Single pass | 7-day windows |
| Progress | Job run status only | Per-chunk progress via BackfillJob |
| Queue | `sync` | `backfill` (dedicated) |

### Tier Limits

Backfill depth is gated by organization billing tier:

| Tier | Max Days |
|------|----------|
| Community | 30 |
| Team | 90 |
| Enterprise | Unlimited |

### Components

- `backfill/chunker.py` -- `chunk_date_range()` splits date ranges into windows
- `backfill/runner.py` -- `run_backfill_for_config()` orchestrates chunked sync
- `backfill/cli.py` -- `dev-hops backfill run` CLI command
- `workers/tasks.py` -- `run_backfill` Celery task on `backfill` queue
- `models/backfill.py` -- `BackfillJob` PostgreSQL model for progress tracking
- `api/services/backfill.py` -- `BackfillJobService` async CRUD for API layer

## Storage Schema Highlights

### ClickHouse Tables

Tables are `MergeTree` partitioned by `toYYYYMM(day)`:

```sql
CREATE TABLE repo_metrics_daily (
    repo_id UUID,
    day Date,
    computed_at DateTime,
    -- metrics columns
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);
```

### PostgreSQL Tables

Managed via Alembic migrations in `alembic/`:

```bash
# Generate migration
alembic revision --autogenerate -m "description"

# Apply migrations
alembic upgrade head
```

---

## Environment Variables

| Variable | Purpose | Required |
|----------|---------|----------|
| `DATABASE_URI` | Primary database connection | Yes |
| `SECONDARY_DATABASE_URI` | Secondary sink (with `--sink both`) | No |
| `DB_ECHO` | Enable SQL logging | No |
| `BATCH_SIZE` | Records per batch insert | No (default: 100) |
| `MAX_WORKERS` | Parallel workers | No (default: 4) |

---

## Adding New Pipeline Components

### New Connector

1. Create `connectors/newprovider.py`
2. Implement async fetch methods
3. Register in `connectors/__init__.py`
4. Add CLI integration in `cli.py`

### New Metric

1. Define model in `models/`
2. Add sink in `metrics/sinks/`
3. Implement computation in `metrics/`
4. Create Alembic migration if using Postgres
5. Update dev-health-web or OTLP dashboards as needed

### Rules When Modifying

- Never bypass sinks for persistence
- Always handle pagination
- Add tests under `tests/`
- Respect existing async patterns
