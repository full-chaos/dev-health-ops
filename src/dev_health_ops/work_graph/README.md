# Work Graph Module

The work graph module builds and persists derived relationships between work items, pull requests, commits, and files.

> [!IMPORTANT]
> This module is a **derived analytics layer**. It reads from API outputs and writes to derived tables. It does NOT participate in provider orchestration or define domain meaning.

## What This Module Does

- Extracts relationships that are **already implicit** in the data
- Writes derived edges to ClickHouse tables
- Provides deterministic, idempotent edge generation

## What This Module Does NOT Do

- ❌ Mutate API state
- ❌ Participate in provider orchestration
- ❌ Define domain meaning
- ❌ Schedule itself or own lifecycle
- ❌ Perform inference, scoring, or categorization beyond explicit references
- ❌ Act as a second ingestion pipeline

## Architecture

The work graph is a directed graph stored in ClickHouse with:

- **Nodes**: Work items (issues), PRs, commits, files
- **Edges**: Relationships between nodes with provenance and confidence tracking

### Tables

Located in the `stats` schema:

| Table                    | Purpose                                       |
| ------------------------ | --------------------------------------------- |
| `work_graph_edges`       | Generic edge store with full provenance       |
| `work_graph_issue_pr`    | Fast path for issue ↔ PR links                |
| `work_graph_pr_commit`   | Fast path for PR ↔ commit links               |
| `work_graph_commit_file` | VIEW over `git_commit_stats` (no duplication) |

All tables use `ReplacingMergeTree` with `last_synced` for deduplication.

## Allowed Relationships (Strict Scope)

Only relationships already implicit in data:

| Relationship       | Source                          | Provenance    |
| ------------------ | ------------------------------- | ------------- |
| issue ↔ issue      | `work_item_dependencies` table  | native        |
| issue ↔ PR         | Explicit text refs in PR title  | explicit_text |
| PR ↔ commit        | Provider-native (API)           | native        |
| commit ↔ file      | `git_commit_stats` (already canonical) | native   |

Every emitted edge includes:
- `source_id`
- `target_id`
- `relationship_type`
- `provenance`
- `confidence`
- `evidence`
- `timestamp`

## Edge Types

### Issue-to-Issue (from `work_item_dependencies`)

- `blocks` / `is_blocked_by`
- `relates` / `is_related_to`
- `duplicates` / `is_duplicate_of`
- `parent_of` / `child_of`

### Issue-to-PR

- `references` - PR mentions issue
- `implements` - PR implements/closes issue
- `fixes` - PR fixes issue

### PR-to-Commit

- `contains` - PR contains commit

### Commit-to-File

- `touches` - Commit modifies file

## Provenance

Each edge tracks how it was discovered:

| Provenance      | Description       | Example                        |
| --------------- | ----------------- | ------------------------------ |
| `native`        | From provider API | `work_item_dependencies` table |
| `explicit_text` | Parsed from text  | "ABC-123" in PR title          |
| `heuristic`     | Inferred by rules | Same repo + time window match  |

## Confidence Scores

Edges include a confidence score (0.0 - 1.0):

- **1.0**: Native API relationships
- **0.9**: Closing keywords ("fixes #123")
- **0.7**: Plain references ("#123")
- **0.3**: Heuristic matches (configurable)

## CLI Usage

CHAOS-5303 r1 (P2): `builder.py` used to have its own standalone
`python -m work_graph.builder ...` CLI, undocumented anywhere outside this
file and receiving no Go pre-step coverage. It has been deleted -- it
duplicated the CLI below with a strictly smaller flag set. Build the work
graph via the tracked, documented entry point instead
(`work_graph/runner.py`'s `run_work_graph_build`, wired to `dev-hops
work-graph build` -- see `docs/operate/runbooks/operator-commands.md`, which
already lists this as **legacy**, direct Python compute that bypasses the
worker's own dispatch/idempotency; prefer `workgraph trigger` where possible):

```bash
dev-hops work-graph build --db "clickhouse://localhost:9000/default"
```

## Module Structure

```
work_graph/
├── __init__.py
├── builder.py           # Main entry point and CLI
├── models.py            # Data classes (WorkGraphEdge, etc.)
├── ids.py               # Deterministic ID generation
├── extractors/
│   ├── __init__.py
│   └── text_parser.py   # Parse issue refs from PR text
└── writers/
    ├── __init__.py
    └── clickhouse.py    # Persist to ClickHouse
```

## Idempotency

All operations are idempotent:

1. Edge IDs are deterministic hashes of (source, target, edge_type)
2. Tables use `ReplacingMergeTree` with `last_synced`
3. Re-running produces same results with updated timestamps

## Example Queries

### Find all PRs linked to an issue

```sql
SELECT
    pr_number,
    confidence,
    provenance
FROM stats.work_graph_issue_pr
WHERE work_item_id = 'jira:ABC-123'
ORDER BY confidence DESC
```

### Trace issue to files touched

```sql
WITH issue_prs AS (
    SELECT pr_number
    FROM stats.work_graph_issue_pr
    WHERE work_item_id = 'jira:ABC-123'
),
pr_commits AS (
    SELECT commit_hash
    FROM stats.work_graph_pr_commit
    WHERE pr_number IN (SELECT pr_number FROM issue_prs)
)
SELECT DISTINCT file_path
FROM stats.work_graph_commit_file
WHERE commit_hash IN (SELECT commit_hash FROM pr_commits)
```

### High-confidence edges only

```sql
SELECT *
FROM stats.work_graph_edges
WHERE confidence >= 0.7
ORDER BY discovered_at DESC
LIMIT 100
```
