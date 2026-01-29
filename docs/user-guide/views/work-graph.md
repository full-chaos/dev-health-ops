# Work Graph View

The Work Graph View visualizes entity relationships across issues, PRs, commits, and files.

## Purpose

Answer questions like:
- Which PRs implement this issue?
- What commits are in this PR?
- Which files are most touched by this work stream?
- What issues are blocking progress?

## Data Model

### Node Types
| Type | Description | Example ID |
|------|-------------|------------|
| `issue` | Work item from Jira/GitHub/GitLab | `PROJ-123`, `github:org/repo#42` |
| `pr` | Pull request / Merge request | `github:org/repo:pr:99` |
| `commit` | Git commit | `abc123def...` |
| `file` | Source file | `src/lib/utils.ts` |

### Edge Types

**Issue ↔ Issue**
- `blocks` / `is_blocked_by` — Blocking relationships
- `relates` / `is_related_to` — Related work
- `duplicates` / `is_duplicate_of` — Duplicate issues
- `parent_of` / `child_of` — Hierarchy (epics/subtasks)

**Issue ↔ PR**
- `implements` — PR implements the issue
- `fixes` — PR fixes the issue (typically bugs)
- `references` — PR mentions the issue

**PR ↔ Commit**
- `contains` — PR contains the commit

**Commit ↔ File**
- `touches` — Commit modifies the file

### Provenance
Each edge has a provenance indicating how it was discovered:
- `native` — From provider API (e.g., Jira issue links)
- `explicit_text` — Parsed from text (e.g., "Closes #123" in PR description)
- `heuristic` — Inferred by rules (e.g., branch naming patterns)

### Confidence
Edges have a confidence score (0.0 - 1.0):
- `1.0` — Native provider links
- `0.8-0.9` — Explicit text patterns (e.g., "fixes #123")
- `0.5-0.7` — Heuristic inference

## GraphQL API

### Query
```graphql
query WorkGraphEdges($orgId: String!, $filters: WorkGraphEdgeFilterInput) {
  workGraphEdges(orgId: $orgId, filters: $filters) {
    edges {
      edgeId
      sourceType
      sourceId
      targetType
      targetId
      edgeType
      provenance
      confidence
      evidence
      repoId
      provider
    }
    totalCount
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }
  }
}
```

### Filters
| Filter | Type | Description |
|--------|------|-------------|
| `repoIds` | `[String]` | Filter by repository IDs |
| `sourceType` | `WorkGraphNodeType` | Filter by source node type |
| `targetType` | `WorkGraphNodeType` | Filter by target node type |
| `edgeType` | `WorkGraphEdgeType` | Filter by relationship type |
| `nodeId` | `String` | Find all edges connected to a node |
| `limit` | `Int` | Max results (default 1000) |

### Example: Find PRs that implement an issue
```graphql
{
  workGraphEdges(orgId: "my-org", filters: {
    nodeId: "PROJ-123",
    edgeType: IMPLEMENTS
  }) {
    edges {
      sourceId  # PR ID
      targetId  # Issue ID (PROJ-123)
      evidence  # "Implements PROJ-123" from PR title
    }
  }
}
```

### Example: Find commits in a PR
```graphql
{
  workGraphEdges(orgId: "my-org", filters: {
    sourceType: PR,
    sourceId: "github:org/repo:pr:99",
    edgeType: CONTAINS
  }) {
    edges {
      targetId    # Commit hash
      confidence  # 1.0 for native
    }
  }
}
```

## UI Components (Planned)

### Related Entities Section
On issue/PR detail pages, show:
- Linked PRs/issues with relationship type
- Commits (on PR pages)
- Blocking/blocked-by indicators

### Work Graph Explorer
Interactive graph visualization:
- Start from a seed node (e.g., selected issue)
- Expand to see connected entities
- Filter by node/edge types
- Click to navigate to entity details

See: [dev-health-web#88](https://github.com/full-chaos/dev-health-web/issues/88)

## Interpretation Guidelines

1. **Provenance matters** — Native edges are authoritative; heuristic edges are suggestions
2. **Missing edges ≠ no relationship** — Depends on provider configuration and parsing
3. **Use for navigation, not scoring** — The graph supports exploration, not metrics
4. **Evidence is extractive** — `evidence` field contains the actual text that created the edge

## Related Documentation

- [Work Graph Contract](../work-graph.md) — Core model documentation
- [Investment View](investment-mix.md) — How themes map to work
- [PR Flow](pr-flow.md) — PR lifecycle visualization
