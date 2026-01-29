# Work Graph

The work graph is the structure that links evidence and supports drill-down.

## What it is
A graph-like model of WorkUnits and relationships:
- issue ↔ PR ↔ commits ↔ files (with provenance tracking)

## Node Types
- **Issue**: Work items from Jira, GitHub Issues, GitLab Issues
- **PR**: Pull requests / Merge requests
- **Commit**: Git commits
- **File**: Source files touched by commits

## Edge Types
- **Issue ↔ Issue**: blocks, relates, duplicates, parent_of/child_of
- **Issue ↔ PR**: implements, fixes, references
- **PR ↔ Commit**: contains
- **Commit ↔ File**: touches

## Why it exists
- Enables explainability without recomputation.
- Provides drill-down paths from aggregates to evidence.
- Supports flow and investment distribution materialization.
- Powers "Related Entities" views on detail pages.

## What it is not
- A replacement for provider-native objects.
- A scoring layer.

## API and Visualization

See [Work Graph View](views/work-graph.md) for:
- GraphQL API documentation (`workGraphEdges` query)
- Filter options
- UI component plans (Related Entities, Work Graph Explorer)
