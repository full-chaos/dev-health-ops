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
- **Issue ↔ Commit**: implements, references (direct link via commit message)
- **PR ↔ Commit**: contains
- **Commit ↔ File**: touches

## Identity Linking (Edge Discovery)

Edges between issues and code are discovered through multiple parsing strategies:

### Issue → PR Edges
Extracted from PR title, body, and branch name:
- **Jira keys**: `ABC-123`, `PROJ-456`
- **GitHub/GitLab refs**: `#123`, `fixes #456`, `closes #789`
- **Branch names**: `feature/ABC-123-add-login` → links to ABC-123

### Issue → Commit Edges (Direct)
Extracted from commit messages:
- **Jira keys**: `ABC-123: fix typo`, `Fixes ABC-123`
- **GitHub/GitLab refs**: `fixes #123`, `closes #456`

### Edge Confidence
- **Native** (1.0): Provider-reported links (e.g., GitHub's linked issues)
- **Explicit text** (0.85-0.9): Parsed from text with clear patterns
- **Heuristic** (0.3): Time-window matching when no explicit link found

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
