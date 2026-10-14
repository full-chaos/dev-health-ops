---
page_id: ref-work-graph
summary: Canonical Work Graph node, relationship, identity, source, and attribution concepts.
content_type: reference
owner: platform-api
source_of_truth:
  - current Work Graph IDs, builder, GraphQL schema, and team-attribution contracts
applicability: current
lifecycle: active
---

# Work Graph model

Work Graph connects supported work, source, and organizational records through stable tenant-scoped identities.

Concepts include:

- organization-scoped node identity;
- repositories, commits, pull requests, reviews, work items, teams, and identities;
- provider-native IDs retained for reconciliation;
- typed relationships with explicit source and direction;
- primary team attribution selected by the current precedence model;
- evidence and coverage states for missing or unresolved relationships.

Graph proximity is not causation or ownership. A missing edge can represent unavailable source data, unsupported mapping, or incomplete processing.

## Deployment pull request link tier

A deployment row stores the pull or merge request number it was linked to (`deployments.pull_request_number`) and, beside it, the rule that chose that number (`deployments.pull_request_tier`). A consumer must read the tier before it names the pull request as the cause of a deployment.

| Tier | Provider | Rule |
|---|---|---|
| `exact_merge_sha` | GitHub | A merged pull request whose `merge_commit_sha` is the deployed sha. |
| `merged_pr` | GitHub, GitLab | No exact match: the first merged pull or merge request listed for the sha. |
| `first_listed` | GitHub | No merged pull request: the first one listed (it can be unmerged, and `merged_at` stays empty). |
| `pushed` | Push API | The caller asserted the number; no provider rule ran. |
| empty | any | No link, or a row written before the column existed. |

A row written before the column existed keeps its number with an empty tier: the tier is unknown until the next sync of its unit rewrites the row. No backfill is run. The tier is kept together with the number and `merged_at` when a lookup fails.

## Team attribution

Every work item's primary team is selected by a staged, provider-agnostic precedence — the resolver
evaluates every applicable source and persists a candidate row per match, then the highest-precedence
match present becomes the primary (`is_primary`) attribution:

| Rank | Source | Resolves from |
|--:|---|---|
| 0 | `native_team` | The work item's own native team key (Linear only) |
| 1 | `issue_project` | The native issue's project → owning team |
| 2 | `project_ownership` | Explicit team-to-project ownership (GitLab, Jira, Linear) |
| 3 | `repo_ownership` | Explicit team-to-repository ownership (GitHub) |
| 4 | `assignee_membership` | The assignee's team membership |
| 5 | `linked_issue` | A linked issue's own team, inherited across a supported dependency edge (a GitHub/GitLab PR closing a Linear issue, for example) |
| 6 | `author_membership` | The PR/MR author's (reporter's) team membership, when unambiguous and not a bot — never overrides a real linked issue |
| 7 | `manual_fallback` | An explicit admin-configured fallback rule (repo, project, member, or issue-key prefix) |
| 8 | `unassigned` | Nothing matched |

Ranks 2 and 3 are provider-disjoint by design, not overlapping coverage: GitHub has no native Project
entity and writes only repo ownership; GitLab, Jira, and Linear write only project ownership. Every
provider is covered across teams, projects, members, and issues — attribution logic never branches on
provider.

Team identity for a work item is read from the latest primary attribution record at query time; it is
not denormalized onto every downstream table, so a small number of read paths (documented on the full
page) intentionally read a stored team column instead.

Repositories, pull requests, issues, and projects form one entity tree; see
[Entity tree: repositories, pull requests, issues, projects](../../contribute/architecture/data-and-storage.md#entity-tree-repositories-pull-requests-issues-projects).
The issue <> pull request link of record is `work_graph_issue_pr`, with a `provenance` tier of native > explicit_text > heuristic; all three count as links, and a lower tier is never presented as native.

For the full precedence decision tree, the source reference matrix, the provider coverage contract,
symptom-to-fix diagnostics, and the recovery/backfill runbook, see
[Work-item team attribution](../../contribute/architecture/team-attribution.md).

For how a captured PR↔issue link (per-provider primary mapping vs. text-parse fallback) reaches
*investment* work-unit evidence specifically — a separate consumer from the cycle-time attribution
path above, with its own current defect — see
[Investment work-graph consumption](../../contribute/architecture/team-attribution.md#investment-work-graph-consumption-structural-evidence-bridge-chaos-4752).
