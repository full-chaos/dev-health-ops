# AI Automation Opportunity Detector

The AI opportunity detector is a rule-based, read-only backend computation behind the `aiOpportunities` GraphQL field. It reads existing ClickHouse analytics tables and returns ranked `AIOpportunity` recommendations with stable IDs and inspectable evidence references.

## Persistence decision

CHAOS-1586 uses inline detection in the GraphQL resolver rather than adding a new persisted table. The detector only reads already-computed daily rollups and lightweight attribution evidence, so the first backend implementation stays small and avoids introducing another scheduled write path. If recommendation volumes or latency grow, the same output contract can be moved behind a ClickHouse sink later.

## Rules

| Opportunity kind | Trigger |
| --- | --- |
| `REPETITIVE_CHANGE` | At least five AI-assisted PRs in the last 30 days share the same author, work type, and three-word title prefix. |
| `HIGH_REVIEW_LOAD` | AI-assisted PRs have `reviews_per_pr` at least 1.5× the human baseline and at least 10 AI PRs in the window. |
| `HIGH_REWORK` | AI-assisted PRs have rework rate at least 0.25 and at least +0.10 above the human baseline with at least 10 AI PRs. |
| `SLOW_CYCLE` | AI-assisted PRs have average cycle time at least 1.25× the human baseline with at least 10 AI PRs. |
| `UNCOVERED_TEST_AREA` | AI-assisted PRs have `test_gap_rate` at least 0.50 with at least 10 AI PRs. |
| `TEST_GENERATION` | Human-authored PRs have `test_gap_rate` at least 0.50 with at least 10 human PRs (CHAOS-2189). |
| `DEPENDENCY_UPDATES` | At least five human-authored, non-bot PRs in the last 30 days have dependency-bump titles (`bump`/`update`/`upgrade` plus dependency vocabulary); AI-attributed PRs are excluded via anti-join (CHAOS-2189). |
| `MECHANICAL_MIGRATIONS` | At least five human-authored, non-bot PRs in the last 30 days have migration-style titles (`migrat…`, `codemod`, `mass rename`, …); AI-attributed PRs are excluded via anti-join (CHAOS-2189). |
| `DOCUMENTATION_DRIFT` | At least 20 code commits landed in the last 30 days with zero documentation-file changes (`*.md`, `*.rst`, `*.adoc`, `docs/`) in the same window (CHAOS-2189). |
| `FLAKY_TEST_TRIAGE` | Case-weighted `flake_rate` from `testops_test_metrics_daily` is at least 0.05 across at least 50 test-case executions in the last 30 days (CHAOS-2189). |

The five CHAOS-2189 rules detect *human/manual* toil or quality gaps where AI should be applied next, so (unlike the AI-bucket rules) they gate on human-authored activity. The SQL-backed CHAOS-2189 rules return nothing under a team-scoped filter, mirroring `REPETITIVE_CHANGE`, because the underlying raw tables carry no team scoping.

## GraphQL contract

`aiOpportunities(scope, limit)` returns `AIOpportunitiesResult`:

- `orgId`: current organization scope.
- `detectorReady`: `true` when the rule detector was available for the request.
- `recommendations`: ranked `AIOpportunity` items, capped at 100 even when a larger limit is requested.

Each recommendation includes:

- `opportunityId`: stable hash of `(kind, repo_id, team_id)`.
- `kind`: one of the ten canonical opportunity kinds above.
- `repoId` / `teamId`: the scope where the rule fired.
- `title` and `rationale`: short, numeric explanation of the breach.
- `score`: 0..1 breach-magnitude ranking score.
- `evidenceRefs`: ClickHouse-backed references such as `ai_impact_metrics_daily:rework_rate:<repo_id>` or `git_pull_requests:<repo_id>:<number>`.
