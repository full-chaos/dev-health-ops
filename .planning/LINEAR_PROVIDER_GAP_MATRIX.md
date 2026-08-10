# Linear provider parity gap matrix

Baseline: `990ed4a20` in `feat/go-worker-linear-oracle-prep`.

The Python implementation is the behavior authority for this audit:
`src/dev_health_ops/providers/linear/provider.py`, `client.py`, and
`normalize.py`. The five matrix aliases (`work-items`, `work-item-labels`,
`work-item-projects`, `work-item-history`, and `work-item-comments`) collapse to
one canonical `work-items` crawl; they must not become independent crawlers.

| Surface | Python producer | Go at baseline | This continuation | Remaining evidence/gap |
| --- | --- | --- | --- | --- |
| Issue rows | `iter_issues_pages` + `linear_issue_to_work_item` | issue + basic fields | retain, add control flags | live end-to-end provider batch and sink readback |
| History/transitions | inline `history` + `extract_linear_status_transitions` | transitions only | retain, add reopen rows | complete history pagination policy is still deferred |
| Reopen events | `detect_linear_reopen_events` | absent | add | derived daily destinations remain deferred |
| Comments/interactions | inline comments + `linear_comment_to_interaction_event` | absent | add non-empty comments, bounded second page | live provider retry behavior under transient GraphQL failure |
| Attachments/dependencies | full attachment fallback + `extract_linear_dependencies` | absent | add trusted PR/MR, relation and inverse edges | deferred attachment/relation retry behavior beyond bounded page |
| Cycles/sprints | team `iter_cycles` + `linear_cycle_to_sprint` | issue cycle fields only | add team-scoped cycle collection, sprint rows, and reference-sprint cache | org-wide mode |
| Teams | `get_team_by_key`, `iter_teams` fallback | absent | route validates scoped team through GraphQL; reference rows resolve by id/name/native key/project key | global discovery |
| Members | `get_team_members` (autoimport/reference path) | absent | no provider-unit write | separate team/member reference follow-up |
| Projects | `iter_projects` + status/trashed fields | absent | no provider-unit write | separate project catalog/reference follow-up |
| Priority | `_priority_from_linear` | basic mapping | retain + oracle cases | unknown/null parity is covered only at normalizer boundary |
| Pagination | cursor pages, bounded relation reads, project structural validation | issue-page cap only | bounded issue + nested connection reads | query complexity/rate/cancellation matrix |
| Effect destinations | 16 canonical destinations | 2 (`work_items`, transitions) | 6 raw facts (adds deps/reopen/interactions/sprints) | 10 derived destinations need engine-specific parity before route completion |
| Five aliases | planner collapse + per-alias watermark/audit | untouched | untouched | must be handled by separate planner/route activation lane |
| Lifecycle/lease | Python sync job claim/effect/watermark contract | no production wiring | no wiring | route executor, lease recovery, alias atomicity remain deferred |

Canonical destination audit (the route remains intentionally partial):

- Implemented in this continuation: `work_items`, `work_item_transitions`,
  `work_item_dependencies`, `work_item_reopen_events`,
  `work_item_interactions`, `sprints`.
- Deferred derived effects: `ai_attribution`, `estimate_coverage_metrics_daily`,
  `investment_classifications_daily`, `investment_metrics_daily`,
  `issue_type_metrics_daily`, `work_item_cycle_times`,
  `work_item_metrics_daily`, `work_item_state_durations_daily`,
  `work_item_team_attributions`, `work_item_user_metrics_daily`.

The six implemented entries are raw provider facts, with direct ClickHouse
write/readback coverage for all six destinations under the integration
fixture. Their presence does not claim that the ten derived processors,
compute-time taxonomy, attribution precedence, or live provider API retry
behavior has reached Linear parity.

This is an intentionally partial provider slice. It does not set matrix
`go_executor`, `native_shadow`, or `route_ready`, and it does not register a
handler or change scheduling/configuration.
