# Jira provider parity gap matrix

This is the provider-only audit for the unregistered Jira work-item route on
`feat/go-worker-jira-oracle-prep`. It is an explicit partial-parity record, not
a route-readiness or cutover claim. The Python source of truth is the current
`src/dev_health_ops/metrics/work_items.py`, Jira provider implementation, and
the team/reference discovery worker in the same checkout.

## Matrix shape

The provider matrix has six Jira aliases:

| Matrix alias | Canonical family | Current route state | Scope |
| --- | --- | --- | --- |
| `incidents` | incidents | native and route-ready | JSM incident rows only; separate handler/effects |
| `work-items` | work-items | native handler prepared, not registered/route-ready | canonical work-item family |
| `work-item-comments` | work-items | not route-ready | same canonical family; comments are an enrichment, not a separate Go producer |
| `work-item-history` | work-items | not route-ready | same canonical family; changelog is an enrichment |
| `work-item-labels` | work-items | not route-ready | same canonical family; labels are fields on the work item |
| `work-item-projects` | work-items | not route-ready | same canonical family; project scope/reference data is enrichment |

Linear has the same five work-item aliases (without Jira's separate
incidents pair). Alias count must not be mistaken for five independent
implementations: all five aliases must reach the one canonical family with
the same persisted destinations.

## Provider breadth

| Surface | Python behavior | Go in this slice | Gap / disposition |
| --- | --- | --- | --- |
| Project/window discovery | `fetch_jira_work_items_with_extras` builds one JQL per configured project, with `JIRA_JQL` and `JIRA_FETCH_ALL` overrides. | Requires a non-empty `SourceExternalID` project key and builds the equivalent bounded JQL. | Direct project/window behavior is covered by the live batch oracle; project enumeration is not part of this route. |
| Project/team/member identity | `workers/team_autoimport_jira.py` discovers Jira teams, projects, members, memberships, ownership, and board sprints; `IdentityResolver` aliases are applied to both roster and assignee paths. | No Go dimension-discovery implementation in this provider-only route. | Deferred to the reference/dimension parity slice; do not add registry or wiring here. |
| Work-item identity and fields | `jira_issue_to_work_item` emits project identity, title/description, status and status-category fallback, type, labels, priority/service class, timestamps, assignee/reporter, story points, sprint, parent/epic, URL, and due date. | Normalizes the same semantic fields. | JSON assignee/reporter identity currently differs from shipped Python (`CHAOS-3713`); do not hide it with a Go-only exclusion. |
| Changelog/history | Python sorts Jira histories by timestamp, emits status transitions, and derives started/completed lifecycle. | Same transition ordering, status mapping, reopen derivation, and lifecycle helpers. | Direct row is covered; real producer batch comparison is the required proof boundary. |
| Comments/interactions | Legacy producer fetches comments per issue, applies `JIRA_COMMENTS_LIMIT`, converts valid comments, and continues on a best-effort fetch error. | Paginated comment fetch, bounded limit, typed incomplete marker, and watermark withholding. | Semantics are covered in the route harness; compare the real producer batch and optional-failure behavior before activation. |
| Links/dependencies | Legacy normalizer extracts outward/inward links and canonicalizes blocker direction. | Same direction and relationship normalization, with canonical semantics version. | Direct row is covered; real batch and ClickHouse recovery proof remains `CHAOS-3712`. |
| Reopen/priority | Reopen events derive from terminal-to-nonterminal transitions; priority maps to service class. | Same derivation and mapping. | Direct row is covered by the semantic/route tests. |
| Sprint/reference cache | Legacy producer collects issue sprint IDs, reuses Jira reference sprints, fetches missing sprints, and writes newly fetched references. | Fetches issue-linked sprints but does not enumerate boards or persist a reusable reference cache in this route. | Cache/reference and incomplete-watermark semantics are deferred to `CHAOS-3714`. |
| Boards/sprints | `team_autoimport_jira.py` enumerates boards and board sprints; the Atlassian provider can optionally fetch board sprints. | No board enumeration or board-sprint path. | Deferred to the Jira Atlassian enrichment follow-up; issue-linked sprint rows are the only current Go surface. |
| Worklogs | Atlassian provider optionally fetches worklogs through GraphQL with REST fallback under `JIRA_FETCH_WORKLOGS`; legacy batch has no worklog list. | No worklog producer/effect in this route. | Worklogs are not one of the 16 work-item matrix destinations, but are still provider behavior and remain intentionally deferred. |
| Atlassian alternate path | `JiraProvider`'s Atlassian path emits canonical work items/transitions/reopens and optional worklogs/sprints, but currently returns empty dependencies and interactions. | This route models the legacy REST/JQL path only. | Must not be called provider-complete until Atlassian dependency/comment parity is resolved. |
| JSM incidents | Separate native JSM path and `operational_incidents` destination. | Already native and route-ready. | Out of scope for this work-item continuation; do not modify incident handler/effects/readback. |

## Destination coverage

The Jira and Linear work-item aliases each name the same 16 canonical
destinations:

| Destination | Python job behavior | Go status in this slice |
| --- | --- | --- |
| `work_items` | direct normalized work-item sink | provider handler/effect prepared; not registered |
| `work_item_transitions` | direct changelog sink | provider handler/effect prepared; not registered |
| `work_item_dependencies` | direct link sink | provider handler/effect prepared; not registered |
| `work_item_reopen_events` | direct reopen sink | provider handler/effect prepared; not registered |
| `work_item_interactions` | direct comment sink | provider handler/effect prepared; not registered |
| `sprints` | direct issue/reference sprint sink | provider handler/effect prepared; board/reference breadth deferred |
| `ai_attribution` | optional persisted attribution records | no Jira producer/effect in this slice |
| `estimate_coverage_metrics_daily` | compute-time derived daily output | no Go compute/effect in this slice |
| `investment_classifications_daily` | compute-time derived daily output | no Go compute/effect in this slice |
| `investment_metrics_daily` | compute-time derived daily output | no Go compute/effect in this slice |
| `issue_type_metrics_daily` | compute-time derived daily output | no Go compute/effect in this slice |
| `work_item_cycle_times` | compute-time derived lifecycle output | no Go compute/effect in this slice |
| `work_item_metrics_daily` | compute-time derived daily output | no Go compute/effect in this slice |
| `work_item_state_durations_daily` | compute-time derived transition output | no Go compute/effect in this slice |
| `work_item_team_attributions` | compute-time attribution using reference teams and bounded donors | no Go compute/effect in this slice |
| `work_item_user_metrics_daily` | compute-time user rollup | no Go compute/effect in this slice |

The first six are raw provider rows, not the whole Jira capability. The ten
derived destinations remain a separate compute/effect gap, and no source
inspection, sink projection, skipped route, or status claim closes it.

## Evidence and intentional deferrals

- `jira_work_items_oracle_prep_test.go` compares the semantic work-item row to
  the live Python normalizer.
- `jira_work_items_batch_oracle_test.go` compares a non-empty multi-issue
  producer batch, including transitions, links, reopens, comments, and sprints.
- `jira_work_items_sink_oracle_test.go` compares the six direct ClickHouse
  projections, but its input rows are constructed at the sink boundary; it is
  not evidence that a real Jira producer populated them.
- `CHAOS-3712` owns the real `fetch_jira_work_items_with_extras` plus direct
  sink/recovery proof.
- `CHAOS-3713` owns the shipped Python JSON user-identity defect before a Go
  identity comparison is made authoritative.
- `CHAOS-3714` owns sprint/reference cache reuse, optional incomplete markers,
  and recovery semantics.
- Jira Atlassian worklogs, board enumeration, and Atlassian-path
  dependency/interaction parity are intentionally deferred to a separate
  follow-up. No registry, matrix, config, route switch, or deployment wiring
  is changed here.

Therefore this branch is **partial provider parity preparation**, not Jira
provider completion and not route readiness.
