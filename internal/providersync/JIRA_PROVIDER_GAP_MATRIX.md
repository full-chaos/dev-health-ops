# Jira provider parity gap matrix

This is the provider-only audit for the unregistered Jira work-item route. The
provider-local route now composes the six canonical facts, Jira worklogs, and
all ten governed derived dispositions. It is still not an activation or
cutover claim: shared registry, matrix, constructor selection, admission, and
deployment wiring remain outside this slice. The Python source of truth is the
current `src/dev_health_ops/metrics/job_work_items.py`, Jira provider
implementation, and team/reference discovery worker in the same checkout.

## Matrix shape

The provider matrix has six Jira aliases:

| Matrix alias | Canonical family | Current route state | Scope |
| --- | --- | --- | --- |
| `incidents` | incidents | native and route-ready | JSM incident rows only; separate handler/effects |
| `work-items` | work-items | provider-local complete; not registered | canonical work-item family |
| `work-item-comments` | work-items | provider-local complete; not registered | same canonical family; comments are an enrichment, not a separate Go producer |
| `work-item-history` | work-items | provider-local complete; not registered | same canonical family; changelog is an enrichment |
| `work-item-labels` | work-items | provider-local complete; not registered | same canonical family; labels are fields on the work item |
| `work-item-projects` | work-items | provider-local complete; not registered | same canonical family; project scope/reference data is enrichment |

Linear has the same five work-item aliases (without Jira's separate
incidents pair). Alias count must not be mistaken for five independent
implementations: all five aliases must reach the one canonical family with
the same persisted destinations.

## Provider breadth

| Surface | Python behavior | Go in this slice | Gap / disposition |
| --- | --- | --- | --- |
| Project/window discovery | `fetch_jira_work_items_with_extras` builds one JQL per configured project, with `JIRA_JQL` and `JIRA_FETCH_ALL` overrides. | Requires a non-empty `SourceExternalID` project key and builds the equivalent bounded JQL. | Direct project/window behavior is covered by the live batch oracle; project enumeration is not part of this route. |
| Project/team/member identity | `workers/team_autoimport_jira.py` discovers Jira teams, projects, members, memberships, ownership, and board sprints; `IdentityResolver` aliases are applied to both roster and assignee paths. | Derived computation consumes the migrated, tenant-scoped ClickHouse team/project/member context and bounded donor rows. | Dimension discovery remains a separate provider unit; this route does not add registry or wiring. |
| Work-item identity and fields | `jira_issue_to_work_item` emits project identity, title/description, status and status-category fallback, type, labels, priority/service class, timestamps, assignee/reporter, story points, sprint, parent/epic, URL, and due date. | Normalizes the same semantic fields, including Atlassian account/name identity and GraphQL-to-REST worklog fallback. | Covered by live producer oracles and route tests. |
| Changelog/history | Python sorts Jira histories by timestamp, emits status transitions, and derives started/completed lifecycle. | Same transition ordering, status mapping, reopen derivation, and lifecycle helpers. | Direct row is covered; real producer batch comparison is the required proof boundary. |
| Comments/interactions | Legacy producer fetches comments per issue, applies `JIRA_COMMENTS_LIMIT`, converts valid comments, and continues on a best-effort fetch error. | Paginated comment fetch, bounded limit, typed incomplete marker, and watermark withholding while landed effects remain recoverable. | Covered by the live legacy-batch oracle and Atlassian route harness. |
| Links/dependencies | Legacy normalizer extracts outward/inward links and canonicalizes blocker direction. | Same direction and relationship normalization, with canonical semantics version. | Direct row is covered; real batch and ClickHouse recovery proof remains `CHAOS-3712`. |
| Reopen/priority | Reopen events derive from terminal-to-nonterminal transitions; priority maps to service class. | Same derivation and mapping. | Direct row is covered by the semantic/route tests. |
| Sprint/reference cache | Legacy producer collects issue sprint IDs, reuses Jira reference sprints, fetches missing sprints, and writes newly fetched references. | Reuses tenant-scoped reference sprints, enumerates board sprints only when references miss, and exposes a typed reference sink. | Reference reuse, board enumeration, and incomplete-watermark behavior are route-tested. |
| Boards/sprints | `team_autoimport_jira.py` enumerates boards and board sprints; the Atlassian provider can optionally fetch board sprints. | Implements bounded board/sprint enumeration and the reference-cache skip path. | Covered by the Atlassian live producer pair. |
| Worklogs | Atlassian provider optionally fetches worklogs through GraphQL with REST fallback under `JIRA_FETCH_WORKLOGS`; legacy batch has no worklog list. | Typed worklog effect, identity parity, and GraphQL-attempt/REST-fallback observations are covered by the Atlassian route. | Worklogs are not one of the 16 work-item matrix destinations; durable completion payload records the typed fetch observations. |
| Atlassian alternate path | `JiraProvider`'s Atlassian path emits canonical work items/transitions/reopens and optional worklogs/sprints, but currently returns empty dependencies and interactions. | One provider-local route composes Atlassian worklogs/boards with the legacy dependency/comment semantics required by the canonical family. | Identity, GraphQL fallback observations, sprint references, dependencies, and interactions are covered together before the watermark advances. |
| JSM incidents | Separate native JSM path and `operational_incidents` destination. | Already native and route-ready. | Out of scope for this work-item continuation; do not modify incident handler/effects/readback. |

## Destination coverage

The Jira and Linear work-item aliases each name the same 16 canonical
destinations:

| Destination | Python job behavior | Go status in this slice |
| --- | --- | --- |
| `work_items` | direct normalized work-item sink | typed direct effect and migrated ClickHouse adapter |
| `work_item_transitions` | direct changelog sink | typed direct effect and migrated ClickHouse adapter |
| `work_item_dependencies` | direct link sink | typed direct effect and migrated ClickHouse adapter |
| `work_item_reopen_events` | direct reopen sink | typed direct effect and migrated ClickHouse adapter |
| `work_item_interactions` | direct comment sink | typed direct effect and migrated ClickHouse adapter |
| `sprints` | direct issue/reference sprint sink | typed direct effect, board/reference breadth, and migrated ClickHouse adapter |
| `ai_attribution` | Jira `ProviderBatch.ai_attributions` is explicitly empty; no Jira AI producer runs | evaluated-empty readback-required effect; rejects rows, performs no ClickHouse I/O, and reads `EffectAbsent` |
| `estimate_coverage_metrics_daily` | compute-time derived daily output | live-Python-matched typed compute/effect and migrated adapter |
| `investment_classifications_daily` | compute-time derived daily output | live-Python-matched typed compute/effect and migrated adapter |
| `investment_metrics_daily` | compute-time derived daily output | live-Python-matched typed compute/effect and migrated adapter |
| `issue_type_metrics_daily` | compute-time derived daily output | live-Python-matched typed compute/effect and migrated adapter |
| `work_item_cycle_times` | compute-time derived lifecycle output | live-Python-matched typed compute/effect and migrated adapter |
| `work_item_metrics_daily` | compute-time derived daily output | live-Python-matched typed compute/effect and migrated adapter |
| `work_item_state_durations_daily` | compute-time derived transition output | live-Python-matched typed compute/effect and migrated adapter |
| `work_item_team_attributions` | compute-time attribution using reference teams and bounded donors | live-Python-matched typed compute/effect with fail-closed donor context |
| `work_item_user_metrics_daily` | compute-time user rollup | live-Python-matched typed compute/effect and migrated adapter |

`worklogs` is a seventeenth Jira-only effect outside the governed canonical
sixteen. It retains DateTime64(6) precision and its own migrated adapter. The
provider route emits all seventeen effects in one recoverable batch; a route
without the injected deriver reports all ten derived destinations as missing
and withholds its watermark.

## Evidence and intentional deferrals

- `jira_work_items_oracle_prep_test.go` compares the semantic work-item row to
  the live Python normalizer.
- `jira_work_items_batch_oracle_test.go` compares a non-empty multi-issue
  producer batch, including transitions, links, reopens, comments, and sprints.
- `jira_work_items_sink_oracle_test.go` compares the six direct ClickHouse
  projections, but its input rows are constructed at the sink boundary; it is
  not evidence that a real Jira producer populated them.
- `jira_work_item_derived_oracle_test.go` executes all nine checked-in Python
  compute producers over provider=`jira`, including multi-day and donor cases.
- `jira-work_items_atlassian.py` now compares the reflected, explicitly empty
  `ProviderBatch.ai_attributions` field as well as worklogs and sprints.
- `jira_work_item_derived_effects_integration_test.go` uses the production
  ClickHouse migration chain for the attribution-context loader plus tenant,
  replay, lease, and readback proof.
- `jira_work_items_derived.json` is the shared-harness mutation plan for the
  provider-local composition and evaluated-empty AI disposition.
- No registry, matrix, shared constructor, admission, route switch, scheduler,
  or deployment wiring is changed here.

Therefore this branch closes Jira's **provider-local semantic route**. It is
not activated route readiness or cutover until the separately owned shared
wiring and admission gates select this handler and composite sink.
