---
page_id: ref-metric-defs
summary: Contract for generated metric definitions and the source fields every entry must expose.
content_type: generated-reference
owner: product-analytics
source_of_truth:
  - current metrics schema and computation code
applicability: current
lifecycle: active
---

# Canonical metric definitions

Generate one entry per currently supported metric with:

- stable key and display label;
- question answered;
- unit and value domain;
- included population and exclusions;
- event time and window semantics;
- exact formula and aggregation;
- allowed scope and filters;
- measured-zero, null, unavailable, partial, and stale behavior;
- source tables, fields, and computation code;
- version or applicability;
- interpretation limits.

Do not publish a metric from an old planning document when no current computation or product surface supports it.

## Ask Dev V1 metric registry

Ask Dev V1 exposes exactly these eight metrics. The registry version is
`ask-dev-metrics.v1`; an unknown or deferred metric key is rejected rather than
silently mapped to a different measure.

| Stable key | Unit | Window aggregation | Source |
| --- | --- | --- | --- |
| `items_completed` | items | Sum the latest daily work-scope rows | `work_item_metrics_daily` |
| `cycle_time_p50_hours` | hours | Average the persisted latest daily/scope p50 values | `work_item_metrics_daily` |
| `avg_wip` | items | Average the latest daily status snapshots | `work_item_state_durations_daily` |
| `deployments_count` | deployments | Sum the latest daily repository rows | `deploy_metrics_daily` |
| `change_failure_rate` | ratio | Total failed deployments divided by total deployments | `deploy_metrics_daily` |
| `investment_allocation_pct` | percent | Canonical-theme completed-work share | `investment_metrics_daily` |
| `cyclomatic_per_kloc` | cyclomatic complexity per KLOC | Average the latest daily repository density | `repo_complexity_daily` |
| `compounding_risk_score` | score from 0 to 1 | Mean of the latest persisted scoped scores | `compounding_risk_daily` |

Change failure rate is weighted over the whole selected window. It is never an
average of daily percentages and never falls back to PR reverts, incidents, or
`repo_metrics_daily`. A zero deployment denominator is insufficient evidence;
it is distinct from a measured zero failure rate.

Investment allocation uses only the five canonical themes and
`work_items_completed` as its denominator. Unclassified work is excluded from
that denominator. Compounding risk returns its persisted score, components,
weights, and thresholds without recomputing the score at query time; a stable
digest identifies the component/weight/threshold version represented by the
returned rows.

Every definition publishes its unit, aggregation, display precision, null and
zero semantics, supported scopes and dimensions, range limits, comparison
rule, definition version, query version, source version, and freshness policy.
The query response preserves source evidence references and reports the prior
immediately preceding window of equal duration when comparison is requested.

## Blocked hours in `work_item_state_durations_daily`

`work_item_state_durations_daily` holds, for each day, the hours that work
items spent in each normalized status. Blocked Work reads the rows whose
status is `blocked`. An item's hours are `blocked` in two cases:

1. **Status name or label.** The provider status maps to `blocked` (for
   example a status named "Blocked" or "On Hold", or a label `blocked`).
2. **An open blocker.** Another work item blocks it. This comes from the
   blocking relations the providers report (`work_item_dependencies`): Jira
   issue links, GitLab issue links, Linear relations, and for GitHub the
   words "blocked by", "depends on" or "blocks" before an issue reference in
   the issue text. GitHub has no native blocking relation in this data.

For case 2 the item is `blocked` only while all of these hold:

- its own status is not `done` or `canceled`;
- the blocker is a synced work item of the same organization;
- the relation is known to exist;
- the blocker is open (until its completion time).

In that interval `blocked` replaces the item's own status. The item's total
hours in the day do not change.

### Per-item Blocked Work evidence

`work_item_blocked_durations_daily` stores the item-level evidence for a
Blocked Work list and count. For every item the daily state worker processes
that contributes state time to a day, it stores the blocked part of that time
in `duration_hours`, together with the provider, work scope and team that the
worker resolved for that snapshot.

A row with `duration_hours = 0` is deliberate. It is written when a later
compute finds that a previously blocked item has no blocked hours for that
day. The stable row identity is organization, day, provider and work item;
work scope and team are snapshot values, so a team or scope change cannot
keep an old blocked result alive.

Readers select the latest row by `computed_at` for each stable identity and
only then keep rows with a positive duration. Filtering before selecting the
latest row could show an item that a recompute has removed. The Blocked Work
list and count read those latest positive rows. A `POST /api/v1/drilldown/issues`
request with `filters.how.blocked: true` returns one row per provider and work
item in its window, plus a `count` before the response limit. Team scope keeps
the item-days recorded for that team before the window is reduced to one row.

When the relation is known to exist:

- **Start.** The provider's own time of the link, when the synced data
  carries one (GitLab issue links and Linear relations carry it; Jira links
  and relations read from text do not). If it does not, the first time a
  sync saw the relation. The
  second is too late when the link is older than the first sync that saw it,
  so blocked hours can be too low. They are never too high: the creation time
  of the two items is never used as the start.
- **End.** When the provider no longer reports the relation, the last time a
  sync saw it. A removed link is noticed when an item that carries it is
  synced again by a sync that reads its relations. For a link of the
  provider (Jira issue links, Linear relations, GitLab issue links) that is
  either of the two items. For a relation read from text (GitHub, GitLab
  description keywords, an issue key in text) it is the item that holds the
  text; a sync of the other item says nothing about it. The GitHub Projects
  v2 board sync reads no issue text, so it ends no relation. This end is
  never too late. It can be too early by the time between the last sync that
  saw the relation and its removal, and for the two cases named in the
  limits below. The blocked hours of earlier days do not change when a link
  is removed.

Limits of the relation data:

- A relation with no stored start gives no blocked hours.
- A blocker that is not a synced work item, or a finished blocker that has no
  completion time, gives no blocked hours. Missing data is not estimated.
- The current status of an item (for example in `work_item_cycle_times`) is
  not changed by a relation. Only the daily hours are.
- **Relations and items not synced since the writer and read times were
  stored.** Two cases end too early for data written before then, until the
  next sync that reads the relation:
  - **GitLab, description keyword `blocks`.** Such a stored relation does not
    say if it came from an issue link or from the word in a description. It
    then ends at its last sync when the blocked issue is synced later than
    the blocker, and is open again when the blocker is synced again. A
    relation synced since stores which issue writes it, and ends only when
    that issue is synced without it.
  - **GitHub issues on a Projects v2 board.** For an issue whose relations
    were last read before the read time was stored, a board sync looks like
    a sync that read the text, and its text relations end at the last sync
    that read the text. Once a sync that reads the text has stored the read
    time, a board sync ends nothing.

  In both cases blocked hours can be too low, never too high.
- **GitHub relations read from a comment** (a Linear bot's link-back). The
  comments of an issue are optional data: when a sync cannot read them, or
  reads only the first comments, it still writes the issue, and a relation
  that only a comment held ends at the last sync that read it. Blocked hours
  can be too low, never too high.

## Daily run marker: did the day's metrics run succeed

`repo_metrics_daily` and the other daily tables have a row only for a day with
activity. A day with no activity and a day that was never computed both show
as no row. `daily_metrics_run_marker` (ClickHouse migration 105) records the
state of the daily metrics run so a reader can tell the two apart.

| Column | Meaning |
| --- | --- |
| `org_id` | Organization. Every read filters on it. |
| `target_day` | The calendar day the run computed (UTC). |
| `generation` | The run generation the event came from. Informational, not ordered. |
| `state` | `succeeded` or `reopened`. |
| `finalized_at` | The version as a timestamp. |
| `version` | Postgres clock reading in milliseconds, taken in the transaction that changed the run state. Orders the events of one day. |

**The invariant.** The marker says `succeeded` for an organization and day
only when, at the moment of the append, committed Postgres says the latest run
of that day that computes the whole organization is succeeded. Whether a run
computes the whole organization is recorded when it is created
(`daily_metrics_runs.full_org`, true when no explicit repository list was
given): the scheduled fan-out, the post-sync run, a manual run without
`--repo-id`, and the external-recompute all-repository fallback all qualify. A
run started with a repository list computes only those repositories and never
certifies the day. Runs created before the column existed are classed by their generation: only
the scheduled fan-out counts. Older post-sync, manual and external-recompute
runs stay unmarked, because the post-sync site passed the triggering sync's
repository ids until 2026-08-25 (CHAOS-4263) and nothing stored says which kind
an old row is; their days read unknown, never a false succeeded.

The table is append-only and has two writers of `succeeded`: the function that
runs after a finalize commits, and the backfill. They are the same function
(`markerSync`): it takes the lock, reads committed Postgres, and appends
`succeeded` only if the day's latest full-org run is succeeded, `reopened` if
that run is not, and nothing if the table already agrees. `reopened` is also
appended when a full-org run is claimed for dispatch (so a day that a run is
recomputing is not certified) and when a redrive or partition recompute
reopens a succeeded run, inside the transaction that does it and before its
commit; if the append fails, that transaction rolls back and nothing changes.
Versions come from one clock, the Postgres server, read under the lock, never
from the writing host.

**Reader rule.** For each `(org_id, target_day)` take the row with the greatest
`version` over all generations, and let `reopened` win a tie:

```sql
SELECT target_day, argMax(state, (version, state = 'reopened')) AS state
FROM daily_metrics_run_marker
WHERE org_id = {org} AND target_day BETWEEN {from} AND {to}
GROUP BY org_id, target_day
```

- `succeeded`: a full-org run finished for that organization and day. A day
  with no repository row is a day with no activity.
- `reopened`, or no row at all: unknown. Never read this as zero activity.

A failed `succeeded` append never fails the run. The day stays unknown, the
failure is logged, and `dev_health_daily_metrics_run_marker_appends_total`
counts it with `outcome="failed"`. `dho workers metrics daily-marker-backfill
--org <uuid> --from <day> --to <day>` runs the same function for each day of a
range. It appends only when the table differs from Postgres, so a second run
appends nothing. A run with status `no_repositories` is not marked.

Concurrency. Every writer of one organization and day takes one Postgres
advisory lock: the sync function, the dispatch claim, and the redrive and
recompute resets. The sync function takes it first, then reads ClickHouse, then
reads Postgres, then appends, then commits. A claim or reopen that is still
inside its transaction therefore blocks the sync until it commits or rolls
back, and the sync then reads the state after it. The guarantee: no `succeeded`
is appended from a Postgres state older than a transition that had already
written its `reopened`. A reopen whose transaction rolled back after writing
its marker leaves the day unknown until the next sync, which heals it because
Postgres still says succeeded. A run that was created but not yet claimed does
not hide an older `succeeded` (it has not started computing); the claim does.
