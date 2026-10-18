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

## Team counts

A team's daily commit, after-hours commit, weekend commit and other count numbers are the repository's count times the team's share of that repository, summed over the repositories the team owns. They can be fractional, and ratios are recomputed from the shared counts. The full contract is in [Team counts at the owner's share](team-counts-at-owner-share.md).

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
| `change_failure_rate` | ratio | Deployments linked to an incident divided by deployments, over the window's summed counts (target; see the note below) | `repo_change_failure_daily` (target) |
| `investment_allocation_pct` | percent | Canonical-theme completed-work share | `investment_metrics_daily` |
| `cyclomatic_per_kloc` | cyclomatic complexity per KLOC | Average the latest daily repository density | `repo_complexity_daily` |
| `compounding_risk_score` | score from 0 to 1 | Mean of the latest persisted scoped scores | `compounding_risk_daily` |

The daily job no longer writes `investment_metrics_daily` (CHAOS-9264): the daily completion source is `work_item_completion_daily`, which has no investment area, and the canonical theme distribution is stored in `work_unit_investments`. The `investment_allocation_pct` source above is the table as the Ask Dev registry names it; the table was dropped by ClickHouse migration 120 (CHAOS-9302).

State today: the Ask Dev service (acr) still reads the deprecated `repo_metrics_daily.change_failure_rate` column, so its answer is not yet this definition; moving it to `repo_change_failure_daily` is ticket 9018. Query-api (Home, `/explain`, operating review, report) already follows this definition.

Change failure rate is weighted over the whole selected window. It is never an
average of daily percentages and never falls back to PR reverts or to the
deployment-status ratio. A zero deployment denominator is "not applicable", and
a window with no incident evidence is "unknown"; both are distinct from a
measured zero failure rate. See [Change failure rate](#change-failure-rate).

Investment allocation uses only the five canonical themes and
`work_items_completed` as its denominator. Unclassified work is excluded from
that denominator. Compounding risk returns its persisted score, components,
weights, and thresholds without recomputing the score at query time; a stable
digest identifies the component/weight/threshold version represented by the
returned rows. The score is the weighted mean over the inputs that have data
(churn, complexity change, ownership, review latency), with the weights
renormalized over the present inputs, and is served with a `coverage` value:
the share of the weight that was present, from 0 to 1. A missing input is not
counted as zero, the score is never capped at a low coverage, and a row with no
input at all has no score (unknown), never 0. Every input covers the 30 days that
end on the row's day; review latency is the 90th percentile of the hours from
creation to first review of the pull requests merged in those 30 days. Each row
also stores the pull requests merged in the window, how many of them had a first
review, and that share (no share when nothing merged). The share is stored, not
scored.

Every definition publishes its unit, aggregation, display precision, null and
zero semantics, supported scopes and dimensions, range limits, comparison
rule, definition version, query version, source version, and freshness policy.
The query response preserves source evidence references and reports the prior
immediately preceding window of equal duration when comparison is requested.

## Change failure rate

Change failure rate is the share of deployments that failed: a deployment
that an incident names, or whose own status is failed. It answers "how often
does a change we ship cause a failure?", so it needs evidence: an incident that
ties to the view, or a deployment with a final status. Without either, the
platform cannot tell a healthy repository from one whose outcomes are not
recorded.

- **Deployments.** The deployments of the view's repositories whose day
  (`deployed_at`, else `finished_at`, else `started_at`, else `last_synced`)
  is in the view's window, except a cancelled one: it never reached its
  environment, so it is not a deployment.
- **Deployment status.** One meaning for every provider and every delivery
  metric (`internal/jobs/metrics/deploymentstatus`): `failure`, `failed` and
  `error` are failed (GitHub says `failure`, GitLab `failed`); `success` is
  succeeded; `canceled` and `cancelled` are cancelled and counted nowhere;
  every other value (in progress, queued, pending, inactive, blocked, an unknown
  string, none) is a deployment that is neither failed nor an outcome yet.
  Deployment failure rate and the deploy metrics of a day use the same meaning.
- **Failed deployments.** The deployments among them that at least one
  deployment-incident link names, or whose own status is failed. A failed
  deployment that an incident also names counts once. A deployment with a native link counts as
  native; a deployment with only heuristic links counts as heuristic. Today
  the daily link producer writes heuristic links only: an incident links to
  every deployment of its repository on the incident's day. The stored counts
  keep the two tiers apart, and `/explain` names the weakest tier behind a
  measured rate in `link_tier` (`heuristic` or `native`); a deployment failed by
  its own status alone has no link tier.
- **Incident evidence.** The incidents that started in the window and tie to
  the view's repositories. An incident ties to a repository directly, through
  a service-to-repository mapping. Only when it has no direct tie does it tie
  through the repository of its linked deployment. A deployment id that names
  more than one repository ties nothing.
- **Status evidence.** The deployments among them with a final status
  (succeeded or failed), K of the D deployments. With them the rate is
  measured even where no incident source exists, and a view whose deployments
  all succeeded reads a measured zero. A GitHub deployment's status comes
  from its statuses history (the deployments list carries none). A lookup that
  fails or is rate limited stores no status for that deployment in that run (a
  status already stored is kept, never cleared), and a unit goes on with the
  next deployments until three lookups of it have exhausted the rate limit;
  the rest of that unit's lookups are then skipped. A deployment that has no
  status at all stays without one and stays out of K. A deployment missed that
  way is filled by looking it up again: `dho sync deployments --provider github
  --owner <owner> --repo <repo> --since <day>` (or `--group <org> -s <name
  fragment>` for a few repositories) lists the repository's deployments again
  and looks up each one. The run logs one `github_deployments.status_lookups` line
  with the counts: stored, empty, failed and skipped.
- **Evidence basis and base.** A measured rate stands on `incident` (incidents
  tie to the view, no deployment has a final status: failed / D), `both`
  (incidents and final statuses: failed by link or by status, each deployment
  once, / D) or `status` (no incident ties to the view: failed by status / K,
  because a deployment without an outcome is neither failed nor healthy). The
  answer carries D, K and the coverage K / D, so deployments without an
  outcome (a status lookup that was skipped) stay visible. `/explain` serves
  the basis in `evidence_basis`. It is derived from the stored sums, never
  stored. One function holds the rule: `changefailure.ApplyRule`.
- **Unknown, three ways.** No stored row: `not_collected`. Stored rows and no
  deployment: `no_deployments_in_window`. Deployments, no incident evidence and
  no final status: `deployments_without_outcome`. `Outcome.UnknownReason`
  carries them for every Go reader. In the stored table they are: no row (or
  all rows absent), `deployments_count = 0`, and `deployments_count > 0` with
  `deployments_status_known = 0` and no incident count.
- **Subject.** A repository view counts that repository. A team view counts
  the repositories the team owns. An organization view counts every
  repository. The window is the view's time filter; a view without one is all
  time.

| Window state | Value | State |
| --- | --- | --- |
| No stored counts in the window | none | no state (empty) |
| Stored counts, no deployment | none: not applicable | `not_applicable_no_deployments` |
| Deployments, no incident evidence and no deployment with a final status | none: unknown | `unknown_no_incident_evidence` |
| Deployments, and incident evidence or a deployment with a final status | failed deployments / D (incident, both) or / K (status only); 0 is a measured zero | `measured` |

The three states with no value are different answers, so every surface that
shows the rate also gives the state: `/api/v1/explain` in `rate_state` (beside
`link_tier`), and the GraphQL `home` deltas and `operatingReview` metrics in
`rateState`. The state is empty for every other metric. "No stored counts"
means that nothing was counted for the window: the days were not computed, or
no repository of the view had a deployment or an incident.

What the rate stands on (CHAOS-9630). `/explain` serves the basis in
`evidence_basis`. The GraphQL `home` deltas and `operatingReview` metrics serve
`changeFailureEvidence` (type `ChangeFailureEvidence`): `basis` (`status`,
`incident` or `both`; null when there is no rate), `deployments` (D),
`deploymentsWithStatus` (K), `coverage` (K / D, from 0 to 1; null when D is 0)
and `unknownReason` (`not_collected`, `no_deployments_in_window` or
`deployments_without_outcome`; null when the rate is measured). The field is
served for change failure rate whatever its state, so an unknown rate names its
reason, and is null for every other metric. All five values are read off the
`Outcome` of the one rule (`Outcome.Evidence`), so they never disagree with the
rate. The REST Home answer and the frozen operating review documents do not
carry it. `not_collected` also covers a window whose read failed: the operating
review drops a failed read and serves it as nothing stored.

A report chart of change failure rate uses the same rule for each of its
buckets (a day, a week, a repository, the total): a bucket with no measured
rate has no point.

The daily inputs are stored per repository and day in
`repo_change_failure_daily` (deployments, failed deployments by tier, failed
deployments by status alone, deployments with a final status, incidents by
tie). A repository and day with no deployment and no incident has
no row. Readers sum the counts over the view, so the rate is never an average
of daily rates. `repo_metrics_daily.change_failure_rate_incident` holds the
one-day value for the repository-day rows of that table, and is empty when the
day is not applicable or unknown.

When a day is computed again and a repository has nothing left to count (its
incident was deleted, or its service now maps to another repository), the
stored counts are replaced by a row of zeros. Zeros read like no row: no
deployment and no incident.

**DEPRECATED: `repo_metrics_daily.change_failure_rate`.** This column is not
the change failure rate. It still holds the revert ratio (reverted / merged
pull requests, with a forced denominator of 1, so `0` when nothing merged),
with its old type, so that an older release keeps reading what it read before.
Do not read it for change failure rate. CHAOS-9017 removes it.

### Revert rate

Revert rate is reverted pull requests divided by merged pull requests. It is
**not measured yet**: no writer detects a reverted pull request (the loader does
not read the pull request title), so `repo_metrics_daily.revert_rate` is empty
(`NULL`) on every row and every surface shows no data, never 0%. The deprecated
`change_failure_rate` column is not copied into it: the reverted count behind
that column was always 0. A real detector is a follow-up.

### Deployment failure rate

Deployment failure rate is failed deployment runs divided by deployments, from
the deployment status. It is stored in `dora_metrics_daily` under
`metric_name = deployment_failure_rate`. A run that fails to deploy is not a
change that caused a failure in production, so this ratio is not change
failure rate.

**DEPRECATED: `dora_metrics_daily` rows with `metric_name =
change_failure_rate`.** They hold the same deployment-status ratio under its
old name, written before the rename or by an older release. They are kept as
they are. CHAOS-9017 renames or removes them.

## Pull request rework ratio

The pull request rework ratio is, of the merged pull requests that have
**review evidence**, the share that got a changes-requested review. A pull
request with no review data says nothing about rework: it is in neither the
numerator nor the denominator. **Missing is not healthy**: a repository whose
reviews were never read must not show `0%`.

- **Review evidence** of a pull request: the sync stored one review or more on
  it (`git_pull_requests.reviews_count > 0`), or a changes-requested review.
- **Capability, not provider name.** The ratio is measured only for a provider
  whose normalizer can store a changes-requested review. That is one
  declaration at the provider layer
  (`internal/providerfoundation/pull_request_review_states.go`), held against
  the normalizers by a test; the metric asks it and names no provider. Today
  GitHub's normalizer stores the provider's review state as it is, and the
  GitLab normalizer rebuilds reviews from approvals and notes and stores no
  review that asks for changes. A provider with no declaration, and a
  repository with no known provider, have no signal. Merged pull requests with
  no signal are counted as "no rework signal", never as reviewed.
- **Stored inputs** on `repo_metrics_daily`, one row for each repository and
  day (migration `114_pr_rework_ratio_review_basis.sql`): `prs_merged`,
  `prs_merged_reviewed`, `prs_merged_rework`, `prs_merged_no_rework_signal`.
  A row written before the migration holds `NULL` counts: not measured.
- **One rule** (`internal/jobs/metrics/prrework`, `Rate` in Go and
  `WindowRateSQL` in ClickHouse), applied to the counts summed over the view's
  window and repositories (a team is its owned repositories):

| The view holds | Value | State |
| --- | --- | --- |
| no row with stored counts | none | none |
| no merged pull request | none | `not_applicable_no_merged_pull_requests` |
| merged pull requests, none reviewed, all of a provider with no changes-requested event | none | `not_applicable_no_rework_signal` |
| merged pull requests, none reviewed | none | `unknown_no_review_evidence` |
| one reviewed pull request or more | `prs_merged_rework / prs_merged_reviewed` (0 is a measured 0) | `measured` |

- **A window is the ratio of the sums**, never a mean of daily ratios: a day
  with one reviewed pull request does not weigh as much as a day with fifty.
- **Coverage** is the reviewed pull requests over the merged pull requests of
  the view: the share of the merged pull requests the ratio speaks for, from 0
  to 1 (not a percent). The numerator is `prs_merged_reviewed`. The denominator
  is `prs_merged` of EVERY stored row of the view, also of a row that holds no
  counts (a day stored before the counts existed and not computed again): such
  a day is in the denominator only, so a window that is partly not counted has
  a low coverage. The ratio and its state are of the rows that hold counts
  only. The GraphQL Home delta serves it as `rateCoverage`: 0 when no merged
  pull request has review data of a provider that stores a changes-requested
  review (the states `unknown_no_review_evidence` and
  `not_applicable_no_rework_signal`); no value when no stored row of the view
  holds a merged pull request, when no stored row holds counts (the state is
  empty), and for every other metric. The pull requests of a provider with no
  such review are in the denominator only: 6 reviewed pull requests of one
  provider and 4 pull requests of a provider with no signal give 0.6. A
  repository-day with no stored row at all is not in the denominator: the
  coverage cannot show a day that was never computed. The REST Home response,
  the analytics measure, the flow opportunity, the recommendations job and the
  report chart do not serve it.
- **Readers** that apply the rule: Home (`pr_rework_ratio`, with the state in
  `rateState` and the coverage in `rateCoverage`), the analytics measure `PR_REWORK_RATIO`, the "high rework" flow
  opportunity, the recommendations job, and the weekly report chart.
- **DEPRECATED: `repo_metrics_daily.pr_rework_ratio`.** It keeps its old
  meaning (changes requested / ALL merged pull requests, `0` when nothing
  merged) and is still written, so a pod of the release before this change and
  a rollback read what they always read. No reader of this release reads it.
  `pr_rework_ratio_reviewed` is the one-day value of the rule for a reader of
  one row: `NULL` when the day is not measured.

## Pull request cycle time

The cycle time of one pull request is `merged_at - created_at`, in hours. A
pull request that was open for a long time and merged late counts with its
full cycle time: it is a real input, not an outlier to remove.

- **Day.** `repo_metrics_daily.median_pr_cycle_hours` is the median over the
  pull requests of the repository merged on that UTC day. A day on which many
  old pull requests merge can have a median of hundreds of hours.
- **Week or month.** The repository cycle-time axis of Quadrants is the median
  over **every** pull request of the repository merged in that week or month,
  read from `git_pull_requests` (latest synced version of each pull request).
  Each pull request has one weight. It is never a mean or a median of the
  daily medians: those give a day with one merge the same weight as a day with
  149 merges.
- **Median.** With an even count, the median is the mean of the two middle
  values, the same rule as the daily value.
- **No merge.** A week or month with no merged pull request has no value. It
  is not zero.

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
| `version` | Postgres clock reading in milliseconds, taken in the transaction that appends the row: the claim or reset that writes `reopened`, or the sync function's own transaction for `succeeded` (which runs after the finalize commit). Orders the events of one day. |

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
appends nothing. A run with status `no_repositories` never writes `succeeded`, but a full-org run that was claimed first has already written `reopened`, so its day reads unknown (never certified).

The day's latest full-org run is the one with the greatest `created_at`, and that column is stamped from the Postgres clock when the run is created, not from the worker's, so a skewed worker cannot sort a newer run before an older one.

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

## Refused manual sync triggers

`dev_health_sync_manual_trigger_refused_total{reason}` counts the manual sync
triggers (Sync now, Backfill, `dho backfill run`, webhook sync requests and the
initial backfill) that `synchandoff.Mint`, the only writer of
`sync_manual_triggers`, refused before it wrote anything. Each refusal also
writes one WARN log entry.

| `reason` | Meaning | Log entry |
| --- | --- | --- |
| `unselected_source` | The trigger names an enabled source that the planner-managed sync configuration does not select. | `sync_trigger.refused_unselected_source` (`sync_config_id`, `caller`, `triggered_by`, `mode`, `refused_source_count`, `requested_source_count`) |
| `unnamed_caller` | The trigger path names no caller. Every production path names one, so a count here is a code defect. | `sync_trigger.refused_unnamed_caller` (`sync_config_id`, `triggered_by`, `mode`) |

The api and the scheduler expose the counter on their `/metrics`. `dho backfill
run` is a short-lived command with no metrics endpoint: its refusals are in the
log entry only (and its exit code 3).
