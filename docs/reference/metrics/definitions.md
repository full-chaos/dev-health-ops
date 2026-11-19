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

The daily job no longer writes `investment_metrics_daily`: the daily completion source is `work_item_completion_daily`, which has no investment area, and the canonical theme distribution is stored in `work_unit_investments`. The `investment_allocation_pct` source above is the table as the Ask Dev registry names it; the table was dropped by ClickHouse migration 120.

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

What the rate stands on. `/explain` serves the basis in
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
Do not read it for change failure rate. It is planned for removal.

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
they are. They are planned for rename or removal.

## TestOps P95 durations and queue time

`PIPELINE_DURATION_P95`, `TEST_SUITE_DURATION_P95` and `PIPELINE_QUEUE_TIME` are pooled over samples. They are never
a mean of the stored per-repository-day values of `testops_pipeline_metrics_daily` or `testops_test_metrics_daily`: a
95th percentile of a pool cannot be made from the 95th percentiles of its parts.

The samples of a repository-day are the ones the daily job stored with that day's newest row, so a tile, a report chart
and the stored row use one sample set. A run that is synced after the daily job ran counts after the next run of the daily
job for its day, together with the stored row. A day whose newest row was written before the samples were stored
(ClickHouse migration 161) uses the raw runs of that day. A repository-day with more than 100,000 samples of one kind stores none: it uses its raw rows, so for that day only a
row synced after the daily job ran counts at once and the chart can differ from the stored row until the next run.

| Measure | Value of a group | Sample |
| --- | --- | --- |
| `PIPELINE_DURATION_P95` | 95th percentile, linear between closest ranks | a run of `ci_pipeline_runs`: `duration_seconds` when not negative, else `finished_at - started_at` when not negative |
| `TEST_SUITE_DURATION_P95` | 95th percentile, linear between closest ranks | a suite of `test_suite_results`: `duration_seconds` when not negative |
| `PIPELINE_QUEUE_TIME` | mean of the queue samples: their total seconds divided by the number of runs with a queue sample | a run: `queue_seconds` when not negative, else `started_at - queued_at` when not negative |

The samples follow the daily job's own rules. A run belongs to the UTC day it started; a suite to the day it started,
else finished. Only the repository-days that the daily job computed are read, so every series covers the days of the
stored rows. A group with no sample is null, never 0.

- A team series pools the runs of the repositories the team owns at the request. A co-owned repository counts whole for
  each owner team.
- The organization series pools each run once, under the request's repository and team filters. It is never a mean of
  the team series.
- A week or month bucket and a breakdown over a window pool every run of the bucket or window.

A day with stored samples reads one daily row. A day without them reads the raw tables, which have no day key, so that
read scans the organization's run history. One statement reads at most `analytics.TestopsPooledMaxRowsToRead` rows
(5,000,000). Past it the answer is unavailable: the GraphQL error carries
code `UNAVAILABLE`, reason `testops_pooled_read_bound` and the measure, and the analytics answer is null. It is never a
partial value or a 0.

### AI report charts

The AI report charts of the six TestOps duration and queue metrics read the same pooled definition, from the same shared
source and expressions (`internal/queryapi/testopspooled`), so a report and a tile give one number for one window and
scope. They are never a mean of the stored per-repository-day values.

| Report metric | Value of a bucket | Sample |
| --- | --- | --- |
| `p95_duration_seconds` | 95th percentile | a run, as `PIPELINE_DURATION_P95` |
| `median_duration_seconds` | median: linear between the two middle samples of an even count | a run, as `PIPELINE_DURATION_P95` |
| `avg_queue_seconds` | mean of the queue samples | a run, as `PIPELINE_QUEUE_TIME` |
| `p95_queue_seconds` | 95th percentile | a run with a queue sample, as `PIPELINE_QUEUE_TIME` |
| `suite_duration_p95_seconds` | 95th percentile | a suite, as `TEST_SUITE_DURATION_P95` |
| `suite_duration_p50_seconds` | median | a suite, as `TEST_SUITE_DURATION_P95` |

`median_duration_seconds`, `p95_queue_seconds` and `suite_duration_p50_seconds` have no GraphQL measure: only the report
reads them. For these six metrics, a report team group or team filter reads the owners of each repository when the chart runs, a co-owned
repository whole for each owner. The chart range includes its last day. A bucket with no sample has no point, never 0.
A pooled chart read past the row bound above fails the report chart as unavailable.

The report charts of `success_count`, `failure_count`, `cancelled_count`, `success_rate`, `failure_rate`, `cancel_rate`
and `rerun_rate` read the stored pipeline rows of each repository-day. For these seven metrics, a team is the owner of
the repository when the report runs, for every day of the range. The stored `team_id` (the main owner at the end of the
day) is not read:

- A team filter keeps the repositories the filtered teams own. Two filtered teams that share a repository read it once.
- A team group gives one bucket per owner team. A co-owned repository counts whole in each owner's bucket, so the buckets
  can add up to more than the organization. A repository with no owner is in no team bucket.
- A team that owns no repository has a count of 0 and no rate point.
- A chart with no team filter and no team group is the organization, each repository once.
- A bucket of `cancel_rate` or `rerun_rate` with no row has no point, never NaN. This holds for the organization too.
- `success_rate` has no service dimension: a service group is one `unscoped` bucket.

The report charts of `passed_count`, `failed_count`, `skipped_count`, `quarantined_count`, `total_cases`, `total_suites`,
`pass_rate`, `flake_rate`, `retry_dependency_rate` and `failure_recurrence_score` read the stored test rows of each
repository-day. For these ten metrics, a team is the owner of the repository when the report runs, for every day of the
range. The stored `team_id` (the main owner at the end of the day) is not read:

- A team filter keeps the repositories the filtered teams own. Two filtered teams that share a repository read it once.
- A team group gives one bucket per owner team. A co-owned repository counts whole in each owner's bucket, so the buckets
  can add up to more than the organization. A repository with no owner is in no team bucket.
- A team that owns no repository has a count of 0 and no rate point.
- A chart with no team filter and no team group is the organization, each repository once, as before.
- A rate with no row in its bucket has no point, never NaN. This holds for the organization too: the three mean columns
  are nullable.
- A service group reads the stored service.

The report charts of the 32 per-person metrics of `user_metrics_daily` (one row per person, repository and day: for
example `commits_count`, `loc_added`, `prs_merged`, `reviews_given`, `pr_cycle_p90_hours`) take the team from the
repository of each row. A team is the owner of that repository when the report runs, for every day of the range. The
stored `team_id` (the person's first team membership, a label) is not read:

- A team filter keeps the rows of the repositories the filtered teams own, whoever the person is a member of. Two
  filtered teams that share a repository read it once.
- A team group gives one bucket per owner team. A co-owned repository counts whole in each owner's bucket, so the buckets
  can add up to more than the organization. A repository with no owner is in no team bucket, and `unassigned` is no
  bucket.
- A team that owns no repository: the four sum metrics (`commits_count`, `context_spread_count`, `large_commits_count`,
  `delivery_units`) are 0; the other metrics are a mean of the stored values and have no point.
- `work_items_completed` and `work_items_active` are on the person's row of no repository: work items belong to no
  repository. Under a team filter or a team group they are not applicable, with that reason, never 0. The work items of a
  team are in the work-item charts, by project ownership.
- `delivery_units` of a team counts the merged pull requests of the team's repositories only; the chart notes that its
  completed work items have no team value.
- A chart with no team filter and no team group is the organization, as before. A mean with no row in its bucket has no
  point, never NaN; this holds for the organization too.

### Benchmarking flake and retry dependency rates

The benchmarking point of `flake_rate` and `retry_dependency_rate` for a scope and a day is the sum of the numerators
over the sum of the denominators of the repository-days in the scope. It is not the mean of the stored rates, so a
repository-day weighs by its distinct cases. The denominator is the stored `distinct_cases`. The numerator of
`flake_rate` is the stored `flake_cases`. The numerator of `retry_dependency_rate` is the stored rate times
`distinct_cases`: a float, the same form as `rerun_rate` times `pipelines_count`, and not a stored count. A point whose
denominator sums to 0 is no point, never 0.

A repository-day is in neither sum when its rate is NULL (it measured no case), or its `distinct_cases` is NULL, or, for
`flake_rate`, its `flake_cases` is NULL. A row stored before the counts existed has NULL counts, so it drops out of the
point. A point made only of such rows has no value.

### AI report charts of CI/CD durations and queue time

The AI report charts of three CI/CD metrics are taken over the samples of the window's pipeline runs that the CI/CD daily
job stored with each repository-day's newest row (`cicd_metrics_daily`). A day whose newest row was written before the
samples were stored uses its raw runs (`ci_pipeline_runs`) with the same rules. A run that is synced after the daily job
ran counts after the next run of the daily job for its day. They are never a mean of the stored per-repository-day
values. No GraphQL measure reads them. A repository-day with more than 100,000 samples of one kind stores none: it uses its raw rows, so for that day only a
row synced after the daily job ran counts at once and the chart can differ from the stored row until the next run.

| Report metric | Value of a bucket | Sample |
| --- | --- | --- |
| `avg_duration_minutes` | mean | a run: finished at minus started at, in minutes |
| `p90_duration_minutes` | 90th percentile: linear between the closest ranks | a run, as above |
| `avg_queue_minutes` | mean | a run with a queued time: started at minus queued at, in minutes |

- A run counts on the UTC day it starts, and only when it also finishes on that day. A run that crosses midnight UTC
  counts on no day, and a run that has not finished counts nowhere. This is the daily job's rule.
- A re-run keeps its run id. The run counts once, with its latest attempt. Its queue sample is the time from the run's
  first creation to the latest attempt's start.
- A negative duration or queue time is no sample. The stored `duration_seconds` and `queue_seconds` are not read.
- Every status counts, except a deleted run.
- Only the repository-days that the daily job computed are read.
- The chart range includes its last day. A bucket with no sample has no point, never 0. The read has the same row bound
  as the TestOps pooled read.

The report chart of `pipelines_count` sums the stored count of each repository-day the daily job wrote.

For these four CI/CD metrics, a team is the owner of the repository when the report runs, for every day of the range:

- A team filter keeps the repositories the filtered teams own. Two filtered teams that share a repository read it once.
- A team group gives one bucket per owner team. A co-owned repository counts whole in each owner's bucket, so the buckets
  can add up to more than the organization. A repository with no owner is in no team bucket.
- A team that owns no repository has a pipelines count of 0 and no duration or queue point.
- A chart with no team filter and no team group is the organization, each repository once.
- These metrics have no service dimension: a service group is one `unscoped` bucket.

### AI report charts of deploy time, pull request size and MTTR

The AI report charts of six delivery metrics are taken over the samples that the daily job stored with each
repository-day's newest row. A day whose newest row was written before the samples were stored uses its raw rows with the
same rules. A row that is synced after the daily job ran counts after the next run of the daily job for its day. They are
never a mean of the stored per-repository-day values. A repository-day with more than 100,000 samples of one kind stores none: it uses its raw rows, so for that day only a
row synced after the daily job ran counts at once and the chart can differ from the stored row until the next run.

| Report metric | Value of a bucket | Sample |
| --- | --- | --- |
| `deploy_time_p50_hours` | median | a deployment: finished at minus started at, in hours |
| `pr_size_p50_loc` | median | a merged pull request: additions plus deletions |
| `pr_size_p90_loc` | 90th percentile | a merged pull request, as above |
| `mttr_hours` | mean | a bug work item: completed at minus started at, in hours; the operating review tile serves this pooled mean, or the pooled incident median when one exists, with the incident mappings that the daily job read when it ran |
| `mttr_p50_hours` | median | an incident: resolved at minus started at, in hours |
| `mttr_p90_hours` | 90th percentile | an incident, as above |

- A percentile is linear between the closest ranks, as the daily job computes it.
- A deployment counts on the UTC day it deployed. A cancelled deployment is not a deployment. A deployment without a
  start or a finish time, or with a negative duration, is no sample.
- A pull request counts on the UTC day it merged. A size of 0 is no sample.
- Deployments and pull requests are read as the daily job reads them: a stored version that is not merged away yet is
  a sample too.
- A bug work item counts on the UTC day it completed, when it has a start time and a completion time. A negative
  repair time is a sample, as the daily job's mean includes it.
- An incident counts on the UTC day it started, only when it is also resolved on that day, once for each repository
  that its service maps to. A deleted incident is no incident. A negative repair time is no sample. The service
  mappings are the ones that were active and valid when the daily job ran for that day. A mapping that changes later
  changes the chart and the stored row at the next run of the daily job. If that run finds no incident for the
  repository-day, it writes no row, so the old row and its samples stay (ticket 14148).
- Only the repository-days that the daily job computed are read.
- These metrics have no team or service dimension. A team filter does not narrow the chart, and a team or service group
  is one `unscoped` bucket.
- The chart range includes its last day. A bucket with no sample has no point, never 0. The read has the same row bound
  as the TestOps pooled read.

## TestOps cancel rate and rerun rate in report charts

A report chart of `cancel_rate` or `rerun_rate` is the sum of the numerator over the sum of the denominator of the
`testops_pipeline_metrics_daily` rows in its bucket. It is never a mean of the stored per-repository-day rates: a day of
1 run would weigh the same as a day of 500.

| Metric | Numerator | Denominator |
| --- | --- | --- |
| `cancel_rate` | `SUM(cancelled_count)` | `SUM(pipelines_count)` |
| `rerun_rate` | `SUM(rerun_rate * pipelines_count)` | `SUM(pipelines_count)` |

- The denominator is every run counted, the same as the stored rate. Cancelled, skipped and unfinished runs are in it.
- No column stores the rerun count. The stored `rerun_rate` is reruns over `pipelines_count`, so the product is the count.
- A bucket with no run has no point, never 0.

## TestOps release confidence and quality drag: flake and recurrence

The daily risk job (`testops_release_confidence`, `testops_quality_drag`) uses the day's test flake rate and failure
recurrence. A repository-day can have no test row. That has two causes, and the job tells them apart:

- **Not applicable.** The repository has no tests. Flake is a true 0: the score keeps its flake credit and quality drag
  keeps flake hours of 0.
- **Unknown.** The repository has CI runs that day and tests exist, but no test report was read for the day. Tests exist
  when the repository kept a test suite on a day of the 29 days before the day, or when a run that started on the day has a
  newest attempt that its report completeness row names partial (`complete = 0`).

An unknown day leaves the flake and recurrence factors out, as a day that measured no case does. `flake_rate` and
`failure_recurrence` are null in `factors_json`, the score gets no flake credit and no flake or recurrence penalty, and
`flake_investigation_hours` is NULL, so `drag_hours` is the sum of the known parts. Release confidence of such a day is 0.1
lower than the not-applicable reading.

Residual: a repository whose last test report is older than 29 days reads as not applicable.

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
- **Person, 30 days (landscape).** `ic_landscape_rolling_30d.cycle_p50_30d_hours`
  is the median of the person's daily medians over the 30 days ending on the
  point's day, read only from the days with a merged pull request. A day with no
  merged pull request is left out, not read as 0 hours. A person with no such day
  has no value (NULL) and no point on the cycle map.

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
repository ids until 2026-08-25 and nothing stored says which kind
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
--org <uuid> --from <day> --to <day> --reason <code> --correlation-id <id>` runs the same function for each day of a
range (the operator write is audited; `--dry-run` takes neither flag). It appends only when the table differs from Postgres, so a second run
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

## Workgraph build permanent cancels

`dev_health_workgraph_build_permanent_cancels_total` counts `workgraph.build`
requests that the build handler cancelled permanently after a step failure.
Label `error_class` is one of `context_canceled`, `deadline_exceeded`,
`dependency_unavailable` or `unclassified`. A transient step failure retries
and is not counted. The matching Warn log line carries the same class and the
root Go type, never the error text.

## Project ensure rows (Linear, Jira)

A Linear or Jira work-items unit names every project it saw by an ensure row in `projects`. A Linear row carries an epoch version, so a catalog row always replaces it. A Jira row carries the unit's time, so a unit run again for the same instant, as the windows of one first sync are, leaves the stored row alone. The unit writes an ensure row only when it can change the newest stored row. A stored row of a newer version, or of the same version with the same content, is left alone. A missing, older or changed row is written.

`dev_health_project_catalog_ensure_total{provider,outcome}` counts the rows:

- `written`: the row was written.
- `skipped`: the stored row already decides the newest value.
- `read_error`: the stored rows could not be read. The whole batch was written and a Warn line `project_catalog_ensure_read_failed` was logged. The unit does not fail.

## Completed work items

`items_completed` (and the other completion columns of `work_item_metrics_daily` and `work_item_user_metrics_daily`: unassigned completions, story points, bug ratio, cycle and lead time) counts an item on a day only when its status is terminal (`done` or `canceled`) and its `completed_at` is on that day. This is the rule of `work_item_cycle_times`. A provider can reopen an item and keep its `completed_at`: that open item completes on no day.

## Work items in progress at the end of a day

`wip_count_end_of_day` (and `wip_unassigned_end_of_day`, the WIP age percentiles, `wip_congestion_ratio`, `predictability_score` and the person rows' WIP) counts an item at the end of a day when it started before the day ends and has not ended by then. Its end is the earlier of `completed_at` and `closed_at`, and it counts only when the status is terminal (`done` or `canceled`). A provider can reopen an item and keep its `completed_at` or `closed_at`: that open item stays in progress at the end of every later day.

## Work items stored under more than one repository

One work item (provider and id) can be stored under two repository ids. Example: a GitHub issue that the repository sync stores under its repository and that the Projects v2 board sync stores under no repository. Each stored row is a copy. The current copy is the copy with the newest `last_synced`; on a tie, the copy of the lower repository id.

`work_item_cycle_times`, `work_item_metrics_daily` and `work_item_user_metrics_daily` decide on the current copy. An item counts on a day only when its current copy belongs to that day: created before the day ends, and not done before the day starts. An older copy never makes the item count. So an item whose current copy is done with no completion time counts on no day, and an item completes once, on its current copy's completion day. The request-time read of the work items linked to a repository (Home and Quadrant under a repository filter) decides on the same current copy.

The scope view (`row_view = 'scope'`) applies the same rule inside each work scope: the current copy of the item in that scope. An item with copies in two scopes still counts in each scope (the sum over scopes can be more than the organization total). So the older copy's scope can count a completion that the organization view and `work_item_cycle_times` do not count.

Not yet on this rule: `work_item_state_durations_daily` and `estimate_coverage_metrics_daily`. These still choose, for each day, the newest copy among the copies that belong to that day. For an item with two copies, they can differ from the tables above.

## Work-item person rows: persons in more than one team

`work_item_user_metrics_daily` keeps one row for each person, work scope and day: its key has no team. A person whose work items of one day belong to more than one team gets one row that sums all the person's items and takes its cycle percentiles over all of them. The team columns of that row name the team with the most of the person's items that day (the lowest team id on a tie).

The counter `dev_health_work_item_user_rows_spanning_teams_total{provider}` counts, for each computed partition, the persons whose row is labelled this way. A rise says that people work across teams in that provider; it is not a fault. The Debug line `work_item_user_metrics_rows` carries the same count with the number of user rows of the partition.

## Person rows: the team column of `user_metrics_daily`

`user_metrics_daily.team_id` is a membership label, NOT team attribution; ownership-based team answers must not read it. The row the finalize step writes for a person holds the person's first team membership on the day, else `unassigned`: the git row is always written with `unassigned`, and the IC finalize step puts the membership team over it. Limit: a person who is a member of several teams gets only the FIRST of them, in the rank order of the attribution (primary first, then the more specific, then the lower priority number, the newer row, the team id); the other teams are not in the column. The rank applies after the teams whose newest row is inactive are left out: the next active membership is taken, else `unassigned`. This is the rule of a live row the finalize step writes; a retraction row (the row of zeros of the stale-key rule) carries the `team_id` stored on the row it retracts. The column is a label, never team attribution. A team answer takes its team from repository or project ownership (for a per-person table, through `repo_id` and the owner shares), never from this column. The AI analytics reviewer concentration (`aiReviewLoad`) and the cognitive load reader follow this rule: a team scope is the repositories the team owns (`teamscope.HeldRepoIDs`), and the census `TestNoAIAnalyticsOrCognitiveLoadReaderScopesUserMetricsDailyByTeamID` (`internal/queryapi/aianalytics`) fails on a read of `user_metrics_daily` in those packages that filters `team_id`. The recommendations review-concentration rule follows it too: its reviewer Gini reads the reviews given in the repositories the team owns at the window end (`teamownership.OwnedRepoIDs`, the same repositories as its review latency), by anyone, and a co-owned repository counts whole in each owner team. A team that owns no repository has no Gini (no read is made), so the rule does not fire for it. The census `TestNoRecommendationsReaderScopesUserMetricsDailyByTeamID` (`internal/jobs/metrics/remaining`) fails on a read of `user_metrics_daily` in that package that filters `team_id`.

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
