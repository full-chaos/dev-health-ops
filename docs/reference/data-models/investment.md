---
page_id: ref-investment-model
summary: Latest work-unit Investment record, effort value, distributions, evidence, quality, and materialization concepts.
content_type: reference
owner: platform-api
source_of_truth:
  - internal/jobs/investment/teamownership.go
  - internal/jobs/investment/chquery/teamownership.go
  - internal/jobs/investment/hierarchycascade.go
  - internal/jobs/investment/materialize.go
  - src/dev_health_ops/investment_taxonomy.py
  - src/dev_health_ops/work_graph/investment/
  - internal/queryapi/investment/reader.go
applicability: current
lifecycle: active
---

# Investment data model

An Investment work-unit record contains a tenant-scoped work-unit identity, interval, repository or allocation context, effort metric and value, theme and subcategory distributions, structural evidence, evidence quality, categorization status, model/run provenance, and computation time.

The request path uses the latest materialized row for each organization and work unit. Multi-repository allocation can distribute a unit's effort across repositories while preserving the total effort invariant.

Theme and subcategory distributions are probabilistic contributions, not duplicated labels. See [Investment taxonomy](../taxonomies/investment.md) and [Weighting and aggregation](../metrics/weighting-and-aggregation.md).

## Repository inheritance

The native materializer first uses a work unit's own repository evidence. A
single repository on its graph edges, or a single repository from its PR or
commit churn allocations, can also supply repository evidence to related units.
Churn allocations that name multiple repositories or contain a missing repository
cannot supply a single-repository inheritance result.

For a unit without its own repository result, the materializer checks ancestors
before direct children. The resolved relatives in a tier must agree on one
repository. Each pass reads the same prior results and publishes its new results
together. Further passes can inherit from units resolved by earlier passes. A
resolved unit keeps its result. The cascade stops when a pass adds no result or
after ten passes; each parent-chain walk is also cycle-safe and bounded to ten
levels. Units without a valid result remain unassigned.

Inheritance preserves work-unit identities, component boundaries, effort, and
categorization. Persisted `repo_source` is `own_edges`, `ancestor:<issue_id>`, or
`children`, as applicable. The ancestor form names the original issue whose own
repository evidence supplied the result, including when inheritance crosses
multiple components. Inherited effort rows use
`allocation_source=hierarchy_cascade`; existing churn allocation keeps precedence.
This repository inheritance does not change the
[work-item team attribution rules](../../contribute/architecture/team-attribution.md).

## Team ownership fallback

After direct repository and hierarchy evidence, unresolved units can use the
latest persisted primary team attribution of their member issues. Eligible
sources are `native_team`, `issue_project`, `project_ownership`, and
`repo_ownership`. Membership and manual sources are excluded. `linked_issue`
is also excluded from this fallback because its current persisted evidence does
not identify the donor's source; that donor can itself come from membership.
Existing linked PR and hierarchy behavior keeps its precedence.

Each active donor team contributes all its live, sync-derived
`team_repo_ownership` records. The reader accepts `native`, `jira_legacy`,
`provider_access`, and `inferred` ownership sources. It validates the ownership
interval at the END of the work unit's own span (`to_ts`), never later than the
materialization timestamp, and joins to this organization's current repository
catalog. A unit that ended before a team's first ownership row of a repository gets
no share of that repository on main today; the rule of record (chris 2026-10-12, D5987
option C) is that the repository's first recorded owners apply backward, tracked in
CHAOS-9404. A later change of a team's repositories does not move the effort of a unit that
ended before the change. One run holds units of many end times, so the reader
loads the donor teams of the issues, and the ownership rows of those donor teams
that began at or before the materialization timestamp, once; each unit selects
the rows valid at its own end time. The ownership rows of a team that is no
donor of the run's issues are not read. Name-only ownership resolves by provider and
case-insensitive full repository name. Missing, expired, future, manual,
foreign-organization, and unmatched repository records cannot supply a share.
A failed donor query fails materialization before new output is written.

Each repository of the distinct union of eligible repository IDs is weighted by
its linked share (chris 2026-10-12, D5988, from D5432): the donor teams'
`team_repo_ownership.share`, the team's issue-to-PR links of every tier over all
links of the repository, summed over the unit's donor teams and renormalised
over the unit's repositories so the weights sum to one. A unit never splits
equally because it spans several repositories. Known limit, the same as the
owner shares of the team readers: `share` NULL means not measured (a
`provider_access`, native or manual row), which is not zero. When any donor row
of the unit has no measured share, or the measured shares sum to zero, no weight
is invented and the unit splits equally, `1/N`. Multiple teams owning the same
repository and repeated ownership generations do not multiply effort (a team's
share of a repository is the largest share of its rows that hold). Ranking
fields (`is_primary`, `specificity`, `priority`) are not effort weights. This is
an allocation convention, not measured churn.
Each repository row carries `allocation_source=team_ownership` and
`repo_source=team:<sorted contributing team IDs>`. The unit's scalar `repo_id`
stays null; no single repository is selected as primary.

Both zero-effort and positive-effort units keep their total effort. Allocation
weights sum to one; allocated effort sums to the unit's effort. Units without
eligible ownership keep their existing unassigned allocation and stay in the
denominator. Repeated materialization writes a new allocation generation, and
readers select only the latest generation for each unit. Categorization and
work-unit identities do not change.

### Read-time allocation weight

Scoped readers (a team or repository scope, and the Investment flow) read one
row per unit and repository and weight each row by its share of the unit: the
repository's effort over the sum of the unit's repository efforts. A unit whose
repository efforts sum to 0 (a unit of effort 0 is the common case) splits
`1/n` over its `n` repository rows. Every unit therefore sums to exactly one
across its rows and is counted once, whatever its stored unit effort, so the
flow total equals the unit count of the same window and scope. A unit with no
repository row keeps weight 1.

### Precedence, in order

The allocator checks these in this order and stops at the first that applies.
The order is the contract; the tests assert each step by name.

| Order | Condition | Result |
| --- | --- | --- |
| 1 | The unit already resolved a single repository of its own, by edges or by hierarchy inheritance | `own_repo` -- fallback does not run |
| 2 | An existing effort row carries an allocation source other than empty or active-hours-unassigned | `stronger_allocation` -- churn allocation keeps precedence |
| 3 | The unit's edges, PRs or commits name any real repository, even ambiguously or at zero churn | `direct_repo_evidence` -- ambiguity is not erased by a team convention |
| 4 | No member issue has eligible ownership evidence | `no_eligible_owner` -- the unit keeps its unassigned allocation |
| 5 | Otherwise | `allocated` -- linked-share weights over the distinct union (equal `1/N` only when a share is not measured) |

The zero UUID is not a repository at any step: a PR or commit node carrying it
is not direct evidence, and an ownership record resolving to it is not a share.

### Provider coverage

This fallback is provider-agnostic and is tested across the full
`{jira, gitlab, github, linear}` x `{teams, projects, members, issues}` matrix
required by the repository's
[team attribution coverage contract](../../contribute/architecture/team-attribution.md).
Never Linear-only: jira/github/gitlab work items carry no native team key, so
their attribution rides entirely on the auto-imported team, project and member
dimension.

| Entity | How it participates |
| --- | --- |
| issues | The member issues' latest primary attribution is the donor. All four eligible sources (`native_team`, `issue_project`, `project_ownership`, `repo_ownership`) are exercised, one per provider. |
| teams | Only an `is_active` team donates. Several teams owning the same repository produce one share, and the provenance names all of them. |
| projects | Project ownership reaches this fallback as sync-derived `inferred` repository ownership. GitHub has no native project entity -- the repository is the scope -- so its cell is n/a. |
| members | **Negative by contract.** `assignee_membership` and `author_membership` attributions and `team_memberships` rows never donate, even when the team owns live repositories. Team authorization is ownership-derived, never person to membership to team. |

The work-tracking provider and the code-host provider are separate axes: a Jira
team legitimately owns GitHub repositories, and the ownership-to-repository join
matches on the CODE HOST's provider, never the tracker's.

### Telemetry

Each run emits one `investment team repository fallback` record whose
`allocated`, `own_repo`, `stronger_allocation`, `direct_repo_evidence`,
`no_eligible_owner` and `window_skipped` counts partition every component in the
run, alongside `repo_shares`, `donor_rows`, `donor_issues`, the `ownership_as_of`
timestamp that bounds the ownership rows the run read, and
`ownership_as_of_first_unit` and `ownership_as_of_last_unit`, the earliest and the
latest unit end time an interval was evaluated at. Every field is
written on every run, including at zero, so that "nothing to allocate" can be
told apart from "the counter was never computed". Field-by-field meaning:
[CLI reference](../cli/index.md#investment).

## Measure: work-unit count or issue size

Investment measures effort, and effort is the size of the issues a unit worked on (D5999). Lines of code are never an
Investment measure. Two measures read the same rows:

| Measure | What a unit weighs |
| --- | --- |
| `COUNT` (and `THROUGHPUT`, the same) | 1 work unit |
| `ISSUE_SIZE` | The unit's newest issue size (`work_unit_issue_size`): the sum of the story points of its issues. A unit with no size weighs 1 work unit, so it is never dropped from the mix. |

`ISSUE_SIZE` is served only where its sizing coverage rides in the same answer (D6027). It is the default of the
REST `/investment` response, which then carries `sizing_coverage`, `unsized_unit_count` and, when fewer than half of
the units carry a size, `sizing_note`, and of the explanation, which carries the same three fields on each finding's
evidence (CHAOS-9809). The sunburst, the Home tile "Investment by theme" and the Operating Review investment section
carry no sizing coverage yet and stay on `COUNT` until they do.

In both, a unit's weight is spread over its subcategories by their probabilities and over its repositories by their
allocation weights, and a team scope takes each repository at the share the teams in view hold, so the sum of a
unit's rows is its weight. When no unit of a scope carries a size, `ISSUE_SIZE` equals `COUNT` exactly. Provider
scales are not normalized: a Linear estimate, Jira story points and a GitLab weight add as stored.

`SankeyCoverage.sizingCoverage` is the share of the window's work units that carry a size and
`SankeyCoverage.unsizedUnitCount` the units that do not: read them before reading an `ISSUE_SIZE` mix. When less
than half of the units carry a size, the mix is mostly unsized: "Mostly unsized: n of m work units have an issue size.
The other u have no size, so each counts as 1 work unit." Both are counted for every measure on the work-unit path,
and are null where they are not measured (the non-investment path, or a window with no unit). A Sankey read in
`ISSUE_SIZE` names its unit `ISSUE_SIZE`; every other Investment Sankey names `WORK_UNITS`. Under `ISSUE_SIZE` every
weighted coverage share (`teamCoverage`, `repoCoverage` and its split) is weighed the same way as the chart.

## Team coverage: ownership, not the attribution vote

`teamCoverage` is the share of effort whose work unit has a team, and a unit has a team when a team **owns** one of
the unit's repositories (the same ownership rule the team scope uses). It no longer follows the work-item attribution
vote, so a unit in an owned repository counts as assigned even when no linked work item names a team, and a unit in an
unowned repository counts as unassigned even when a work item names one. A unit counts once, however many owning teams
it reaches. The denominator is unchanged. The TEAM grouping of the Investment flow, breakdown, timeseries and the flow matrix reads the same ownership
(CHAOS-9297): each effort row takes the owner of its repository, a co-owned repository is shared among its
owners by their linked share (the main owner by its share, each lesser owner by its own lesser share; equally only on a tie or when no share is measured), and a repository with no owner is `unassigned`, so the unassigned share of the flow is `1 - teamCoverage`
except for a unit that spans an owned and an unowned repository (the coverage assigns it whole, the flow splits its
effort). Only the work-unit team list still shows the vote.

## Repository coverage: two numbers, not one

Repository coverage is the share of attributed effort that resolves to a
repository. Since the team-ownership fallback above exists, that single share
answers two different questions at once, and the API reports them separately.

`repoCoverage` is unchanged: all effort that reached any repository, over all
effort in the window. Beside it:

| Field | Question it answers | Rows counted |
| --- | --- | --- |
| `directRepoCoverage` | Do we know which repository this work actually touched? | Everything whose `repo_source` is not `team:...` -- direct edges, `own_edges`, `ancestor:<issue_id>`, `children`, and rows with no recorded provenance |
| `teamFallbackRepoCoverage` | Or do we only know a team that owns some repositories? | Rows whose `repo_source` starts with `team:` |
| `repoFanoutReposPerUnit` | How wide is that fallback? | Distinct `(work unit, repository)` pairs allocated by the fallback, divided by the distinct units that produced them |

The two shares use the same denominator as `repoCoverage` and add up to it
exactly: they split the same rows on complementary conditions, so no effort is
counted twice and none is lost. A row that reached a repository with no recorded
`repo_source` counts as direct, because the question direct coverage answers is
"is a specific repository known", and for that row one is.

Both counts are of DISTINCT keys -- `(work unit, repository)` pairs on top, work units below -- never of query rows. Two things make a row count unusable here. A filtered view of the same window joins more rows per unit than an unfiltered one, so a row-based width would report a wider fallback for the same data simply because a filter was applied. And the underlying table is a ReplacingMergeTree, so a raw row count also moves with background merge state: 5310, then 10626, then 3612 rows were observed for unchanged logical content, the last two twenty minutes apart. Any census of that table should count units or distinct keys, never rows.

**A work-category filter selects work units; it does not weight them.** Filtering to a category changes which units are counted and nothing else, so a unit that matches the filter in two ways counts exactly as much as one that matches in a single way. This was not always true: until CHAOS-5498 the filter re-weighted units by their number of matching subcategories, which moved `repoCoverage` and `teamCoverage` as well as the split -- measured at 0.5 unfiltered against 0.333 filtered on identical data. Coverage figures taken from a filtered view before that fix are not comparable with ones taken after it.

A work unit is in scope for a category when it carries POSITIVE weight in that category, not merely a subcategory entry for it. Distributions are dense -- every unit carries an entry for every subcategory, weight zero where it does not apply -- so selecting on the presence of an entry would put every unit in scope and the filter would never remove anything. On the reference organization, all units carry a `feature_delivery` entry while about 71 percent carry positive `feature_delivery` weight.

The Go plane is the source of truth for these figures.

What the split guarantees, and has guaranteed throughout: direct plus team-fallback always equals the headline `repoCoverage` numerator exactly.

The third field is the one to read first when the first two look good. The
fallback allocates across every repository a team owns (by linked share, equally only when no share is measured), so one unit with a
team that owns nine repositories produces nine rows. On the reference
organization, repository coverage moved from 53.1/57.5/67.2 percent (7/30/90
days) to 100.0/100.0/99.5 percent when the fallback shipped, at a fan-out of
about nine rows per unit. Nothing became more precisely attributed. A high
`teamFallbackRepoCoverage` beside a large `repoFanoutReposPerUnit` means the
coverage is broad, not that it is specific -- read it as a prompt to improve
direct evidence, never as a sign that attribution is solved.

Both shares are null, not zero, when a query cannot measure them. Only the
work-unit investment path carries repository provenance; the deprecated daily
path below has no such column, so it reports null there. Null means "not
measured here"; zero would mean "measured, and none of it is team fallback".

## Explain request fields

`POST /api/v1/investment/explain` explains what the Investment page shows: every share, and the sum behind it,
equals what `InvestmentBreakdown` reads for the same request (CHAOS-9555). The request body takes the page's own fields
next to `filters`:

| Field | Meaning | Default |
|---|---|---|
| `measure` | `ISSUE_SIZE` (the page's measure: each unit weighs its issue size, 1 when unsized), `COUNT` or `THROUGHPUT` (same as `COUNT`); any other name, `CHURN_LOC` included, is refused. The explanation never weighs by a unit's `effort_value` | `ISSUE_SIZE` |
| `date_range` | `{"start_date", "end_date"}`, bound as the page binds `dateRange`: the window is `[start_date, end_date)`. It replaces `filters.time`. | `filters.time` (a relative `range_days` ends tomorrow, so it covers today) |
| `use_investment` | Accepted for parity with the page; no effect, because theme and subcategory always use the canonical categorization on both surfaces | none |

`filters.scope.level` keeps its lowercase values. Scope `org` and scope `team` with no ids cover the same rows, as on the page.

Each finding's `evidence` carries `measure`, `numerator` and `denominator`: `share_pct` is `numerator / denominator * 100`, where
the numerator is the finding's subcategory (or theme) sum and the denominator is the sum of all themes in the window. Under `ISSUE_SIZE` each finding's `evidence` also carries `sizing_coverage` (the share of the mix's work units that carry an issue size), `unsized_unit_count` (the units that count 1 because they have none) and, when fewer than half of the units carry a size, `sizing_note` (the "Mostly unsized" sentence), read live on every answer, a cached one too. The text of a
finding names what the share counts ("of work").

## Deprecated: `investment_metrics_daily` / `investment_areas.yaml`

> **Removed (CHAOS-9200, chris 2026-10-11).** The rule classifier, `investment_areas.yaml` and the Go and Python
> code that loaded it are deleted: no rule-based investment classification is computed or stored. The daily
> family `work_item_investment` writes `work_item_completion_daily` (see above); `investment_metrics_daily` and
> `investment_classifications_daily` were dropped by ClickHouse migration 120 (CHAOS-9302): the tables and their rows
> are gone. The migrations that created them stay in the chain. The paragraphs below describe how those tables were written and read BEFORE this change (the
> daily writer and the newest-row-per-key read rule): history for reading old rows, not current behaviour.

`investment_metrics_daily` and its feeder rule set `src/dev_health_ops/config/investment_areas.yaml`
are a **pre-WorkUnit legacy path** (`src/dev_health_ops/analytics/investment.py`). The YAML file's
own header states this: "feeds only the pre-WorkUnit daily `investment_*` metric tables... do not
use this file for canonical WorkUnit categorization." Its `investment_area` values (e.g.
`security`, `infrastructure`) are free-form legacy labels — **not** the fixed five-theme taxonomy
above, and not interchangeable with it.

**Who writes the legacy daily tables, and how to read them.** `investment_metrics_daily`,
`investment_classifications_daily` and `issue_type_metrics_daily` are plain (append-only)
`MergeTree` tables. The daily metric job computes them from stored work items: the families
`work_item_investment` and `work_item_issue_type` (`internal/jobs/metrics/daily`), with the
compute in `internal/jobs/metrics/workitemengine`. The daily job is their one writer: a work-items
sync unit writes raw rows only, and its sink refuses a row for these tables. After a sync, the
post-sync fan-out starts the daily run of each day the sync touched, so between a sync and that
run a reader sees the last daily result of the day (complete, some minutes old), or no row for a
day that has none yet. Rows that a sync unit of an earlier version appended were computed from
only the items that unit fetched; the next run of the daily job for that day supersedes them.
Nothing is ever replaced in place, so a reader **must** take the newest
`computed_at` per key (`argMax(..., computed_at)` grouped by the key, as the readers in
`internal/queryapi` do) and must never sum the raw rows. When a completion leaves a key (the item
was reopened, or its team or labels changed), the daily job writes a row of zeros for that key so
that the newest row is the true one. A day with no data gets no row.

`issue_type_metrics_daily.lead_p50_hours` is the upper median of `completed_at - created_at`, in hours, over the items
completed on the day (the same bucketing as the cycle p50). An item with no `created_at`, or created after its
completion, is left out of the median. A key with no such item holds 0, which then means "no lead time", the same
as an empty cycle p50. Rows written before this rule hold 0 for every key until the day is recomputed.

**`work_item_completion_daily` replaces `investment_metrics_daily` as the daily completion source (CHAOS-9264).** The
analytics measures that count or time completed work by team, repository or work type (count, throughput,
cycle time) read `work_item_completion_daily`: one row per (organization, repository, team, day) with the delivery
units, the completed items and one cycle p50 for the whole key. It has no `investment_area` or `project_stream`
column; the rule classifier that produced them is no longer run by the daily family. The family keeps its name
`work_item_investment`. Counts and delivery units equal the old rows summed over the areas; the cycle p50 is the p50 of
all the key's cycle hours, not a mean of per-area p50s, so a cycle time can differ from the older number. The old table
gets no new rows and no reader. A work-category filter on the daily path (`useInvestment` false) is refused: the
categories are the canonical themes, which only the investment path reads.

The canonical theme/subcategory distribution comes from `work_unit_investments`
(`theme_distribution_json`/`subcategory_distribution_json`, computed once at categorization time,
deterministic roll-up, never recomputed at read time). `latest_work_unit_investments` is **not** a
persisted table — it is a query-time CTE (`LATEST_WORK_UNIT_INVESTMENTS_CTE`,
`api/queries/investment.py`) that dedups `work_unit_investments` to one row per
`(org_id, work_unit_id)` via `argMax(..., computed_at)`, org-scoped. Team-scoping joins that CTE to
`work_item_team_attributions` through the shared `build_unit_team_subquery` helper (also in
`api/queries/investment.py`) — a `PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE` subquery selecting
`is_primary = 1`, latest `computed_at` per work item, which already carries ownership precedence
(CHAOS-2600). `fetch_investment_team_edges` is the reference caller of this join shape; there is no
`fetch_investment_breakdown(include_team_id=...)` parameter — that phrasing describes the pattern,
not a real function signature.

**`work_unit_investments.evidence_quote_count` (migration 106, `Nullable(UInt32)`)** is the number of distinct
`(source_id, quote)` the row's run wrote to `work_unit_investment_quotes`. It is written by the materializer
(`internal/jobs/investment`) for every row of a run that persists evidence snippets, and read only by the
materializer's skip-existing check: a unit whose row is otherwise fresh (same unit, input hash and model version) is
rewritten when the quotes visible under the row's `categorization_run_id` are not that many. The quote key has no run
id, so a later run's equal-text quote replaces the older run's after a ClickHouse merge; if that run dies before it
writes its investment row, the row the readers show would keep fewer or no quotes for good without this check.
`NULL` means "not recorded" (rows written before migration 106, or a run that did not persist snippets) and counts as
complete. No reader selects it, no read model joins it, and the recorded Python answers compare other columns only
(`internal/jobs/investment/materialize_frozen_python_golden_test.go` pins the outcome, rollup and gate decisions, not a
whole row). See the operator runbook note "Quotes of a crashed rewrite".

**`work_unit_issue_size` (migration 130, CHAOS-9619)** holds the issue size of each work unit: Investment measures
effort, and effort is the size of the issues a unit worked on (D5999); lines of code are never a size. One row per
unit and materialization run: `issue_size` is the sum of the positive `work_items.story_points` of the unit's distinct
issues, `sized_issue_count` how many of them carry one and `issue_count` how many issues the unit has. Every provider
writes its size to that one column (Jira story points from the configured field, GitLab weight, Linear estimate, GitHub
Projects number field). A size that is 0, negative or not finite is no size. `issue_size` 0 means the unit carries no
size, and a reader then counts it 1 work unit, so a scope with no size at all reads the work-unit count. The row is
written for every unit of every run, also for a unit whose investment row is skipped as unchanged, so a size set later
in the provider reaches the next run. Readers take the newest row per `(org_id, work_unit_id)` with `argMax` over
`computed_at`; a unit with no row is unsized. The operator rollback is `internal/chmigrate/rollback/130_*.down.sql`.

**Consumers outside this repo must read the canonical join, never `investment_metrics_daily`.**
CHAOS-4398 found that `dev-health-acr`'s `FactInvestment` producer (CHAOS-4363/#308) reads
`investment_metrics_daily` and therefore surfaces the deprecated legacy taxonomy, not the
canonical one — the same class of gap CHAOS-4347 named for repository status and CHAOS-4365 named
for cognitive load: a real, existing, deterministic metric with no producer reading the correct
source. A new acr producer reading `latest_work_unit_investments` LEFT JOIN
`work_item_team_attributions` (via `build_unit_team_subquery`, as `fetch_investment_team_edges`
does) is required before any team-scoped investment-mix consumer (e.g. cohort ranking) can trust
its numbers. There is no stable, exported query API for this join today outside
`api/queries/investment.py` itself — a new acr producer needs either a new typed query function
there or an equivalent join built the same way, not a guess at table access.
