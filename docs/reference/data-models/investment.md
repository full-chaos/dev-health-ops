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
  - src/dev_health_ops/api/queries/investment.py
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
interval at the materialization timestamp and joins to this organization's
current repository catalog. Name-only ownership resolves by provider and
case-insensitive full repository name. Missing, expired, future, manual,
foreign-organization, and unmatched repository records cannot supply a share.
A failed donor query fails materialization before new output is written.

The distinct union of eligible repository IDs receives equal shares, `1/N`.
Multiple teams owning the same repository and repeated ownership generations do
not multiply effort. Ranking fields (`is_primary`, `specificity`, `priority`)
are not effort weights. This is an allocation convention, not measured churn.
Each repository row carries `allocation_source=team_ownership` and
`repo_source=team:<sorted contributing team IDs>`. The unit's scalar `repo_id`
stays null; no single repository is selected as primary.

Both zero-effort and positive-effort units keep their total effort. Allocation
weights sum to one; allocated effort sums to the unit's effort. Units without
eligible ownership keep their existing unassigned allocation and stay in the
denominator. Repeated materialization writes a new allocation generation, and
readers select only the latest generation for each unit. Categorization and
work-unit identities do not change.

### Precedence, in order

The allocator checks these in this order and stops at the first that applies.
The order is the contract; the tests assert each step by name.

| Order | Condition | Result |
| --- | --- | --- |
| 1 | The unit already resolved a single repository of its own, by edges or by hierarchy inheritance | `own_repo` -- fallback does not run |
| 2 | An existing effort row carries an allocation source other than empty or active-hours-unassigned | `stronger_allocation` -- churn allocation keeps precedence |
| 3 | The unit's edges, PRs or commits name any real repository, even ambiguously or at zero churn | `direct_repo_evidence` -- ambiguity is not erased by a team convention |
| 4 | No member issue has eligible ownership evidence | `no_eligible_owner` -- the unit keeps its unassigned allocation |
| 5 | Otherwise | `allocated` -- equal `1/N` shares over the distinct union |

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
run, alongside `repo_shares`, `donor_rows`, `donor_issues` and the
`ownership_as_of` timestamp the intervals were evaluated at. Every field is
written on every run, including at zero, so that "nothing to allocate" can be
told apart from "the counter was never computed". Field-by-field meaning:
[CLI reference](../cli/index.md#investment).

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

"No category at all" means out of scope under a category filter: a work unit whose subcategory distribution is empty is excluded, as it always was.

Note that the Python implementation still carries the old behaviour, so the two API planes can disagree on filtered coverage. The Go plane is the source of truth for these figures.

What the split guarantees, and has guaranteed throughout: direct plus team-fallback always equals the headline `repoCoverage` numerator exactly.

The third field is the one to read first when the first two look good. The
fallback allocates `1/N` across every repository a team owns, so one unit with a
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

## Deprecated: `investment_metrics_daily` / `investment_areas.yaml`

`investment_metrics_daily` and its feeder rule set `src/dev_health_ops/config/investment_areas.yaml`
are a **pre-WorkUnit legacy path** (`src/dev_health_ops/analytics/investment.py`). The YAML file's
own header states this: "feeds only the pre-WorkUnit daily `investment_*` metric tables... do not
use this file for canonical WorkUnit categorization." Its `investment_area` values (e.g.
`security`, `infrastructure`) are free-form legacy labels — **not** the fixed five-theme taxonomy
above, and not interchangeable with it.

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
