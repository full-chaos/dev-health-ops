---
page_id: ref-team-counts-owner-share
summary: What a team's commit, after-hours, weekend and other count numbers mean when several teams own one repository, how they are computed, and why.
content_type: reference
owner: product-analytics
source_of_truth:
  - docs/contribute/architecture/team-attribution.md
applicability: current
lifecycle: active
---

# Team counts at the owner's share

A team's daily count numbers are taken **at the team's share of each repository it owns**. A count can therefore be a fraction. This is expected, and it is not a rounding error or missing data.

## What the number is

These team numbers are counts: commits, after-hours commits, weekend commits, lines of code and function counts of the code the team owns, the review and interruption load counts, the context-spread count, the pooled pull request counts of the risk view, and the pull request counts of the AI impact views.

For each one, the team reads **its own part** of the repository's count. A team that owns a repository alone reads the whole count. A team that shares a repository reads a part of it, so a sample team can show 5.85 commits for a day. The teams that own one repository together read it exactly once between them.

Two things are never counted at a share:

- **Ratios and averages.** The after-hours share of commits, the weekend share of commits, cycle time and review time are recomputed or kept as the repository's own value. They are not scaled down.
- **Counts of repositories and authors.** These stay whole numbers.

## What one number covers

A team count in a Context Fabric answer is **one day**: the latest day of the window, shown in the fact's `day` field. It is not an average over the window and not a total for the window. To see more days, read the daily series.

## How it is computed

1. Each repository has one value for the day, for example its commits.
2. Each owning team has a **share** of that repository, a number from 0 to 1. The shares of all owners of one repository add up to 1.
3. The team's number for the day is the sum, over the repositories it owns, of the repository's value for that day times the team's share. For example, commits for the day are the sum of each owned repository's commits times the team's share of it.
4. Ratios are then recomputed from those summed counts. For example, the after-hours ratio is the team's after-hours commits divided by the team's commits, both taken at the share. A ratio is never an average of the owners' ratios.

Nothing is rounded, capped or cut off by a threshold. A small share gives a small fraction.

### How the share is set

The share comes from the work the team has linked to the repository: the team's issue-to-pull-request links over all links of the repository, using every link level. The team with the largest linked share is the **main owner** and stays the main owner for the whole history. Every other owner keeps its own, smaller share. Where any owner of a repository has no measured share, the owners of that repository split it equally. See [Investment data model](../data-models/investment.md) for how the same share weighs effort.

## Why

A repository that several teams own must not be counted at full weight for each of them. If it were, the organization total would count the same commit once per owner, and a busy shared repository would make every owning team look as busy as the one team that does most of the work.

The rule is: **one main owner, lesser owners keep their own share.** It is not an equal split. A team with 70 percent of the linked work reads 70 percent of the repository, and a team with 30 percent reads 30 percent. Together they read the repository once.

## How to read it

- Compare a team with itself over time, or with another team on the same basis. Both read at their own share.
- A fraction means a shared repository. It does not mean a partial commit.
- To see the whole repository, open the repository view. It reads the full count.
- A team view shows the teams' parts; they add up to the repository, not above it.

## Related

- [Weighting and aggregation](weighting-and-aggregation.md)
- [Canonical metric definitions](definitions.md)
- [Teams and repositories](../../admin/teams-and-identities/teams-and-repositories.md)
- [Scope and time](../../use/navigate/scope-and-time.md)
- Rule of record: [Team attribution](../../contribute/architecture/team-attribution.md), the section on owner shares.
