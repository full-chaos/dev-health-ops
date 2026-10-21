---
page_id: ref-weighting
summary: Exact Investment contribution, aggregation, and displayed-share contract.
content_type: reference
owner: product-analytics
source_of_truth:
  - internal/queryapi/investment/reader.go
  - internal/queryapi/investment/response.go
applicability: current
lifecycle: active
---

# Weighting and aggregation

## Contract

For each latest materialized work-unit row whose end falls in the selected period, the Investment breakdown expands the subcategory distribution and aggregates:

```text
subcategory contribution = Σ(subcategory probability × effort value)
theme contribution       = Σ(subcategory contributions whose key has that theme prefix)
displayed share          = contribution ÷ Σ(positive theme contributions)
```

The query groups by subcategory and theme and orders positive contributions from largest to smallest. The service omits non-positive values from the returned theme and subcategory maps. The web chart computes percentages against the total positive theme value in the current response.

## Team counts

Team count numbers (commits, after-hours and weekend commits, and the other team counts) are not Investment contributions. They are taken at each owner's share of a repository, so a co-owning team can show a fraction. See [Team counts at the owner's share](team-counts-at-owner-share.md) for what, how and why.

## Time and scope

A work unit is dated by its END day (CHAOS-9016, D6022). It is included when its end falls in the selected window:

```text
selected_start <= to_ts AND to_ts < selected_end
```

Each unit therefore sits in exactly one window: windows are additive, so the breakdowns of adjacent windows add up to the breakdown of the whole period. A unit that spans a window boundary counts whole in the window it ends in, never pro rata, never in both. The Investment page breakdown, sunburst and quality counts, the Home tile and the Operating Review all use this one rule (`analytics.InvestmentWindowClause`).

Organization isolation is applied before aggregation. Repository or team scope adds the resolved repository filter for the selected context.

## Latest-row behavior

The API reads the latest investment row for each `(organization, work_unit_id)` using the most recent computation timestamp. Older materializations for the same work unit do not contribute to the current response.

## Empty and missing states

- If the required table or required columns are absent, the service returns empty distributions.
- A missing row or unavailable input is not converted into a measured zero.
- Evidence quality is returned separately from contribution values.
- Synthetic or fixture rows can trigger provenance warnings and must not be presented as customer evidence.

## Labels

Theme and subcategory keys come from the [canonical Investment taxonomy](../taxonomies/investment.md). Provider-native labels are inputs to categorization, not public output keys.

## Implementation sources

- `internal/queryapi/investment/reader.go`
- `internal/queryapi/investment/response.go::BuildResponse`
- the investment mix chart section in the web repo
