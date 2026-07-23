---
page_id: ref-weighting
summary: Exact Investment contribution, aggregation, and displayed-share contract.
content_type: reference
owner: product-analytics
source_of_truth:
  - src/dev_health_ops/api/queries/investment.py
  - src/dev_health_ops/api/services/investment.py
  - src/components/work/investment/charts/InvestmentMixSection.tsx
applicability: current
lifecycle: active
---

# Weighting and aggregation

## Contract

For each latest materialized work-unit row that overlaps the selected period, the Investment breakdown expands the subcategory distribution and aggregates:

```text
subcategory contribution = Σ(subcategory probability × effort value)
theme contribution       = Σ(subcategory contributions whose key has that theme prefix)
displayed share          = contribution ÷ Σ(positive theme contributions)
```

The query groups by subcategory and theme and orders positive contributions from largest to smallest. The service omits non-positive values from the returned theme and subcategory maps. The web chart computes percentages against the total positive theme value in the current response.

## Time and scope

A row is included when its interval overlaps the selected window:

```text
from_ts < selected_end AND to_ts >= selected_start
```

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

- `src/dev_health_ops/api/queries/investment.py::fetch_investment_breakdown`
- `src/dev_health_ops/api/services/investment.py::build_investment_response`
- `src/components/work/investment/charts/InvestmentMixSection.tsx`
