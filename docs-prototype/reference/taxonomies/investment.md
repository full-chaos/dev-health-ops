---
page_id: ref-investment-taxonomy
summary: Canonical Investment theme and subcategory keys.
content_type: generated-reference
owner: product-analytics
source_of_truth:
  - src/dev_health_ops/investment_taxonomy.py
  - src/dev_health_ops/core/taxonomy.py
applicability: current
lifecycle: active
---

# Investment taxonomy

The following keys are the current canonical Investment vocabulary. They are not workspace-configurable labels.

## Themes

| Key | Display label |
| --- | --- |
| `feature_delivery` | Feature Delivery |
| `operational` | Operational / Support |
| `maintenance` | Maintenance / Tech Debt |
| `quality` | Quality / Reliability |
| `risk` | Risk / Security |

## Subcategories

| Theme | Keys |
| --- | --- |
| Feature Delivery | `feature_delivery.customer`, `feature_delivery.roadmap`, `feature_delivery.enablement` |
| Operational / Support | `operational.incident_response`, `operational.on_call`, `operational.support` |
| Maintenance / Tech Debt | `maintenance.refactor`, `maintenance.upgrade`, `maintenance.debt` |
| Quality / Reliability | `quality.testing`, `quality.bugfix`, `quality.reliability` |
| Risk / Security | `risk.security`, `risk.compliance`, `risk.vulnerability` |

A subcategory key always maps to the theme before the first `.`. Documentation, APIs, filters, and generated references must not define a competing vocabulary.

## Source

This page follows `src/dev_health_ops/investment_taxonomy.py`. Taxonomy drift checks should compare the rendered keys with that registry.
