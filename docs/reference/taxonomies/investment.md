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

## Generated keys

The block below is generated from `src/dev_health_ops/investment_taxonomy.py`; run `make docs:generate-taxonomy` after changing the registry. Do not edit it by hand.

<!-- BEGIN GENERATED TAXONOMY -->
```text
# THEMES
feature_delivery
operational
maintenance
quality
risk

# SUBCATEGORIES (theme.subcategory)
feature_delivery.customer
feature_delivery.roadmap
feature_delivery.enablement
operational.incident_response
operational.on_call
operational.support
maintenance.refactor
maintenance.upgrade
maintenance.debt
quality.testing
quality.bugfix
quality.reliability
risk.security
risk.compliance
risk.vulnerability

# SUBCATEGORY_TO_THEME
feature_delivery.customer -> feature_delivery
feature_delivery.roadmap -> feature_delivery
feature_delivery.enablement -> feature_delivery
operational.incident_response -> operational
operational.on_call -> operational
operational.support -> operational
maintenance.refactor -> maintenance
maintenance.upgrade -> maintenance
maintenance.debt -> maintenance
quality.testing -> quality
quality.bugfix -> quality
quality.reliability -> quality
risk.security -> risk
risk.compliance -> risk
risk.vulnerability -> risk
```
<!-- END GENERATED TAXONOMY -->
