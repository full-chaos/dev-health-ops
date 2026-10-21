---
page_id: ref-investment-taxonomy
---

# Investment taxonomy

The following keys are the current canonical Investment vocabulary. They are not workspace-configurable labels.

## Themes

| Key | Display label |
| --- | --- |
| `feature_delivery` | feature_delivery |
| `maintenance` | maintenance |
| `operational` | operational |
| `quality` | quality |
| `risk` | risk |

## Subcategories

- `feature_delivery.customer`
- `feature_delivery.enablement`
- `feature_delivery.roadmap`
- `maintenance.debt`
- `maintenance.refactor`
- `maintenance.upgrade`
- `operational.incident_response`
- `operational.on_call`
- `operational.support`
- `quality.bugfix`
- `quality.reliability`
- `risk.compliance`
- `risk.vulnerability`

Placeholder shape: `theme.subcategory`. Registry: `src/dev_health_ops/investment_taxonomy.py`.

## Generated keys

<!-- BEGIN GENERATED TAXONOMY -->
```text
# THEMES
feature_delivery
maintenance
operational
quality
risk

# SUBCATEGORIES (theme.subcategory)
feature_delivery.customer
feature_delivery.enablement
feature_delivery.roadmap
maintenance.debt
maintenance.refactor
maintenance.upgrade
operational.incident_response
operational.on_call
operational.support
quality.bugfix
quality.reliability
quality.testing
risk.compliance
risk.security
risk.vulnerability

# SUBCATEGORY_TO_THEME
feature_delivery.customer -> feature_delivery
feature_delivery.enablement -> feature_delivery
feature_delivery.roadmap -> feature_delivery
maintenance.debt -> maintenance
maintenance.refactor -> maintenance
maintenance.upgrade -> maintenance
operational.incident_response -> operational
operational.on_call -> operational
operational.support -> operational
quality.bugfix -> quality
quality.reliability -> quality
quality.testing -> quality
risk.compliance -> risk
risk.security -> risk
risk.vulnerability -> risk
```
<!-- END GENERATED TAXONOMY -->
Trailing text.
