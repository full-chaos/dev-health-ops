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
- `quality.testing`
- `risk.compliance`
- `risk.security`
- `risk.vulnerability`

Placeholder shape: `theme.subcategory`. Registry: `src/dev_health_ops/investment_taxonomy.py`.

## Generated keys

<!-- BEGIN GENERATED TAXONOMY -->
```text
# THEMES
risk
quality
operational
maintenance
feature_delivery

# SUBCATEGORIES (theme.subcategory)
risk.vulnerability
risk.security
risk.compliance
quality.testing
quality.reliability
quality.bugfix
operational.support
operational.on_call
operational.incident_response
maintenance.upgrade
maintenance.refactor
maintenance.debt
feature_delivery.roadmap
feature_delivery.enablement
feature_delivery.customer

# SUBCATEGORY_TO_THEME
risk.vulnerability -> risk
risk.security -> risk
risk.compliance -> risk
quality.testing -> quality
quality.reliability -> quality
quality.bugfix -> quality
operational.support -> operational
operational.on_call -> operational
operational.incident_response -> operational
maintenance.upgrade -> maintenance
maintenance.refactor -> maintenance
maintenance.debt -> maintenance
feature_delivery.roadmap -> feature_delivery
feature_delivery.enablement -> feature_delivery
feature_delivery.customer -> feature_delivery
```
<!-- END GENERATED TAXONOMY -->
Trailing text.
