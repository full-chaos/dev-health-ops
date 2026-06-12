# Investment Taxonomy

The canonical, fixed vocabulary for the Investment View. This page is the **shared
semantic source** — both user-facing and developer docs link here instead of
redefining terms.

- **Source of truth:** `src/dev_health_ops/investment_taxonomy.py`
  (`THEMES`, `SUBCATEGORIES`, `SUBCATEGORY_TO_THEME`).
- The taxonomy is **fixed**: no synonyms, no overrides, no per-team configuration.
- Every WorkUnit gets a probability distribution over these keys — never a single hard
  label, and never "unknown". See the
  [Investment Categorization Pipeline](../architecture/investment-categorization-pipeline.md).

> The key list below must match `investment_taxonomy.py` exactly. A CI drift check that
> enforces this is planned (Anti-Drift milestone); until then, treat the Python registry
> as authoritative if the two ever disagree.

---

## Themes (5, fixed)

| Key | Display name | What it captures |
| --- | ------------ | ---------------- |
| `feature_delivery` | Feature Delivery | Building new capability and shipping value to users. |
| `operational` | Operational / Support | Keeping the lights on — incidents, on-call, and supporting users. |
| `maintenance` | Maintenance / Tech Debt | Keeping the codebase healthy — refactors, upgrades, paying down debt. |
| `quality` | Quality / Reliability | Making the product correct and dependable — testing, bug fixes, reliability. |
| `risk` | Risk / Security | Protecting the product and the business — security, compliance, vulnerabilities. |

Theme probabilities are a **deterministic roll-up** of the subcategory probabilities
below (sum of subcategories sharing the same prefix), normalized across the 5 themes.

---

## Subcategories (15, fixed)

Each subcategory key is `theme.subcategory`. The theme is always the prefix before the
dot.

### Feature Delivery

| Key | Plain-language meaning |
| --- | ---------------------- |
| `feature_delivery.customer` | Work driven by a specific customer ask or commitment. |
| `feature_delivery.roadmap` | Planned roadmap features and enhancements. |
| `feature_delivery.enablement` | Platform/tooling that enables others to build (internal enablement, SDKs, APIs). |

### Operational / Support

| Key | Plain-language meaning |
| --- | ---------------------- |
| `operational.incident_response` | Responding to and resolving active incidents/outages. |
| `operational.on_call` | On-call duties and operational toil outside named incidents. |
| `operational.support` | Helping users — support tickets, questions, troubleshooting. |

### Maintenance / Tech Debt

| Key | Plain-language meaning |
| --- | ---------------------- |
| `maintenance.refactor` | Restructuring existing code without changing behavior. |
| `maintenance.upgrade` | Dependency/runtime/platform upgrades and migrations. |
| `maintenance.debt` | Paying down accumulated technical debt. |

### Quality / Reliability

| Key | Plain-language meaning |
| --- | ---------------------- |
| `quality.testing` | Adding or improving tests and test infrastructure. |
| `quality.bugfix` | Fixing defects in delivered functionality. |
| `quality.reliability` | Reliability, resilience, and stability improvements. |

### Risk / Security

| Key | Plain-language meaning |
| --- | ---------------------- |
| `risk.security` | Proactive security hardening and controls. |
| `risk.compliance` | Meeting compliance, regulatory, or audit requirements. |
| `risk.vulnerability` | Remediating specific known vulnerabilities. |

---

## Canonical key list

The exact keys, as they appear in `investment_taxonomy.py`. (This block is intended to
be **generated** from the registry by the Anti-Drift tooling; for now it is maintained by
hand and must match the code.)

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
```

---

## Guarantees

- Subcategory keys are canonical and fixed; provider-native labels are inputs only and
  are normalized away.
- Theme roll-up is deterministic via `theme_of(subcategory)`.
- Categorization always produces a distribution — it never returns "unknown".

## Related

- [Investment View](../user-guide/investment-view.md) — how to read the distribution (user-facing)
- [Investment Categorization Pipeline](../architecture/investment-categorization-pipeline.md) — how it is computed
- [LLM Categorization Contract](../llm/categorization-contract.md) — the strict LLM schema
