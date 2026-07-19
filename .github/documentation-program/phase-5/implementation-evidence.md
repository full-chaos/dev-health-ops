# Phase 5 implementation evidence

**Decision:** the project owner authorized implementation to continue through Phase 9. This does not authorize production cutover.

## Implemented vertical slice

- `/`
- `/get-started/`, `/get-started/choose-a-task/`, `/get-started/prerequisites/` as a provisional experiment
- `/use/`
- `/use/investment/`
- `/use/investment/investigate-effort/`
- `/use/investment/investment-mix/`
- `/use/investment/follow-evidence/`
- `/reference/taxonomies/investment/`
- `/reference/metrics/weighting-and-aggregation/`
- `/use/troubleshooting/no-or-incomplete-data/`
- contextual administrator and operator escalation

## Primary source verification

| Contract | Source |
| --- | --- |
| Latest investment row, time overlap, scope, and weighted aggregation | `src/dev_health_ops/api/queries/investment.py` |
| Empty-table/column behavior and positive-value response maps | `src/dev_health_ops/api/services/investment.py` |
| Canonical taxonomy | `src/dev_health_ops/investment_taxonomy.py`, `src/dev_health_ops/core/taxonomy.py` |
| Treemap/sunburst labels, percentage, selection, evidence opacity, and Work Graph action | `full-chaos/dev-health-web` `InvestmentMixSection.tsx`, `investmentMix.ts` |

## Gate interpretation

The owner directive approves scaling implementation through Phase 9. The following remain required before production:

- strict build and link evidence for each PR;
- source and editorial review of migrated pages;
- accessibility and responsive review;
- natural-language search verification;
- final redirect and publication inventory;
- the Phase 10 quality gate and Phase 11/12 cutover gates.

`/get-started/` remains provisional until the direct-route comparison is reviewed. Its content is newly authored and does not reuse the prior onboarding sequence.
