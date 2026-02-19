# Testing Governance and Quality Gates

> **Document Status**: Active  
> **Last Updated**: 2026-02-19

This policy defines the minimum quality bar for pull requests in `dev-health-ops`.

## Required CI checks

Every pull request must pass:

1. `Lint` workflow (`.github/workflows/lint.yml`)
2. `Tests` workflow (`.github/workflows/test.yml`)
3. `Governance Gate` workflow (`.github/workflows/governance.yml`)

The governance gate is intentionally narrow:

- It only evaluates pull requests that modify files under `src/`.
- It passes when either:
  - Tests are updated (`tests/**` or Python test files), or
  - The PR body includes meaningful `TEST-EVIDENCE:` and `RISK-NOTES:` markers.

## PR evidence requirements

For `src/` changes, include:

- `TEST-EVIDENCE:` with commands run and high-level results.
- `RISK-NOTES:` with blast radius, rollback approach, and any follow-up issues.

Example:

```text
TEST-EVIDENCE: ./ci/run_tests.sh unit (pass), pytest tests/test_metrics_daily.py -q (pass)
RISK-NOTES: Low risk; metrics-only logic change, rollback by reverting commit.
```

## Minimum local validation

Before requesting review on `src/` changes, run:

```bash
./ci/run_tests.sh unit
```

If the change is high impact (schema, sinks, connector auth, or critical metrics), also run targeted tests for the touched area and include those commands in `TEST-EVIDENCE:`.

## Escalation path

If the policy cannot be met normally:

1. If tests are blocked by environment/secrets/flakes, include the blocker and partial validation in `TEST-EVIDENCE:` and `RISK-NOTES:`.
2. Open or link a follow-up issue for unresolved test coverage or CI instability.
3. Request maintainer review before merge for any exception or high-risk change.
