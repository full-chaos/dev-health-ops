# TestOps + AI Reports: Staged Rollout Plan

This document outlines the staged rollout strategy for the TestOps and AI Reports milestone. The plan appears to balance rapid internal feedback with a cautious approach to production data ingestion and analytics.

---

## Table of Contents

1. [Feature Flag Definitions](#feature-flag-definitions)
2. [Rollout Stages](#rollout-stages)
    - [Stage 0: Internal Dogfood](#stage-0-internal-dogfood)
    - [Stage 1: Alpha](#stage-1-alpha)
    - [Stage 2: Beta](#stage-2-beta)
    - [Stage 3: GA](#stage-3-ga)
3. [Monitoring and Observability](#monitoring-and-observability)
4. [Rollback Procedures](#rollback-procedures)

---

## Feature Flag Definitions

The following feature flags control the activation of TestOps capabilities. These flags should be managed via environment variables or the centralized configuration service.

| Flag | Description | Impact Area |
|------|-------------|-------------|
| `TESTOPS_PIPELINE_INGESTION` | Enables CI/CD pipeline data ingestion from providers (GitHub Actions, GitLab CI). | Connectors, Processors |
| `TESTOPS_TEST_INGESTION` | Enables ingestion of test execution results (JUnit XML) and coverage reports. | Connectors, Processors |
| `TESTOPS_METRICS` | Enables computation of TestOps daily metrics (e.g., failure rates, duration p95). | Metrics |
| `TESTOPS_RISK_MODELS` | Enables risk model computation (Release Confidence, Quality Drag). | Metrics, Analytics |
| `TESTOPS_AI_REPORTS` | Enables the AI-driven report generation engine. | LLM, Reports |
| `TESTOPS_UX` | Enables the TestOps dashboard and related views in the web UI. | Web UI |
| `TESTOPS_SAVED_REPORTS` | Enables report template saving, parameterization, and scheduling. | API, Web UI |

---

## Rollout Stages

### Stage 0: Internal Dogfood
**Target**: Dev Health core team and internal engineering org.
**Duration**: 1 week.
**Flags**: All flags `ON`.

- **Entry Criteria**:
    - All TestOps unit and integration tests passing in CI.
    - ClickHouse schema migrations for TestOps tables completed.
    - AI Report engine verified with synthetic data.
- **Success Metrics**:
    - 100% ingestion success rate for internal repos.
    - Zero critical UI bugs reported by the team.
    - AI reports generated within < 30 seconds.
- **Rollback Triggers**:
    - Data corruption in ClickHouse analytics tables.
    - Significant performance degradation in the main API.

### Stage 1: Alpha
**Target**: Select partner organizations (3-5 orgs).
**Duration**: 2 weeks.
**Flags**: `TESTOPS_PIPELINE_INGESTION`, `TESTOPS_TEST_INGESTION`, `TESTOPS_METRICS` `ON`. Others `OFF`.

- **Entry Criteria**:
    - Stage 0 success criteria met.
    - Documentation for TestOps ingestion configuration completed.
- **Success Metrics**:
    - Stable ingestion across diverse CI pipeline structures.
    - Metrics computation completes within the daily window for all Alpha orgs.
- **Rollback Triggers**:
    - Provider API rate limiting issues affecting other platform features.
    - Inconsistent metric values compared to provider-native dashboards.

### Stage 2: Beta
**Target**: 10% of all organizations.
**Duration**: 2 weeks.
**Flags**: All flags `ON`.

- **Entry Criteria**:
    - Stage 1 success criteria met.
    - Risk models validated against historical failure data.
- **Success Metrics**:
    - Positive feedback on AI report relevance and accuracy.
    - Risk models correctly identify at least 80% of known "high-risk" releases.
- **Rollback Triggers**:
    - High error rates in AI report generation (LLM timeouts or malformed JSON).
    - User reports of "hallucinated" metrics in AI summaries.

### Stage 3: GA
**Target**: All organizations.
**Duration**: Permanent.
**Flags**: All flags `ON`.

- **Entry Criteria**:
    - Stage 2 success criteria met.
    - Load testing confirms system stability at 5x current volume.
- **Success Metrics**:
    - TestOps views become a top-3 visited section in the platform.
    - Reduction in reported "flaky test" noise for GA users.
- **Rollback Triggers**:
    - System-wide latency spikes exceeding 2 seconds for standard queries.

---

## Monitoring and Observability

The following signals suggest the health of the TestOps rollout:

- **Error Rates**: Monitor `Connectors` and `Processors` for 4xx/5xx errors from providers.
- **Ingestion Latency**: Track the time from CI job completion to data availability in ClickHouse.
- **Metric Accuracy**: Periodic automated checks comparing `test_pass_rate` in ClickHouse vs raw provider artifacts.
- **LLM Performance**: Monitor token usage, latency, and "repair attempt" frequency for AI reports.
- **Data Quality**: Track the `evidence_quality` score emitted by the categorization engine.

---

## Rollback Procedures

If a feature flag needs to be disabled due to instability:

1. **Identify the Scope**: Determine if the issue is isolated to a specific flag (e.g., `TESTOPS_AI_REPORTS`).
2. **Disable the Flag**: Update the environment configuration to set the flag to `OFF`.
3. **Verify UI State**: Ensure the `TESTOPS_UX` flag correctly hides affected components to prevent broken links.
4. **Data Cleanup (Optional)**: If data corruption occurred, use `dev-hops backfill` to re-sync the affected period after the fix is deployed.
5. **Post-Mortem**: Document the trigger and resolution before re-attempting the rollout stage.
