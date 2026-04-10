# TestOps + AI Reports: Internal Demo Pack

This guide provides a structured walkthrough for demonstrating the TestOps and AI Reports capabilities. It appears to cover the full lifecycle from raw pipeline ingestion to high-level AI-driven quality insights.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Demo Scenarios](#demo-scenarios)
3. [Seed Prompts for AI Reports](#seed-prompts-for-ai-reports)
4. [Talking Points](#talking-points)
5. [Known Limitations](#known-limitations)

---

## Prerequisites

To ensure a rich demo experience, you should seed the environment with realistic TestOps data.

### Seeding Demo Data
Use the `dev-hops` CLI to generate 30 days of synthetic TestOps fixtures:

```bash
# Generate fixtures for the last 30 days
CLICKHOUSE_URI=clickhouse://... dev-hops fixtures generate --sink "$CLICKHOUSE_URI" --days 30
```

This command appears to populate the following tables:
- `ci_pipeline_runs`
- `ci_job_runs`
- `test_executions`
- `coverage_snapshots`

---

## Demo Scenarios

### 1. Pipeline Health Overview
**Navigation**: `/testops/pipelines`
**Action**: Show the aggregate pipeline success rate and duration trends.
**Insight**: Identify if the platform team's pipelines are becoming slower or more prone to failure over time.

### 2. Flaky Test Investigation
**Navigation**: `/testops/tests`
**Action**: Use the test view heatmap to locate tests with high `test_flake_rate`.
**Insight**: Demonstrate how the system leans toward identifying tests that fail and then pass within the same pipeline run, suggesting instability rather than code defects.

### 3. Coverage Trend Analysis
**Navigation**: `/testops/coverage`
**Action**: View the `coverage_line_pct` and `coverage_delta_pct` for a specific repository.
**Insight**: Show how the system tracks coverage changes per PR, highlighting where quality drag might be accumulating.

### 4. Release Risk Assessment
**Navigation**: `/testops/risk`
**Action**: Examine the `release_confidence_score` and `quality_drag_hours` for the upcoming release.
**Insight**: Explain how the risk model suggests release readiness based on recent pipeline stability and test reliability.

### 5. Generate an AI Report
**Navigation**: `/reports/new`
**Action**: Enter a natural language prompt to generate a quality summary.
**Example Prompt**:
> "Generate a weekly quality report for the platform team covering pipeline stability, test reliability, and coverage trends for the last 2 weeks"

### 6. Save and Schedule Reports
**Navigation**: `/reports/saved`
**Action**: Show how to save the generated report as a template and set a weekly schedule.
**Insight**: Demonstrate the automation of quality reporting for leadership.

---

## Seed Prompts for AI Reports

Use these prompts to showcase the flexibility of the AI report engine:

1. "What's the pipeline health trend for [team] over the last month?"
2. "Show me which test suites have the highest flake rate"
3. "Compare coverage between [repo-a] and [repo-b]"
4. "Generate a sprint-end quality report for the frontend team"
5. "What's our release confidence for the next deployment?"
6. "Identify the top 3 bottlenecks in our CI/CD pipeline duration"
7. "Summarize the impact of recent flaky tests on our total quality drag"
8. "How has coverage changed across the organization since the start of the quarter?"
9. "Which repositories have the highest pipeline failure rate but no associated test failures?"
10. "Generate a monthly executive summary of engineering quality and risk"

---

## Talking Points

- **Evidence-Based**: "The system appears to base all insights on raw CI/CD artifacts, ensuring that AI summaries are grounded in verifiable data."
- **Trend-Focused**: "We lean toward showing trajectories rather than absolute scores, as the direction of quality matters more than a single point in time."
- **Actionable Risk**: "The risk models suggest where human attention is most needed, rather than providing a verdict on release readiness."
- **Reduced Noise**: "By identifying flaky tests, the platform suggests which failures can be safely ignored and which require immediate investigation."

---

## Known Limitations

- **Provider Support**: Currently appears to support GitHub Actions and GitLab CI; Jenkins support is on the roadmap.
- **Report Latency**: AI report generation may take up to 30 seconds depending on the volume of evidence being summarized.
- **Historical Depth**: Risk models require at least 14 days of continuous ingestion to suggest reliable confidence scores.
- **Coverage Formats**: Currently supports LCOV and Cobertura formats; others may require custom processors.
