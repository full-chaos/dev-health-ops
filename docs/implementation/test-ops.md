# Milestone: TestOps + AI Generative Reports

## Milestone Goal
Extend Dev Health Metrics with:
1. **TestOps** for CI/CD, test execution, coverage, release confidence, and quality drag
2. **AI Generative Reports** for natural-language report generation, live charts, saved templates, and scheduled delivery

## Milestone Success Criteria
- CI/CD pipelines, test results, and coverage are ingested and normalized
- Core TestOps metrics are queryable by team, repo, service, PR, and time range
- Release confidence and quality-drag signals are computed and exposed
- Natural-language prompts can compile into grounded report plans
- Generated reports can render markdown + live charts from validated metrics
- Saved templates and scheduled reports work for supported report types
- All narrative output is provenance-backed and scope-bound

## Execution Strategy
Split work into parallel streams with narrow interfaces so agent teams can work asynchronously:

- **Track A: Data Contracts and Platform Foundations**
- **Track B: TestOps Ingestion**
- **Track C: TestOps Metrics and Risk Models**
- **Track D: TestOps UX and Reporting Surfaces**
- **Track E: AI Report Planning and Grounded Generation**
- **Track F: Saved Reports, Scheduling, and Delivery**
- **Track G: Guardrails, Provenance, and Evaluation**

## Dependency Model
### Must happen first
- Canonical event/data contracts
- Metric registry additions
- Entity mapping for repo/service/team/PR linkage

### Can run in parallel after foundations
- CI ingestion
- Test result ingestion
- Coverage ingestion
- Report planner
- Chart rendering primitives
- Provenance model

### Depends on ingestion + metrics
- Release confidence
- Quality drag
- TestOps dashboards
- AI narrative grounded on metric payloads

### Depends on planner + persistence
- Saved reports
- Scheduled reports
- Report delivery channels

---

# Linear Structure

## Parent Milestone / Container Issue
### Title
**Milestone: TestOps + AI Generative Reports**

### Description
Deliver the next major product milestone for Dev Health Metrics:
- TestOps ingestion and analytics
- AI-generated, grounded reports with live charts
- Saved and scheduled reporting workflows

This milestone is explicitly structured for asynchronous implementation by multiple agent teams. Child issues are intentionally split by contracts, ingestion, metrics, UX, report planning, scheduling, and trust/guardrails.

### Acceptance Criteria
- [ ] Canonical schemas and interfaces defined for TestOps and Reports
- [ ] CI/test/coverage ingestion implemented for priority systems
- [ ] TestOps metrics available through API/query layer
- [ ] Report planner compiles prompts into structured report plans
- [ ] Markdown reports and live chart definitions render from validated data
- [ ] Saved templates and scheduling supported
- [ ] Provenance and anti-hallucination guardrails enforced
- [ ] Documentation and rollout plan complete

---

# Epic 1
## Title
**Phase 0: TestOps + Reports Foundations**

### Objective
Define the contracts, schemas, interfaces, and dependency boundaries that let all downstream teams implement independently.

### Why this exists
Without this, teams will collide on payload shape, entity identity, metric names, and report-plan contracts.

### Acceptance Criteria
- [ ] Canonical schema for `pipeline_run`, `job_run`, `test_case_result`, `test_suite_result`, `coverage_snapshot`
- [ ] Canonical schema for `report_plan`, `chart_spec`, `insight_block`, `provenance_record`
- [ ] Repo/service/team/PR/entity mapping documented
- [ ] Metric registry updated with TestOps metrics and report-visible metric names
- [ ] Data freshness and backfill rules documented
- [ ] Interfaces frozen for v1 implementation

### Implementation Notes
> **Existing infrastructure to extend (not replace):**
> - `metrics/schemas.py` already defines `PipelineRunRow`, `DeploymentRow`, `IncidentRow`, `DORAMetricsRecord`
> - `migrations/clickhouse/000_raw_tables.sql` defines `ci_pipeline_runs`, `deployments`, `incidents`
> - `migrations/clickhouse/023b_dora_metrics.sql` defines `dora_metrics_daily`
>
> New TestOps schemas (test_case_result, test_suite_result, coverage_snapshot) must coexist with and reference these existing structures. Report schemas (report_plan, chart_spec, insight_block, provenance_record) are net-new.

### Child Issues
1. **Define canonical TestOps event model**
2. **Define AI report DSL and rendered artifact contract**
3. **Define entity resolution and ownership mapping for repo/service/team/PR**
4. **Add TestOps metrics to canonical metric registry**
5. **Document dependency boundaries and interface contracts for agent teams**

---

# Epic 2
## Title
**TestOps Ingestion: CI/CD Pipeline Events**

### Objective
Ingest normalized pipeline and job execution data from target CI/CD systems.

### Acceptance Criteria
- [ ] Pipeline run ingestion supports status, queue time, runtime, retries, cancel reason, trigger source
- [ ] Job/stage ingestion supported
- [ ] Commit, branch, PR, repo, service, and team linkage works
- [ ] Backfill + incremental sync supported
- [ ] Failed and partial runs handled explicitly
- [ ] Ingestion observability added

### Implementation Notes
> **Existing infrastructure to extend:**
> - `connectors/github.py` and `connectors/gitlab.py` already fetch CI pipeline data
> - `storage/mixins/cicd.py` already upserts pipeline runs, deployments, and incidents
> - ClickHouse table `ci_pipeline_runs` already exists in `000_raw_tables.sql`
>
> v1 scope: GitHub Actions, GitLab CI, Jenkins/Buildkite. CircleCI and Azure DevOps deferred to v2.

### Child Issues
1. **Implement CI provider adapter: GitHub Actions**
2. **Implement CI provider adapter: GitLab CI**
3. **Implement CI provider adapter: Jenkins / Buildkite abstraction layer**
4. **Build normalized pipeline ingestion pipeline**
5. **Add pipeline backfill and incremental sync jobs**
6. **Add ingestion observability, retries, and dead-letter handling**

---

# Epic 3
## Title
**TestOps Ingestion: Test Execution and Coverage**

### Objective
Ingest test results and coverage artifacts in a normalized way.

### Acceptance Criteria
- [ ] Test suite and test case results ingested
- [ ] Retries, skips, quarantines, and durations preserved
- [ ] Coverage artifacts ingested for supported formats
- [ ] Changed-file / changed-code coverage contract defined and implemented where possible
- [ ] Ownership mapping from tests/suites to repo/service/team exists

### Child Issues
1. **Implement normalized test-result ingestion pipeline**
2. **Add support for JUnit/xUnit-style test artifacts**
3. **Add support for JS and Python ecosystem coverage artifacts**
4. **Implement changed-code coverage computation**
5. **Implement test ownership and suite-to-service attribution**
6. **Persist raw artifacts and normalized rollups for drill-down**

---

# Epic 4
## Title
**TestOps Metrics Engine**

### Objective
Compute the core TestOps metrics and expose them through the analytics/query layer.

### Acceptance Criteria
- [ ] Pipeline metrics available
- [ ] Test reliability metrics available
- [ ] Coverage metrics available
- [ ] Metrics slice by org/team/repo/service/PR/time range
- [ ] API/query layer supports dashboard and report consumers
- [ ] Metric definitions are documented and test-covered

### Implementation Notes
> **Existing infrastructure to extend:**
> - `metrics/compute_cicd.py` already computes `pipelines_count`, `success_rate`, `avg_duration_minutes`, `p90_duration_minutes`, `avg_queue_minutes`
> - `metrics/job_dora.py` already computes deployment frequency, lead time, MTTR, change failure rate
> - `metrics/schemas.py` defines `CICDMetricsDailyRecord` and `DORAMetricsRecord`
>
> New TestOps metrics should follow the existing `compute_*.py` pattern. Pipeline health metrics extend (not replace) existing CICD metrics with finer-grained breakdowns (rerun rate, cancel rate, p95). Test reliability and coverage metrics are entirely new.

### Child Issues
1. **Implement pipeline health metrics**
   - success rate
   - failure rate
   - median duration
   - p95 duration
   - queue time
   - rerun rate
   - cancel rate
2. **Implement test reliability metrics**
   - pass rate
   - failure rate
   - flake rate
   - retry dependency rate
   - quarantine count
   - failure recurrence
3. **Implement coverage metrics**
   - global coverage
   - changed-code coverage
   - coverage regression
   - uncovered change count
4. **Expose TestOps metrics through API/query layer**
5. **Add metric tests, fixtures, and regression coverage**

---

# Epic 5
## Title
**TestOps Risk Models: Release Confidence and Quality Drag**

### Objective
Turn raw TestOps data into decision-useful risk signals.

### Acceptance Criteria
- [ ] Release confidence score computed from explicit inputs
- [ ] Pipeline stability index computed
- [ ] Test reliability index computed
- [ ] Quality drag hours computed
- [ ] Escaped defect risk placeholder or correlation model defined
- [ ] Scoring methodology documented and explainable

### Child Issues
1. **Define deterministic scoring model for release confidence**
2. **Implement quality drag computation from failures, reruns, and queueing**
3. **Implement pipeline stability index**
4. **Implement test reliability index**
5. **Add explainability payloads for every derived risk score**
6. **Validate risk models against historical data samples**

---

# Epic 6
## Title
**TestOps UX: Dashboards, Drill-down, and PR Surfaces**

### Objective
Expose TestOps in product surfaces that users can actually act on.

### Acceptance Criteria
- [ ] TestOps dashboard ships with Pipelines, Tests, Coverage, Release Risk views
- [ ] Drill-down works from team to repo to suite/test
- [ ] Heatmaps and time-series views render
- [ ] PR-level widget shows changed-code coverage, likely failing suites, release confidence
- [ ] Correlation panels supported

### Child Issues
1. **Build TestOps dashboard shell and route structure**
2. **Build pipeline health views and trend panels**
3. **Build test reliability and flaky-test heatmaps**
4. **Build coverage and changed-code coverage views**
5. **Build release-risk and quality-drag panels**
6. **Add PR-level TestOps summary widget**

---

# Epic 7
## Title
**AI Generative Reports: Prompt Parsing and Report Planning**

### Objective
Convert natural-language prompts into validated report plans.

### Acceptance Criteria
- [ ] Prompt parser extracts scope, metrics, time range, grouping, and comparisons
- [ ] Unsupported asks are rejected cleanly
- [ ] Planner outputs canonical `report_plan`
- [ ] Planner supports report templates for weekly, monthly, quality, and risk reports
- [ ] Planner confidence / validation errors surfaced clearly

### Child Issues
1. **Implement natural-language prompt parser**
2. **Implement metric/entity/time-range resolver**
3. **Implement structured report planner**
4. **Add support for recommended report templates**
5. **Add planner validation and unsupported-request handling**
6. **Create planner test corpus from representative prompts**

---

# Epic 8
## Title
**AI Generative Reports: Grounded Report Rendering**

### Objective
Generate markdown reports, charts, and insight blocks from validated metric payloads only.

### Acceptance Criteria
- [ ] Reports render from structured plans, not direct freeform prompts
- [ ] Chart specs are generated from validated metric queries
- [ ] Narrative only uses available metric outputs
- [ ] Every insight carries provenance metadata
- [ ] Report output supports markdown export

### Child Issues
1. **Implement report execution engine from `report_plan`**
2. **Implement chart-spec generation and live chart rendering**
3. **Implement grounded narrative generation pipeline**
4. **Implement insight blocks with confidence labels**
5. **Implement markdown renderer**
6. **Add provenance panel and report metadata surfaces**

---

# Epic 9
## Title
**Saved Reports and Scheduling**

### Objective
Let users save, rerun, templatize, and schedule generated reports.

### Acceptance Criteria
- [ ] Users can save report definitions
- [ ] Users can clone and edit saved templates
- [ ] Scheduled execution supported for weekly/monthly/end-of-sprint
- [ ] Parameterized reports supported by team/repo/date range
- [ ] Delivery state and execution logs visible

### Child Issues
1. **Persist saved report definitions**
2. **Implement template cloning and parameterization**
3. **Implement scheduled report execution**
4. **Implement report run history and status tracking**
5. **Implement v1 delivery target: in-app report center**
6. **Add export pipeline for markdown artifacts**

---

# Epic 10
## Title
**Trust, Guardrails, and Evaluation**

### Objective
Prevent the AI layer from becoming a hallucination engine and ensure TestOps metrics remain credible.

### Acceptance Criteria
- [ ] Narrative claims require supporting metric payloads
- [ ] Every report shows time range, scope, and filters
- [ ] Unsupported metrics omitted or explicitly flagged
- [ ] Confidence states supported: direct fact / inferred / hypothesis
- [ ] Evaluation suite exists for parser, planner, and generated reports
- [ ] Audit logging added for generated outputs

### Child Issues
1. **Implement provenance enforcement for generated narrative**
2. **Implement confidence labeling for insights**
3. **Implement unsupported-metric handling and graceful failures**
4. **Build report evaluation suite and golden examples**
5. **Add audit logs for report generation and execution**
6. **Document trust model and operator controls**

---

# Epic 11
## Title
**Documentation, Rollout, and Operational Readiness**

### Objective
Make the milestone operable, testable, and shippable.

### Acceptance Criteria
- [ ] Architecture docs updated
- [ ] Runbooks written for ingestion and report execution failures
- [ ] Metrics definitions documented
- [ ] Launch checklist and staged rollout defined
- [ ] Internal demo scripts and seed prompts created

### Child Issues
1. **Document TestOps architecture and metric definitions**
2. **Document AI report architecture, DSL, and grounding model**
3. **Create runbooks for ingestion failures and bad artifacts**
4. **Create runbooks for report-generation failures and mis-scoped prompts**
5. **Create staged rollout plan and feature flags**
6. **Prepare internal demo pack and canned prompts**

---

# Epic 12
## Title
**Scoring Model Integration: TestOps Signals into Platform Dimensions**

### Objective
Connect TestOps metrics to the existing Delivery, Durability, Well-being, and Dynamics computation and visualization layers so TestOps data influences the platform's aggregate health signals.

### Acceptance Criteria
- [ ] Delivery dimension extends existing `compute_cicd.py` and `job_dora.py` with TestOps pipeline granularity
- [ ] Durability dimension includes coverage quality, test reliability, and release confidence signals via `quality.py` extension
- [ ] Well-being dimension extends `compute_wellbeing.py` with rerun burden and failed-build interruption rate
- [ ] Dynamics dimension adds team failure ownership and quality burden concentration to frontend quadrant zones
- [ ] All composite signals are documented with explicit input metrics and weights
- [ ] Frontend `quadrantZones.ts` updated to accept new TestOps-derived zone inputs

### Child Issues
1. **Extend Delivery dimension with TestOps pipeline metrics**
2. **Add Durability signals from coverage and test reliability**
3. **Extend Well-being dimension with CI burden metrics**
4. **Add Dynamics zone inputs for team failure ownership and quality burden**
5. **Document composite scoring methodology and weights**

---

# Epic 13
## Title
**Benchmarking and Insights Pipeline**

### Objective
Enable teams to compare TestOps metrics against internal baselines and surface actionable insights from metric patterns.

### Acceptance Criteria
- [ ] Teams can compare current period vs prior period for all TestOps metrics
- [ ] Internal baseline computation exists (rolling averages, percentile bands)
- [ ] Maturity bands (stable / watch / degraded / critical) computable for pipeline health, test reliability, and coverage
- [ ] Insight generation pipeline surfaces top anomalies, regressions, and correlations
- [ ] Insights are structured (not freeform) with metric references and confidence

### Child Issues
1. **Implement period-over-period comparison for TestOps metrics**
2. **Implement internal baseline computation (rolling averages, percentile bands)**
3. **Define and implement maturity band classification**
4. **Build insight generation pipeline for anomalies and regressions**
5. **Add correlation insights (e.g., flake rate vs PR lead time, pipeline instability vs cycle time)**

---

# Recommended Agent Team Split

## Team A: Foundations + Contracts
Own:
- Epic 1
- shared schemas
- metric registry
- interface boundaries

## Team B: Ingestion
Own:
- Epic 2
- Epic 3

## Team C: Metrics + Risk + Scoring
Own:
- Epic 4
- Epic 5
- Epic 12

## Team D: Product Surfaces + Benchmarking
Own:
- Epic 6
- Epic 13
- chart/view layer dependencies from Epic 8

## Team E: AI Planning + Rendering
Own:
- Epic 7
- Epic 8

## Team F: Persistence + Scheduling + Ops
Own:
- Epic 9
- Epic 11

## Team G: Guardrails + Evaluation
Own:
- Epic 10
- cross-cutting reviews into Epics 7, 8, and 9

---

# Sequencing Recommendation

## Wave 1
- Epic 1
- start Epic 2
- start Epic 3
- start Epic 7 in parallel, but only against mocked contracts

## Wave 2
- Epic 4
- Epic 6 shell
- Epic 8 execution engine with stubbed metrics
- Epic 10 provenance framework

## Wave 3
- Epic 5
- Epic 12 (scoring integration)
- complete Epic 6
- complete Epic 8
- Epic 9 scheduling/persistence

## Wave 4
- Epic 13 (benchmarking + insights)
- Epic 11
- evaluation hardening
- rollout

---

# Priority Order

1. Phase 0: TestOps + Reports Foundations
2. TestOps Ingestion: CI/CD Pipeline Events
3. TestOps Ingestion: Test Execution and Coverage
4. TestOps Metrics Engine
5. AI Generative Reports: Prompt Parsing and Report Planning
6. AI Generative Reports: Grounded Report Rendering
7. TestOps Risk Models: Release Confidence and Quality Drag
8. Scoring Model Integration: TestOps Signals into Platform Dimensions
9. TestOps UX: Dashboards, Drill-down, and PR Surfaces
10. Saved Reports and Scheduling
11. Trust, Guardrails, and Evaluation
12. Benchmarking and Insights Pipeline
13. Documentation, Rollout, and Operational Readiness
