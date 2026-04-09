# PRD: TestOps
Linear Milestone: https://linear.app/fullchaos/project/dev-health-ops-f947bce19f4c/overview

## Product Name
**TestOps for Dev Health Metrics**

## Summary
TestOps extends Dev Health Metrics beyond source control and workflow analytics into build, test, and release execution. It ingests fine-grained CI/CD and test telemetry, normalizes it, and turns it into team-level and system-level signals about build health, test effectiveness, release risk, and engineering drag.

The goal is not just to show pipeline numbers. The goal is to connect delivery performance to test quality, defect risk, and developer health.

## Problem
The current model covers delivery flow, code risk, collaboration, and cognitive load well, but it underweights the execution layer where engineering quality is either validated or exposed. Without TestOps:

- teams can ship fast while silently accumulating flaky tests and unstable pipelines
- leaders cannot distinguish slow delivery caused by review/process from slow delivery caused by broken CI
- test coverage is often reported as a vanity metric rather than a risk metric
- test failures, retry behavior, and flaky suites are not tied back to repos, teams, services, or changesets
- there is no reliable way to quantify quality drag on developer throughput

## Goals
- Ingest CI/CD, test execution, and coverage data from major delivery systems
- Quantify pipeline health, test reliability, and release risk at org, team, repo, service, and PR levels
- Attribute build and test pain to teams, code areas, and change events
- Expose actionable insights, not just dashboards
- Integrate TestOps signals into Delivery, Durability, and Developer Well-being scores

## Non-Goals
- Replace CI/CD vendors or test runners
- Act as a full test management system
- Author or orchestrate test execution directly in v1
- Provide root-cause analysis beyond supported correlations and heuristics

## Target Users
- Engineering leaders
- Dev productivity / developer experience teams
- QA / quality engineering leaders
- Platform engineering teams
- EMs and tech leads
- Release managers

## Core User Stories
- As an engineering leader, I want to see which teams lose the most time to CI instability so I know where platform investment is justified.
- As a dev productivity owner, I want to identify flaky tests by suite, owner, service, and impact so I can prioritize cleanup.
- As a team lead, I want to understand whether release risk is driven by low coverage, unstable tests, or recent defect escape.
- As a developer, I want to know whether my PR is likely to fail deployment based on historical signals.
- As a QA lead, I want to see whether increased test volume is improving confidence or just increasing runtime.

## Key Concepts
- **Pipeline Health**: execution speed, reliability, retry burden, queueing, and failure modes
- **Test Reliability**: pass consistency, flakiness, quarantine rate, rerun dependence
- **Coverage Quality**: meaningful change coverage, not just aggregate line coverage
- **Release Confidence**: probability a change can move through CI/CD without regressions
- **Quality Drag**: time lost to failed pipelines, reruns, flaky tests, and blocked deploys

## Data Sources
- GitHub Actions
- GitLab CI
- CircleCI
- Jenkins
- Buildkite
- Azure DevOps pipelines
- Test frameworks and artifacts:
  - JUnit/XML
  - pytest
  - Jest
  - Playwright
  - Cypress
  - xUnit variants
  - coverage reports such as lcov, Cobertura, JaCoCo
- Deployment systems where available
- Existing repo, PR, issue, and code ownership data in Dev Health Metrics

## Functional Requirements

### 1. CI/CD Ingestion
- Ingest pipeline runs, jobs, stages, queue time, runtime, result, retry count, cancel reason, and trigger source
- Associate pipeline runs to commit, branch, PR/MR, repo, service, and team
- Support both polling and webhook ingestion patterns
- Handle backfills and incremental sync

### 2. Test Execution Ingestion
- Ingest test suite, test case, duration, status, retries, skipped/quarantined state, environment, and artifact links
- Track failures over time at test case and suite level
- Detect test ownership through repo/service/team mappings

### 3. Coverage Ingestion
- Ingest overall and changed-file coverage
- Track delta coverage on modified files and touched code paths
- Support branch, PR, repo, and service views

### 4. Metrics Engine
Compute at least the following:

#### Pipeline Metrics
- Pipeline success rate
- Pipeline failure rate
- Median pipeline duration
- P95 pipeline duration
- Queue time
- Rerun rate
- Cancel rate
- Deployment frequency
- Change failure rate
- MTTR for failed deploys
- Failed deployment recovery time

#### Test Metrics
- Test pass rate
- Test failure rate
- Flake rate
- Retry dependency rate
- Test suite duration
- Critical path test duration
- Slowest suites/tests
- Quarantined test count
- Failure recurrence score

#### Coverage Metrics
- Global coverage
- Changed-code coverage
- Critical-path coverage
- Coverage regression rate
- Uncovered change count
- Coverage-to-defect correlation

#### Derived Risk Metrics
- Release confidence score
- Test reliability index
- Pipeline stability index
- Quality drag hours
- Escaped defect risk
- Merge risk score

### 5. Entity-Level Views
Allow all metrics to be sliced by:
- org
- business unit
- team
- repo
- service
- application
- branch
- PR/MR
- developer
- date range
- environment

### 6. Insights and Alerts
Surface insights such as:
- 40% of failed pipelines in Team A are caused by 12 flaky end-to-end tests
- Changed-code coverage dropped below threshold in the payments service for 3 consecutive weeks
- Build queue time increased 2.3x after runner capacity saturation
- Service X has the highest release risk due to low coverage plus high failure recurrence

### 7. Benchmarking
- Compare teams against internal baselines
- Compare current period vs prior period
- Support maturity bands such as stable / watch / degraded / critical

## UX / Reporting Requirements
- TestOps dashboard with tabs for Pipelines, Tests, Coverage, Release Risk, and Cost of Quality
- Drill-down from org to team to repo to failing suite to failing test
- Time-series views with weekly and monthly trends
- Correlation panels:
  - pipeline instability vs cycle time
  - flake rate vs PR lead time
  - changed-code coverage vs defect escape
- Heatmaps for flaky tests and unstable repos
- PR-level widget showing:
  - changed-code coverage
  - likely failing suites
  - release confidence
  - historical impact area risk

## Scoring Model Integration
TestOps should feed the main framework as follows:

- **Delivery**
  - queue time
  - pipeline duration
  - deploy frequency
  - failed deployment recovery
- **Durability**
  - coverage quality
  - defect escape
  - test reliability
  - release confidence
- **Developer Well-being**
  - rerun burden
  - failed build interruption rate
  - after-hours recovery/retry behavior
- **Dynamics**
  - team ownership of failures
  - quality burden concentration
  - cross-team dependency failures

## API Requirements
### Example Objects
- pipeline_run
- pipeline_stage
- job_run
- test_case_result
- test_suite_result
- coverage_snapshot
- release_risk_score

### Required API Capabilities
- fetch normalized metrics by dimension/entity/time window
- fetch raw execution events for drill-down
- fetch ranked insights and anomalies
- fetch scorecard summaries for dashboards and reports

## Success Metrics
- % of active repos with CI/CD ingestion enabled
- % of active repos with test result ingestion enabled
- % of PRs with changed-code coverage
- reduction in flaky test volume
- reduction in rerun burden
- reduction in p95 pipeline duration
- improvement in release confidence
- decrease in failed deployment recovery time

## Risks
- Coverage data quality is inconsistent across ecosystems
- CI vendor schemas vary and can be noisy
- Flake detection can create false positives if heuristics are simplistic
- Teams may game test quantity rather than test quality
- Attribution to team ownership can break in shared repos/services

## Open Questions
- Which CI/CD systems are highest priority for v1?
- Is changed-code coverage mandatory for v1 or v2?
- How should we model monorepo ownership cleanly?
- Should release confidence be deterministic, heuristic, or ML-assisted in v1?
- Do we expose cost metrics such as runner spend and wasted compute in v1?

## Phased Delivery
### Phase 1
- CI ingestion
- basic test result ingestion
- pipeline and flake metrics
- dashboard and team views

### Phase 2
- changed-code coverage
- release confidence
- risk scoring
- alerting and anomaly detection

### Phase 3
- predictive failure models
- recommended remediation
- cost-of-quality analytics
- PR-level quality guidance

---

# PRD: AI Generative Reports

## Product Name
**AI Generative Reports for Dev Health Metrics**

## Summary
AI Generative Reports let users ask natural-language questions and receive generated reports, narrative analysis, live charts, and reusable reporting workflows based on their Dev Health data.

This is not just a chatbot on top of dashboards. It is a report-generation layer that composes metrics, trends, comparisons, explanations, and visuals from the underlying analytics engine.

Example:
> Create a weekly report that shows cycle time, review bottlenecks, flaky test growth, and after-hours work for the platform team, with live charts and a summary of what changed.

## Problem
The product can accumulate excellent metrics and still fail if users must manually assemble dashboards every week. Current analytics products often break at the last mile:

- leaders need answers, not panels
- teams need recurring summaries, not dashboard archaeology
- chart creation is too manual
- natural-language questions are not mapped cleanly to the metric model
- generated insights often hallucinate or ignore actual data boundaries

## Goals
- Let users generate trustworthy, data-grounded reports from natural language
- Automatically create charts, summaries, comparisons, and anomalies from live metrics
- Support reusable scheduled reports without requiring dashboard authoring
- Preserve strict traceability from narrative claims back to computed metrics
- Reduce time-to-insight for leaders and managers

## Non-Goals
- Open-ended general chat unrelated to Dev Health data
- Unbounded autonomous analysis across systems without guardrails
- Replacing analysts for bespoke deep-dive work in v1
- Generating unsupported conclusions without metric provenance

## Target Users
- Engineering executives
- VPs and directors
- EMs
- Dev productivity teams
- Program/release leads
- Team leads
- Individual developers who want personal or team summaries

## Core User Stories
- As a VP, I want a weekly engineering health summary with charts and narrative so I can review trends in minutes.
- As an EM, I want to ask why cycle time worsened this week and get a grounded explanation with supporting metrics.
- As a platform lead, I want a reusable report for CI instability and flaky tests by team every Monday morning.
- As a team lead, I want a monthly health review covering throughput, durability, collaboration, and burnout indicators.
- As a developer, I want to ask which code areas and PR patterns are creating the most rework in my team.

## Product Principles
- **Grounded first**: every narrative claim must map to actual metric outputs
- **Structured generation**: LLM composes from validated metric payloads, not raw guesswork
- **Visual by default**: charts and tables are first-class outputs
- **Reusable**: any ad hoc prompt can become a saved report template
- **Explainable**: generated output must show what data, time range, and filters were used

## Supported Output Types
- Executive summary
- Weekly team report
- Monthly business review
- Incident/release health review
- Delivery risk report
- Quality trend report
- Developer well-being summary
- Custom ad hoc analysis
- Slide-ready export
- Markdown export
- JSON/report spec export

## Functional Requirements

### 1. Natural Language Query Interface
Users can ask prompts such as:
- Create a weekly report for the platform team
- Why did review time spike in March?
- Show me teams with worsening build stability and burnout risk
- Generate a monthly report on delivery vs durability for all backend teams

The system must:
- parse entities, metrics, time windows, comparisons, and grouping instructions
- resolve ambiguous prompt elements to known metric model concepts where possible
- reject unsupported requests cleanly instead of inventing data

### 2. Report Planner
Convert user intent into a structured report plan:
- audience
- time range
- scope
- metrics requested
- comparison periods
- narrative sections
- required charts
- required insights
- confidence/provenance requirements

### 3. Metrics Retrieval Layer
- fetch only validated metrics from the core analytics system
- support aggregation, ranking, slicing, filtering, and trend analysis
- support cross-domain joins such as cycle time + flake rate + after-hours work

### 4. Chart Generator
Support auto-generation of:
- line charts
- bar charts
- stacked composition charts
- heatmaps
- ranking tables
- trend deltas
- scorecards

Charts must be:
- live against current data
- configurable by team, repo, service, and time range
- embeddable into saved reports

### 5. Narrative Generation
The system generates:
- key findings
- trend summaries
- anomaly descriptions
- comparison narratives
- risk callouts
- recommended next questions

Narrative must be constrained by:
- available metrics only
- explicit time window
- known entity scope
- thresholded confidence rules

### 6. Saved Reports
Users can:
- save a generated report definition
- rerun it on demand
- parameterize team, repo, and date ranges
- clone and modify templates

### 7. Scheduled Reports
Users can schedule recurring reports:
- weekly
- monthly
- post-release
- post-incident
- end-of-sprint

Delivery targets can include:
- in-app dashboard/report center
- markdown export
- email or Slack delivery in later phases

### 8. Recommended Reports
System suggests report templates such as:
- Weekly Engineering Health
- Team Delivery and Quality Review
- CI Stability and Test Reliability
- Burnout Risk and Flow State Trends
- Release Readiness Overview

## Example Report Structure
### Weekly Engineering Health Report
- Executive summary
- Delivery trends
- Quality and TestOps trends
- Collaboration and review health
- Well-being signals
- Risks and anomalies
- Recommended actions

## UX Requirements
- Prompt box with examples
- Preview of parsed report scope before execution
- Editable report outline
- Live chart rendering in the report body
- Ability to pin or remove sections
- Provenance panel showing:
  - data sources used
  - metrics used
  - time range
  - filters applied
- One-click save as template
- One-click export to markdown

## Trust and Guardrails
This feature will fail if it becomes a hallucination engine. Requirements:

- narrative claims must reference computed metrics
- unsupported metrics must be omitted or explicitly marked unsupported
- every report must display time window and filter scope
- every generated insight should have a confidence state:
  - direct metric fact
  - inferred from correlated signals
  - hypothesis needing further validation
- no freeform recommendations without supporting evidence

## Report DSL / Internal Spec
Every generated report should compile to an internal structured format, for example:

```yaml
report_type: weekly_engineering_health
scope:
  teams: ["platform"]
time_range:
  start: 2026-04-01
  end: 2026-04-07
sections:
  - summary
  - delivery
  - quality
  - testops
  - wellbeing
charts:
  - metric: cycle_time
    type: line
    group_by: week
  - metric: flaky_test_rate
    type: bar
    group_by: repo
insights:
  - trend_deltas
  - anomalies
  - top_risks
