# TestOps Dependency Boundaries and Interface Contracts

This document defines the parallel work tracks, ownership boundaries, and interface contracts for the TestOps agent teams. It ensures that independent teams can implement their respective tracks without overlapping responsibilities or creating circular dependencies.

## Parallel Tracks and Ownership

The TestOps implementation is divided into seven parallel tracks. Each track has a specific scope of ownership and defined dependencies.

### Track A: Data Contracts and Platform Foundations
Track A provides the foundational definitions for the entire TestOps system.
* **Owns**: Schema definitions, metric registry, and interface boundaries.
* **Consumes**: Nothing.
* **Produces**: Canonical schema types and table structures.
* **Interface Contract**: Defines all TypedDicts and dataclasses in `metrics/testops_schemas.py` and ClickHouse table structures in `migrations/clickhouse/029_testops_tables.sql`.

### Track B: TestOps Ingestion
Track B handles the retrieval and normalization of raw data from external providers.
* **Owns**: Connectors and processors for CI/CD and test data.
* **Consumes**: Track A schemas.
* **Produces**: Raw ingested data persisted in ClickHouse.
* **Interface Contract**: Writes to `ci_pipeline_runs`, `ci_job_runs`, `test_suite_results`, `test_case_results`, and `coverage_snapshots`.

### Track C: TestOps Metrics and Risk Models
Track C transforms raw ingested data into actionable metrics and scores.
* **Owns**: Metric computation and derived risk scores.
* **Consumes**: Track B ingested data.
* **Produces**: Aggregated metrics and risk assessments.
* **Interface Contract**: Reads from ingestion tables; writes to `testops_pipeline_metrics_daily`, `testops_test_metrics_daily`, and `testops_coverage_metrics_daily`.

### Track D: TestOps UX and Reporting Surfaces
Track D provides the visual interface for users to interact with TestOps data.
* **Owns**: Dashboards, drill-down views, and PR widgets in `dev-health-web`.
* **Consumes**: Track C metrics.
* **Produces**: User-facing visualizations and reporting components.
* **Interface Contract**: Consumes metrics tables via the GraphQL analytics API.

### Track E: AI Report Planning and Grounded Generation
Track E manages the intelligent generation of narrative reports based on metrics.
* **Owns**: Prompt parsing, report planner, and narrative renderer.
* **Consumes**: Track A schemas and Track C metrics.
* **Produces**: Report plans, chart specifications, and insight blocks.
* **Interface Contract**: Produces `ReportPlan` and `ChartSpec` objects.

### Track F: Saved Reports, Scheduling, and Delivery
Track F handles the persistence and distribution of generated reports.
* **Owns**: Report persistence, scheduling logic, and delivery targets.
* **Consumes**: Track E rendered reports.
* **Produces**: Scheduled deliveries and persisted report instances.
* **Interface Contract**: Consumes `InsightBlock` and `ProvenanceRecord` objects.

### Track G: Guardrails, Provenance, and Evaluation
Track G ensures the trust and accuracy of AI-generated content.
* **Owns**: Cross-cutting trust enforcement and evaluation.
* **Consumes**: Track E and Track F outputs.
* **Produces**: Evaluation scores and provenance verification.
* **Interface Contract**: Reviews `InsightBlock` and `ProvenanceRecord` for accuracy and grounding.

## Dependency DAG

The following sequence defines the order of implementation and data flow:
1. **Track A** must complete first to freeze schemas and table structures.
2. **Tracks B and E** can start once Track A is complete.
3. **Track C** depends on Track B for ingested data.
4. **Computed metrics** from Track C are required for Track D.
5. **Track F** depends on Track E for report rendering.
6. **Track G** is cross-cutting and reviews outputs from Tracks E and F.

## Frozen Interfaces

### Ingestion Layer
The ingestion process writes to the following ClickHouse tables:
* `ci_pipeline_runs` (extended)
* `ci_job_runs`
* `test_suite_results`
* `test_case_results`
* `coverage_snapshots`

### Metrics Layer
The metrics process reads from the ingestion tables and writes to:
* `testops_pipeline_metrics_daily`
* `testops_test_metrics_daily`
* `testops_coverage_metrics_daily`

### Reporting Layer
* **Report Planner**: Produces `ReportPlan` and `ChartSpec`.
* **Report Renderer**: Consumes `ReportPlan` and metrics; produces `InsightBlock` and `ProvenanceRecord`.
* **UX**: Consumes metrics tables via the GraphQL analytics API.

## Data Freshness and Persistence Rules

### Freshness Rules
* **Ingestion**: Data is ingested incrementally with support for backfilling historical data.
* **Metrics**: Metrics are computed as daily rollups. The `computed_at` field is used for versioning to allow for re-computations.

### Persistence Rule
Persistence must go through sinks only. The system doesn't support file exports or debug dumps for data storage. All data must be persisted to the designated ClickHouse or Postgres backends via the established sink patterns.
