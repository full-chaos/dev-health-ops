# AI Generative Reports Architecture

## Overview
The AI Generative Reports system provides a natural-language interface for generating grounded, data-backed engineering health reports. It follows a deterministic pipeline that converts user prompts into structured plans, ensuring that every insight and chart is derived directly from persisted metric data.

## Report DSL
The system uses a structured Domain Specific Language (DSL) to represent report plans and their components.

### ReportPlan
The `ReportPlan` is the central contract for report generation. It defines the scope, time range, and sections of a report.
- **report_type**: The template to use (e.g., `weekly_health`, `monthly_review`).
- **audience**: The target audience level (`executive`, `team_lead`, `developer`).
- **scope**: Lists of teams, repos, and services to include.
- **time_range**: Explicit start and end dates for the analysis.

### ChartSpec
Defines a specific visualization to be included in the report.
- **chart_type**: The visual representation (`line`, `bar`, `heatmap`, `scorecard`).
- **metric**: The canonical metric name from the registry.
- **group_by**: The dimension used for aggregation (e.g., `team`, `week`).

### InsightBlock
Represents a structured observation derived from data analysis.
- **insight_type**: The nature of the insight (`trend_delta`, `anomaly`, `regression`).
- **confidence**: The level of certainty (`direct_fact`, `inferred`, `hypothesis`).
- **supporting_metrics**: References to the specific data points that back the insight.

### ProvenanceRecord
Provides an audit trail for every artifact in a report. It tracks the data sources, filters, and time ranges used to generate a specific chart or insight, ensuring transparency and reproducibility.

## Pipeline Stages

### 1. Prompt Parsing
The `parser.py` module uses regex and keyword matching to extract intent from natural-language prompts.
- **Intent Extraction**: Identifies the requested report type, audience, and metrics.
- **Scope Resolution**: Extracts mentions of teams, repositories, and services.
- **Time Range Parsing**: Handles relative terms (e.g., "last week", "past 30 days") and explicit date ranges.

### 2. Metric & Entity Resolution
The `resolver.py` module maps parsed terms to canonical system entities.
- **Metric Mapping**: Uses the `metric_registry.py` to resolve aliases (e.g., "ci success" -> `success_rate`).
- **Entity Catalog**: Matches team and repo names against the `EntityCatalog` to resolve internal IDs.

### 3. Report Planning
The `planner.py` module assembles the `ReportPlan` and `ChartSpec` objects.
- **Template Selection**: Selects a pre-defined template from `templates.py` based on the report type.
- **Validation**: Ensures that the requested metrics and entities exist and that the time range is valid.
- **Chart Generation**: Populates chart specifications based on template defaults and user overrides.

### 4. Report Rendering (Planned)
The rendering engine is currently in the planning phase. It will be responsible for:
- **Execution**: Querying ClickHouse for the required metric data.
- **Narrative Generation**: Constructing a grounded narrative based on the `ReportPlan`.
- **Insight Extraction**: Identifying trends and anomalies to populate `InsightBlock` records.

## Trust Model
The architecture is built on a foundation of trust and verifiability.
- **Data Grounding**: Only persisted metric data is used for report generation. Freeform LLM claims are forbidden.
- **Confidence Labeling**: Every insight is tagged with a confidence level to distinguish between direct facts and inferred patterns.
- **Provenance**: Every artifact includes a link to its data source and generation parameters.
- **Language Rules**: The system uses cautious language (*appears*, *suggests*) rather than definitive statements (*is*, *was*) for inferred insights.

## Report Templates
The system includes several pre-built templates in `templates.py`:

| Template | Purpose | Key Metrics |
| :--- | :--- | :--- |
| **Weekly Health** | Team-level weekly summary. | `items_completed`, `cycle_time_p50_hours`, `flake_rate` |
| **Monthly Review** | Executive-level monthly trend analysis. | `lead_time_p50_hours`, `success_rate`, `line_coverage_pct` |
| **Quality Trend** | Deep dive into testing and reliability. | `flake_rate`, `failure_rate`, `coverage_regression_count` |
| **CI Stability** | Infrastructure and pipeline health review. | `success_rate`, `median_duration_seconds`, `avg_queue_seconds` |
