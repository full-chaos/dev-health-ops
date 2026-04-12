# Report Center

The Report Center lets you create, schedule, and manage AI-generated reports that summarize engineering health metrics across your organization.

---

## Overview

Reports combine metric data from ClickHouse with AI-generated insights to produce rendered markdown summaries. Each report is defined once and can be triggered manually or on a cron schedule.

### Key Concepts

| Concept | Description |
|---------|-------------|
| **SavedReport** | A report definition with name, parameters, and optional schedule |
| **ReportRun** | A single execution of a report, with status, duration, and rendered output |
| **ReportPlan** | The structured specification that drives the rendering engine |
| **Parameters** | User-facing configuration (scope, date range, metrics) stored on the report |

---

## Creating a Report

Navigate to **Reports > New Report** in the web UI to create a report.

### Configuration Options

| Field | Description | Example |
|-------|-------------|---------|
| **Name** | Display name for the report | "Weekly Engineering Health" |
| **Description** | What the report covers | "DORA metrics overview for the platform team" |
| **Scope** | Organization, Team, or Repository | Organization |
| **Date Range** | Time window for metrics | Last 7 Days |
| **Metrics** | Which metrics to include | Deployment Frequency, Lead Time, Change Failure Rate |
| **Schedule** | How often to auto-run | Weekly, Monthly, or Manual only |

When a report is created without an explicit `ReportPlan`, the system generates a default plan from the parameters at execution time.

---

## Running a Report

### Manual Trigger

Click **Run Now** on the report detail page. This creates a `ReportRun` with status `PENDING` and dispatches a Celery task to the `reports` queue.

### Scheduled Execution

Reports with a schedule (Weekly or Monthly) are triggered automatically by the `dispatch_scheduled_reports` beat task, which runs every 5 minutes and checks for due reports based on their cron expression.

| Schedule | Cron Expression |
|----------|----------------|
| Weekly | `0 9 * * 1` (Mondays at 09:00 UTC) |
| Monthly | `0 9 1 * *` (1st of month at 09:00 UTC) |

---

## Execution Pipeline

```
Trigger (UI or scheduler)
  → ReportRun created (PENDING)
  → Celery task dispatched to `reports` queue
  → Worker builds/loads ReportPlan
  → Engine fetches metrics from ClickHouse
  → Charts rendered, insights generated
  → Markdown assembled with provenance
  → ReportRun updated (SUCCESS + rendered content)
```

If execution fails, the `ReportRun` is marked `FAILED` with the error message and traceback stored for debugging. See the [Report Failures Runbook](../ops/runbook-report-failures.md) for troubleshooting.

---

## Report Detail Page

The detail page shows:

- **Latest Rendered Report** — The markdown output from the most recent successful run
- **Configuration** — Scope, date range, schedule, and selected metrics
- **Run History** — Table of all executions with status, duration, and trigger type

### Actions

| Action | Description |
|--------|-------------|
| **Edit** | Inline edit of report name and description |
| **Clone** | Create a copy with a new name |
| **Delete** | Permanently remove the report and its schedule |
| **Run Now** | Trigger immediate execution |

---

## GraphQL API

Reports are managed entirely through the GraphQL API. The web UI is a consumer of these operations.

### Queries

```graphql
# List all reports for an org
query {
  savedReports(orgId: "my-org", limit: 50) {
    items { id name lastRunStatus lastRunAt }
    total
  }
}

# Get a single report with full details
query {
  savedReport(orgId: "my-org", reportId: "uuid") {
    id name description parameters scheduleId
    lastRunAt lastRunStatus
  }
}

# Get run history
query {
  reportRuns(orgId: "my-org", reportId: "uuid", limit: 10) {
    items { id status startedAt durationSeconds renderedMarkdown triggeredBy }
    total
  }
}
```

### Mutations

```graphql
# Create a report
mutation {
  createSavedReport(orgId: "my-org", input: {
    name: "Weekly Health"
    description: "DORA metrics summary"
    scheduleCron: "0 9 * * 1"
    parameters: { scope: "org", dateRange: "last_7_days", metrics: ["Lead Time"] }
  }) {
    id name
  }
}

# Trigger execution
mutation {
  triggerReport(orgId: "my-org", reportId: "uuid") {
    id status startedAt
  }
}
```

---

## Infrastructure Requirements

- **ClickHouse** must be running and accessible (`CLICKHOUSE_URI`)
- **PostgreSQL** stores report definitions and run records
- **Celery worker** must be consuming the `reports` queue:
  ```bash
  dev-hops workers start-worker --queues default metrics sync reports
  ```
- **Celery beat** must be running for scheduled reports

---

## Data Model

### SavedReport (PostgreSQL)

| Field | Type | Description |
|-------|------|-------------|
| `id` | UUID | Primary key |
| `org_id` | String | Organization scope |
| `name` | String | Display name |
| `description` | String | Optional description |
| `report_plan` | JSON | Structured ReportPlan (or empty for auto-generation) |
| `parameters` | JSON | User-facing config (scope, dateRange, metrics) |
| `schedule_id` | UUID | FK to ScheduledJob (if scheduled) |
| `is_active` | Boolean | Whether the report is active |
| `last_run_at` | DateTime | Timestamp of last execution |
| `last_run_status` | String | Status of last execution |

### ReportRun (PostgreSQL)

| Field | Type | Description |
|-------|------|-------------|
| `id` | UUID | Primary key |
| `report_id` | UUID | FK to SavedReport |
| `status` | String | PENDING, RUNNING, SUCCESS, FAILED |
| `rendered_markdown` | Text | The generated report content |
| `duration_seconds` | Float | Execution time |
| `provenance_records` | JSON | Audit trail of data sources used |
| `triggered_by` | String | "manual" or "scheduler" |
| `error` | Text | Error message (if failed) |
