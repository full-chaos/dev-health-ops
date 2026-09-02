// Package cicd is the native Go port of the cicd metrics.daily family
// (CHAOS-4292): compute_cicd_metrics_daily (src/dev_health_ops/metrics/
// compute_cicd.py), fed by load_cicd_data's pipeline half (src/dev_health_ops/
// metrics/loaders/clickhouse.py:1196), writing cicd_metrics_daily.
//
// Fidelity notes (the things a straightforward re-read of compute_cicd.py
// would NOT tell you, verified against the actual Python call path in
// job_daily.py and loaders/clickhouse.py):
//
//   - DOUBLE WINDOW FILTER: load_cicd_data's ClickHouse query filters
//     ci_pipeline_runs by finished_at IN [day start, day end). compute_cicd_
//     metrics_daily THEN re-filters the same rows by started_at IN the SAME
//     window, in-process. A pipeline that started on day D but finished on
//     day D+1 is therefore NEVER counted for cicd, on either day: the loader
//     never fetches it for day D (finished_at is out of window), and it is
//     fetched for day D+1 but then dropped by the started_at re-filter. This
//     package reproduces BOTH filters (LoadPipelineRuns applies the
//     finished_at filter in SQL; ComputeCICDMetricsDaily re-applies the
//     started_at filter in-process), matching Python exactly rather than
//     "fixing" what looks like an odd double filter.
//   - NO FINAL: the loader's query does not use FINAL on ci_pipeline_runs
//     (a ReplacingMergeTree), even though sibling testops-pipeline queries in
//     the same file do. This is parity, not an oversight -- see
//     LoadPipelineRuns' doc comment.
//   - A repo with zero pipeline runs in the window produces NO row at all,
//     not a pipelines_count=0 row -- by can never observe: nothing appends
//     to by_repo unless at least one row survives both filters, so the
//     `if pipelines else 0.0` success_rate guard is dead code in the real
//     pipeline. ComputeCICDMetricsDaily preserves this: a repo with no
//     surviving rows gets no CICDMetric.
package cicd

import (
	"time"

	"github.com/google/uuid"
)

// PipelineRunRow mirrors PipelineRunRow (src/dev_health_ops/metrics/
// schemas.py:119): one row read from ci_pipeline_runs.
type PipelineRunRow struct {
	RepoID     uuid.UUID
	RunID      string
	Status     string // "" means Python's None/empty
	QueuedAt   *time.Time
	StartedAt  time.Time
	FinishedAt *time.Time
}

// CICDMetric mirrors CICDMetricsDailyRecord (schemas.py:624): one
// cicd_metrics_daily row.
type CICDMetric struct {
	RepoID             uuid.UUID
	Day                time.Time
	PipelinesCount     int
	SuccessRate        float64
	AvgDurationMinutes *float64
	P90DurationMinutes *float64
	AvgQueueMinutes    *float64
	ComputedAt         time.Time
}
