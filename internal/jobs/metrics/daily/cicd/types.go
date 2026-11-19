// Package cicd is the native Go port of the cicd metrics.daily family
// (CHAOS-4292): compute_cicd_metrics_daily (src/dev_health_ops/metrics/
// compute_cicd.py), fed by load_cicd_data's pipeline half (src/dev_health_ops/
// metrics/loaders/clickhouse.py:1196), writing cicd_metrics_daily.
//
// Fidelity notes (the things a straightforward re-read of compute_cicd.py
// would NOT tell you, verified against the actual Python call path in
// job_daily.py and loaders/clickhouse.py):
//
//   - DAY OF A RUN: a finished run counts on the UTC day it FINISHES
//     (finished_at in [day start, day end)). started_at is not a day filter:
//     it only feeds the duration (finished_at - started_at) and queue
//     (started_at - queued_at) samples. A run that crosses UTC midnight
//     therefore counts once, on the day it ends. The Python original counted
//     such a run on no day (it also re-filtered by started_at); Python is gone
//     and that rule is not kept.
//   - FINAL: the loader reads ci_pipeline_runs (a ReplacingMergeTree) with
//     FINAL -- see PipelineRunsSource's doc comment.
//   - A repo with zero pipeline runs in the window produces NO row at all,
//     not a pipelines_count=0 row -- by can never observe: nothing appends
//     to by_repo unless at least one row survives the filter, so the
//     `if pipelines else 0.0` success_rate guard is dead code in the real
//     pipeline. ComputeCICDMetricsDaily preserves this: a repo with no
//     surviving rows gets no CICDMetric. A repo whose stored row of the day
//     is a measurement and that a recompute no longer counts gets a
//     retraction row instead (FenceDay, CHAOS-14016).
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
	// PooledRunMinutes/PooledQueueMinutes are the samples of the values above, one element per counted run (parallel,
	// nil = no sample), stored with the row for the pooled report charts.
	PooledRunMinutes   []*float64
	PooledQueueMinutes []*float64
	// PooledSamplesOverCap marks a row whose samples are over the daily cap: it stores none (pooled_samples_stored 2).
	PooledSamplesOverCap bool
	// Retracted marks the row that retires a key the compute no longer produces (FenceDay): no measure, no sample.
	Retracted bool
}
