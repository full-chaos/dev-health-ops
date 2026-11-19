package testops

import "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"

// PipelineFailureRateWindowSQL is the pipeline failure rate of a set of testops_pipeline_metrics_daily rows (already
// the newest row of each key): their failures over their completed runs (succeeded or failed), NULL when none
// completed. The stored failure_rate column holds 0 on a day with no completed run and is not read.
const PipelineFailureRateWindowSQL = `if(sum(success_count) + sum(failure_count) = 0, NULL, ` +
	`toFloat64(sum(failure_count)) / toFloat64(sum(success_count) + sum(failure_count)))`

// PipelineSuccessRateWindowSQL is the pipeline success rate of the same rows: their successes over their completed
// runs (succeeded or failed), NULL when none completed. The stored success_rate column holds 0 on a day with no
// completed run and is not read.
const PipelineSuccessRateWindowSQL = `if(sum(success_count) + sum(failure_count) = 0, NULL, ` +
	`toFloat64(sum(success_count)) / toFloat64(sum(success_count) + sum(failure_count)))`

// PipelineRateCountColumns are the testops_pipeline_metrics_daily columns the two window rules read.
var PipelineRateCountColumns = []string{"success_count", "failure_count"}

// PipelineCancelRateWindowSQL is the pipeline cancel rate of the same rows: their cancelled runs over all their runs
// (pipelines_count, the denominator of the stored cancel_rate), NULL when they have no run. The stored cancel_rate
// column holds 0 on a day with no run and is not read: a mean of it weighs a day of 1 run like a day of 500.
const PipelineCancelRateWindowSQL = `if(sum(pipelines_count) = 0, NULL, ` +
	`toFloat64(sum(cancelled_count)) / toFloat64(sum(pipelines_count)))`

// PipelineRerunRateWindowSQL is the pipeline rerun rate of the same rows: their reruns over all their runs, NULL when
// they have no run. No column stores the rerun count; the stored rerun_rate is reruns / pipelines_count, so
// rerun_rate times pipelines_count is the count (the terms query-api reads). rerun_rate is not Nullable.
const PipelineRerunRateWindowSQL = `if(sum(pipelines_count) = 0, NULL, ` +
	`sum(rerun_rate * pipelines_count) / toFloat64(sum(pipelines_count)))`

// PipelineCancelRateCountColumns and PipelineRerunRateCountColumns are the columns the two run-count rules read.
var (
	PipelineCancelRateCountColumns = []string{"pipelines_count", "cancelled_count"}
	PipelineRerunRateCountColumns  = []string{"pipelines_count", "rerun_rate"}
)

// TestPassRateWindowSQL is the test pass rate of a set of testops_test_metrics_daily rows (already the newest row of
// each key): their passed cases over their passed and failed cases, NULL when they have neither. The stored pass_rate
// column divides by every case, skipped ones too, holds 0 on a day where every case was skipped, and is not read.
const TestPassRateWindowSQL = `if(sum(passed_count) + sum(failed_count) = 0, NULL, ` +
	`toFloat64(sum(passed_count)) / toFloat64(sum(passed_count) + sum(failed_count)))`

// TestRateCountColumns are the testops_test_metrics_daily columns TestPassRateWindowSQL reads.
var TestRateCountColumns = []string{"passed_count", "failed_count"}

// StabilityLiveRowsSQL is the one read of testops_pipeline_stability values: the newest stability_index of each
// (day, repo_id) of {org_id:String} between {start:Date} and {end:Date}, without the repo-days whose newest
// testops_pipeline_metrics_daily row has no completed run. The stability writer writes no row for such a day, but a
// row written before the success rate counted completed runs only stays the newest row of its key (the table keeps
// it), so the read leaves it out: a day with no completed run has no stability, never an old 0. A stability row with
// no pipeline row is kept (the outer side of the join reads NULL under join_use_nulls = 1, which the caller sets). A
// repo-day whose newest stability row retires the day (package liverow) has no stability either.
var StabilityLiveRowsSQL = `SELECT stability.day AS day, stability.repo_id AS repo_id, stability.stability_index AS stability_index
FROM (
    SELECT day, repo_id, argMax(stability_index, computed_at) AS stability_index
    FROM testops_pipeline_stability
    WHERE org_id = {org_id:String}
      AND day >= {start:Date}
      AND day <= {end:Date}
    GROUP BY day, repo_id
    HAVING ` + liverow.NewestPredicate("testops_pipeline_stability", "") + `
) AS stability
LEFT JOIN (
    SELECT day, repo_id, argMax(success_count + failure_count, computed_at) AS completed_runs
    FROM testops_pipeline_metrics_daily
    WHERE org_id = {org_id:String}
      AND day >= {start:Date}
      AND day <= {end:Date}
    GROUP BY day, repo_id
) AS pipeline ON pipeline.day = stability.day AND pipeline.repo_id = stability.repo_id
WHERE pipeline.completed_runs IS NULL OR pipeline.completed_runs > 0`
