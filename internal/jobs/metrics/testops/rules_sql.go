package testops

// PipelineFailureRateWindowSQL is the pipeline failure rate of a set of testops_pipeline_metrics_daily rows (already
// the newest row of each key): their failures over their completed runs (succeeded or failed), NULL when none
// completed. The stored failure_rate column holds 0 on a day with no completed run and is not read.
const PipelineFailureRateWindowSQL = `if(sum(success_count) + sum(failure_count) = 0, NULL, ` +
	`toFloat64(sum(failure_count)) / toFloat64(sum(success_count) + sum(failure_count)))`

// PipelineRateCountColumns are the testops_pipeline_metrics_daily columns PipelineFailureRateWindowSQL reads.
var PipelineRateCountColumns = []string{"success_count", "failure_count"}

// StabilityLiveRowsSQL is the one read of testops_pipeline_stability values: the newest stability_index of each
// (day, repo_id) of {org_id:String} between {start:Date} and {end:Date}, without the repo-days whose newest
// testops_pipeline_metrics_daily row has no completed run. The stability writer writes no row for such a day, but a
// row written before the success rate counted completed runs only stays the newest row of its key (the table keeps
// it), so the read leaves it out: a day with no completed run has no stability, never an old 0. A stability row with
// no pipeline row is kept (the outer side of the join reads NULL under join_use_nulls = 1, which the caller sets).
const StabilityLiveRowsSQL = `SELECT stability.day AS day, stability.repo_id AS repo_id, stability.stability_index AS stability_index
FROM (
    SELECT day, repo_id, argMax(stability_index, computed_at) AS stability_index
    FROM testops_pipeline_stability
    WHERE org_id = {org_id:String}
      AND day >= {start:Date}
      AND day <= {end:Date}
    GROUP BY day, repo_id
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
