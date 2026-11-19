package testops

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

// TestPassRateWindowSQL is the test pass rate of a set of testops_test_metrics_daily rows (already the newest row of
// each key): their passed cases over their passed and failed cases, NULL when they have neither. The stored pass_rate
// column divides by every case, skipped ones too, holds 0 on a day where every case was skipped, and is not read.
const TestPassRateWindowSQL = `if(sum(passed_count) + sum(failed_count) = 0, NULL, ` +
	`toFloat64(sum(passed_count)) / toFloat64(sum(passed_count) + sum(failed_count)))`

// TestRateCountColumns are the testops_test_metrics_daily columns TestPassRateWindowSQL reads.
var TestRateCountColumns = []string{"passed_count", "failed_count"}

// TestFlakeRateWindowSQL is the test flake rate of a set of testops_test_metrics_daily rows (already the newest row of
// each key): their flake cases over their distinct cases, NULL when no row is measured. A row is measured when its
// flake_rate, flake_cases and distinct_cases are all non-NULL (a day that measured no case, or a row stored before the
// counts existed, is in neither sum). The stored flake_rate column is a mean-able ratio of unequal days and is not read
// as a value.
const TestFlakeRateWindowSQL = `if(sumIf(distinct_cases, flake_rate IS NOT NULL AND flake_cases IS NOT NULL AND distinct_cases IS NOT NULL) = 0, NULL, ` +
	`toFloat64(sumIf(flake_cases, flake_rate IS NOT NULL AND flake_cases IS NOT NULL AND distinct_cases IS NOT NULL)) / ` +
	`toFloat64(sumIf(distinct_cases, flake_rate IS NOT NULL AND flake_cases IS NOT NULL AND distinct_cases IS NOT NULL)))`

// TestRetryDependencyRateWindowSQL is the test retry dependency rate of the same rows: their retry-dependent cases over
// their distinct cases, NULL when no row is measured. No column stores the retry-dependent count; the stored rate is
// that count over distinct_cases, so retry_dependency_rate times distinct_cases is the count (a float, not a stored
// integer). A row is measured when its retry_dependency_rate and distinct_cases are non-NULL.
const TestRetryDependencyRateWindowSQL = `if(sumIf(distinct_cases, retry_dependency_rate IS NOT NULL AND distinct_cases IS NOT NULL) = 0, NULL, ` +
	`sumIf(retry_dependency_rate * distinct_cases, retry_dependency_rate IS NOT NULL AND distinct_cases IS NOT NULL) / ` +
	`toFloat64(sumIf(distinct_cases, retry_dependency_rate IS NOT NULL AND distinct_cases IS NOT NULL)))`

// TestFlakeRateCountColumns and TestRetryDependencyRateCountColumns are the columns the two case-count rules read.
var (
	TestFlakeRateCountColumns           = []string{"flake_rate", "flake_cases", "distinct_cases"}
	TestRetryDependencyRateCountColumns = []string{"retry_dependency_rate", "distinct_cases"}
)

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
