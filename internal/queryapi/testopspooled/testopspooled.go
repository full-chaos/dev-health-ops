// Package testopspooled holds the one definition of the pooled TestOps duration and queue measures. The tile
// (internal/queryapi/analytics) and the AI report registry (internal/jobs/report) both read it, so they cannot give two
// numbers for one window. The TestOps P95 durations and the queue time are pooled over the raw samples, never taken from the stored
// per-repository-day values: a p95 of a pool cannot be made from the p95s of its parts, and the stored rows hold no
// count of the runs with a queue sample. PIPELINE_DURATION_P95 is the 95th percentile of every run duration of the
// group; TEST_SUITE_DURATION_P95 the same over suite durations; PIPELINE_QUEUE_TIME the mean of the queue samples (their
// total seconds divided by the number of runs with a queue sample). Every series reads the same source and expression: a TEAM series (the owners at the
// request, a co-owned repository WHOLE for each owner), a REPO series, the ORG series (each run once), a breakdown and a
// sankey differ only in the rows they cover.
//
// A sample is the daily writer's own (internal/jobs/metrics/testops/compute.go, safeDurationSeconds and
// TestAccumulator.AddSuite), and a run with the stored status deleted is no sample (ExcludedFromPipelineCounts): a run
// duration is duration_seconds when it is not negative, else finished_at - started_at when that is not negative, else no
// sample; a queue sample is queue_seconds when not negative, else started_at -
// queued_at when not negative; a suite duration is duration_seconds when not negative, with no timestamp fallback. A
// run belongs to the UTC day it started, a suite to the day it started (else finished), as the writer reads them, and
// only the repository-days the writer computed are read (the semi-join on the daily table's keys), so every series
// covers the repository-days of the stored rows. The percentile is linear between closest ranks, the writer's rule
// (quantileExactInclusive).

package testopspooled

const (
	PipelineRunsAlias = "testops_pipeline_runs"
	SuiteRunsAlias    = "testops_suite_runs"

	DurationSample = "duration_sample"
	QueueSample    = "queue_sample"
)

// CountedRunPredicate leaves out a run whose stored status is deleted (the provider no longer has it), as the
// daily writer does. The text is the writer's own predicate (testops.StatusDeleted); query-api does not import the
// writer's package, so a test pins the two to each other.
const CountedRunPredicate = "lowerUTF8(trimBoth(ifNull(status, ''))) != 'deleted'"

// The pooled sources. Neither carries a team column: a TestOps team is the owner of the repository at the request.
const PipelineRunsSource = `(
    SELECT
        org_id,
        repo_id,
        service_id,
        toDate(started_at) AS day,
        if(duration_seconds >= 0, duration_seconds,
            if(finished_at >= started_at, dateDiff('millisecond', started_at, finished_at) / 1000, NULL)) AS duration_sample,
        if(queue_seconds >= 0, queue_seconds,
            if(started_at >= queued_at, dateDiff('millisecond', queued_at, started_at) / 1000, NULL)) AS queue_sample
    FROM ci_pipeline_runs FINAL
    WHERE org_id = {org_id:String}
      AND toDate(started_at) >= {start_date:Date} AND toDate(started_at) < {end_date:Date}
      AND ` + CountedRunPredicate + `
      AND (repo_id, toDate(started_at)) IN (
          SELECT repo_id, day FROM testops_pipeline_metrics_daily
          WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date})
) AS testops_pipeline_runs`

// NewestAttemptSuites keeps, of a run's suite rows, the newest attempt of each report slot: a re-run job's report replaces
// the earlier attempt's. A legacy row (no slot: written before the slot columns, or by GitLab) counts unless its run also has
// slotted rows, which replace it. It is the daily writer's own rule (testopsNewestAttemptSuites in
// internal/jobs/metrics/daily/testops_native_clickhouse.go), taken, as there, over every suite row of the run and not the
// window's; the writer binds its statement to a repository list, this reads the organization, and the chart cells hold the two
// to the same value on re-run data.
const NewestAttemptSuites = `(
        (report_slot != '' AND (repo_id, run_id, report_slot, run_attempt) IN (
          SELECT repo_id, run_id, report_slot, max(run_attempt) FROM test_suite_results FINAL
          WHERE org_id = {org_id:String} AND report_slot != ''
          GROUP BY repo_id, run_id, report_slot))
        OR (report_slot = '' AND (repo_id, run_id) NOT IN (
          SELECT repo_id, run_id FROM test_suite_results FINAL
          WHERE org_id = {org_id:String} AND report_slot != '')))`

const SuiteRunsSource = `(
    SELECT
        org_id,
        repo_id,
        service_id,
        toDate(coalesce(started_at, finished_at)) AS day,
        if(duration_seconds >= 0, duration_seconds, NULL) AS duration_sample
    FROM test_suite_results FINAL
    WHERE org_id = {org_id:String}
      AND toDate(coalesce(started_at, finished_at)) >= {start_date:Date} AND toDate(coalesce(started_at, finished_at)) < {end_date:Date}
      AND ` + NewestAttemptSuites + `
      AND (repo_id, toDate(coalesce(started_at, finished_at))) IN (
          SELECT repo_id, day FROM testops_test_metrics_daily
          WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date})
) AS testops_suite_runs`

// P95 is the 95th percentile of a group's duration samples, NULL for a group with none.
const P95 = "if(count(" + DurationSample + ") = 0, NULL, quantileExactInclusive(0.95)(" + DurationSample + "))"

// QueueMean is the mean of the queue samples of a group (exactSums takes it as the exact sum over the non-NULL
// count), NULL for a group with none.
const QueueMean = "AVG(" + QueueSample + ")"

// Median is the median of a group's duration samples (linear between the two middle samples of an even count, the writer's
// median), NULL for a group with none.
const Median = "if(count(" + DurationSample + ") = 0, NULL, quantileExactInclusive(0.5)(" + DurationSample + "))"

// QueueP95 is the 95th percentile of a group's queue samples, NULL for a group with none.
const QueueP95 = "if(count(" + QueueSample + ") = 0, NULL, quantileExactInclusive(0.95)(" + QueueSample + "))"

// MaxRowsToRead is the default row bound of one pooled statement; see analytics.TestopsPooledMaxRowsToRead.
const MaxRowsToRead uint64 = 5_000_000
