// Package testopspooled holds the one definition of the pooled TestOps duration and queue measures. The tile
// (internal/queryapi/analytics) and the AI report registry (internal/jobs/report) both read it, so they cannot give two
// numbers for one window. The TestOps P95 durations and the queue time are pooled over the samples, never taken from the stored
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
// queued_at when not negative, and a re-run (retry_count above 0) has none; a suite duration is duration_seconds when not negative, with no timestamp fallback. A
// run belongs to the UTC day it started, a suite to the day it started (else finished), as the writer reads them. The
// samples of a repository-day are the ones the writer stored with its newest daily row, so every series covers the
// repository-days of the stored rows and reads their sample set; a repository-day whose newest row holds no samples
// (written before migration 161) reads its raw rows. The percentile is linear between closest ranks, the writer's rule
// (quantileExactInclusive).

package testopspooled

import "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"

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

// NewestStoredRows is the newest stored row (the newest computed_at, the stored-value readers' rule) of each
// repository-day of a daily table in the read window, with its pooled_samples_stored marker and the named columns.
func NewestStoredRows(table string, columns ...string) string {
	selected := ""
	for _, column := range columns {
		selected += ", " + column
	}
	return `(
            SELECT org_id, repo_id, day, pooled_samples_stored` + selected + `
            FROM ` + table + `
            WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}
            ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
            LIMIT 1 BY org_id, repo_id, day
        )`
}

// LegacyRepoDays are the repository-days of the read window whose newest stored row holds no samples: a row written
// before migration 161 (pooled_samples_stored 0) or a row over the writers' cap of samples per row (2). Only their
// samples are read from the raw rows, with the daily writer's rules.
func LegacyRepoDays(table string) string {
	return `SELECT repo_id, day FROM ` + NewestStoredRows(table) + ` WHERE pooled_samples_stored != 1`
}

// LiveRepoDays is the live gate of a pooled source, over BOTH of its arms: the repository-days of the read window whose
// NEWEST row of table is a measurement (package liverow). A retired repository-day (its newest row holds retracted = 1, a row
// written with the column defaults, so pooled_samples_stored 0) is left out: the retracted flag wins over the pooled marker,
// so neither the older row's stored samples nor a raw row synced after the retraction is a sample. The rule is applied to
// the newest row of a key, after the GROUP BY of the key. A table with no rule panics (liverow.NewestPredicate).
func LiveRepoDays(table string) string {
	return `(repo_id, day) IN (
        SELECT repo_id, day FROM ` + table + `
        WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}
        GROUP BY repo_id, day
        HAVING ` + liverow.NewestPredicate(table, "") + `)`
}

// The pooled sources. Neither carries a team column: a TestOps team is the owner of the repository at the request. A
// repository-day's samples are the ones the daily writer stored with its newest row (one result row per stored run or
// suite), so a source and the stored value read one sample set; a row synced after the writer ran counts after its next
// run. Only a repository-day whose newest row holds no samples reads the raw rows.
var PipelineRunsSource = `(
    SELECT org_id, repo_id, service_id, day, duration_sample, queue_sample
    FROM (
        SELECT
            org_id,
            repo_id,
            stored_service_id AS service_id,
            day,
            stored_duration AS duration_sample,
            stored_queue AS queue_sample
        FROM ` + NewestStoredRows("testops_pipeline_metrics_daily", "pooled_service_ids", "pooled_durations", "pooled_queues") + `
        ARRAY JOIN pooled_service_ids AS stored_service_id, pooled_durations AS stored_duration, pooled_queues AS stored_queue
        WHERE pooled_samples_stored = 1
        UNION ALL
        SELECT
            org_id,
            repo_id,
            service_id,
            toDate(started_at) AS day,
            if(duration_seconds >= 0, duration_seconds,
                if(finished_at >= started_at, dateDiff('millisecond', started_at, finished_at) / 1000, NULL)) AS duration_sample,
            if(retry_count = 0, if(queue_seconds >= 0, queue_seconds,
                if(started_at >= queued_at, dateDiff('millisecond', queued_at, started_at) / 1000, NULL)), NULL) AS queue_sample
        FROM ci_pipeline_runs FINAL
        WHERE org_id = {org_id:String}
          AND toDate(started_at) >= {start_date:Date} AND toDate(started_at) < {end_date:Date}
          AND ` + CountedRunPredicate + `
          AND (repo_id, toDate(started_at)) IN (` + LegacyRepoDays("testops_pipeline_metrics_daily") + `)
    )
    WHERE ` + LiveRepoDays("testops_pipeline_metrics_daily") + `
) AS testops_pipeline_runs`

// SuiteRunsSource reads, for a repository-day without stored samples, the suite rows the readers count: the newest
// attempt of each report slot, a legacy row only when no kept slotted row lists its id (SuiteRowsNewestAttempt). A var:
// the fragment is built, not written. Both arms read only the keys whose NEWEST row is live (package liverow): a retired
// repository-day (newest row retracted = 1, a row that holds no samples) admits no stored sample and no raw suite.
var SuiteRunsSource = `(
    SELECT org_id, repo_id, service_id, day, duration_sample
    FROM (
        SELECT
            org_id,
            repo_id,
            stored_service_id AS service_id,
            day,
            stored_duration AS duration_sample
        FROM ` + NewestStoredRows("testops_test_metrics_daily", "pooled_service_ids", "pooled_durations") + `
        ARRAY JOIN pooled_service_ids AS stored_service_id, pooled_durations AS stored_duration
        WHERE pooled_samples_stored = 1
        UNION ALL
        SELECT
            org_id,
            repo_id,
            service_id,
            toDate(coalesce(started_at, finished_at)) AS day,
            if(duration_seconds >= 0, duration_seconds, NULL) AS duration_sample
        FROM test_suite_results FINAL
        WHERE org_id = {org_id:String}
          AND toDate(coalesce(started_at, finished_at)) >= {start_date:Date} AND toDate(coalesce(started_at, finished_at)) < {end_date:Date}
          AND ` + SuiteRowsNewestAttempt("", Scope{}) + `
          AND (repo_id, toDate(coalesce(started_at, finished_at))) IN (` + LegacyRepoDays("testops_test_metrics_daily") + `)
    )
    WHERE ` + LiveRepoDays("testops_test_metrics_daily") + `
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
