package analytics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/chclient"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// The TestOps P95 durations and the queue time are pooled over the raw samples, never taken from the stored
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

const (
	pooledPipelineRunsAlias = testopspooled.PipelineRunsAlias
	pooledSuiteRunsAlias    = testopspooled.SuiteRunsAlias

	pooledDurationSample = testopspooled.DurationSample
	pooledQueueSample    = testopspooled.QueueSample
)

// The pooled expressions and sources are the shared package's (testopspooled): the AI report reads the same text.
const (
	pooledCountedRunPredicate = testopspooled.CountedRunPredicate
	pooledPipelineRunsSource  = testopspooled.PipelineRunsSource
	pooledP95                 = testopspooled.P95
	pooledQueueMean           = testopspooled.QueueMean
)

// pooledSuiteRunsSource is built from the shared newest-attempt fragment, so it is a var.
var pooledSuiteRunsSource = testopspooled.SuiteRunsSource

// pooledSource returns the source of a pooled measure; ok is false for every other measure.
func pooledSource(measure Measure) (source, alias string, ok bool) {
	switch measure {
	case MeasurePipelineDurationP95, MeasurePipelineQueueTime:
		return pooledPipelineRunsSource, pooledPipelineRunsAlias, true
	case MeasureTestSuiteDurationP95:
		return pooledSuiteRunsSource, pooledSuiteRunsAlias, true
	}
	return "", "", false
}

// orgPooledTerms returns the ORG value and the sample count of a pooled measure: the same expression as the dimension
// series, so the ORG series and a team series differ only in the rows they cover. ok is false for every other measure.
func orgPooledTerms(measure Measure) (value, samples string, ok bool) {
	switch measure {
	case MeasurePipelineDurationP95, MeasureTestSuiteDurationP95:
		return pooledP95, "count(" + pooledDurationSample + ")", true
	case MeasurePipelineQueueTime:
		return pooledQueueMean, "count(" + pooledQueueSample + ")", true
	}
	return "", "", false
}

// TestopsPooledMaxRowsToRead bounds the rows one pooled statement reads. The raw tables are keyed by (org_id, repo_id,
// run_id[, suite_id]) with no day key, so a read scans the organization's whole run history before it filters by day.
// 5,000,000 is 14 times the whole production ci_pipeline_runs table and 30 times test_suite_results when it was set
// (350,232 and 162,930 rows over every organization). Past it the read is refused, never served in part. A variable so a
// test can lower it.
var TestopsPooledMaxRowsToRead uint64 = testopspooled.MaxRowsToRead

// pooledReadBoundReason is the reason a refused pooled read carries to the client.
const pooledReadBoundReason = "testops_pooled_read_bound"

// statementSettings is the SETTINGS clause of a statement over source: the time bound, and for a pooled source the
// row bound, which throws (read_overflow_mode = 'throw') so a read past it is refused instead of served in part.
func statementSettings(source string, timeoutSeconds int) string {
	settings := settingsMaxExecutionTime(timeoutSeconds)
	if source == pooledPipelineRunsSource || source == pooledSuiteRunsSource {
		settings += fmt.Sprintf(", max_rows_to_read = %d, read_overflow_mode = 'throw'", TestopsPooledMaxRowsToRead)
	}
	return settings
}

// PooledReadBoundError is a pooled read that hit TestopsPooledMaxRowsToRead (ClickHouse TOO_MANY_ROWS): the measure is
// unavailable, never a partial value or a 0.
type PooledReadBoundError struct {
	Measure Measure
	Code    int32
	err     error
}

func (e *PooledReadBoundError) Error() string {
	return fmt.Sprintf("analytics: %s is unavailable: its read is over the row bound (%d rows)", e.Measure, TestopsPooledMaxRowsToRead)
}

func (e *PooledReadBoundError) Unwrap() error { return e.err }

// Reason is the reason the client is told.
func (e *PooledReadBoundError) Reason() string { return pooledReadBoundReason }

// clickHouseTooManyRows is TOO_MANY_ROWS, the code of max_rows_to_read.
const clickHouseTooManyRows = 158

// pooledReadRefusal turns the error of a pooled measure's read that hit the row bound into a PooledReadBoundError and
// logs it once; every other error is returned as it is.
func pooledReadRefusal(ctx context.Context, orgID string, measure Measure, err error) error {
	if err == nil {
		return nil
	}
	if _, _, pooled := pooledSource(measure); !pooled {
		return err
	}
	var refused *PooledReadBoundError
	if errors.As(err, &refused) {
		return err
	}
	if bound, code := chclient.BoundHit(err); bound != chclient.BoundResult || code != clickHouseTooManyRows {
		return err
	}
	slog.WarnContext(ctx, "analytics: a pooled TestOps read hit its row bound",
		"operation", "analytics.testops_pooled", "org_id", orgID, "measure", orgMeasureLabel(measure),
		"max_rows_to_read", TestopsPooledMaxRowsToRead, "clickhouse_code", clickHouseTooManyRows)
	return &PooledReadBoundError{Measure: measure, Code: clickHouseTooManyRows, err: err}
}
