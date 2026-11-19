package daily

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// TestopsRiskExecutor is the NATIVE implementation of the testops_risk
// metrics.daily family (CHAOS-4294): testops_release_confidence,
// testops_quality_drag, testops_pipeline_stability.
//
// # Fidelity
//
// The Python job is the authority (src/dev_health_ops/metrics/
// compute_testops_risk.py, invoked from job_daily.py:1904-1937). See this
// package's testops_risk_native_clickhouse.go doc comment for the two
// load-bearing subtleties this executor's structure exists to reproduce:
//
//  1. The Python bridge calls run_daily_metrics_job ONCE PER repo_id with
//     backfill_days=1 (worker_metrics.py:1749-1760, CHAOS-4264), so every
//     input this family reads -- including pipeline_stability's "7-day"
//     buffer -- is scoped to exactly one repo and one day per call, never
//     more. This executor's own per-repo loop below mirrors that call
//     boundary exactly, the same way TeamWellbeingExecutor's
//     computeWellbeingPerRepo mirrors it for team_wellbeing.
//  2. testops_pipeline_metrics/testops_test_metrics/testops_coverage_metrics
//     are LOCAL, in-process Python values (job_daily.py:1602-1626), not
//     re-read from ClickHouse -- and Go's native families compute BEFORE
//     the compatibility bridge (daily.go Work()), so those bridge-written
//     tables do not carry today's rows yet when this executor runs. This
//     executor therefore recomputes the same pipeline/test/coverage
//     aggregation Python performs in-process, via the sibling
//     internal/jobs/metrics/testops package (a full, exported port of
//     compute_testops.py -- see that package's doc comment for why it is
//     its own package rather than private helpers here: CHAOS-4284 is
//     meant to import and reuse it verbatim) -- purely as an in-memory
//     input to this executor's own three risk-model functions. This
//     executor never writes testops_{pipeline,test,coverage}_metrics_daily
//     itself; those tables stay the Python bridge's responsibility
//     (CHAOS-4284, a separate family, still "pending" in families.json).
type TestopsRiskExecutor struct {
	conn   driver.Conn
	nowUTC func() time.Time
}

var errTestopsRiskUnavailable = errors.New("testops_risk native executor unavailable")

// NewTestopsRiskExecutor fails closed, mirroring NewTeamWellbeingExecutor's
// construction contract.
func NewTestopsRiskExecutor(conn driver.Conn) (*TestopsRiskExecutor, error) {
	if conn == nil {
		return nil, errTestopsRiskUnavailable
	}
	return &TestopsRiskExecutor{conn: conn, nowUTC: func() time.Time { return time.Now().UTC() }}, nil
}

// ComputeFamily runs the testops_risk computation for one partition.
func (executor *TestopsRiskExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errTestopsRiskUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}
	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}

	// TestOps rows use the repository's one authoritative owner, as of the
	// end of the target day. This is the same resolver the native
	// pipeline/test/coverage executors use (CHAOS-8512).
	day := chDate(run.TargetDay)
	end := day.Add(24 * time.Hour)
	repoResolver, err := loadTestopsRepoOwnerResolver(ctx, executor.conn, run.OrganizationID, end)
	if err != nil {
		return 0, err
	}
	start := day
	historyStart := day.AddDate(0, 0, -29)
	priorCoverageStart := day.AddDate(0, 0, -30)
	computedAt := executor.nowUTC()

	var releaseConfidence []testopsReleaseConfidenceRow
	var qualityDrag []testopsQualityDragRow
	var pipelineStability []testopsPipelineStabilityRow

	testsUnknown, err := loadTestopsRiskTestsUnknown(ctx, executor.conn, run.OrganizationID, repoIDs, historyStart, start, end)
	if err != nil {
		return 0, err
	}

	for _, repoID := range repoIDs {
		repoName := repoID.String()

		// Stream straight into the same accumulators the sibling
		// testops_pipeline/testops_test native families use
		// (testops_native_clickhouse.go), instead of materialising a whole
		// day's ci_pipeline_runs/test_suite_results/test_case_results into
		// slices first. test_case_results is reduced to one row per
		// case_name INSIDE ClickHouse (loadNativeTestopsCaseAggregate) -- the
		// allocation choke a large CI day used to hit here -- so this path
		// carries no row cap.
		pipelineAccumulator := testops.NewPipelineAccumulator(repoID, repoName, repoResolver)
		if err := loadNativeTestopsPipelineRuns(
			ctx, executor.conn, pipelineAccumulator, run.OrganizationID, repoID, start, end,
		); err != nil {
			return 0, err
		}
		pipelineMetrics := pipelineAccumulator.Finish()

		// The same record testops_test writes. Within one partition pass the
		// two families share its computation, so the repo's case history is
		// scanned once rather than once per family -- see
		// testopsTestMetricShare for why that leaves each family's failure
		// and ledger semantics its own.
		testMetrics, _, err := loadTestopsTestMetrics(
			ctx, executor.nowUTC, executor.conn, run.OrganizationID, repoID, repoName, repoResolver, historyStart, start, end,
		)
		if err != nil {
			return 0, err
		}

		// coverage_snapshots per repo/day is small (a handful of rows), so
		// the single-row-per-window pushdown reduction below is a
		// simplification, not a memory fix -- but reusing it avoids a second
		// copy of the same "latest by (run_id, snapshot_id)" query.
		coverageRows, err := loadNativeTestopsLatestCoverage(ctx, executor.conn, run.OrganizationID, repoID, start, end)
		if err != nil {
			return 0, err
		}
		priorCoverageRows, err := loadNativeTestopsLatestCoverage(ctx, executor.conn, run.OrganizationID, repoID, priorCoverageStart, start)
		if err != nil {
			return 0, err
		}
		coverageMetric := testops.ComputeCoverageMetric(repoID, coverageRows, priorCoverageRows, repoName, repoResolver)

		// pipe_by_repo[repo_id] in Python ends up as whichever
		// (team_id, service_id) group sorts LAST for this repo -- see
		// testops.ComputePipelineMetrics's doc comment. Its own per-repo
		// output is already in that sorted order, so the last element is
		// the exact same "representative" pipeline row Python's dict
		// overwrite would leave.
		var pipeRepresentative *testops.PipelineMetric
		if n := len(pipelineMetrics); n > 0 {
			pipeRepresentative = &pipelineMetrics[n-1]
		}
		var testRepresentative *testops.TestMetric
		if len(testMetrics) > 0 {
			testRepresentative = &testMetrics[0]
		}
		// No test row, CI ran, and tests exist for the repo (a suite in the 29 days before, or a run of the day named
		// partial): the tests were not read, so the flake and recurrence factors are unknown, not 0.
		if _, unknown := testsUnknown[repoID]; unknown && testRepresentative == nil && pipeRepresentative != nil {
			testRepresentative = unknownTestMetric(repoID, pipeRepresentative)
			testMetrics = []testops.TestMetric{*testRepresentative}
		}

		confidencePipe, confidenceTest := repoSummedConfidenceInputs(pipeRepresentative, pipelineMetrics, testRepresentative, testMetrics)
		if row := computeReleaseConfidence(repoID, day, confidencePipe, confidenceTest, coverageMetric, computedAt); row != nil {
			releaseConfidence = append(releaseConfidence, *row)
		}
		if row := computeQualityDrag(repoID, day, pipeRepresentative, testRepresentative, computedAt); row != nil {
			qualityDrag = append(qualityDrag, *row)
		}
		if row := computePipelineStability(repoID, day, pipelineMetrics, computedAt); row != nil {
			pipelineStability = append(pipelineStability, *row)
		}
	}

	written, err := writeTestopsRisk(ctx, executor.conn, run.OrganizationID, releaseConfidence, qualityDrag, pipelineStability)
	if err != nil {
		// codex sweep (CHAOS-5190 r3 follow-up, team-lead-requested):
		// writeTestopsRisk's OWN return value already correctly threads the
		// true partial count through all three of its sequential-table
		// failure paths (testops_release_confidence, testops_quality_drag,
		// testops_pipeline_stability) -- but this call site used to discard
		// it with a bare `return 0, err`, exactly the class already fixed in
		// work_item_state/work_item/work_item_estimate/work_graph_edges/
		// ai_governance/repo_user_commit. A later table's failure after an
		// earlier one already landed rows must not read as a full refusal.
		return wrapTestopsRiskPartialWrite(written, err)
	}
	retired, err := retireTestopsRiskDays(ctx, executor.conn, run.OrganizationID, day, repoIDs, computedAt,
		releaseConfidence, qualityDrag, pipelineStability)
	if err != nil {
		return wrapTestopsRiskPartialWrite(written+retired, err)
	}
	return written + retired, nil
}

// retireTestopsRiskDays applies the stale-key rule (stale_team_keys.go) to the three tables after the family wrote
// its rows: a repository of the partition whose stored row of the day is live and that the compute gave no row for
// that table now gets a row that retires the key, so the day reads as absent, never 0. A repository with no stored
// row gets nothing: missing stays missing. Each table is decided alone, since one compute can produce a row for one
// table and none for another. The marker carries computedAt, the snapshot of the compute (taken before its reads):
// a key whose stored row is not older than it was written by a newer or equal compute and is left alone.
func retireTestopsRiskDays(
	ctx context.Context, conn staleKeyConn, organizationID string, day time.Time, repoIDs []uuid.UUID, computedAt time.Time,
	releaseConfidence []testopsReleaseConfidenceRow, qualityDrag []testopsQualityDragRow,
	pipelineStability []testopsPipelineStabilityRow,
) (int, error) {
	repositories := make([][]string, 0, len(repoIDs))
	for _, repoID := range repoIDs {
		repositories = append(repositories, []string{repoID.String()})
	}
	scope := newStaleKeyScope(repositories...)
	var confidenceKeys, dragKeys, stabilityKeys []staleKey
	for _, row := range releaseConfidence {
		confidenceKeys = append(confidenceKeys, staleKey{row.RepoID.String()})
	}
	for _, row := range qualityDrag {
		dragKeys = append(dragKeys, staleKey{row.RepoID.String()})
	}
	for _, row := range pipelineStability {
		stabilityKeys = append(stabilityKeys, staleKey{row.RepoID.String()})
	}
	retired := 0
	count, err := supersedeStaleTeamKeys(ctx, conn, teamkeytables.TestopsReleaseConfidence, organizationID, day, scope,
		confidenceKeys, computedAt)
	retired += count
	if err != nil {
		return retired, fmt.Errorf("retire stale testops_release_confidence days: %w", err)
	}
	count, err = supersedeStaleTeamKeys(ctx, conn, teamkeytables.TestopsQualityDrag, organizationID, day, scope,
		dragKeys, computedAt)
	retired += count
	if err != nil {
		return retired, fmt.Errorf("retire stale testops_quality_drag days: %w", err)
	}
	count, err = supersedeStaleTeamKeys(ctx, conn, teamkeytables.TestopsPipelineStability, organizationID, day, scope,
		stabilityKeys, computedAt)
	retired += count
	if err != nil {
		return retired, fmt.Errorf("retire stale testops_pipeline_stability days: %w", err)
	}
	return retired, nil
}

// wrapTestopsRiskPartialWrite mirrors the sibling wrap helpers' exact
// shape: written == 0 returns the error unwrapped (nothing landed);
// written > 0 wraps ErrPartialWrite naming the true row count already
// durable across testops_release_confidence/testops_quality_drag/
// testops_pipeline_stability.
func wrapTestopsRiskPartialWrite(written int, err error) (int, error) {
	if written == 0 {
		return 0, err
	}
	return written, fmt.Errorf(
		"%w: testops_risk failed after %d row(s) already landed across testops_release_confidence/testops_quality_drag/testops_pipeline_stability: %w",
		ErrPartialWrite, written, err)
}

var _ NativeFamilyExecutor = (*TestopsRiskExecutor)(nil)

// repoSummedConfidenceInputs returns the pipeline and test metrics the release confidence reads: the
// representative groups (they carry the row's team, service and the flake inputs) with their counts replaced
// by the sums over every (team, service) group of the repository, so the factors are the repository's rate and
// not one group's. The quality drag and the stability rows keep the representatives and the group list.
func repoSummedConfidenceInputs(
	pipe *testops.PipelineMetric, pipelines []testops.PipelineMetric,
	test *testops.TestMetric, tests []testops.TestMetric,
) (*testops.PipelineMetric, *testops.TestMetric) {
	if pipe != nil {
		summed := *pipe
		summed.SuccessCount, summed.FailureCount = 0, 0
		for _, group := range pipelines {
			summed.SuccessCount += group.SuccessCount
			summed.FailureCount += group.FailureCount
		}
		pipe = &summed
	}
	if test != nil {
		summed := *test
		summed.PassedCount, summed.FailedCount = 0, 0
		for _, group := range tests {
			summed.PassedCount += group.PassedCount
			summed.FailedCount += group.FailedCount
		}
		test = &summed
	}
	return pipe, test
}
