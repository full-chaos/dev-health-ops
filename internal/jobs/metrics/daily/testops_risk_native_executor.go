package daily

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
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

	// job_daily.py builds repo_team_resolver/repo_names_by_id ONCE per run
	// (build_repo_pattern_resolver(teams_data), discovered_repos) and
	// passes the SAME pair into every family's compute call, testops
	// included. Reuse the identical LoadWellbeingTeams/NewRepoPatternResolver/
	// LoadRepoNames this package already built for team_wellbeing (CHAOS-4276)
	// rather than a second implementation of the same query+resolver --
	// codex adversarial review round 1 (CHAOS-4294) caught that testops_risk
	// had NO team resolution at all in an earlier revision, silently
	// dropping every repo-pattern-derived team_id to nil.
	teams, err := LoadWellbeingTeams(ctx, executor.conn, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	repoResolver := NewRepoPatternResolver(teams)
	repoNamesByID, err := LoadRepoNames(ctx, executor.conn, run.OrganizationID, repoIDs)
	if err != nil {
		return 0, err
	}

	day := chDate(run.TargetDay)
	start := day
	end := start.Add(24 * time.Hour)
	historyStart := day.AddDate(0, 0, -29)
	priorCoverageStart := day.AddDate(0, 0, -30)
	computedAt := executor.nowUTC()

	var releaseConfidence []testopsReleaseConfidenceRow
	var qualityDrag []testopsQualityDragRow
	var pipelineStability []testopsPipelineStabilityRow

	for _, repoID := range repoIDs {
		repoName := repoNamesByID[repoID.String()]

		pipelineRuns, err := loadTestopsPipelineRuns(ctx, executor.conn, run.OrganizationID, repoID, start, end)
		if err != nil {
			return 0, err
		}
		pipelineMetrics := testops.ComputePipelineMetrics(repoID, pipelineRuns, repoName, repoResolver)

		suites, cases, err := loadTestopsSuiteAndCaseRows(ctx, executor.conn, run.OrganizationID, repoID, start, end)
		if err != nil {
			return 0, err
		}
		historicalFailedNames, err := loadHistoricalFailedCaseNames(
			ctx, executor.conn, run.OrganizationID, repoID, historyStart, start, end,
		)
		if err != nil {
			return 0, err
		}
		testMetrics := testops.ComputeTestMetrics(repoID, suites, cases, historicalFailedNames, repoName, repoResolver)

		coverageRows, err := loadTestopsCoverageSnapshots(ctx, executor.conn, run.OrganizationID, repoID, start, end)
		if err != nil {
			return 0, err
		}
		priorCoverageRows, err := loadTestopsCoverageSnapshots(ctx, executor.conn, run.OrganizationID, repoID, priorCoverageStart, start)
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

		if row := computeReleaseConfidence(repoID, day, pipeRepresentative, testRepresentative, coverageMetric, computedAt); row != nil {
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
		return 0, err
	}
	return written, nil
}

var _ NativeFamilyExecutor = (*TestopsRiskExecutor)(nil)
