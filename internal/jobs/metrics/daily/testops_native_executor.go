package daily

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// -----------------------------------------------------------------------
// CHAOS-4284: the three NATIVE metrics.daily families
// testops_pipeline / testops_test / testops_coverage.
//
// # Why three executors and not one
//
// families.json declares them as three separate families, and
// PartitionHandler.computeNativeFamilies (daily.go:576) is per-family
// fail-open: an executor returning an error leaves ONLY its own family off
// the bridge's skip list, so Python still computes and writes that one while
// the other two stay native. Folding all three into a single executor would
// couple their failure modes -- a ClickHouse hiccup reading coverage_snapshots
// would silently push pipeline and test back onto the bridge too, and the
// telemetry (ObserveDailyMetricsNativeFamily, which is keyed by family name)
// would attribute the refusal to whichever name the combined executor was
// registered under. Three executors keep the blast radius equal to the
// declared unit.
//
// They share loaders and a resolver, not state.
//
// # Scope per call
//
// Same as TestopsRiskExecutor: the Python bridge invokes run_daily_metrics_job
// once PER repo_id with backfill_days=1 (worker_metrics.py:1749-1760,
// CHAOS-4264), so each family is computed for one repo and one day at a time.
// The per-repo loop below mirrors that call boundary exactly.
//
// # repoName is the repo_id string, deliberately
//
// Identical reasoning to TestopsRiskExecutor's own note: the live bridge call
// site never passes repo_name, so discover_repos falls back to
// `full_name = repo_name or str(repo_id)` (job_daily.py:135). In production
// repo_names_by_id[repo_id] IS the stringified UUID, so repo_team_resolver can
// never match a real pattern on this path. Loading the real name here would
// make Go MORE accurate than live Python rather than row-identical to it,
// which the standing port rule forbids (reproduce Python's behaviour,
// including its latent defects; a fix goes in its own ticket).
// -----------------------------------------------------------------------

var errTestopsNativeUnavailable = errors.New("testops native executor unavailable")

// testopsNativeBase carries what all three executors need. Embedded rather
// than duplicated so a change to the construction contract cannot apply to
// two of the three.
type testopsNativeBase struct {
	conn   driver.Conn
	nowUTC func() time.Time
}

func newTestopsNativeBase(conn driver.Conn) (testopsNativeBase, error) {
	if conn == nil {
		return testopsNativeBase{}, errTestopsNativeUnavailable
	}
	return testopsNativeBase{conn: conn, nowUTC: func() time.Time { return time.Now().UTC() }}, nil
}

// testopsNativeScope is the validated, per-partition setup all three
// executors perform identically: check the run, parse the repo scope, build
// the same repo-pattern resolver job_daily.py builds once per run, and derive
// the day window.
type testopsNativeScope struct {
	repoIDs    []uuid.UUID
	resolver   testops.RepoTeamResolver
	day        time.Time
	start      time.Time
	end        time.Time
	computedAt time.Time
}

func (base testopsNativeBase) scope(
	ctx context.Context, run Run, partition Partition,
) (testopsNativeScope, error) {
	if base.conn == nil {
		return testopsNativeScope{}, errTestopsNativeUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return testopsNativeScope{}, fmt.Errorf(
			"%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}
	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return testopsNativeScope{}, fmt.Errorf(
			"%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	// job_daily.py builds repo_team_resolver ONCE per run
	// (build_repo_pattern_resolver(teams_data)) and passes it into every
	// family's compute call. Reuse team_wellbeing's existing loader/resolver
	// rather than a second implementation of the same query -- CHAOS-4294's
	// codex round 1 caught testops_risk shipping with NO team resolution at
	// all, silently nulling every repo-pattern-derived team_id.
	teams, err := LoadWellbeingTeams(ctx, base.conn, run.OrganizationID)
	if err != nil {
		return testopsNativeScope{}, err
	}
	day := chDate(run.TargetDay)
	return testopsNativeScope{
		repoIDs:    repoIDs,
		resolver:   NewRepoPatternResolver(teams),
		day:        day,
		start:      day,
		end:        day.Add(24 * time.Hour),
		computedAt: base.nowUTC(),
	}, nil
}

// -----------------------------------------------------------------------
// testops_pipeline
// -----------------------------------------------------------------------

// TestopsPipelineExecutor is the NATIVE implementation of the
// testops_pipeline family: testops_pipeline_metrics_daily, from
// compute_testops.py:114 compute_pipeline_metrics_daily.
type TestopsPipelineExecutor struct{ testopsNativeBase }

// NewTestopsPipelineExecutor fails closed, mirroring every other native
// family's construction contract.
func NewTestopsPipelineExecutor(conn driver.Conn) (*TestopsPipelineExecutor, error) {
	base, err := newTestopsNativeBase(conn)
	if err != nil {
		return nil, err
	}
	return &TestopsPipelineExecutor{testopsNativeBase: base}, nil
}

func (executor *TestopsPipelineExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil {
		return 0, errTestopsNativeUnavailable
	}
	scope, err := executor.scope(ctx, run, partition)
	if err != nil {
		return 0, err
	}
	var metrics []testops.PipelineMetric
	for _, repoID := range scope.repoIDs {
		accumulator := testops.NewPipelineAccumulator(repoID, repoID.String(), scope.resolver)
		if err := loadNativeTestopsPipelineRuns(
			ctx, executor.conn, accumulator, run.OrganizationID, repoID, scope.start, scope.end,
		); err != nil {
			return 0, err
		}
		metrics = append(metrics, accumulator.Finish()...)
	}
	return writeTestopsPipelineMetrics(
		ctx, executor.conn, run.OrganizationID, scope.day, scope.computedAt, metrics)
}

// -----------------------------------------------------------------------
// testops_test
// -----------------------------------------------------------------------

// TestopsTestExecutor is the NATIVE implementation of the testops_test
// family: testops_test_metrics_daily, from compute_testops.py:216
// compute_test_metrics_daily.
//
// This is the family the allocation freeze was traced to: its Python loader
// materialised every test_case_results row for the day and refused at 200k
// (DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS -> TestopsRowCapExceeded ->
// resource_exhausted). loadNativeTestopsCaseGroups reduces those rows to one
// per case_name inside ClickHouse, so this executor has no cap and no
// equivalent refusal path.
type TestopsTestExecutor struct{ testopsNativeBase }

func NewTestopsTestExecutor(conn driver.Conn) (*TestopsTestExecutor, error) {
	base, err := newTestopsNativeBase(conn)
	if err != nil {
		return nil, err
	}
	return &TestopsTestExecutor{testopsNativeBase: base}, nil
}

func (executor *TestopsTestExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil {
		return 0, errTestopsNativeUnavailable
	}
	scope, err := executor.scope(ctx, run, partition)
	if err != nil {
		return 0, err
	}
	// job_daily.py's historical window for failure_recurrence_score is the 29
	// days BEFORE the target day (CHAOS-4350 PR2's SQL aggregate), with the
	// current day's own runs excluded by run_id -- see
	// loadNativeHistoricalFailedCaseNames.
	historyStart := scope.day.AddDate(0, 0, -29)

	var metrics []testops.TestMetric
	for _, repoID := range scope.repoIDs {
		accumulator := testops.NewTestAccumulator(repoID, repoID.String(), scope.resolver)
		if err := loadNativeTestopsSuites(
			ctx, executor.conn, accumulator, run.OrganizationID, repoID, scope.start, scope.end,
		); err != nil {
			return 0, err
		}
		if err := loadNativeTestopsCaseGroups(
			ctx, executor.conn, accumulator, run.OrganizationID, repoID, scope.start, scope.end,
		); err != nil {
			return 0, err
		}
		historicalFailedNames, err := loadNativeHistoricalFailedCaseNames(
			ctx, executor.conn, run.OrganizationID, repoID, historyStart, scope.start, scope.end,
		)
		if err != nil {
			return 0, err
		}
		metrics = append(metrics, accumulator.Finish(historicalFailedNames)...)
	}
	return writeTestopsTestMetrics(
		ctx, executor.conn, run.OrganizationID, scope.day, scope.computedAt, metrics)
}

// -----------------------------------------------------------------------
// testops_coverage
// -----------------------------------------------------------------------

// TestopsCoverageExecutor is the NATIVE implementation of the
// testops_coverage family: testops_coverage_metrics_daily, from
// compute_testops.py:371 compute_coverage_metrics_daily.
//
// coverage_delta_pct needs the PRIOR window's latest snapshot as well as the
// current one; Python's prior window is the 30 days before the target day
// (job_daily.py's prior_coverage_rows read), so both reads are the same
// single-row reduction over different bounds.
type TestopsCoverageExecutor struct{ testopsNativeBase }

func NewTestopsCoverageExecutor(conn driver.Conn) (*TestopsCoverageExecutor, error) {
	base, err := newTestopsNativeBase(conn)
	if err != nil {
		return nil, err
	}
	return &TestopsCoverageExecutor{testopsNativeBase: base}, nil
}

func (executor *TestopsCoverageExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil {
		return 0, errTestopsNativeUnavailable
	}
	scope, err := executor.scope(ctx, run, partition)
	if err != nil {
		return 0, err
	}
	priorStart := scope.day.AddDate(0, 0, -30)

	var metrics []testops.CoverageMetric
	for _, repoID := range scope.repoIDs {
		current, err := loadNativeTestopsLatestCoverage(
			ctx, executor.conn, run.OrganizationID, repoID, scope.start, scope.end)
		if err != nil {
			return 0, err
		}
		if len(current) == 0 {
			// Python's latest_current_by_repo lookup miss: no row for this
			// repo at all, not a zero-valued row.
			continue
		}
		prior, err := loadNativeTestopsLatestCoverage(
			ctx, executor.conn, run.OrganizationID, repoID, priorStart, scope.start)
		if err != nil {
			return 0, err
		}
		if metric := testops.ComputeCoverageMetric(
			repoID, current, prior, repoID.String(), scope.resolver,
		); metric != nil {
			metrics = append(metrics, *metric)
		}
	}
	return writeTestopsCoverageMetrics(
		ctx, executor.conn, run.OrganizationID, scope.day, scope.computedAt, metrics)
}

var (
	_ NativeFamilyExecutor = (*TestopsPipelineExecutor)(nil)
	_ NativeFamilyExecutor = (*TestopsTestExecutor)(nil)
	_ NativeFamilyExecutor = (*TestopsCoverageExecutor)(nil)
)
