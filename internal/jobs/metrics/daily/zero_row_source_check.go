package daily

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/cicd"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// SourceDataChecker flags families whose upstream source data exists for a
// partition's repositories and day, but whose metrics.daily output table has
// zero rows for that same scope (CHAOS-4263, chris's ruling 2026-08-25). A
// partition that "succeeds" despite this is exactly the failure mode the RCA
// found: ci_pipeline_runs/deployments fresh every day, cicd_metrics_daily/
// deploy_metrics_daily stale for weeks while nothing surfaced it. The point is
// distinguishing that from a genuinely empty day (no source data at all,
// nothing to compute) -- only the former is an anomaly worth failing loudly
// over. A nil checker is a legal no-op: this capability is additive to the
// existing compatibility-execution contract, not a replacement for it.
//
// Scoped to the four families CHAOS-4263's RCA named stale: cicd, deploy,
// incident, testops_risk. The other 19 families in families.json (repo_user_
// commit, work_item, etc.) are out of this ticket.
//
// The check has two halves. SourceSnapshot reads the source BEFORE the
// families compute; ZeroRowFamilies reads the outputs AFTER they wrote. Rows
// a sync lands after the snapshot are in neither half, so the check no longer
// fails a partition whose family computed every row it could read. Each
// family's source read is the family's own source predicate (see
// cicd.PipelineRunsSource, deploySource, incidentsStartedSQL,
// testopsPipelineRunsSource, testopsSuiteResultsSource), so the check
// counts exactly the rows the family keeps.
type SourceDataChecker interface {
	SourceSnapshot(ctx context.Context, partitionID string, asOf time.Time) (SourceSnapshot, error)
	ZeroRowFamilies(ctx context.Context, snapshot SourceSnapshot) ([]string, error)
}

// SourceSnapshot is what the source half saw for one partition.
type SourceSnapshot struct {
	orgID         string
	day           string
	repositoryIDs []uuid.UUID

	cicd                     bool
	deploy                   bool
	testopsPipeline          bool
	testopsCompletedPipeline bool
	testopsSuite             bool
	incident                 bool
}

// ClickHouseSourceDataChecker reads a partition's scope (target day,
// repository ids) from Postgres and cross-references ClickHouse source and
// output tables. It never mutates either store.
type ClickHouseSourceDataChecker struct {
	pool *pgxpool.Pool
	conn repositoryRows
}

func NewClickHouseSourceDataChecker(pool *pgxpool.Pool, conn repositoryRows) (*ClickHouseSourceDataChecker, error) {
	if pool == nil || conn == nil {
		return nil, ErrUnavailable
	}
	return &ClickHouseSourceDataChecker{pool: pool, conn: conn}, nil
}

// SourceSnapshot implements SourceDataChecker. asOf is the instant the
// incident mapping validity is read at; the incident family reads at the same
// instant (sourceAsOf).
func (checker *ClickHouseSourceDataChecker) SourceSnapshot(
	ctx context.Context, partitionID string, asOf time.Time,
) (SourceSnapshot, error) {
	if checker == nil || checker.pool == nil || checker.conn == nil || !validUUID(partitionID) {
		return SourceSnapshot{}, ErrUnavailable
	}
	var orgID, day, rawRepositoryIDs string
	if err := checker.pool.QueryRow(ctx, `
SELECT run.org_id::text, run.target_day::text, partition.repo_ids::text
FROM public.daily_metrics_partitions AS partition
JOIN public.daily_metrics_runs AS run ON run.id = partition.run_id
WHERE partition.id = $1::uuid`, partitionID).Scan(&orgID, &day, &rawRepositoryIDs); err != nil {
		return SourceSnapshot{}, ErrUnavailable
	}
	var typedRepositoryIDs []RepositoryID
	if err := json.Unmarshal([]byte(rawRepositoryIDs), &typedRepositoryIDs); err != nil {
		return SourceSnapshot{}, ErrInvalidState
	}
	if len(typedRepositoryIDs) == 0 {
		// No repositories in this partition: MaterializeScheduledFanout
		// already terminalizes that case as no_repositories. Nothing to check.
		return SourceSnapshot{}, nil
	}
	repositoryIDs, err := repositoryIDUUIDs(typedRepositoryIDs)
	if err != nil {
		return SourceSnapshot{}, ErrInvalidState
	}
	dayStart, err := time.Parse("2006-01-02", day)
	if err != nil {
		return SourceSnapshot{}, ErrInvalidState
	}
	dayEnd := dayStart.AddDate(0, 0, 1)
	snapshot := SourceSnapshot{orgID: orgID, day: day, repositoryIDs: repositoryIDs}

	if snapshot.cicd, err = checker.existsWith(ctx, "SELECT 1"+cicd.PipelineRunsSource+"\nLIMIT 1",
		cicd.SourceArgs(orgID, repositoryIDs, dayStart, dayEnd)...); err != nil {
		return SourceSnapshot{}, err
	}
	if snapshot.deploy, err = checker.existsWith(ctx, "SELECT 1"+deploySource+"\nLIMIT 1",
		deploySourceArgs(orgID, repositoryIDs, dayStart, dayEnd)...); err != nil {
		return SourceSnapshot{}, err
	}

	// testops_risk's three outputs have DIFFERENT source eligibility
	// (codex adversarial review, round 2, CHAOS-4263):
	// compute_release_confidence and compute_quality_drag
	// (compute_testops_risk.py) accept a repo with EITHER pipeline metrics OR
	// test metrics alone -- a pipeline-only or suite-only day legitimately
	// produces rows for both. compute_pipeline_stability takes ONLY the
	// pipeline runs that COMPLETED (succeeded or failed, the statuses of
	// testops.CompletedRunStatuses): a suite-only day, or a day whose runs
	// were all cancelled, skipped or never finished, has no success rate to
	// derive stability from and legitimately writes zero pipeline_stability
	// rows -- that is not an anomaly. Treating "pipeline OR suite", or "any
	// pipeline run", as pipeline_stability's eligibility pinned a healthy day
	// as a permanent failure. A day with a completed run and no stability row
	// is still flagged.
	testopsArgs := testopsSourceArgs(orgID, repositoryIDs, dayStart, dayEnd)
	if snapshot.testopsPipeline, err = checker.existsWith(ctx, "SELECT 1"+testopsPipelineRunsSource+"\nLIMIT 1",
		testopsArgs...); err != nil {
		return SourceSnapshot{}, err
	}
	if snapshot.testopsCompletedPipeline, err = checker.existsWith(ctx, "SELECT 1"+testopsPipelineRunsSource+`
  AND lower(trimBoth(coalesce(status, ''))) IN {completed_statuses:Array(String)}
LIMIT 1`, append(testopsArgs, clickhouse.Named("completed_statuses", testops.CompletedRunStatuses()))...); err != nil {
		return SourceSnapshot{}, err
	}
	if snapshot.testopsSuite, err = checker.existsWith(ctx, "SELECT 1"+testopsSuiteResultsSource+"\nLIMIT 1",
		testopsArgs...); err != nil {
		return SourceSnapshot{}, err
	}

	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return SourceSnapshot{}, fmt.Errorf("incident ordering contract: %w", err)
	}
	if snapshot.incident, err = checker.existsWith(ctx, incidentSourceExistsSQL(contract),
		incidentSourceArgs(orgID, repositoryIDs, dayStart, dayEnd, asOf)...); err != nil {
		return SourceSnapshot{}, err
	}
	return snapshot, nil
}

// ZeroRowFamilies implements SourceDataChecker: a family is flagged when the
// snapshot saw its source and its output now has no row for the scope.
func (checker *ClickHouseSourceDataChecker) ZeroRowFamilies(
	ctx context.Context, snapshot SourceSnapshot,
) ([]string, error) {
	if checker == nil || checker.conn == nil {
		return nil, ErrUnavailable
	}
	if len(snapshot.repositoryIDs) == 0 {
		return nil, nil
	}
	orgID, day, repositoryIDs := snapshot.orgID, snapshot.day, snapshot.repositoryIDs
	output := func(table string) (bool, error) {
		if liverow.Registered(table) {
			// A repository-day whose NEWEST row retires the day is no output, also when an older live row is stored
			// under it.
			return checker.exists(ctx, `
SELECT 1 FROM `+table+`
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND day = {day:Date}
GROUP BY repo_id
HAVING `+liverow.NewestPredicate(table, "")+`
LIMIT 1`, orgID, day, repositoryIDs)
		}
		return checker.exists(ctx, `
SELECT 1 FROM `+table+`
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND day = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	}
	cicdOutput, err := checker.exists(ctx, cicdLiveOutputSQL, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}
	deployOutput, err := checker.exists(ctx, liveOutputSQL("deploy_metrics_daily"), orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}

	// testops_risk writes three independent output tables (release_confidence,
	// quality_drag, pipeline_stability -- job_daily.py:1481-1483 logs zero-rows
	// for each separately). A single combined existence check collapses them
	// into one boolean, so a regression that empties one table while another
	// still has rows never gets flagged (round 1). Check each output
	// independently, against its OWN eligible source, and flag the family if
	// ANY eligible output is missing.
	releaseConfidenceOutput, err := output("testops_release_confidence")
	if err != nil {
		return nil, err
	}
	qualityDragOutput, err := output("testops_quality_drag")
	if err != nil {
		return nil, err
	}
	pipelineStabilityOutput, err := output("testops_pipeline_stability")
	if err != nil {
		return nil, err
	}
	testopsEitherSource := snapshot.testopsPipeline || snapshot.testopsSuite
	testopsMissingOutput := (testopsEitherSource && (!releaseConfidenceOutput || !qualityDragOutput)) ||
		(snapshot.testopsCompletedPipeline && !pipelineStabilityOutput)

	incidentOutput, err := checker.exists(ctx, liveOutputSQL("incident_metrics_daily"), orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}

	var flagged []string
	if snapshot.cicd && !cicdOutput {
		flagged = append(flagged, "cicd")
	}
	if snapshot.deploy && !deployOutput {
		flagged = append(flagged, "deploy")
	}
	if testopsMissingOutput {
		flagged = append(flagged, "testops_risk")
	}
	if snapshot.incident && !incidentOutput {
		flagged = append(flagged, "incident")
	}
	return flagged, nil
}

// liveOutputSQL is the output of a family in the scope: a repository-day of table whose NEWEST row is live. A retired
// repository-day (newest row retracted = 1) is no output, also when an older live row is stored under it.
func liveOutputSQL(table string) string {
	return `
SELECT 1 FROM ` + table + `
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND day = {day:Date}
GROUP BY repo_id
HAVING ` + liverow.NewestPredicate(table, "") + `
LIMIT 1`
}

// cicdLiveOutputSQL is the cicd output of the scope (liveOutputSQL).
var cicdLiveOutputSQL = liveOutputSQL("cicd_metrics_daily")

func (checker *ClickHouseSourceDataChecker) exists(
	ctx context.Context, query string, orgID string, day string, repositoryIDs []uuid.UUID, extra ...any,
) (bool, error) {
	rows, err := checker.conn.Query(ctx, query, append([]any{
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("day", day),
		clickhouse.Named("repo_ids", repositoryIDs),
	}, extra...)...)
	if err != nil {
		return false, ErrUnavailable
	}
	defer rows.Close()
	found := rows.Next()
	if err := rows.Err(); err != nil {
		return false, ErrUnavailable
	}
	return found, nil
}

// incidentSourceExistsSQL is the incident family's kept set: its source read,
// resolved in the same window computeIncidentMetricsDaily keeps.
func incidentSourceExistsSQL(contract remaining.OperationalOrderingContract) string {
	return `SELECT 1 FROM (` + incidentsStartedSQL(contract) + `) AS incident
WHERE incident.resolved_at IS NOT NULL
  AND incident.resolved_at >= {start:DateTime64(3, 'UTC')}
  AND incident.resolved_at < {end:DateTime64(3, 'UTC')}
LIMIT 1`
}

func (checker *ClickHouseSourceDataChecker) existsWith(ctx context.Context, query string, args ...any) (bool, error) {
	rows, err := checker.conn.Query(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("source query: %w: %v", ErrUnavailable, err)
	}
	defer rows.Close()
	found := rows.Next()
	if err := rows.Err(); err != nil {
		return false, ErrUnavailable
	}
	return found, nil
}

func repositoryIDUUIDs(ids []RepositoryID) ([]uuid.UUID, error) {
	result := make([]uuid.UUID, len(ids))
	for index, id := range ids {
		parsed, err := uuid.Parse(string(id))
		if err != nil {
			return nil, err
		}
		result[index] = parsed
	}
	return result, nil
}

var _ SourceDataChecker = (*ClickHouseSourceDataChecker)(nil)
