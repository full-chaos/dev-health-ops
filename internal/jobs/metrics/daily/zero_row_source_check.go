package daily

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
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
type SourceDataChecker interface {
	ZeroRowFamiliesWithSourceData(ctx context.Context, partitionID string) ([]string, error)
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

// ZeroRowFamiliesWithSourceData implements SourceDataChecker.
func (checker *ClickHouseSourceDataChecker) ZeroRowFamiliesWithSourceData(
	ctx context.Context, partitionID string,
) ([]string, error) {
	if checker == nil || checker.pool == nil || checker.conn == nil || !validUUID(partitionID) {
		return nil, ErrUnavailable
	}
	var orgID, day, rawRepositoryIDs string
	if err := checker.pool.QueryRow(ctx, `
SELECT run.org_id::text, run.target_day::text, partition.repo_ids::text
FROM public.daily_metrics_partitions AS partition
JOIN public.daily_metrics_runs AS run ON run.id = partition.run_id
WHERE partition.id = $1::uuid`, partitionID).Scan(&orgID, &day, &rawRepositoryIDs); err != nil {
		return nil, ErrUnavailable
	}
	var typedRepositoryIDs []RepositoryID
	if err := json.Unmarshal([]byte(rawRepositoryIDs), &typedRepositoryIDs); err != nil {
		return nil, ErrInvalidState
	}
	if len(typedRepositoryIDs) == 0 {
		// No repositories in this partition: MaterializeScheduledFanout
		// already terminalizes that case as no_repositories. Nothing to check.
		return nil, nil
	}
	repositoryIDs, err := repositoryIDUUIDs(typedRepositoryIDs)
	if err != nil {
		return nil, ErrInvalidState
	}

	// cicd: source is ci_pipeline_runs, keyed by its own repo_id and the day
	// its run finished.
	cicdSource, err := checker.exists(ctx, `
SELECT 1 FROM ci_pipeline_runs
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
  AND started_at IS NOT NULL AND toDate(started_at) = {day:Date}
  AND finished_at IS NOT NULL AND toDate(finished_at) = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}
	cicdOutput, err := checker.exists(ctx, `
SELECT 1 FROM cicd_metrics_daily
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND day = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}

	deploySource, err := checker.exists(ctx, `
SELECT 1 FROM deployments
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
  AND deployed_at IS NOT NULL AND toDate(deployed_at) = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}
	deployOutput, err := checker.exists(ctx, `
SELECT 1 FROM deploy_metrics_daily
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND day = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}

	testopsSource, err := checker.exists(ctx, `
SELECT 1 FROM (
  SELECT repo_id FROM ci_pipeline_runs
  WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
    AND started_at IS NOT NULL AND toDate(started_at) = {day:Date}
  UNION ALL
  SELECT repo_id FROM test_suite_results
  WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
    AND coalesce(started_at, finished_at) IS NOT NULL
    AND toDate(coalesce(started_at, finished_at)) = {day:Date}
)
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}
	// testops_risk writes three independent output tables (release_confidence,
	// quality_drag, pipeline_stability -- job_daily.py:1481-1483 logs zero-rows
	// for each separately). A UNION ALL existence check collapses them into one
	// boolean, so a regression that empties one table while another still has
	// rows never gets flagged (codex adversarial review, round 1, CHAOS-4263).
	// Check each output independently and flag the family if source data
	// exists but ANY expected output is missing.
	releaseConfidenceOutput, err := checker.exists(ctx, `
SELECT 1 FROM testops_release_confidence
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND day = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}
	qualityDragOutput, err := checker.exists(ctx, `
SELECT 1 FROM testops_quality_drag
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND day = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}
	pipelineStabilityOutput, err := checker.exists(ctx, `
SELECT 1 FROM testops_pipeline_stability
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND day = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}
	testopsOutput := releaseConfidenceOutput && qualityDragOutput && pipelineStabilityOutput

	// incident: operational_incidents has no repo_id. The canonical projection
	dayStart, err := time.Parse("2006-01-02", day)
	if err != nil {
		return nil, ErrInvalidState
	}
	incidentSource, err := checker.existsIncident(ctx, orgID, dayStart, dayStart.AddDate(0, 0, 1), repositoryIDs)
	if err != nil {
		return nil, err
	}
	incidentOutput, err := checker.exists(ctx, `
SELECT 1 FROM incident_metrics_daily
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND day = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}

	var flagged []string
	if cicdSource && !cicdOutput {
		flagged = append(flagged, "cicd")
	}
	if deploySource && !deployOutput {
		flagged = append(flagged, "deploy")
	}
	if testopsSource && !testopsOutput {
		flagged = append(flagged, "testops_risk")
	}
	if incidentSource && !incidentOutput {
		flagged = append(flagged, "incident")
	}
	return flagged, nil
}

func (checker *ClickHouseSourceDataChecker) exists(
	ctx context.Context, query string, orgID string, day string, repositoryIDs []uuid.UUID,
) (bool, error) {
	rows, err := checker.conn.Query(ctx, query,
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("day", day),
		clickhouse.Named("repo_ids", repositoryIDs),
	)
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

func (checker *ClickHouseSourceDataChecker) existsIncident(
	ctx context.Context, orgID string, start, end time.Time, repositoryIDs []uuid.UUID,
) (bool, error) {
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return false, fmt.Errorf("incident ordering contract: %w", err)
	}
	projection := remaining.IncidentProjectionQuery(
		remaining.IncidentWindowStarted,
		" AND mapping.repo_id IN {repo_ids:Array(UUID)}",
		contract,
	)
	asOf := time.Now().UTC()
	rows, err := checker.conn.Query(ctx, `SELECT 1 FROM (`+projection+`) AS incident
WHERE incident.resolved_at IS NOT NULL
  AND incident.resolved_at >= {start:DateTime64(3, 'UTC')}
  AND incident.resolved_at < {end:DateTime64(3, 'UTC')}
LIMIT 1`,
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("start", remaining.DateTime64Argument(
			start, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("end", remaining.DateTime64Argument(
			end, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("as_of", remaining.DateTime64Argument(
			asOf, remaining.DateTime64MicrosecondPrecision)),
		clickhouse.Named("repo_ids", repositoryIDs),
	)
	if err != nil {
		return false, fmt.Errorf("incident source query: %w: %v", ErrUnavailable, err)
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
