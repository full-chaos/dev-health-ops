package daily

import (
	"context"
	"encoding/json"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
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
	// clickhouse-go's Array(String) named-parameter binding is verified against
	// plain []string, not an arbitrary named string type; converting once here
	// keeps every query below on that verified path.
	repositoryIDs := repositoryIDStrings(typedRepositoryIDs)

	// cicd: source is ci_pipeline_runs, keyed by its own repo_id and the day
	// its run finished.
	cicdSource, err := checker.exists(ctx, `
SELECT 1 FROM ci_pipeline_runs
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(String)}
  AND finished_at IS NOT NULL AND toDate(finished_at) = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}
	cicdOutput, err := checker.exists(ctx, `
SELECT 1 FROM cicd_metrics_daily
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(String)} AND day = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}

	// deploy: source is deployments. The four-way coalesce matches the
	// native DORA executor's own window predicate (dora_native_clickhouse.go
	// deploymentWindowQuery) -- this is deliberately the same fallback chain,
	// not a simplified two-value one.
	deploySource, err := checker.exists(ctx, `
SELECT 1 FROM deployments
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(String)}
  AND toDate(coalesce(deployed_at, finished_at, started_at, last_synced)) = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}
	deployOutput, err := checker.exists(ctx, `
SELECT 1 FROM deploy_metrics_daily
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(String)} AND day = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}

	// testops_risk: writes testops_release_confidence, testops_quality_drag,
	// and testops_pipeline_stability together (families.json), reading
	// already-computed test/pipeline metrics as its own inputs rather than a
	// single raw event table. test_suite_results is the closest genuine raw
	// "test results" source (CHAOS-4263 ruling's own phrase); output presence
	// only checks release_confidence and pipeline_stability, the two tables
	// the RCA itself found stale -- testops_quality_drag is not checked here.
	testopsSource, err := checker.exists(ctx, `
SELECT 1 FROM test_suite_results
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(String)}
  AND finished_at IS NOT NULL AND toDate(finished_at) = {day:Date}
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}
	testopsOutput, err := checker.exists(ctx, `
SELECT 1 FROM (
  SELECT repo_id FROM testops_release_confidence
  WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(String)} AND day = {day:Date}
  UNION ALL
  SELECT repo_id FROM testops_pipeline_stability
  WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(String)} AND day = {day:Date}
)
LIMIT 1`, orgID, day, repositoryIDs)
	if err != nil {
		return nil, err
	}

	// incident: operational_incidents carries no repo_id (it is a generic
	// operational entity keyed by service_id; repo attribution is itself a
	// separate, still-"pending" family -- work_graph_edges). The source check
	// here is therefore org-scoped, not repo-scoped, which is a known
	// imprecision: an org-wide incident that day does not prove any one of
	// this partition's specific repos owns it. See RISK-NOTES.
	incidentSource, err := checker.existsOrgScoped(ctx, `
SELECT 1 FROM operational_incidents
WHERE org_id = {org_id:String} AND is_deleted = 0
  AND ((started_at IS NOT NULL AND toDate(started_at) = {day:Date})
       OR (source_event_at IS NOT NULL AND toDate(source_event_at) = {day:Date}))
LIMIT 1`, orgID, day)
	if err != nil {
		return nil, err
	}
	incidentOutput, err := checker.exists(ctx, `
SELECT 1 FROM incident_metrics_daily
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(String)} AND day = {day:Date}
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
	ctx context.Context, query string, orgID string, day string, repositoryIDs []string,
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

func (checker *ClickHouseSourceDataChecker) existsOrgScoped(
	ctx context.Context, query string, orgID string, day string,
) (bool, error) {
	rows, err := checker.conn.Query(ctx, query,
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("day", day),
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

var _ SourceDataChecker = (*ClickHouseSourceDataChecker)(nil)
