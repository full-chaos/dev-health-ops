package daily

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workgraphedges"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// ViaDeploymentLinks is the result of LoadViaDeploymentIncidentLinks.
type ViaDeploymentLinks struct {
	Ties  []changefailure.IncidentTie
	Links []changefailure.Link
	// Ambiguous counts link rows whose deployment_id names more than one
	// repository of the organization: they tie the incident to no repository.
	Ambiguous int
}

// LoadViaDeploymentIncidentLinks reads the second incident-to-repository tier
// of change failure rate (CHAOS-8981): a persisted deployment-incident link
// row that carries no repo_id ties its incident to the repository of its
// deployment, but only when the incident has no direct tie (no active
// service-to-repository mapping for its service) and the incident started in
// [dayStart, dayEnd). A deployment_id that names more than one repository of
// the organization ties nothing; it is counted, logged and skipped, never
// guessed. Only ties to repoIDs are returned.
//
// The daily work_graph_edges family always writes repo_id, so today these rows
// come from other writers of the table; the tier is read from the table, not
// derived again, because no producer in this binary emits it. The deployment
// read is limited to the deployment ids those rows name, so an organization
// with no such row (the usual case) reads no deployment here.
func LoadViaDeploymentIncidentLinks(
	ctx context.Context, conn repositoryRows, organizationID string,
	repoIDs []uuid.UUID, dayStart, dayEnd, asOf time.Time,
) (ViaDeploymentLinks, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !dayStart.Before(dayEnd) {
		return ViaDeploymentLinks{}, ErrInvalidState
	}
	if len(repoIDs) == 0 {
		return ViaDeploymentLinks{}, nil
	}
	orgUUID, err := uuid.Parse(organizationID)
	if err != nil {
		// work_graph_deployment_incident_edges keys org_id as a UUID: an
		// organization that is not one has no row there.
		return ViaDeploymentLinks{}, nil
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return ViaDeploymentLinks{}, fmt.Errorf("load via-deployment incident links: %w", err)
	}
	currentIncidents := remaining.CurrentOperationalRowsSQL("operational_incidents", []string{
		"is_deleted = 0",
		"started_at >= {start:DateTime64(3, 'UTC')}",
		"started_at < {end:DateTime64(3, 'UTC')}",
	}, contract)
	currentMappings := activeServiceRepositoryMappingsSQL(contract)
	rows, err := conn.Query(ctx, `
SELECT edge.incident_id, edge.deployment_id, edge.source, deployment.deployment_repo_id, deployment.repo_count
FROM (
    SELECT incident_id, deployment_id, source
    FROM work_graph_deployment_incident_edges FINAL
    WHERE org_id = {org_uuid:UUID} AND repo_id IS NULL
) AS edge
INNER JOIN (
    SELECT deployment_id, min(repo_id) AS deployment_repo_id, uniqExact(repo_id) AS repo_count
    FROM deployments FINAL
    WHERE org_id = {org_id:String}
      AND deployment_id IN (
          SELECT deployment_id
          FROM work_graph_deployment_incident_edges FINAL
          WHERE org_id = {org_uuid:UUID} AND repo_id IS NULL
      )
    GROUP BY deployment_id
) AS deployment ON edge.deployment_id = deployment.deployment_id
INNER JOIN `+currentIncidents+` AS incident
    ON incident.org_id = {org_id:String} AND incident.id = edge.incident_id
WHERE (incident.service_id IS NULL OR incident.service_id NOT IN (
    SELECT service_id FROM `+currentMappings+` AS mapping
    WHERE mapping.org_id = {org_id:String} AND mapping.service_id IS NOT NULL
))
ORDER BY edge.incident_id, edge.deployment_id, edge.source`,
		clickhouse.Named("org_uuid", orgUUID.String()),
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("start", remaining.DateTime64Argument(dayStart, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("end", remaining.DateTime64Argument(dayEnd, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("as_of", remaining.DateTime64Argument(asOf, remaining.DateTime64MicrosecondPrecision)),
	)
	if err != nil {
		return ViaDeploymentLinks{}, fmt.Errorf("load via-deployment incident links: %w", err)
	}
	defer rows.Close()

	wanted := make(map[uuid.UUID]struct{}, len(repoIDs))
	for _, id := range repoIDs {
		wanted[id] = struct{}{}
	}
	var out ViaDeploymentLinks
	for rows.Next() {
		var (
			incidentID, deploymentID, source string
			repoID                           uuid.UUID
			repoCount                        uint64
		)
		if err := rows.Scan(&incidentID, &deploymentID, &source, &repoID, &repoCount); err != nil {
			return ViaDeploymentLinks{}, fmt.Errorf("scan via-deployment incident link: %w", err)
		}
		if repoCount != 1 {
			out.Ambiguous++
			continue
		}
		if _, ok := wanted[repoID]; !ok {
			continue
		}
		incidentID = pythonparity.DecodeClickHouseStringValue(incidentID)
		deploymentID = pythonparity.DecodeClickHouseStringValue(deploymentID)
		out.Ties = append(out.Ties, changefailure.IncidentTie{RepoID: repoID, IncidentID: incidentID, ViaDeployment: true})
		out.Links = append(out.Links, changefailure.Link{RepoID: repoID, DeploymentID: deploymentID, IncidentID: incidentID, Source: source})
	}
	if err := rows.Err(); err != nil {
		return ViaDeploymentLinks{}, fmt.Errorf("iterate via-deployment incident links: %w", err)
	}
	if out.Ambiguous > 0 {
		slog.Default().WarnContext(ctx,
			"metrics daily: deployment-incident links without repo_id name a deployment_id of more than one repository; no tie",
			"org_id", organizationID, "count", out.Ambiguous)
	}
	return out, nil
}

// LoadStoredChangeFailureRepositories returns the repositories of repoIDs
// that have a repo_change_failure_daily row for the day, whatever it counts.
// The writer needs them to replace a row whose evidence is gone with a row of
// zeros (see repouser.ApplyChangeFailure).
func LoadStoredChangeFailureRepositories(
	ctx context.Context, conn repositoryRows, organizationID string, repoIDs []uuid.UUID, dayStart time.Time,
) ([]uuid.UUID, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx, `
SELECT DISTINCT repo_id
FROM repo_change_failure_daily
WHERE org_id = {org_id:String} AND day = {day:Date} AND repo_id IN {repo_ids:Array(UUID)}
ORDER BY repo_id`,
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("day", dayStart.UTC().Format(time.DateOnly)),
		clickhouse.Named("repo_ids", repositoryUUIDStrings(repoIDs)),
	)
	if err != nil {
		return nil, fmt.Errorf("load stored change failure repositories: %w", err)
	}
	defer rows.Close()
	var stored []uuid.UUID
	for rows.Next() {
		var repoID uuid.UUID
		if err := rows.Scan(&repoID); err != nil {
			return nil, fmt.Errorf("scan stored change failure repository: %w", err)
		}
		stored = append(stored, repoID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stored change failure repositories: %w", err)
	}
	return stored, nil
}

// changeFailureCounts derives the day's change-failure counts from the same
// rows and the same extraction function
// (workgraphedges.ExtractReviewDeploymentIncidentEdges) the work_graph_edges
// family writes its deployment-incident links with (that family runs after
// this one, so its table cannot be read for the same day), plus the persisted
// via-deployment tier.
func changeFailureCounts(
	deployments []workgraphedges.DeploymentRow,
	started []IncidentRow,
	via ViaDeploymentLinks,
	organizationID string,
	now time.Time,
) (map[uuid.UUID]changefailure.Counts, error) {
	// One extraction over all rows: every heuristic link stays inside one
	// repository, so the link set is the one the edge family's per-provider
	// split writes. The organization id and the provider only feed the edge id
	// and the edge's own columns, which the counts do not use, so an
	// organization id that is not a UUID (the edge family writes no edge for
	// it) still gets its counts.
	orgUUID, err := uuid.Parse(organizationID)
	if err != nil {
		orgUUID = uuid.Nil
	}
	extracted, err := workgraphedges.ExtractReviewDeploymentIncidentEdges(workgraphedges.Params{
		OrgID: orgUUID, Deployments: deployments, Incidents: workGraphEdgeIncidents(started), Now: now,
	})
	if err != nil {
		return nil, err
	}
	days := make([]changefailure.Deployment, 0, len(deployments))
	for _, row := range deployments {
		days = append(days, changefailure.Deployment{RepoID: row.RepoID, DeploymentID: row.DeploymentID})
	}
	ties := make([]changefailure.IncidentTie, 0, len(started)+len(via.Ties))
	for _, row := range started {
		ties = append(ties, changefailure.IncidentTie{
			RepoID: row.RepoID, IncidentID: pythonparity.DecodeClickHouseStringValue(row.IncidentID),
		})
	}
	ties = append(ties, via.Ties...)
	links := make([]changefailure.Link, 0, len(extracted.DeploymentIncidentEdges)+len(via.Links))
	for _, edge := range extracted.DeploymentIncidentEdges {
		if edge.RepoID == nil {
			continue
		}
		links = append(links, changefailure.Link{
			RepoID: *edge.RepoID, DeploymentID: edge.DeploymentID, IncidentID: edge.IncidentID, Source: edge.Source,
		})
	}
	links = append(links, via.Links...)
	return changefailure.CountDay(days, ties, links), nil
}
