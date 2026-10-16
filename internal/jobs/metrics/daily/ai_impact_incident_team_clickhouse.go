package daily

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiimpact"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/teamactive"
)

// The readers below give ai_impact the facts it needs to count an incident
// once, for one team (D6040, D6041; aiimpact.incidentTeam):
//
//   - the team the incident source names: the owning team of the incident's
//     service (LoadAIImpactIncidentSourceTeams);
//   - the triggering pull request, with its owning team, bucket and work type
//     (LoadAIImpactIncidentTriggers);
//   - for tied main owners, the repository's merged pull requests with their
//     owning team (LoadAIImpactRepoMergedPRTeams).

// aiImpactPullRequestOwnerSQL selects, for each pull request, the one team that
// owned it: the primary team of the work item linked to it in
// work_graph_issue_pr (the actual link row, never an issue key prefix). Only
// the best provenance tier of a pull request's links counts (native >
// explicit_text > heuristic): a pull request with a native link takes no team
// from a lower-tier link. A pull request whose best-tier links name no team, or
// name more than one, has no owner here. extraLinkFilter narrows the link read.
func aiImpactPullRequestOwnerSQL(extraLinkFilter string) string {
	return `(
    SELECT link.repo_id AS repo_id, link.pr_number AS pr_number,
           anyIf(attr.team_id, attr.team_id IS NOT NULL AND attr.team_id != '') AS team_id
    FROM (
        SELECT repo_id, work_item_id, pr_number, tier_rank,
               max(tier_rank) OVER (PARTITION BY repo_id, pr_number) AS best_rank
        FROM (
            SELECT repo_id, work_item_id, pr_number,
                   multiIf(provenance = 'native', 3, provenance = 'explicit_text', 2, provenance = 'heuristic', 1, 0) AS tier_rank
            FROM work_graph_issue_pr FINAL
            WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} ` + extraLinkFilter + `
        )
    ) AS link
    LEFT JOIN (
        SELECT work_item_id, team_id
        FROM work_item_team_attributions FINAL
        WHERE org_id = {org_id:String}
          AND is_primary = 1
          AND (work_item_id, computed_at) IN (
              SELECT work_item_id, max(computed_at)
              FROM work_item_team_attributions
              WHERE org_id = {org_id:String}
              GROUP BY work_item_id
          )
    ) AS attr ON attr.work_item_id = link.work_item_id
    WHERE link.tier_rank = link.best_rank
    GROUP BY link.repo_id, link.pr_number
    HAVING uniqExactIf(attr.team_id, attr.team_id IS NOT NULL AND attr.team_id != '') = 1
)`
}

// AIImpactProviderTeamRef is a team as an incident source names it: the
// provider of the service and the provider's own team reference.
type AIImpactProviderTeamRef struct {
	Provider string
	Ref      string
}

// LoadAIImpactIncidentSourceTeams returns, by incident id, the team the incident
// source names: the owning team of the incident's service, as the provider's
// own reference. An incident whose service names no team is absent. The
// reference is not a Dev Health team id: MapAIImpactProviderTeam resolves it.
func LoadAIImpactIncidentSourceTeams(
	ctx context.Context, conn repositoryRows, organizationID string, incidentIDs []string,
) (map[string]AIImpactProviderTeamRef, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	result := make(map[string]AIImpactProviderTeamRef)
	if len(incidentIDs) == 0 {
		return result, nil
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("load ai impact incident source teams: %w", err)
	}
	currentIncidents := remaining.CurrentOperationalRowsSQL("operational_incidents", []string{
		"is_deleted = 0",
		"id IN {incident_ids:Array(String)}",
	}, contract)
	currentServices := remaining.CurrentOperationalRowsSQL("operational_services", []string{
		"is_deleted = 0",
		"owning_team_id IS NOT NULL",
		"owning_team_id != ''",
	}, contract)
	rows, err := conn.Query(ctx, `
SELECT incident.id AS incident_id, service.provider AS provider, service.owning_team_id AS owning_team_id
FROM `+currentIncidents+` AS incident
INNER JOIN `+currentServices+` AS service
    ON incident.org_id = service.org_id AND incident.service_id = service.id
ORDER BY incident_id, provider, owning_team_id`,
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("incident_ids", incidentIDs),
	)
	if err != nil {
		return nil, fmt.Errorf("load ai impact incident source teams: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var incidentID, provider string
		var team *string
		if err := rows.Scan(&incidentID, &provider, &team); err != nil {
			return nil, fmt.Errorf("scan ai impact incident source team: %w", err)
		}
		if team == nil {
			continue
		}
		if _, seen := result[incidentID]; !seen {
			result[incidentID] = AIImpactProviderTeamRef{Provider: provider, Ref: *team}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact incident source teams: %w", err)
	}
	return result, nil
}

// LoadAIImpactTeamsByNativeKey reads the team catalog the way team attribution
// does: the teams of the organization keyed by provider and native_team_key.
func LoadAIImpactTeamsByNativeKey(
	ctx context.Context, conn repositoryRows, organizationID string,
) (map[AIImpactProviderTeamRef][]string, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	rows, err := conn.Query(ctx, `
SELECT provider, id, native_team_key
FROM (
    SELECT provider, id, tupleElement(argMax(tuple(native_team_key), updated_at), 1) AS native_team_key
    FROM teams
    WHERE org_id = ?
    GROUP BY org_id, provider, id
)
WHERE native_team_key IS NOT NULL AND native_team_key != ''
ORDER BY provider, native_team_key, id`, organizationID)
	if err != nil {
		return nil, fmt.Errorf("load ai impact teams by native key: %w", err)
	}
	defer rows.Close()
	type catalogRow struct {
		ref AIImpactProviderTeamRef
		id  string
	}
	var catalogRows []catalogRow
	for rows.Next() {
		var provider, id string
		var key *string
		if err := rows.Scan(&provider, &id, &key); err != nil {
			return nil, fmt.Errorf("scan ai impact team native key: %w", err)
		}
		if key != nil {
			catalogRows = append(catalogRows, catalogRow{AIImpactProviderTeamRef{Provider: provider, Ref: *key}, id})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact team native keys: %w", err)
	}
	inactive, err := teamactive.LoadInactive(ctx, conn, organizationID)
	if err != nil {
		return nil, fmt.Errorf("load ai impact teams by native key: %w", err)
	}
	result := make(map[AIImpactProviderTeamRef][]string)
	for _, row := range teamactive.Keep(catalogRows, inactive, func(row catalogRow) string { return row.id }) {
		result[row.ref] = append(result[row.ref], row.id)
	}
	return result, nil
}

// MapAIImpactProviderTeam resolves a source's team reference to a Dev Health
// team: the one active team of the organization whose provider and
// native_team_key are the reference's. A reference that equals a team id but is
// not in the catalog under that provider maps to nothing, and so does one that
// names several teams.
func MapAIImpactProviderTeam(
	ref AIImpactProviderTeamRef, catalog map[AIImpactProviderTeamRef][]string, active map[string]struct{},
) (string, bool) {
	var found string
	for _, id := range catalog[ref] {
		if _, ok := active[id]; !ok {
			continue
		}
		if found != "" {
			return "", false
		}
		found = id
	}
	return found, found != ""
}

type aiImpactTriggerCandidate struct {
	rank         int
	deploymentID string
	repoID       uuid.UUID
	number       uint32
}

// aiImpactEdgeTierRank is the provenance order of a link: native outranks
// explicit_text outranks heuristic. Any other value ranks below all three.
func aiImpactEdgeTierRank(source string) int {
	switch source {
	case "native":
		return 3
	case "explicit_text":
		return 2
	case "heuristic":
		return 1
	}
	return 0
}

// LoadAIImpactIncidentTriggers returns, by incident id, the pull request that
// triggered the incident. The path is: the incident's deployment-incident edge
// of the best tier (native > explicit_text > heuristic; ties by deployment id)
// to its deployment, and the deployment's pull_request_number. An edge without a
// repository whose deployment id names more than one repository ties nothing
// (never guessed). An incident without such a pull request is absent.
//
// Each trigger carries the pull request's owning team (aiImpactPullRequestOwnerSQL)
// and, as the pull request's own rows do, its AI bucket and work type: the AI
// attribution when there is one, else BucketUnknown and "pull_request".
func LoadAIImpactIncidentTriggers(
	ctx context.Context, conn repositoryRows, organizationID string, incidentIDs []string,
) (map[string]aiimpact.IncidentTrigger, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	triggers := make(map[string]aiimpact.IncidentTrigger)
	orgUUID, err := uuid.Parse(organizationID)
	if err != nil || len(incidentIDs) == 0 {
		// work_graph_deployment_incident_edges keys org_id as a UUID: an
		// organization that is not one has no row there.
		return triggers, nil
	}

	rows, err := conn.Query(ctx, `
SELECT edge.incident_id, edge.source, edge.deployment_id, edge.repo_id IS NULL AS edge_has_no_repo,
       deployment.repo_id, deployment.pull_request_number
FROM (
    SELECT incident_id, deployment_id, repo_id, source
    FROM work_graph_deployment_incident_edges FINAL
    WHERE org_id = {org_uuid:UUID} AND incident_id IN {incident_ids:Array(String)}
) AS edge
INNER JOIN (
    SELECT deployment_id, repo_id, pull_request_number
    FROM deployments FINAL
    WHERE org_id = {org_id:String} AND pull_request_number IS NOT NULL AND pull_request_number > 0
) AS deployment ON edge.deployment_id = deployment.deployment_id
    AND (edge.repo_id IS NULL OR edge.repo_id = deployment.repo_id)
ORDER BY edge.incident_id, edge.deployment_id, deployment.repo_id`,
		clickhouse.Named("org_uuid", orgUUID.String()),
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("incident_ids", incidentIDs),
	)
	if err != nil {
		return nil, fmt.Errorf("load ai impact incident triggers: %w", err)
	}
	defer rows.Close()

	type edgeKey struct{ incidentID, deploymentID string }
	candidates := make(map[string][]aiImpactTriggerCandidate)
	repoSets := make(map[edgeKey]map[uuid.UUID]struct{})
	noRepoEdges := make(map[edgeKey]bool)
	for rows.Next() {
		var (
			incidentID, source, deploymentID string
			edgeHasNoRepo                    uint8
			repoID                           uuid.UUID
			number                           *uint32
		)
		if err := rows.Scan(&incidentID, &source, &deploymentID, &edgeHasNoRepo, &repoID, &number); err != nil {
			return nil, fmt.Errorf("scan ai impact incident trigger: %w", err)
		}
		if number == nil || *number == 0 {
			continue
		}
		key := edgeKey{incidentID, deploymentID}
		if repoSets[key] == nil {
			repoSets[key] = make(map[uuid.UUID]struct{})
		}
		repoSets[key][repoID] = struct{}{}
		noRepoEdges[key] = noRepoEdges[key] || edgeHasNoRepo == 1
		candidates[incidentID] = append(candidates[incidentID], aiImpactTriggerCandidate{
			rank: aiImpactEdgeTierRank(source), deploymentID: deploymentID, repoID: repoID, number: *number,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact incident triggers: %w", err)
	}

	type prKey struct {
		repoID uuid.UUID
		number uint32
	}
	chosen := make(map[string]aiImpactTriggerCandidate)
	for incidentID, list := range candidates {
		var best *aiImpactTriggerCandidate
		for index := range list {
			candidate := list[index]
			key := edgeKey{incidentID, candidate.deploymentID}
			if noRepoEdges[key] && len(repoSets[key]) > 1 {
				continue
			}
			if best == nil || candidate.rank > best.rank ||
				(candidate.rank == best.rank && (candidate.deploymentID < best.deploymentID ||
					(candidate.deploymentID == best.deploymentID && candidate.repoID.String() < best.repoID.String()))) {
				best = &list[index]
			}
		}
		if best != nil {
			chosen[incidentID] = *best
		}
	}
	if len(chosen) == 0 {
		return triggers, nil
	}

	repoSet := make(map[uuid.UUID]struct{})
	numberSet := make(map[uint32]struct{})
	for _, candidate := range chosen {
		repoSet[candidate.repoID] = struct{}{}
		numberSet[candidate.number] = struct{}{}
	}
	repoIDs := make([]uuid.UUID, 0, len(repoSet))
	for repoID := range repoSet {
		repoIDs = append(repoIDs, repoID)
	}
	numbers := make([]uint32, 0, len(numberSet))
	for number := range numberSet {
		numbers = append(numbers, number)
	}

	owners := make(map[prKey]string)
	ownerRows, err := conn.Query(ctx,
		`SELECT repo_id, pr_number, team_id FROM `+
			aiImpactPullRequestOwnerSQL("AND pr_number IN {pr_numbers:Array(UInt32)}"),
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("repo_ids", repositoryUUIDStrings(repoIDs)),
		clickhouse.Named("pr_numbers", numbers),
	)
	if err != nil {
		return nil, fmt.Errorf("load ai impact incident trigger owners: %w", err)
	}
	defer ownerRows.Close()
	for ownerRows.Next() {
		var (
			repoID uuid.UUID
			number uint32
			team   *string
		)
		if err := ownerRows.Scan(&repoID, &number, &team); err != nil {
			return nil, fmt.Errorf("scan ai impact incident trigger owner: %w", err)
		}
		if team != nil && *team != "" {
			owners[prKey{repoID, number}] = *team
		}
	}
	if err := ownerRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact incident trigger owners: %w", err)
	}

	attributionRows, err := conn.Query(ctx, `
SELECT repo_id, number, kind, work_type
FROM (
    SELECT pr.repo_id AS repo_id, pr.number AS number, attr.kind AS kind,
           coalesce(nullIf(wi.type, ''), 'pull_request') AS work_type
    FROM git_pull_requests AS pr
    INNER JOIN work_graph_issue_pr AS link
        ON link.org_id = pr.org_id AND link.repo_id = pr.repo_id AND link.pr_number = pr.number
    INNER JOIN ai_attribution_resolved AS attr
        ON attr.subject_type = 'pull_request'
        AND attr.subject_id = link.work_item_id
        AND attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
    LEFT JOIN work_items AS wi FINAL
        ON wi.org_id = link.org_id AND wi.repo_id = link.repo_id AND wi.work_item_id = link.work_item_id
    WHERE pr.repo_id IN {repo_ids:Array(UUID)} AND pr.number IN {pr_numbers:Array(UInt32)}
      AND pr.org_id = {org_id:String} AND toString(attr.org_id) = {org_id:String}
    UNION ALL
    SELECT pr.repo_id AS repo_id, pr.number AS number, attr.kind AS kind, 'pull_request' AS work_type
    FROM git_pull_requests AS pr
    INNER JOIN ai_attribution_resolved AS attr
        ON attr.subject_type = 'pull_request'
        AND attr.repo_id = pr.repo_id
        AND (attr.subject_id = toString(pr.number)
             OR attr.subject_id = concat(toString(pr.repo_id), '#', toString(pr.number)))
        AND attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
    WHERE pr.repo_id IN {repo_ids:Array(UUID)} AND pr.number IN {pr_numbers:Array(UInt32)}
      AND pr.org_id = {org_id:String} AND toString(attr.org_id) = {org_id:String}
)
ORDER BY repo_id, number, work_type`,
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("repo_ids", repositoryUUIDStrings(repoIDs)),
		clickhouse.Named("pr_numbers", numbers),
	)
	if err != nil {
		return nil, fmt.Errorf("load ai impact incident trigger attributions: %w", err)
	}
	defer attributionRows.Close()
	type kindAndType struct{ kind, workType string }
	attributions := make(map[prKey]kindAndType)
	for attributionRows.Next() {
		var (
			repoID   uuid.UUID
			number   uint32
			kind     string
			workType string
		)
		if err := attributionRows.Scan(&repoID, &number, &kind, &workType); err != nil {
			return nil, fmt.Errorf("scan ai impact incident trigger attribution: %w", err)
		}
		// First row wins, as LoadAIImpactAttributions does.
		if _, seen := attributions[prKey{repoID, number}]; !seen {
			attributions[prKey{repoID, number}] = kindAndType{kind, workType}
		}
	}
	if err := attributionRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact incident trigger attributions: %w", err)
	}

	for incidentID, candidate := range chosen {
		key := prKey{candidate.repoID, candidate.number}
		trigger := aiimpact.IncidentTrigger{
			RepoID: candidate.repoID, Number: int64(candidate.number),
			Bucket: aiimpact.BucketUnknown, WorkType: "pull_request",
		}
		if attribution, ok := attributions[key]; ok {
			kind, workType := attribution.kind, attribution.workType
			trigger.Bucket = aiimpact.SafeBucket(&kind)
			trigger.WorkType = workType
		}
		if team, ok := owners[key]; ok {
			owner := team
			trigger.TeamID = &owner
		}
		triggers[incidentID] = trigger
	}
	return triggers, nil
}

// LoadAIImpactRepoMergedPRTeams returns, for each given repository, its merged
// pull requests that a team owned (aiImpactPullRequestOwnerSQL), newest first,
// merged before `before`. It is read only for a repository whose main owners
// tie. At most mergedPRTeamsPerRepo rows are read for each repository.
func LoadAIImpactRepoMergedPRTeams(
	ctx context.Context, conn repositoryRows, organizationID string,
	repoIDs []uuid.UUID, before time.Time,
) (map[uuid.UUID][]aiimpact.MergedPRTeam, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	result := make(map[uuid.UUID][]aiimpact.MergedPRTeam)
	if len(repoIDs) == 0 {
		return result, nil
	}
	rows, err := conn.Query(ctx, `
SELECT pr.repo_id, owner.team_id, pr.merged_at
FROM (
    SELECT repo_id, number, merged_at
    FROM git_pull_requests FINAL
    WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
      AND merged_at IS NOT NULL AND merged_at < {before:DateTime64(3, 'UTC')}
) AS pr
INNER JOIN `+aiImpactPullRequestOwnerSQL("")+` AS owner
    ON owner.repo_id = pr.repo_id AND owner.pr_number = pr.number
ORDER BY pr.repo_id, pr.merged_at DESC, owner.team_id
LIMIT `+fmt.Sprint(mergedPRTeamsPerRepo)+` BY pr.repo_id`,
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("repo_ids", repositoryUUIDStrings(repoIDs)),
		clickhouse.Named("before", remaining.DateTime64Argument(before, remaining.DateTime64MillisecondPrecision)),
	)
	if err != nil {
		return nil, fmt.Errorf("load ai impact repo merged pr teams: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			repoID   uuid.UUID
			team     *string
			mergedAt time.Time
		)
		if err := rows.Scan(&repoID, &team, &mergedAt); err != nil {
			return nil, fmt.Errorf("scan ai impact repo merged pr team: %w", err)
		}
		if team == nil || *team == "" {
			continue
		}
		result[repoID] = append(result[repoID], aiimpact.MergedPRTeam{TeamID: *team, MergedAt: mergedAt.UTC()})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact repo merged pr teams: %w", err)
	}
	return result, nil
}

// mergedPRTeamsPerRepo bounds the tie-break read: the newest team-owned merged
// pull requests of a repository.
const mergedPRTeamsPerRepo = 500
