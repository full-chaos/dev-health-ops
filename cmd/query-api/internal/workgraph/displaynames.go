package workgraph

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// edgeEndpoint is the (id, type) shape batchResolveDisplayNames and
// batchResolveMembership both scan over -- either a real edge's source or
// target endpoint, or (for workGraphArtifacts) a pseudo-edge whose source
// endpoint IS the aggregate node and whose target endpoint is blank so it is
// ignored, mirroring work_graph.py:1468-1476's pseudo_rows construction.
type edgeEndpoint struct {
	sourceID, sourceType string
	targetID, targetType string
}

// batchResolveDisplayNames mirrors work_graph.py:261-416's
// _batch_resolve_display_names: collects unresolved source/target ids across
// the page, partitioned by entity type, then issues exactly ONE ClickHouse
// query PER entity type (PR / deployment / incident) -- no N+1. Every
// sub-query is best-effort: Python wraps each in a blanket
// `except Exception: logger.warning(...)`, so a failed lookup degrades that
// entity type to unresolved (-> displayNameFor returns nil -> client renders
// "Unresolved") rather than failing the whole request. This port reproduces
// that swallow-and-continue contract; it does not additionally reproduce the
// log line (no logger is threaded into this package, matching every other
// Wave 1-3 operation package, none of which take one either).
func batchResolveDisplayNames(ctx context.Context, client QueryClient, orgID string, rows []edgeEndpoint) map[string]string {
	resolved := map[string]string{}

	prIDs := map[string]struct{}{}
	deploymentIDs := map[string]struct{}{}
	incidentIDs := map[string]struct{}{}

	collect := func(entityID, entityType string) {
		entityID = strings.TrimSpace(entityID)
		entityType = strings.ToLower(strings.TrimSpace(entityType))
		if entityID == "" {
			return
		}
		isPRFormat := prEdgeIDRe.MatchString(entityID)
		isBareUUID := looksLikeUUID(entityID)
		isOpaqueHex := opaqueHexIDRe.MatchString(entityID)

		if entityType == "incident" && (isOpaqueHex || isBareUUID) {
			incidentIDs[entityID] = struct{}{}
			return
		}
		if isOpaqueHex {
			return
		}
		if !isPRFormat && !isBareUUID {
			return
		}
		if isPRFormat || entityType == "pr" {
			prIDs[entityID] = struct{}{}
		} else if entityType == "deployment" && isBareUUID {
			deploymentIDs[entityID] = struct{}{}
		}
	}

	for _, row := range rows {
		collect(row.sourceID, row.sourceType)
		collect(row.targetID, row.targetType)
	}

	if len(prIDs) > 0 {
		resolvePRDisplayNames(ctx, client, orgID, prIDs, resolved)
	}
	if len(deploymentIDs) > 0 {
		resolveDeploymentDisplayNames(ctx, client, orgID, deploymentIDs, resolved)
	}
	if len(incidentIDs) > 0 {
		resolveIncidentDisplayNames(ctx, client, orgID, incidentIDs, resolved)
	}
	return resolved
}

// resolvePRDisplayNames mirrors work_graph.py:314-361's PR branch: only
// "{repo_uuid}#pr{N}" ids are resolvable (bare-UUID pr ids never are), via
// ONE query against git_pull_requests FINAL keyed by (repo_id, number).
func resolvePRDisplayNames(ctx context.Context, client QueryClient, orgID string, prIDs map[string]struct{}, resolved map[string]string) {
	type lookup struct {
		repoUUID string
		number   int
	}
	lookups := map[string]lookup{}
	repoUUIDSeen := map[string]struct{}{}
	var repoUUIDs []string
	for prID := range prIDs {
		m := prEdgeIDRe.FindStringSubmatch(prID)
		if m == nil {
			continue
		}
		repoUUID := strings.ToLower(m[1])
		num, err := strconv.Atoi(m[2])
		if err != nil {
			continue
		}
		lookups[prID] = lookup{repoUUID: repoUUID, number: num}
		if _, ok := repoUUIDSeen[repoUUID]; !ok {
			repoUUIDSeen[repoUUID] = struct{}{}
			repoUUIDs = append(repoUUIDs, repoUUID)
		}
	}
	if len(lookups) == 0 || len(repoUUIDs) == 0 {
		return
	}
	sort.Strings(repoUUIDs)

	numberSeen := map[int]struct{}{}
	var numbers []int
	for _, l := range lookups {
		if _, ok := numberSeen[l.number]; !ok {
			numberSeen[l.number] = struct{}{}
			numbers = append(numbers, l.number)
		}
	}
	sort.Ints(numbers)

	query := `
        SELECT toString(repo_id) AS repo_id, number, title
        FROM git_pull_requests FINAL
        WHERE org_id = {org_id:String}
          AND toString(repo_id) IN {repo_ids:Array(String)}
          AND number IN {pr_numbers:Array(String)}
    `
	numberStrs := make([]string, len(numbers))
	for i, n := range numbers {
		numberStrs[i] = strconv.Itoa(n)
	}
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_ids", Value: repoUUIDs},
		{Name: "pr_numbers", Value: numberStrs},
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return
	}
	defer rows.Close()

	type key struct {
		repoID string
		number int
	}
	titles := map[key]string{}
	for rows.Next() {
		var repoID, title string
		var number uint32
		if scanErr := rows.Scan(&repoID, &number, &title); scanErr != nil {
			return
		}
		title = strings.TrimSpace(title)
		repoID = strings.ToLower(repoID)
		if repoID != "" && number != 0 && title != "" {
			titles[key{repoID, int(number)}] = title
		}
	}
	if rows.Err() != nil {
		return
	}

	for prID, l := range lookups {
		if title, ok := titles[key{l.repoUUID, l.number}]; ok && title != "" {
			resolved[prID] = title
		}
	}
}

// resolveDeploymentDisplayNames mirrors work_graph.py:363-386's deployment
// branch: ONE query against deployments FINAL.
func resolveDeploymentDisplayNames(ctx context.Context, client QueryClient, orgID string, deploymentIDs map[string]struct{}, resolved map[string]string) {
	ids := make([]string, 0, len(deploymentIDs))
	for id := range deploymentIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	query := `
        SELECT deployment_id, environment
        FROM deployments FINAL
        WHERE org_id = {org_id:String}
          AND deployment_id IN {dep_ids:Array(String)}
    `
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "dep_ids", Value: ids},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var depID, env string
		if scanErr := rows.Scan(&depID, &env); scanErr != nil {
			return
		}
		env = strings.TrimSpace(env)
		// Only store a label when there is a meaningful environment string
		// -- empty env omits from resolved so displayNameFor returns nil
		// (Unresolved badge) rather than leaking the raw UUID (A8).
		if depID != "" && env != "" {
			resolved[depID] = env + " deploy"
		}
	}
	_ = rows.Err()
}

// resolveIncidentDisplayNames mirrors work_graph.py:388-414's incident
// branch. It reads through current_operational_rows_sql
// (storage/operational_current.py:25-64) with NO explicit
// ordering_contract, which resolves to
// configured_operational_ordering_contract() -- and
// operational_ordering_guard.py:62-69's parse_operational_ordering_contract
// returns OperationalOrderingContract.LEGACY when the env var
// (DEV_HEALTH_OPERATIONAL_ORDERING_CONTRACT) is UNSET, which is the
// contract's documented default. This port hard-codes that LEGACY branch
// (operational_current.py:46-53: `FROM {table} FINAL WHERE {org_scope}`,
// wrapped by the post-selection filter) rather than threading a
// process-env toggle through a fresh Go binary that has never read that
// env var -- if a target deployment has explicitly opted into the CURRENT
// contract (env var = "2"), this diverges and needs the other branch; flag
// this as a documented follow-up, not a silent gap.
func resolveIncidentDisplayNames(ctx context.Context, client QueryClient, orgID string, incidentIDs map[string]struct{}, resolved map[string]string) {
	ids := make([]string, 0, len(incidentIDs))
	for id := range incidentIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	query := `
        SELECT id AS incident_id, normalized_status AS status, title
        FROM (
            SELECT *
            FROM operational_incidents FINAL
            WHERE org_id = {org_id:String}
        )
        WHERE is_deleted = 0 AND id IN {inc_ids:Array(String)}
    `
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "inc_ids", Value: ids},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var incID, status, title string
		if scanErr := rows.Scan(&incID, &status, &title); scanErr != nil {
			return
		}
		status = strings.TrimSpace(status)
		title = strings.TrimSpace(title)
		if incID == "" || status == "" {
			continue
		}
		label := incidentLabel(status)
		if title != "" {
			resolved[incID] = fmt.Sprintf("%s (%s)", title, label)
		} else {
			resolved[incID] = fmt.Sprintf("incident (%s)", label)
		}
	}
	_ = rows.Err()
}
