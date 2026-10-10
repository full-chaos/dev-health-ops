package home

import (
	"context"
	"log"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/activeteams"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/scopelabel"
)

// driverNames is the display names of the drivers a summary sentence names
// (CHAOS-9046): the rows' ids go through the one shared name lookup
// (scopelabel.Resolve, the one the explain route and the analytics breakdowns
// use), and the sentence is built from the names, never from an id.
//
// An id with no name is left out of the sentence: it is never printed. A team
// id that is not an active team (a bare id the team-id carry retired, the
// documented "unassigned" value, a team no longer synced) is not a driver. A
// failed read of names or of the active teams leaves the drivers out of the
// sentence, never turns them into ids. Names keep the order of the rows and are
// listed once.
func driverNames(ctx context.Context, client QueryClient, orgID, group string, rows []driverRow) []string {
	kind := "team"
	if group == "repo_id" {
		kind = "repo"
	}
	var ids []string
	for _, row := range rows {
		if row.ID != "" {
			ids = append(ids, row.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	names := resolveScopeNames(ctx, client, orgID, kind, ids, "home summary drivers")
	var active map[string]bool
	if kind == "team" {
		var err error
		if active, err = activeTeamIDs(ctx, client, orgID); err != nil {
			log.Printf("home summary drivers: could not read the active teams: %v", err)
			return nil
		}
	}
	var out []string
	seen := map[string]bool{}
	for _, id := range ids {
		name, ok := names[id]
		if !ok || (kind == "team" && !active[id]) || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	return out
}

// resolveScopeNames is the one name lookup of the Home prose (CHAOS-9046,
// CHAOS-9116): the shared scopelabel lookup of the repositories or the teams, and
// the one rule for what is no name. A name that is the id is no name (an id
// stored in the name column is still an id; the rule the explain route applies),
// and a bare uuid is no name. An id without a name is absent from the result;
// the caller leaves it out or says it generically, and never prints it. A failed
// read answers an empty map (logged).
func resolveScopeNames(ctx context.Context, client QueryClient, orgID, kind string, ids []string, logPrefix string) map[string]string {
	resolved := scopelabel.Resolve(ctx, client, orgID, kind, ids, scopelabel.Options{Final: true, Log: logPrefix})
	names := make(map[string]string, len(resolved))
	for id, raw := range resolved {
		if name, ok := scopelabel.CleanNameFor(raw, id); ok {
			names[id] = name
		}
	}
	return names
}

// activeTeamIDs is the ids of the active team rows of the organization, by the
// shared rule of package activeteams.
func activeTeamIDs(ctx context.Context, client QueryClient, orgID string) (map[string]bool, error) {
	rows, err := client.Query(ctx, activeteams.IDsSubquery, []dhclickhouse.Binding{{Name: "org_id", Value: orgID}})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	active := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		active[id] = true
	}
	return active, rows.Err()
}

// scopeNames is the display names of the scope ids of the request (a team
// request names teams, a repository request repositories), by the shared lookup.
// A repository request may carry the repository NAME instead of its id (the
// readers accept both: a value that is not a uuid is read as the name): that
// value is already a name and is kept as it is.
func scopeNames(ctx context.Context, client QueryClient, orgID string, f Filters) map[string]string {
	switch f.Scope.Level {
	case "team":
		return resolveScopeNames(ctx, client, orgID, "team", f.Scope.IDs, "home scope labels")
	case "repo":
		var uuids []string
		names := map[string]string{}
		for _, id := range f.Scope.IDs {
			if scopelabel.LooksLikeUUID(id) {
				uuids = append(uuids, id)
			} else if name, ok := scopelabel.CleanName(id); ok {
				names[id] = name
			}
		}
		for id, name := range resolveScopeNames(ctx, client, orgID, "repo", uuids, "home scope labels") {
			names[id] = name
		}
		return names
	}
	return nil
}

// attachTeamNames sets the display name of the team of each recommendation row.
func attachTeamNames(ctx context.Context, client QueryClient, orgID string, rows []RecommendationRow) {
	var ids []string
	for _, row := range rows {
		if row.TeamID != "" {
			ids = append(ids, row.TeamID)
		}
	}
	if len(ids) == 0 {
		return
	}
	names := resolveScopeNames(ctx, client, orgID, "team", ids, "home recommendation teams")
	for i := range rows {
		rows[i].TeamName = names[rows[i].TeamID]
	}
}
