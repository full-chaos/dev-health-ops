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
	names := scopelabel.Resolve(ctx, client, orgID, kind, ids, scopelabel.Options{Final: true, Log: "home summary drivers"})
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
