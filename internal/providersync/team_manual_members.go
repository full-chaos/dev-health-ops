package providersync

import (
	"context"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// teamManualMembersPreserveQuery mirrors Python's
// ClickHouseStore._preserve_existing_manual_members SELECT exactly
// (storage/clickhouse.py:1655) and internal/streamhandlers/external_clickhouse.go's
// own port of it (CHAOS-4321, team-lead ruling 2026-08-26): one batched read
// per teams write, keyed by (org_id, id), never N+1.
const teamManualMembersPreserveQuery = "SELECT id, manual_members FROM teams FINAL WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)}"

// PreserveExistingTeamManualMembers batch-reads the current `manual_members`
// for every team a caller is about to write to `teams`, so the write can
// carry each team's existing admin-set roster override forward instead of
// clobbering it with the schema default `[]` (CHAOS-4321, extended to the
// native provider-sync team-catalog collectors by CHAOS-4446: the Linear
// reference-catalog writer's INSERT omitted the column entirely, silently
// resetting an admin's override on every sync).
//
// A team with no existing row simply has no entry in the returned map --
// callers must default that case to an empty slice themselves (the correct
// value for a genuinely new team), never treat a missing map entry as an
// error. A query failure returns an error and MUST abort the caller's write
// before it prepares any INSERT: papering over it with an empty preserved
// set would silently wipe overrides the same way the original gap did.
//
// Shared by every native team-catalog collector's ClickHouse effects sink
// (Linear today; CHAOS-4431's sibling lanes for GitHub/GitLab call this same
// function) so the carry-forward logic, and its one round trip per batch,
// lives in exactly one place.
func PreserveExistingTeamManualMembers(
	ctx context.Context, conn driver.Conn, orgID string, teamIDs []string,
) (map[string][]string, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" {
		return nil, ErrInvalidConfiguration
	}
	if len(teamIDs) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx, teamManualMembersPreserveQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_ids", teamIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	existing := make(map[string][]string, len(teamIDs))
	for rows.Next() {
		var id string
		var manualMembers []string
		if err := rows.Scan(&id, &manualMembers); err != nil {
			return nil, err
		}
		existing[id] = manualMembers
	}
	return existing, rows.Err()
}

// teamMembersRosterPreserveQuery is PreserveExistingTeamManualMembers's query
// over the `members` column instead of `manual_members`.
const teamMembersRosterPreserveQuery = "SELECT id, members FROM teams FINAL WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)}"

// PreserveExistingTeamMembersRoster batch-reads the current `members` roster
// for every team a caller is about to write to `teams`, for a collector that
// is writing the team row (selections.Teams is on) but did NOT fetch members
// this call (selections.Members is off). CHAOS-4431 codex review P1: Python's
// team_autoimport_linear.py:685-707 preserves the existing roster in exactly
// this situation -- a teams-only run must never overwrite `teams.members`
// with an empty or page-1-only roster just because it never asked Linear for
// members this call. Deliberately a SEPARATE function from
// PreserveExistingTeamManualMembers (not a second column on the same query)
// so that function's existing shared contract -- GitHub/GitLab's own
// collectors already call it -- never changes shape for this narrower, much
// less frequently hit case.
//
// Same missing-row/query-failure contract as PreserveExistingTeamManualMembers:
// a team with no existing row has no map entry (caller defaults to empty,
// correct for a genuinely new team); a query error MUST abort the write.
func PreserveExistingTeamMembersRoster(
	ctx context.Context, conn driver.Conn, orgID string, teamIDs []string,
) (map[string][]string, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" {
		return nil, ErrInvalidConfiguration
	}
	if len(teamIDs) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx, teamMembersRosterPreserveQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_ids", teamIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	existing := make(map[string][]string, len(teamIDs))
	for rows.Next() {
		var id string
		var members []string
		if err := rows.Scan(&id, &members); err != nil {
			return nil, err
		}
		existing[id] = members
	}
	return existing, rows.Err()
}
