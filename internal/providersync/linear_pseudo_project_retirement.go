package providersync

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// linearPseudoProjectPrefix is the id namespace CollectReferenceCatalog's
// per-sync team-key-shaped tombstone rows live under (linear_reference_
// catalog_route.go's tombstone loop): "{org_id}:linear:{team_key}". No real
// Linear project id (a raw provider UUID) can ever collide with this shape.
func linearPseudoProjectPrefix(orgID string) string {
	return orgID + ":linear:"
}

// linearOrphanedPseudoProjectsQuery finds this collector's OWN team-key-
// shaped `projects` rows that are still is_active=1 for an org -- a row
// only reaches that state if a PRIOR sync wrote it and no LATER sync has
// yet retired it.
const linearOrphanedPseudoProjectsQuery = "SELECT id, name FROM projects FINAL WHERE org_id = {org_id:String} AND provider = 'linear' AND is_active = 1 AND startsWith(id, {prefix:String})"

// RetireOrphanedLinearPseudoProjects reconciles this collector's OWN prior
// team-key-shaped pseudo-project writes against the team keys THIS sync
// actually observed (CHAOS-4530 codex review round 2, confirmed real): a
// Linear team deleted, or whose key changed, between two syncs drops out of
// every future GraphQL response entirely, so linear_reference_catalog_
// route.go's per-response tombstone loop (which only iterates the CURRENT
// response's teams) never revisits its OLD identity -- the stale
// is_active=1 row would remain the ReplacingMergeTree FINAL result, and
// active project-catalog queries (Ask Dev search/portfolio) would keep
// exposing a deleted/renamed team as an active project indefinitely.
//
// Returns one retirement tombstone row per orphaned identity, ready to
// append to the batch this call is about to write -- same shape and
// soft-delete convention as the per-response tombstone loop's own rows
// (is_active=0, project_key=nil), just keyed by an identity this call's
// Linear response no longer carries a team payload for. An org with
// nothing orphaned returns an empty slice, not an error; a query failure
// MUST abort the caller's write, the same fail-closed contract every other
// pre-write reconciliation helper in this package uses
// (PreserveExistingTeamManualMembers, PreserveExistingTeamMembersRoster) --
// silently skipping this reconcile step on error would let a stale active
// row survive unnoticed.
func RetireOrphanedLinearPseudoProjects(
	ctx context.Context, conn driver.Conn, orgID string, currentTeamKeys []string, at time.Time,
) ([]linearReferenceProjectRow, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" || at.IsZero() {
		return nil, ErrInvalidConfiguration
	}
	prefix := linearPseudoProjectPrefix(orgID)
	rows, err := conn.Query(ctx, linearOrphanedPseudoProjectsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("prefix", prefix))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	current := make(map[string]bool, len(currentTeamKeys))
	for _, key := range currentTeamKeys {
		current[prefix+key] = true
	}
	at = at.UTC().Truncate(time.Millisecond)
	tombstones := make([]linearReferenceProjectRow, 0)
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		if current[id] {
			continue
		}
		tombstones = append(tombstones, linearReferenceProjectRow{
			ID: id, OrgID: orgID, Provider: "linear", ProjectKey: nil,
			Name: name, IsActive: 0, UpdatedAt: at, LastSynced: at,
		})
	}
	return tombstones, rows.Err()
}
