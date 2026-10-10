// Package teamactive holds the one rule for which teams take part in a
// resolution: a team whose newest row in the ClickHouse teams table has
// is_active = 0 is inactive, and no resolver gives work, a repository or a
// member to it.
//
// A team is set inactive when it is replaced (a carry of a bare id to a
// provider-keyed id) or retired at the provider. Its rows stay in the teams
// table, so a resolver that reads the table without this rule goes on
// resolving to an id that no reader shows.
//
// A team an admin DELETES keeps its row: the delete writes it inactive (with
// the time of the delete in deleted_at), so a deleted team is in the set of
// inactive ids like any other. An id with NO row in teams is not in the set:
// it reads as active here.
//
// The work-item cascade (teamattribution.dropInactiveTeamCandidates) uses the
// same test of the newest row (NewestRowInactive). The repository and member
// resolvers of the daily metric families read the ids here and drop them in Go
// after their own read, because the SQL text of several of those reads is
// held byte for byte against the Python reference. With no inactive team the
// result of every resolver is the same as without the rule.
package teamactive

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// NewestRowInactive is the test of one team in a GROUP BY over teams: the
// newest row decides, so a team that was set inactive and then active again is
// active.
const NewestRowInactive = `argMax(is_active, (updated_at, last_synced, is_active)) = 0`

// inactiveIDsQuery reads the ids of one organization whose newest row is
// inactive. The sorting key of teams is (org_id, id), so the id alone names a
// team; the read has no FINAL because the aggregate takes the newest row.
const inactiveIDsQuery = `SELECT id FROM teams WHERE org_id = ? GROUP BY id HAVING ` + NewestRowInactive

// Querier is what the read needs of a connection.
type Querier interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// Inactive is the set of inactive team ids of one organization.
type Inactive map[string]struct{}

// Has reports whether the team id is inactive. The id is compared with the
// space around it removed, as the cascade does. A nil set holds no id.
func (inactive Inactive) Has(teamID string) bool {
	_, found := inactive[strings.TrimSpace(teamID)]
	return found
}

// LoadInactive reads the inactive team ids of one organization. A failed read
// is returned and never taken as "no inactive team": a resolver that went on
// with an empty set would give work to a replaced team.
func LoadInactive(ctx context.Context, conn Querier, organizationID string) (Inactive, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, fmt.Errorf("teamactive: a connection and an organization id are required")
	}
	rows, err := conn.Query(ctx, inactiveIDsQuery, organizationID)
	if err != nil {
		return nil, fmt.Errorf("load inactive teams: %w", err)
	}
	defer func() { _ = rows.Close() }()
	inactive := Inactive{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan inactive team: %w", err)
		}
		inactive[strings.TrimSpace(id)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate inactive teams: %w", err)
	}
	return inactive, nil
}

// Keep returns the teams that are active, in their order. idOf gives the team
// id of one element. With an empty set it returns the input.
func Keep[Team any](teams []Team, inactive Inactive, idOf func(Team) string) []Team {
	if len(inactive) == 0 {
		return teams
	}
	kept := teams[:0:0]
	for _, team := range teams {
		if inactive.Has(idOf(team)) {
			continue
		}
		kept = append(kept, team)
	}
	return kept
}
