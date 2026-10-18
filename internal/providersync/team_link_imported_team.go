package providersync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// importedTeamIDsQuery reads which team ids have a teams row in the
// organization, in any version (an inactive or deleted team is still a team
// that was imported).
const importedTeamIDsQuery = `SELECT DISTINCT id FROM teams WHERE org_id = {org_id:String} AND id IN {ids:Array(String)}`

// ErrLinkRowsOfAnotherOrganization is a link write that holds a row whose
// organization is not the one written.
var ErrLinkRowsOfAnotherOrganization = errors.New("providersync: link rows of another organization")

// LinkTeamQuerier is the one connection method KeepLinksOfImportedTeams needs.
type LinkTeamQuerier interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// KeepLinksOfImportedTeams returns rows without the OPEN link rows
// (membership or ownership) of a team that has no teams row in the
// organization. The three team catalog switches (teams, members, projects)
// are independent: a run with members or projects on and teams off reads the
// provider's teams but writes no teams row, so a link of a team that was
// never imported would name a team id that no teams row holds (chris
// 2026-10-14 on CHAOS-9123: the switches stay independent, and such link rows
// are not written). open says which rows the rule applies to: an open row of
// a link kind that names a catalog team. A closed row is always kept (it
// closes a row the store holds), and so is a row of a kind whose team id is
// never a teams row (the caller's open returns false for it). A caller writes
// its teams first: a team the same run imports is then a stored team. A failed read is an error, before any write.
func KeepLinksOfImportedTeams[R any](
	ctx context.Context, conn LinkTeamQuerier, orgID, provider, dataset string, rows []R,
	orgOf, teamID func(R) string, open func(R) bool,
) ([]R, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" {
		return nil, ErrInvalidConfiguration
	}
	// A row of another organization is refused, loudly: its team cannot be
	// looked up in this organization, and dropping it would hide the caller's
	// defect.
	foreign := 0
	for _, row := range rows {
		if orgOf(row) != orgID {
			foreign++
		}
	}
	if foreign > 0 {
		slog.Default().WarnContext(ctx, "team_links_of_another_organization_refused",
			"provider", provider, "dataset", dataset, "rows", foreign)
		return nil, fmt.Errorf("%w: %d %s rows of another organization", ErrLinkRowsOfAnotherOrganization, foreign, dataset)
	}
	seen := map[string]bool{}
	var ids []string
	for _, row := range rows {
		if id := teamID(row); open(row) && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return rows, nil
	}
	result, err := conn.Query(ctx, importedTeamIDsQuery, clickhouse.Named("org_id", orgID), clickhouse.Named("ids", ids))
	if err != nil {
		return nil, fmt.Errorf("providersync: read the imported teams of %s links: %w", dataset, err)
	}
	defer result.Close()
	imported := map[string]bool{}
	for result.Next() {
		var id string
		if err := result.Scan(&id); err != nil {
			return nil, err
		}
		imported[id] = true
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	kept := make([]R, 0, len(rows))
	droppedTeams := map[string]bool{}
	dropped := 0
	for _, row := range rows {
		if open(row) && !imported[teamID(row)] {
			dropped++
			droppedTeams[teamID(row)] = true
			continue
		}
		kept = append(kept, row)
	}
	if dropped > 0 {
		slog.Default().InfoContext(ctx, "team_links_of_unimported_teams_not_written",
			"provider", provider, "dataset", dataset, "rows", dropped, "teams", len(droppedTeams))
	}
	return kept, nil
}
