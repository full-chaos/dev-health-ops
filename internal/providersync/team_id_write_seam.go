package providersync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// ErrTeamIDAmbiguous marks a team id without a provider prefix that more
// than one active prefixed team of the organization holds.
var ErrTeamIDAmbiguous = errors.New("team id has no provider prefix and names more than one prefixed team")

// TeamIDWriteError is a refusal of the team id write seam: the id as the
// writer gave it and the reason (teamid.ErrMalformedTeamID or
// ErrTeamIDAmbiguous).
type TeamIDWriteError struct {
	ID  string
	Err error
}

func (e *TeamIDWriteError) Error() string { return fmt.Sprintf("team id %q: %v", e.ID, e.Err) }

func (e *TeamIDWriteError) Unwrap() error { return e.Err }

const teamIDWriteSeamActiveQuery = `SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1 AND id IN {ids:Array(String)}`

// KeyTeamIDsForWrite is the one write seam of a team id a writer takes
// from outside (an admin request): every such writer calls it and writes
// only the ids it returns, in the order given. It carries the
// organization's bare team ids first (CarryTeamIDsBeforeWrite), so a bare
// team the request names has already moved. Then a prefixed id keeps its
// canonical form, and a bare id resolves to the one active prefixed team
// of the organization that holds it (teamid.Candidates); a bare id that no
// active prefixed team holds is the admin's own team, custom:<id> (the id
// the carry gives an admin's bare team). A malformed id (teamid.Malformed)
// and a bare id that two active prefixed teams hold are refused with a
// *TeamIDWriteError before anything but the carry is written. So no writer
// behind it writes a bare team id, and no bare id it names resurrects a
// carried team.
// See docs/contribute/architecture/team-attribution.md "Team ids".
func KeyTeamIDsForWrite(ctx context.Context, conn TeamIDCarryConn, orgID, writer string, ids []string) ([]string, error) {
	for _, id := range ids {
		if teamid.Malformed(id) {
			return nil, refuseTeamIDWrite(ctx, writer, &TeamIDWriteError{ID: id, Err: teamid.ErrMalformedTeamID})
		}
	}
	if err := CarryTeamIDsBeforeWrite(ctx, conn, orgID, writer); err != nil {
		return nil, err
	}
	keyed := make([]string, len(ids))
	var candidates []string
	for i, id := range ids {
		keyed[i] = teamid.Of("", id)
		candidates = append(candidates, teamid.Candidates(id)...)
	}
	if len(candidates) == 0 {
		return keyed, nil
	}
	active := map[string]bool{}
	rows, err := conn.Query(ctx, teamIDWriteSeamActiveQuery, clickhouse.Named("org_id", strings.TrimSpace(orgID)), clickhouse.Named("ids", candidates))
	if err != nil {
		return nil, fmt.Errorf("team id write seam: read teams: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("team id write seam: read teams: %w", err)
		}
		active[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("team id write seam: read teams: %w", err)
	}
	for i, id := range ids {
		var held []string
		for _, candidate := range teamid.Candidates(id) {
			if active[candidate] {
				held = append(held, candidate)
			}
		}
		// A prefixed id has no candidates: the custom branch keeps its prefix.
		switch {
		case len(held) == 1:
			keyed[i] = held[0]
		case len(held) == 0:
			keyed[i] = teamid.Of(teamIDCarryAdminProvider, id)
		default:
			return nil, refuseTeamIDWrite(ctx, writer, &TeamIDWriteError{ID: id, Err: ErrTeamIDAmbiguous})
		}
	}
	return keyed, nil
}

// refuseTeamIDWrite logs a refusal with its reason and writer only (never
// the id) and returns it.
func refuseTeamIDWrite(ctx context.Context, writer string, refusal *TeamIDWriteError) error {
	reason := "malformed"
	if errors.Is(refusal.Err, ErrTeamIDAmbiguous) {
		reason = "ambiguous"
	}
	slog.Default().WarnContext(ctx, "team_id_write_refused", "writer", writer, "reason", reason)
	return refusal
}
