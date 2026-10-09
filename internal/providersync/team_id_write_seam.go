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
// writer gave it and the reason (teamid.ErrMalformedTeamID,
// ErrTeamIDForeign, ErrTeamIDAmbiguous or ErrTeamIDNoOrigin).
type TeamIDWriteError struct {
	ID  string
	Err error
}

func (e *TeamIDWriteError) Error() string { return fmt.Sprintf("team id %q: %v", e.ID, e.Err) }

func (e *TeamIDWriteError) Unwrap() error { return e.Err }

const teamIDWriteSeamActiveQuery = `SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1 AND id IN {ids:Array(String)}`

// TeamIDRef is a team id a writer takes from outside, with the integration
// of the writer and how it uses the id (TeamIDOwner for an import's own
// team, TeamIDAddress for an admin's).
type TeamIDRef struct {
	Provider string
	ID       string
	Mode     TeamIDMode
}

// AdminTeamIDRefs names ids the web admin gives: its integration is
// teamid.Custom (an admin team is a custom team) and it addresses the teams
// it writes.
func AdminTeamIDRefs(ids ...string) []TeamIDRef {
	refs := make([]TeamIDRef, len(ids))
	for i, id := range ids {
		refs[i] = TeamIDRef{Provider: teamid.Custom, ID: id, Mode: TeamIDAddress}
	}
	return refs
}

// KeyTeamIDsForWrite is the one write seam of a team id a writer takes
// from outside (an admin request): every such writer calls it and writes
// only the ids it returns, in the order given. It carries the
// organization's bare team ids first (CarryTeamIDsBeforeWrite), so a bare
// team the request names has already moved, then resolves each id with
// ResolveTeamID against the organization's active prefixed teams: a bare id
// the admin addresses goes to the one team that holds it, else to a new
// custom:<id>; an id an import owns goes to its provider's id. A
// malformed id, a prefixed id of another provider than the one an import
// names (ErrTeamIDForeign) and a bare id two teams hold
// (ErrTeamIDAmbiguous) are refused with a *TeamIDWriteError before anything
// but the carry is written.
// See docs/contribute/architecture/team-attribution.md "Team ids".
func KeyTeamIDsForWrite(ctx context.Context, conn TeamIDCarryConn, orgID, writer string, refs []TeamIDRef) ([]string, error) {
	if err := CarryTeamIDsBeforeWrite(ctx, conn, orgID, writer); err != nil {
		return nil, err
	}
	var candidates []string
	for _, ref := range refs {
		candidates = append(candidates, teamid.Candidates(ref.ID)...)
	}
	active := map[string]bool{}
	if len(candidates) > 0 {
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
	}
	keyed := make([]string, len(refs))
	for i, ref := range refs {
		req := TeamIDRequest{Provider: ref.Provider, ID: ref.ID, Mode: ref.Mode, Holders: map[string]string{}}
		for _, candidate := range teamid.Candidates(ref.ID) {
			if active[candidate] {
				req.Holders[teamIDPrefixProvider(candidate)] = candidate
			}
		}
		resolved, err := ResolveTeamID(req)
		if err != nil {
			return nil, refuseTeamIDWrite(ctx, writer, &TeamIDWriteError{ID: ref.ID, Err: err})
		}
		keyed[i] = resolved
	}
	return keyed, nil
}

// refuseTeamIDWrite logs a refusal with its reason and writer only (never
// the id) and returns it.
func refuseTeamIDWrite(ctx context.Context, writer string, refusal *TeamIDWriteError) error {
	reason := "malformed"
	switch {
	case errors.Is(refusal.Err, ErrTeamIDAmbiguous):
		reason = "ambiguous"
	case errors.Is(refusal.Err, ErrTeamIDForeign):
		reason = "foreign_provider"
	case errors.Is(refusal.Err, ErrTeamIDNoOrigin):
		reason = "no_origin"
	}
	slog.Default().WarnContext(ctx, "team_id_write_refused", "writer", writer, "reason", reason)
	return refusal
}
