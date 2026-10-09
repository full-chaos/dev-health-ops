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
// ErrTeamIDForeign, ErrTeamIDAmbiguous or ErrTeamIDCustomHeld).
type TeamIDWriteError struct {
	ID  string
	Err error
}

func (e *TeamIDWriteError) Error() string { return fmt.Sprintf("team id %q: %v", e.ID, e.Err) }

func (e *TeamIDWriteError) Unwrap() error { return e.Err }

const teamIDWriteSeamActiveQuery = `SELECT id, provider FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1 AND id IN {ids:Array(String)}`

// TeamIDRef is a team id a writer takes from outside, with the provider it
// is named for ("" for an admin id).
type TeamIDRef struct {
	Provider string
	ID       string
}

// AdminTeamIDRefs names ids an admin gives without a provider.
func AdminTeamIDRefs(ids ...string) []TeamIDRef {
	refs := make([]TeamIDRef, len(ids))
	for i, id := range ids {
		refs[i] = TeamIDRef{ID: id}
	}
	return refs
}

// KeyTeamIDsForWrite is the one write seam of a team id a writer takes
// from outside (an admin request): every such writer calls it and writes
// only the ids it returns, in the order given. It carries the
// organization's bare team ids first (CarryTeamIDsBeforeWrite), so a bare
// team the request names has already moved, then resolves each id with
// ResolveTeamID against the organization's active prefixed teams: a bare
// admin id goes to the one team that holds it, else custom:<id>; an id
// named for a provider goes to that provider's id. A malformed id, a
// prefixed id of another provider than the one named (ErrTeamIDForeign), a
// bare id two teams hold (ErrTeamIDAmbiguous), and a custom:<id> that a
// team of another source holds (ErrTeamIDCustomHeld) are refused with a
// *TeamIDWriteError before anything but the carry is written.
// See docs/contribute/architecture/team-attribution.md "Team ids".
func KeyTeamIDsForWrite(ctx context.Context, conn TeamIDCarryConn, orgID, writer string, refs []TeamIDRef) ([]string, error) {
	if err := CarryTeamIDsBeforeWrite(ctx, conn, orgID, writer); err != nil {
		return nil, err
	}
	var candidates []string
	for _, ref := range refs {
		candidates = append(candidates, teamid.Candidates(ref.ID)...)
	}
	// active: prefixed id -> provider of its current row ("" for an admin row).
	active := map[string]string{}
	if len(candidates) > 0 {
		rows, err := conn.Query(ctx, teamIDWriteSeamActiveQuery, clickhouse.Named("org_id", strings.TrimSpace(orgID)), clickhouse.Named("ids", candidates))
		if err != nil {
			return nil, fmt.Errorf("team id write seam: read teams: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var id, provider string
			if err := rows.Scan(&id, &provider); err != nil {
				return nil, fmt.Errorf("team id write seam: read teams: %w", err)
			}
			active[id] = provider
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("team id write seam: read teams: %w", err)
		}
	}
	keyed := make([]string, len(refs))
	for i, ref := range refs {
		req := TeamIDRequest{Provider: ref.Provider, ID: ref.ID, Mode: TeamIDOwner, Holders: map[string]string{}}
		for _, candidate := range teamid.Candidates(ref.ID) {
			rowProvider, ok := active[candidate]
			if !ok {
				continue
			}
			holder := teamIDPrefixProvider(candidate)
			if holder == teamIDCarryAdminProvider && rowProvider != "" {
				req.CustomHeld = true
				continue
			}
			req.Holders[holder] = candidate
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
	case errors.Is(refusal.Err, ErrTeamIDCustomHeld):
		reason = "custom_held"
	}
	slog.Default().WarnContext(ctx, "team_id_write_refused", "writer", writer, "reason", reason)
	return refusal
}
