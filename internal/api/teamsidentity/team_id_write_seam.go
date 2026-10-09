package teamsidentity

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// keyTeamIDs runs the team id write seam (providersync.KeyTeamIDsForWrite)
// for the team ids a request names. Every handler that writes a team id
// calls it before its first read or write of a team and writes only the ids
// it returns (a plain admin id that no team holds comes back as
// custom:<id>). A refusal answers 422 (a malformed id, or a prefixed id
// of another provider than the one an import names) or 409 (a bare id that
// more than one prefixed team holds), before anything is written.
func (h handlers) keyTeamIDs(w http.ResponseWriter, r *http.Request, writer string, refs []providersync.TeamIDRef) ([]string, bool) {
	keyed, err := providersync.KeyTeamIDsForWrite(r.Context(), h.store.Conn, orgIDOf(r.Context()), writer, refs)
	if err == nil {
		return keyed, true
	}
	var refusal *providersync.TeamIDWriteError
	if !errors.As(err, &refusal) {
		h.internal(w, r, "key team ids", err)
		return nil, false
	}
	switch {
	case errors.Is(err, providersync.ErrTeamIDAmbiguous):
		policy.WriteDetail(w, http.StatusConflict, fmt.Sprintf("Team id %q names more than one provider team; use the provider-prefixed id", refusal.ID), nil)
	case errors.Is(err, providersync.ErrTeamIDNoOrigin):
		h.internal(w, r, "key team ids", err)
	case errors.Is(err, providersync.ErrTeamIDForeign):
		policy.WriteDetail(w, http.StatusUnprocessableEntity, fmt.Sprintf("Team id %q carries another provider's prefix than its provider", refusal.ID), nil)
	default:
		policy.WriteDetail(w, http.StatusUnprocessableEntity, fmt.Sprintf("Team id %q is empty or only a provider prefix", refusal.ID), nil)
	}
	return nil, false
}

// readTeamID resolves the team id a read route names
// (providersync.ResolveTeamIDForRead): a bare id reads the one team that
// holds it, two holders answer 409 as a write does, none reads the id as
// given.
func (h handlers) readTeamID(w http.ResponseWriter, r *http.Request, teamID string) (string, bool) {
	resolved, err := providersync.ResolveTeamIDForRead(r.Context(), h.store.Conn, orgIDOf(r.Context()), teamID)
	if err == nil {
		return resolved, true
	}
	if errors.Is(err, providersync.ErrTeamIDAmbiguous) {
		policy.WriteDetail(w, http.StatusConflict, fmt.Sprintf("Team id %q names more than one provider team; use the provider-prefixed id", teamID), nil)
		return "", false
	}
	h.internal(w, r, "resolve team id", err)
	return "", false
}

// checkKeyedTeamID is the store's guard before it writes a team id: an id
// without a provider prefix, or a malformed one, is refused, so a writer
// that did not go through keyTeamIDs fails before its write.
func checkKeyedTeamID(id string) error {
	if !teamid.HasKey(id) || teamid.Malformed(id) {
		return fmt.Errorf("%w: admin write of team id %q", teamid.ErrBareTeamID, id)
	}
	return nil
}
