//go:build integration

package policy

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// pgFixtureSessions is a PGStore whose SessionState answers as the unit fixture does: the token's sid (the user id)
// names a live session of that user, pinned at the users row's token_version read live from PostgreSQL. Every other
// read is the PGStore's. The real auth.sessions read is covered by the pinrig cells (see the 13154 record).
type pgFixtureSessions struct{ PGStore }

func (s pgFixtureSessions) SessionState(ctx context.Context, id uuid.UUID) (SessionRow, bool, error) {
	state, found, err := s.UserState(ctx, id)
	if err != nil || !found {
		return SessionRow{}, false, err
	}
	version := state.TokenVersion
	return SessionRow{PrincipalID: id, LegacyTokenVersion: &version}, true, nil
}

// pgAuthenticator is authenticator over a PostgreSQL store with fixture sessions.
func pgAuthenticator(t *testing.T, store PGStore) *Authenticator {
	t.Helper()
	return authenticator(t, pgFixtureSessions{store})
}
