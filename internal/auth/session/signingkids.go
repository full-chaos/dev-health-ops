package session

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// RecordSigningKid adds kid to sessions.signing_kids of the session. An empty
// kid records an hs256_compat mint: the set becomes '{}' when it is NULL and
// stays as it is otherwise. The statement is one UPDATE per mint and a no-op
// when the kid is present; the row lock serialises two concurrent refreshes of
// one session, and the second re-checks the set after the first commits, so a
// kid never appears twice. A session with no row is ErrSessionNotFound.
func (s *Store) RecordSigningKid(ctx context.Context, sessionID uuid.UUID, kid string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE `+s.q+`.sessions
		SET signing_kids = CASE WHEN $2::text = '' THEN COALESCE(signing_kids, '{}'::text[])
		                        ELSE array_append(COALESCE(signing_kids, '{}'::text[]), $2::text) END
		WHERE id = $1::uuid
		  AND (signing_kids IS NULL OR ($2::text <> '' AND NOT ($2::text = ANY(signing_kids))))`,
		sessionID.String(), kid)
	if err != nil {
		return fmt.Errorf("%w: recording the signing kid: %w", ErrStore, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+s.q+`.sessions WHERE id = $1::uuid)`, sessionID.String()).Scan(&exists); err != nil {
		return fmt.Errorf("%w: recording the signing kid: %w", ErrStore, err)
	}
	if !exists {
		return ErrSessionNotFound
	}
	return nil
}
