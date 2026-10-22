package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// BumpLegacyTokenVersionFunction is the increment-only function the runtime
// role calls to raise go-api's users.token_version (migration 0022). The role
// holds no UPDATE on public.users, so a lower value cannot be written.
const BumpLegacyTokenVersionFunction = "bump_legacy_token_version"

// errLegacyUserMissing is a principal go-api's users table does not hold.
var errLegacyUserMissing = errors.New("session: no go-api user row for the principal")

// revokeAllLive revokes every effectively live session of the principal inside
// tx and returns how many it revoked.
//
// Locks are taken in id order, so two revoke-alls of one principal cannot
// deadlock; a single-session writer holds one lock and cannot either. Every
// unrevoked session is locked, then only the effectively live ones are revoked
// (the same liveness every writer uses).
func (s *Store) revokeAllLive(ctx context.Context, tx audit.TxOps, principal string) (int, error) {
	rows, err := tx.Query(ctx, `
		SELECT s.id::text, `+s.effectiveExpiry()+` > clock_timestamp()
		FROM `+s.q+`.sessions s
		WHERE s.principal_id = $1 AND s.revoked_at IS NULL
		ORDER BY s.id FOR UPDATE OF s`, principal)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		var live bool
		if err := rows.Scan(&id, &live); err != nil {
			rows.Close()
			return 0, err
		}
		if live {
			ids = append(ids, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	return len(ids), s.revokeSessions(ctx, tx, ids, revokedByCaller)
}

// EndAllInTx ends every credential class of the principal inside the caller's
// transaction, so a password change and the end of the older credentials
// commit together or not at all:
//
//   - every live auth-service session and its refresh credentials;
//   - go-api's users.token_version, raised by one through the increment-only
//     function, which ends the access tokens go-api minted without a sid;
//   - the principal's go-api refresh_tokens rows (revoke_all_for_user), which
//     ends the legacy refresh exchange.
//
// Any error is returned for the caller to roll the transaction back.
func (s *Store) EndAllInTx(ctx context.Context, tx audit.TxOps, principalID uuid.UUID) error {
	principal := principalID.String()
	if _, err := s.revokeAllLive(ctx, tx, principal); err != nil {
		return fmt.Errorf("%w: revoking the sessions: %w", ErrStore, err)
	}
	var version *int64
	if err := tx.QueryRow(ctx, `SELECT `+s.q+`.`+BumpLegacyTokenVersionFunction+`($1::uuid)`, principal).Scan(&version); err != nil {
		return fmt.Errorf("%w: raising the go-api token version: %w", ErrStore, err)
	}
	if version == nil {
		return fmt.Errorf("%w: %w", ErrStore, errLegacyUserMissing)
	}
	if _, err := tx.Exec(ctx, `UPDATE public.refresh_tokens SET revoked_at = clock_timestamp()
		WHERE user_id = $1::uuid AND revoked_at IS NULL`, principal); err != nil {
		return fmt.Errorf("%w: revoking the go-api refresh tokens: %w", ErrStore, err)
	}
	return nil
}
