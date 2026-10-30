package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// ErrBusy reports that a session or go-api refresh token row EndOrgInTx must
// revoke is locked by another writer. The caller rolls its whole transaction
// back and tries again: it never waits for such a row while it holds the auth
// lock order's principal locks (an Exchange holds its session row and then
// needs a key-share lock on the principal for its new credential).
var ErrBusy = errors.New("session: a credential row of the membership is locked by another writer")

// lockNotAvailable is PostgreSQL's lock_not_available, the answer to NOWAIT.
const lockNotAvailable = "55P03"

// EndOrgInTx ends the principal's credentials scoped to one organization
// inside the caller's transaction, so a membership removal and the end of its
// sessions commit together or not at all:
//
//   - every live auth-service session of the principal in the organization
//     and its refresh credentials;
//   - the principal's go-api refresh_tokens rows of the organization.
//
// Sessions of other organizations and sessions with no organization are not
// read. Rows are locked NOWAIT: a row another writer holds is ErrBusy.
func (s *Store) EndOrgInTx(ctx context.Context, tx audit.TxOps, principalID, organizationID uuid.UUID) (Ended, error) {
	rows, err := tx.Query(ctx, `
		SELECT s.id::text, `+s.effectiveExpiry()+` > clock_timestamp()
		FROM `+s.q+`.sessions s
		WHERE s.principal_id = $1 AND s.organization_id = $2 AND s.revoked_at IS NULL
		ORDER BY s.id FOR UPDATE OF s NOWAIT`, principalID, organizationID)
	if err != nil {
		return Ended{}, endOrgErr("locking the sessions", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		var live bool
		if err := rows.Scan(&id, &live); err != nil {
			rows.Close()
			return Ended{}, endOrgErr("reading the sessions", err)
		}
		if live {
			ids = append(ids, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Ended{}, endOrgErr("locking the sessions", err)
	}
	if len(ids) > 0 {
		if err := s.revokeSessions(ctx, tx, ids, revokedByCaller); err != nil {
			return Ended{}, endOrgErr("revoking the sessions", err)
		}
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM public.refresh_tokens
		WHERE user_id = $1 AND org_id = $2 AND revoked_at IS NULL ORDER BY token_hash FOR UPDATE NOWAIT`,
		principalID, organizationID); err != nil {
		return Ended{}, endOrgErr("locking the go-api refresh tokens", err)
	}
	tag, err := tx.Exec(ctx, `UPDATE public.refresh_tokens SET revoked_at = clock_timestamp()
		WHERE user_id = $1 AND org_id = $2 AND revoked_at IS NULL`, principalID, organizationID)
	if err != nil {
		return Ended{}, endOrgErr("revoking the go-api refresh tokens", err)
	}
	return Ended{Sessions: len(ids), RefreshTokens: tag.RowsAffected()}, nil
}

func endOrgErr(step string, err error) error {
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == lockNotAvailable {
		return fmt.Errorf("%w: %s", ErrBusy, step)
	}
	return fmt.Errorf("%w: %s: %w", ErrStore, step, err)
}
