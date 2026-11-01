package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// ErrBusy reports that a session or go-api refresh token row EndOrgInTx or
// EndOrganizationInTx must revoke is locked by another writer. The caller rolls
// its whole transaction back and tries again: it never waits for such a row
// while it holds the auth lock order's principal or organization locks (an
// Exchange holds its session row and then needs a key-share lock on the
// principal for its new credential; a legacy exchange holds its go-api refresh
// token and then needs a key-share lock on the organization for its session).
var ErrBusy = errors.New("session: a credential row to end is locked by another writer")

// BusyAttempts bounds the attempts of an organization verb that gets ErrBusy.
const BusyAttempts = 3

var busyBackoff = [BusyAttempts - 1]time.Duration{25 * time.Millisecond, 75 * time.Millisecond}

// WaitBusy waits before retry number attempt (1 or 2) after ErrBusy.
func WaitBusy(ctx context.Context, attempt int) error {
	t := time.NewTimer(busyBackoff[attempt-1])
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

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
	sessions, err := s.endLockedSessions(ctx, tx, `
		SELECT s.id::text, `+s.effectiveExpiry()+` > clock_timestamp()
		FROM `+s.q+`.sessions s
		WHERE s.principal_id = $1 AND s.organization_id = $2 AND s.revoked_at IS NULL
		ORDER BY s.id FOR UPDATE OF s NOWAIT`, principalID, organizationID)
	if err != nil {
		return Ended{}, err
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
	return Ended{Sessions: sessions, RefreshTokens: tag.RowsAffected()}, nil
}

// EndOrganizationInTx ends every credential scoped to one organization inside
// the caller's transaction, whoever holds it (a member, an ex-member, a
// superuser), so a suspend or a deprovision and the end of the sessions commit
// together or not at all:
//
//   - every live auth-service session of the organization and its refresh
//     credentials;
//   - the organization's go-api refresh_tokens rows.
//
// Sessions of other organizations and sessions with no organization are not
// read. The caller holds the organization row FOR UPDATE, so rows are locked
// NOWAIT: a legacy exchange holds its go-api refresh token and then waits for
// a key-share lock on that row (the new session's foreign key), and waiting
// for its token would close a deadlock. A row another writer holds is ErrBusy.
func (s *Store) EndOrganizationInTx(ctx context.Context, tx audit.TxOps, organizationID uuid.UUID) (Ended, error) {
	sessions, err := s.endLockedSessions(ctx, tx, `
		SELECT s.id::text, `+s.effectiveExpiry()+` > clock_timestamp()
		FROM `+s.q+`.sessions s
		WHERE s.organization_id = $1 AND s.revoked_at IS NULL
		ORDER BY s.id FOR UPDATE OF s NOWAIT`, organizationID)
	if err != nil {
		return Ended{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM public.refresh_tokens
		WHERE org_id = $1 AND revoked_at IS NULL ORDER BY token_hash FOR UPDATE NOWAIT`, organizationID); err != nil {
		return Ended{}, endOrgErr("locking the go-api refresh tokens", err)
	}
	tag, err := tx.Exec(ctx, `UPDATE public.refresh_tokens SET revoked_at = clock_timestamp()
		WHERE org_id = $1 AND revoked_at IS NULL`, organizationID)
	if err != nil {
		return Ended{}, endOrgErr("revoking the go-api refresh tokens", err)
	}
	return Ended{Sessions: sessions, RefreshTokens: tag.RowsAffected()}, nil
}

// endLockedSessions runs lock, which reads (id, live) of the sessions to end
// FOR UPDATE NOWAIT, and revokes the live ones.
func (s *Store) endLockedSessions(ctx context.Context, tx audit.TxOps, lock string, args ...any) (int, error) {
	rows, err := tx.Query(ctx, lock, args...)
	if err != nil {
		return 0, endOrgErr("locking the sessions", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		var live bool
		if err := rows.Scan(&id, &live); err != nil {
			rows.Close()
			return 0, endOrgErr("reading the sessions", err)
		}
		if live {
			ids = append(ids, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, endOrgErr("locking the sessions", err)
	}
	if len(ids) > 0 {
		if err := s.endSessions(ctx, tx, ids, revokedByCaller, true); err != nil {
			return 0, endOrgErr("revoking the sessions", err)
		}
	}
	return len(ids), nil
}

func endOrgErr(step string, err error) error {
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == lockNotAvailable {
		return fmt.Errorf("%w: %s", ErrBusy, step)
	}
	return fmt.Errorf("%w: %s: %w", ErrStore, step, err)
}
