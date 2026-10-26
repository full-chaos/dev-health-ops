package local

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// A reset link is bound to the credential state it was issued against: the row
// keeps public.users.token_version as it was at issue (issued_token_version),
// and the link redeems only while the user's counter still has that value.
// Every password write raises the counter in the same statement (go-api's
// change, reset and admin set-password; auth-service's owned write), so a link
// issued before any of them is an unknown link afterwards, whichever service
// wrote the password. A row without a value (issued before the column) never
// redeems.

// liveTokenVersion is the user's current public.users.token_version, for the
// link row in scope.
const liveTokenVersion = `(SELECT COALESCE(u.token_version, 0) FROM public.users u WHERE u.id = link_credentials.principal_id)`

// boundToLiveTokenVersion is the condition PeekResetLink adds: the counter has
// not moved since the link was issued.
const boundToLiveTokenVersion = ` AND c.issued_token_version = (SELECT COALESCE(lu.token_version, 0) FROM public.users lu WHERE lu.id = c.principal_id)`

// IssueBound is Issue with the row bound to the principal's current
// public.users.token_version. The statements, their order and the decoy are
// Issue's; only the insert also writes issued_token_version.
func (l *PGLinks) IssueBound(ctx context.Context, req IssueRequest) error {
	real := req.PrincipalID != uuid.Nil
	principal := req.PrincipalID
	if !real {
		principal = uuid.New()
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("local: storing a link credential: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey("link:"+string(req.Purpose), principal.String())); err != nil {
		return fmt.Errorf("local: locking the issue: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM `+l.q+`.link_credentials
WHERE principal_id = $1::uuid AND purpose = $2 AND consumed_at IS NULL AND $3::boolean`, principal.String(), string(req.Purpose), real); err != nil {
		return fmt.Errorf("local: replacing link credentials: %w", err)
	}
	// $5 false is a decoy: the same statement text, no row.
	if _, err := tx.Exec(ctx, `INSERT INTO `+l.q+`.link_credentials (principal_id, purpose, token_hash, expires_at, created_at, issued_token_version)
SELECT $1::uuid, $2, $3, now() + make_interval(secs => $4::double precision), clock_timestamp(),
       (SELECT COALESCE(u.token_version, 0) FROM public.users u WHERE u.id = $1::uuid)
WHERE $5::boolean`, principal.String(), string(req.Purpose), req.TokenHash[:], req.TTL.Seconds(), real); err != nil {
		return fmt.Errorf("local: storing a link credential: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("local: storing a link credential: %w", err)
	}
	return nil
}

// ConsumeBoundResetLinkInTx is ConsumeResetLinkInTx with the binding. It runs
// in the password transaction after that transaction has raised the counter by
// exactly one (identity.Store.SetPasswordHashFor writes the password, then
// calls the end callback that consumes the link), so the link's value must be
// the current counter minus one: any other password write since the link was
// issued, including one between Reset's read of the link and this transaction,
// leaves it unusable, and the transaction rolls back.
func (l *PGLinks) ConsumeBoundResetLinkInTx(ctx context.Context, tx audit.TxOps, principalID uuid.UUID, tokenHash [32]byte) error {
	tag, err := tx.Exec(ctx, `UPDATE `+l.q+`.link_credentials SET consumed_at = now()
WHERE token_hash = $1 AND purpose = $2 AND principal_id = $3::uuid AND consumed_at IS NULL AND expires_at > now()
  AND issued_token_version = `+liveTokenVersion+` - 1`,
		tokenHash[:], string(PurposePasswordReset), principalID.String())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return identity.ErrLinkUnusable
	}
	return nil
}
