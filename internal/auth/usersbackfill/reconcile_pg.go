package usersbackfill

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// OpenSnapshot begins the one transaction the reconciliation reads both
// schemas in: REPEATABLE READ and READ ONLY, so every page sees one snapshot
// and no statement can write. It proves both settings on the live session and
// refuses to go on when either is missing.
func OpenSnapshot(ctx context.Context, pool *pgxpool.Pool) (pgx.Tx, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, errors.New("usersbackfill: could not open the read-only snapshot")
	}
	var isolation, readOnly string
	if err := tx.QueryRow(ctx, `SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')`).
		Scan(&isolation, &readOnly); err != nil || isolation != "repeatable read" || readOnly != "on" {
		_ = tx.Rollback(context.Background())
		return nil, errors.New("usersbackfill: the session is not a repeatable-read read-only snapshot")
	}
	return tx, nil
}

// TxSource reads public.users inside the snapshot transaction.
type TxSource struct{ Tx pgx.Tx }

// Page returns up to limit users with id greater than after, in id order.
func (s TxSource) Page(ctx context.Context, after *uuid.UUID, limit int) ([]Row, error) {
	return pageUsers(ctx, s.Tx, after, limit)
}

// TxTarget reads the auth schema inside the snapshot transaction.
type TxTarget struct {
	Tx     pgx.Tx
	Schema authschema.ValidatedIdentifier
}

// Page returns up to limit principals with id greater than after, in id order.
func (t TxTarget) Page(ctx context.Context, after *uuid.UUID, limit int) ([]AuthRow, error) {
	q := authschema.Quote(t.Schema)
	rows, err := t.Tx.Query(ctx, `SELECT p.id, p.kind, u.principal_id IS NOT NULL,
       COALESCE(u.email, ''), COALESCE(u.email_lower, ''), p.disabled_at IS NULL, u.email_verified_at IS NOT NULL,
       u.password_hash, COALESCE(u.legacy_token_version, 0), u.last_login_at, p.display_name,
       EXISTS (SELECT 1 FROM `+q+`.platform_role_assignments r WHERE r.principal_id = p.id AND r.role_key = $3),
       EXISTS (SELECT 1 FROM `+q+`.platform_role_assignments r WHERE r.principal_id = p.id AND r.role_key = $3 AND r.granted_by IS NULL)
FROM `+q+`.principals p LEFT JOIN `+q+`.users u ON u.principal_id = p.id
WHERE ($1::uuid IS NULL OR p.id > $1::uuid) ORDER BY p.id LIMIT $2`, after, limit, identity.PlatformAdminRole)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuthRow
	for rows.Next() {
		var r AuthRow
		if err := rows.Scan(&r.ID, &r.Kind, &r.HasUser, &r.Email, &r.EmailLower, &r.Active, &r.Verified,
			&r.PasswordHash, &r.TokenVersion, &r.LastLoginAt, &r.DisplayName, &r.RoleHeld, &r.RoleBackfilled); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// IdentityHolder returns the principal that holds an external identity.
func (t TxTarget) IdentityHolder(ctx context.Context, key identity.ExternalKey) (uuid.UUID, bool, error) {
	var holder uuid.UUID
	err := t.Tx.QueryRow(ctx, `SELECT principal_id FROM `+authschema.Quote(t.Schema)+`.external_identities
WHERE issuer = $1 AND provider = $2 AND subject = $3`, key.Issuer, string(key.Provider), key.Subject).Scan(&holder)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return uuid.Nil, false, nil
	case err != nil:
		return uuid.Nil, false, err
	}
	return holder, true, nil
}
