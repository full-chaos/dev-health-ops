package policy

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Impersonation is an active impersonation_sessions row
// (services/impersonation_cache.py CachedImpersonationSession).
type Impersonation struct {
	ID           uuid.UUID
	AdminUserID  uuid.UUID
	TargetUserID uuid.UUID
	TargetOrgID  uuid.UUID
	TargetRole   string
	TargetEmail  *string
	ExpiresAt    time.Time
}

// PGStore is Store over the api Service's Postgres pool.
type PGStore struct {
	Pool *pgxpool.Pool
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

func (s PGStore) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// UserState reads users.is_active, is_superuser and token_version. NULL
// is_active and is_superuser read as false and NULL token_version as 0, as
// authenticate_access_token's truthiness does.
func (s PGStore) UserState(ctx context.Context, id uuid.UUID) (UserState, bool, error) {
	var active, superuser *bool
	var version *int64
	err := s.Pool.QueryRow(ctx,
		`SELECT is_active, is_superuser, token_version FROM users WHERE id = $1`, id,
	).Scan(&active, &superuser, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return UserState{}, false, nil
	}
	if err != nil {
		return UserState{}, false, classifyStoreError(err)
	}
	state := UserState{IsActive: active != nil && *active, IsSuperuser: superuser != nil && *superuser}
	if version != nil {
		state.TokenVersion = *version
	}
	return state, true, nil
}

// IsMember is user_is_member_of_org's query.
func (s PGStore) IsMember(ctx context.Context, userID, orgID uuid.UUID) (bool, error) {
	var one int
	err := s.Pool.QueryRow(ctx,
		`SELECT 1 FROM memberships WHERE user_id = $1 AND org_id = $2 LIMIT 1`, userID, orgID,
	).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, classifyStoreError(err)
	}
	return true, nil
}

// ActiveImpersonation is impersonation_cache._load_from_db: the admin's
// session that has not ended and expires in the future, joined to the
// target's users row.
func (s PGStore) ActiveImpersonation(ctx context.Context, adminID uuid.UUID) (*Impersonation, error) {
	var session Impersonation
	err := s.Pool.QueryRow(ctx, `
SELECT s.id, s.admin_user_id, s.target_user_id, s.target_org_id, s.target_role, u.email, s.expires_at
FROM impersonation_sessions s
JOIN users u ON u.id = s.target_user_id
WHERE s.admin_user_id = $1 AND s.ended_at IS NULL AND s.expires_at > $2
LIMIT 1`, adminID, s.now().UTC(),
	).Scan(&session.ID, &session.AdminUserID, &session.TargetUserID, &session.TargetOrgID,
		&session.TargetRole, &session.TargetEmail, &session.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, classifyStoreError(err)
	}
	return &session, nil
}

// classifyStoreError maps a driver error to ErrUnavailable when
// _db_temporarily_unavailable would: a timeout or network failure (net.Error,
// which a refused or dropped connection and context.DeadlineExceeded all
// are), an error the driver marks safe to retry (the query never reached the
// server), or a connection-class SQLSTATE (08xxx, admin/crash shutdown,
// cannot connect now). The driver error is kept for errors.Is, never
// rendered to a client.
func classifyStoreError(err error) error {
	var netErr net.Error
	switch {
	// context.DeadlineExceeded is itself a net.Error (Timeout() true), so the
	// net.Error arm covers a timed-out query; a cancelled request is not one.
	case errors.As(err, &netErr),
		pgconn.SafeToRetry(err),
		errors.Is(err, pgx.ErrTxClosed):
		return errors.Join(ErrUnavailable, err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "57P01" || pgErr.Code == "57P03") {
		return errors.Join(ErrUnavailable, err)
	}
	return err
}
