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
		return UserState{}, false, storeError(err, "SELECT on users (id, is_active, is_superuser, token_version)")
	}
	state := UserState{IsActive: active != nil && *active, IsSuperuser: superuser != nil && *superuser}
	if version != nil {
		state.TokenVersion = *version
	}
	return state, true, nil
}

// Membership is user_is_member_of_org's query, reading the row's role too:
// the existence answers the membership check, the role is the user's role in
// that org. A NULL role reads as "".
func (s PGStore) Membership(ctx context.Context, userID, orgID uuid.UUID) (string, bool, error) {
	var role *string
	err := s.Pool.QueryRow(ctx,
		`SELECT role FROM memberships WHERE user_id = $1 AND org_id = $2 LIMIT 1`, userID, orgID,
	).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, storeError(err, "SELECT on memberships (user_id, org_id, role)")
	}
	if role == nil {
		return "", true, nil
	}
	return *role, true, nil
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
		return nil, storeError(err, "SELECT on impersonation_sessions (id, admin_user_id, target_user_id, target_org_id, target_role, expires_at, ended_at) and users (id, email)")
	}
	return &session, nil
}

// GrantError is a read the store's own database role is not allowed to make
// (SQLSTATE 42501): a privilege its manifest declares has not been applied,
// as when a binary rolls before the migration that grants it. It names the
// grant, so the line that logs the refusal says what to apply. It is never a
// reason to answer with less: the read failed and the request is refused.
type GrantError struct {
	Grant string
	Err   error
}

func (e *GrantError) Error() string { return "policy: the database role lacks " + e.Grant }

func (e *GrantError) Unwrap() error { return e.Err }

// MissingGrant is the grant err reports as missing, or "".
func MissingGrant(err error) string {
	var grant *GrantError
	if errors.As(err, &grant) {
		return grant.Grant
	}
	return ""
}

// storeError is classifyStoreError for a read that needs grant: a permission
// denial names the grant; every other failure is classified as before.
func storeError(err error, grant string) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42501" {
		return &GrantError{Grant: grant, Err: err}
	}
	return classifyStoreError(err)
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
