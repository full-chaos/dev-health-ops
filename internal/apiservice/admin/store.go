package admin

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// pgStore is the Postgres surface the admin area reads and writes: orgs,
// users, memberships, invites, and impersonation_sessions.
// pgDB is the query surface pgStore uses: a *pgxpool.Pool (the routes) or a
// pgx.Tx (the operator verbs, which run one Python session's writes as one
// transaction). Both satisfy it, Begin included (a pgx.Tx begins a savepoint).
type pgDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type pgStore struct {
	Pool pgDB
	// UsersOwned is AUTH_USERS_OWNED: auth-service owns users, so every
	// public.users writer of the store refuses before it writes.
	UsersOwned bool
	// PlatformRolesOwned is AUTH_PLATFORM_ROLES_OWNED: auth-service owns the
	// platform roles, so a write of public users.is_superuser refuses.
	PlatformRolesOwned bool
	// AuthSchema is AUTH_DATABASE_SCHEMA, the schema the platform role rule
	// reads while PlatformRolesOwned (empty: "auth").
	AuthSchema string
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

// platformRoles is how this store's users reads decide is_superuser.
func (s pgStore) platformRoles() policy.PlatformRoleRead {
	return policy.PlatformRoleRead{Owned: s.PlatformRolesOwned, AuthSchema: s.AuthSchema}
}

func (s pgStore) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// targetUser is the users row start_impersonation reads to validate the
// impersonation target.
type targetUser struct {
	ID          uuid.UUID
	Email       string
	IsActive    bool
	IsSuperuser bool
}

// targetUserSQL is userByID's read, with is_superuser by the platform roles switch.
func (s pgStore) targetUserSQL() (string, error) {
	superuser, err := s.platformRoles().Superuser("is_superuser", "users.id")
	if err != nil {
		return "", err
	}
	return `SELECT id, email, is_active, ` + superuser + ` FROM users WHERE id = $1`, nil
}

// userByID is impersonation.py's `select(User).where(User.id == ...)`.
func (s pgStore) userByID(ctx context.Context, id uuid.UUID) (*targetUser, error) {
	var user targetUser
	query, err := s.targetUserSQL()
	if err != nil {
		return nil, err
	}
	err = s.Pool.QueryRow(ctx, query, id).Scan(&user.ID, &user.Email, &user.IsActive, &user.IsSuperuser)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &user, nil
}

// firstMembership is _first_membership: the target's earliest-created
// membership row (org_id, role), the source of a fresh impersonation
// session's target org and role.
type firstMembership struct {
	OrgID uuid.UUID
	Role  string
}

func (s pgStore) firstMembership(ctx context.Context, userID uuid.UUID) (*firstMembership, error) {
	var membership firstMembership
	err := s.Pool.QueryRow(ctx, `
SELECT org_id, role FROM memberships
WHERE user_id = $1
ORDER BY created_at ASC
LIMIT 1`, userID,
	).Scan(&membership.OrgID, &membership.Role)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &membership, nil
}

// endActiveImpersonationSessions is start_impersonation's
// `UPDATE ... SET ended_at = now() WHERE admin_user_id = $1 AND ended_at IS
// NULL`: end any session this admin already has open, before starting a new
// one.
func (s pgStore) endActiveImpersonationSessions(ctx context.Context, tx pgx.Tx, adminUserID uuid.UUID) error {
	_, err := tx.Exec(ctx, `
UPDATE impersonation_sessions
SET ended_at = $2
WHERE admin_user_id = $1 AND ended_at IS NULL`, adminUserID, s.now().UTC())
	return err
}

// insertImpersonationSession is start_impersonation's
// `ImpersonationSession(...)`; returns the new row's id.
func (s pgStore) insertImpersonationSession(
	ctx context.Context, tx pgx.Tx,
	adminUserID, targetUserID, targetOrgID uuid.UUID, targetRole string, expiresAt time.Time,
) (uuid.UUID, error) {
	id := uuid.New()
	_, err := tx.Exec(ctx, `
INSERT INTO impersonation_sessions
	(id, admin_user_id, target_user_id, target_org_id, target_role, created_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		id, adminUserID, targetUserID, targetOrgID, targetRole, s.now().UTC(), expiresAt)
	if err != nil {
		return uuid.UUID{}, err
	}
	return id, nil
}

// activeSessionRow is the row stop_impersonation and the DB-fallback path of
// activeImpersonationSession both need: the admin's currently open session.
type activeSessionRow struct {
	ID           uuid.UUID
	TargetUserID uuid.UUID
	TargetOrgID  uuid.UUID
	TargetRole   string
	TargetEmail  *string
	ExpiresAt    time.Time
}

// activeImpersonationSessionTx is stop_impersonation's own lookup: the
// admin's session with ended_at IS NULL AND expires_at > now, inside the
// same transaction the caller ends it in.
func (s pgStore) activeImpersonationSessionTx(ctx context.Context, tx pgx.Tx, adminUserID uuid.UUID) (*activeSessionRow, error) {
	var row activeSessionRow
	err := tx.QueryRow(ctx, `
SELECT id, target_user_id, target_org_id, target_role, expires_at
FROM impersonation_sessions
WHERE admin_user_id = $1 AND ended_at IS NULL AND expires_at > $2
LIMIT 1`, adminUserID, s.now().UTC(),
	).Scan(&row.ID, &row.TargetUserID, &row.TargetOrgID, &row.TargetRole, &row.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// endImpersonationSession is stop_impersonation's
// `active_session.ended_at = now()`.
func (s pgStore) endImpersonationSession(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	_, err := tx.Exec(ctx, `UPDATE impersonation_sessions SET ended_at = $2 WHERE id = $1`, id, s.now().UTC())
	return err
}

// activeImpersonationSession is impersonation_cache.py's _load_from_db: the
// admin's active, unexpired, unended session, joined to the target's email
// -- the impersonationDBLoader Postgres fallback for impersonationCache.
func (s pgStore) activeImpersonationSession(ctx context.Context, adminUserID string) (*cachedImpersonationSession, error) {
	admin, err := parseUUID(adminUserID)
	if err != nil {
		// authenticate_access_token already parsed this id off the JWT
		// sub claim; a malformed value here would be a programmer error,
		// not a real "no session" state. Surface it as a lookup failure
		// (fail-open, per activeSession's own contract), never a false
		// negative-cache write.
		return nil, err
	}
	var row activeSessionRow
	err = s.Pool.QueryRow(ctx, `
SELECT s.id, s.target_user_id, s.target_org_id, s.target_role, u.email, s.expires_at
FROM impersonation_sessions s
JOIN users u ON u.id = s.target_user_id
WHERE s.admin_user_id = $1 AND s.ended_at IS NULL AND s.expires_at > $2
LIMIT 1`, admin, s.now().UTC(),
	).Scan(&row.ID, &row.TargetUserID, &row.TargetOrgID, &row.TargetRole, &row.TargetEmail, &row.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &cachedImpersonationSession{
		ID:           row.ID.String(),
		AdminUserID:  adminUserID,
		TargetUserID: row.TargetUserID.String(),
		TargetOrgID:  row.TargetOrgID.String(),
		TargetRole:   row.TargetRole,
		TargetEmail:  row.TargetEmail,
		ExpiresAt:    row.ExpiresAt,
	}, nil
}

func parseUUID(value string) (uuid.UUID, error) {
	return uuid.Parse(value)
}

// ErrUsersOwned is the refusal of a public.users write while auth-service owns
// users; the routes answer the moved operation and the operator verbs exit refused.
var ErrUsersOwned = usersowned.ErrRefused

// ErrPlatformRolesOwned is the refusal of an is_superuser write while
// auth-service owns the platform roles.
var ErrPlatformRolesOwned = usersowned.ErrPlatformRolesRefused

func (s pgStore) refuseWhileUsersOwned() error { return usersowned.RefuseWhileUsersOwned(s.UsersOwned) }

// refuseSuperuserWhileOwned is the guard of a write that sets is_superuser:
// while auth-service owns the platform roles, it refuses before it writes.
func refuseSuperuserWhileOwned(owned, writesSuperuser bool) error {
	if owned && writesSuperuser {
		return ErrPlatformRolesOwned
	}
	return nil
}
