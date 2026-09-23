package admin

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgStore is the Postgres surface the admin area reads and writes: orgs,
// users, memberships, invites, and impersonation_sessions.
type pgStore struct {
	Pool *pgxpool.Pool
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
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

// userByID is impersonation.py's `select(User).where(User.id == ...)`.
func (s pgStore) userByID(ctx context.Context, id uuid.UUID) (*targetUser, error) {
	var user targetUser
	err := s.Pool.QueryRow(ctx,
		`SELECT id, email, is_active, is_superuser FROM users WHERE id = $1`, id,
	).Scan(&user.ID, &user.Email, &user.IsActive, &user.IsSuperuser)
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

// userHasMembership is _ensure_user_in_scope's membership existence check:
// does userID have ANY membership row in orgID.
func (s pgStore) userHasMembership(ctx context.Context, orgID, userID uuid.UUID) (bool, error) {
	var exists bool
	err := s.Pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM memberships WHERE org_id = $1 AND user_id = $2)`, orgID, userID,
	).Scan(&exists)
	return exists, err
}

// membershipRole is _ensure_org_admin_access's MembershipService.get_membership:
// the caller's own membership role in orgID, or "" when there is none.
func (s pgStore) membershipRole(ctx context.Context, orgID, userID uuid.UUID) (string, error) {
	var role string
	err := s.Pool.QueryRow(ctx,
		`SELECT role FROM memberships WHERE org_id = $1 AND user_id = $2`, orgID, userID,
	).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return role, nil
}
