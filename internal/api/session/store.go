package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// errMultipleRows is SQLAlchemy's MultipleResultsFound from
// scalar_one_or_none/one_or_none: an unhandled exception, a bare 500.
var errMultipleRows = errors.New("session: more than one row where one or none was expected")

// userRow is the users columns the routes read.
type userRow struct {
	ID             uuid.UUID
	Email          string
	Username       *string
	FullName       *string
	PasswordHash   *string
	AuthProvider   *string
	AuthProviderID *string
	IsActive       bool
	IsVerified     bool
	IsSuperuser    bool
	TokenVersion   int64
}

const userColumns = `id, email, username, full_name, password_hash, auth_provider, auth_provider_id,
       is_active, is_verified, is_superuser, token_version`

func scanUsers(rows pgx.Rows) (*userRow, error) {
	defer rows.Close()
	var found *userRow
	for rows.Next() {
		if found != nil {
			return nil, errMultipleRows
		}
		var user userRow
		if err := rows.Scan(&user.ID, &user.Email, &user.Username, &user.FullName, &user.PasswordHash,
			&user.AuthProvider, &user.AuthProviderID, &user.IsActive, &user.IsVerified, &user.IsSuperuser,
			&user.TokenVersion); err != nil {
			return nil, err
		}
		found = &user
	}
	return found, rows.Err()
}

// userByEmail is select(User).where(func.lower(User.email) == email).
func userByEmail(ctx context.Context, tx pgx.Tx, email string) (*userRow, error) {
	rows, err := tx.Query(ctx, `SELECT `+userColumns+` FROM users WHERE lower(email) = $1::text`, email)
	if err != nil {
		return nil, err
	}
	return scanUsers(rows)
}

// userByID is select(User).where(User.id == id).
func userByID(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*userRow, error) {
	rows, err := tx.Query(ctx, `SELECT `+userColumns+` FROM users WHERE id = $1::uuid`, id)
	if err != nil {
		return nil, err
	}
	return scanUsers(rows)
}

// userByProvider is the social-login lookup by provider identity.
func userByProvider(ctx context.Context, tx pgx.Tx, providerUserID, provider string) (*userRow, error) {
	rows, err := tx.Query(ctx, `SELECT `+userColumns+` FROM users WHERE auth_provider_id = $1::text AND auth_provider = $2::text`,
		providerUserID, provider)
	if err != nil {
		return nil, err
	}
	return scanUsers(rows)
}

// touchLastLogin is setattr(user, "last_login_at", now) flushed: users has
// an onupdate on updated_at, so the UPDATE writes it too.
func touchLastLogin(ctx context.Context, tx pgx.Tx, id uuid.UUID, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE users SET last_login_at = $2, updated_at = $2 WHERE id = $1::uuid`, id, now.UTC())
	return err
}

// membershipRow is the memberships columns the routes read.
type membershipRow struct {
	OrgID     uuid.UUID
	Role      *string
	JoinedAt  *time.Time
	CreatedAt time.Time
}

// firstMembershipOrg is select(Membership.org_id).where(user_id).limit(1).
func firstMembershipOrg(ctx context.Context, tx pgx.Tx, userID uuid.UUID) (*uuid.UUID, error) {
	var orgID uuid.UUID
	err := tx.QueryRow(ctx, `SELECT org_id FROM memberships WHERE user_id = $1::uuid LIMIT 1`, userID).Scan(&orgID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &orgID, nil
}

// firstMembership is select(Membership).where(user_id).limit(1).
func firstMembership(ctx context.Context, tx pgx.Tx, userID uuid.UUID) (*membershipRow, error) {
	var m membershipRow
	err := tx.QueryRow(ctx, `SELECT org_id, role, joined_at, created_at FROM memberships WHERE user_id = $1::uuid LIMIT 1`,
		userID).Scan(&m.OrgID, &m.Role, &m.JoinedAt, &m.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// orderedMemberships is the login route's membership list, ordered by
// joined_at then created_at (Postgres ASC: NULLs last).
func orderedMemberships(ctx context.Context, tx pgx.Tx, userID uuid.UUID) ([]membershipRow, error) {
	rows, err := tx.Query(ctx, `SELECT org_id, role, joined_at, created_at FROM memberships
WHERE user_id = $1::uuid ORDER BY joined_at ASC, created_at ASC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []membershipRow
	for rows.Next() {
		var m membershipRow
		if err := rows.Scan(&m.OrgID, &m.Role, &m.JoinedAt, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// membershipRole is the role of userID in orgID, nil when there is no
// membership; found reports whether a row exists.
func membershipRole(ctx context.Context, tx pgx.Tx, userID, orgID uuid.UUID) (role *string, found bool, err error) {
	rows, err := tx.Query(ctx, `SELECT role FROM memberships WHERE user_id = $1::uuid AND org_id = $2::uuid`, userID, orgID)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	for rows.Next() {
		if found {
			return nil, false, errMultipleRows
		}
		if err := rows.Scan(&role); err != nil {
			return nil, false, err
		}
		found = true
	}
	return role, found, rows.Err()
}

func organizationExists(ctx context.Context, tx pgx.Tx, orgID uuid.UUID) (bool, error) {
	var id uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM organizations WHERE id = $1::uuid`, orgID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// hashToken is refresh_tokens._hash_token: sha256 of the jti's UTF-8.
func hashToken(jti string) string {
	digest := sha256.Sum256([]byte(jti))
	return hex.EncodeToString(digest[:])
}

// refreshRecord is one refresh_tokens row.
type refreshRecord struct {
	ID           uuid.UUID
	UserID       uuid.UUID
	OrgID        *uuid.UUID
	FamilyID     uuid.UUID
	ExpiresAt    time.Time
	RevokedAt    *time.Time
	SuccessorJTI *string
	IPAddress    *string
	UserAgent    *string
}

// refreshByHash is find_by_hash (forUpdate=false) or
// find_by_hash_for_update.
func refreshByHash(ctx context.Context, tx pgx.Tx, jti string, forUpdate bool) (*refreshRecord, error) {
	query := `SELECT id, user_id, org_id, family_id, expires_at, revoked_at, successor_jti, ip_address, user_agent
FROM refresh_tokens WHERE token_hash = $1::text`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	rows, err := tx.Query(ctx, query, hashToken(jti))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var found *refreshRecord
	for rows.Next() {
		if found != nil {
			return nil, errMultipleRows
		}
		var record refreshRecord
		if err := rows.Scan(&record.ID, &record.UserID, &record.OrgID, &record.FamilyID, &record.ExpiresAt,
			&record.RevokedAt, &record.SuccessorJTI, &record.IPAddress, &record.UserAgent); err != nil {
			return nil, err
		}
		found = &record
	}
	return found, rows.Err()
}

// insertRefresh is create_refresh_token (the row, not the JWT).
func insertRefresh(ctx context.Context, tx pgx.Tx, id uuid.UUID, record refreshRecord, tokenHash string, now time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO refresh_tokens
	(id, user_id, org_id, token_hash, family_id, expires_at, revoked_at, replaced_by_hash, successor_jti,
	 ip_address, user_agent, created_at)
VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, NULL, $7, $8, $9)`,
		id, record.UserID, record.OrgID, tokenHash, record.FamilyID, record.ExpiresAt.UTC(),
		record.IPAddress, record.UserAgent, now.UTC())
	return err
}

// revokeFamily is revoke_family.
func revokeFamily(ctx context.Context, tx pgx.Tx, familyID uuid.UUID, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = $2 WHERE family_id = $1::uuid AND revoked_at IS NULL`,
		familyID, now.UTC())
	return err
}

// revokeToken is revoke_token.
func revokeToken(ctx context.Context, tx pgx.Tx, jti string, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = $2 WHERE token_hash = $1::text AND revoked_at IS NULL`,
		hashToken(jti), now.UTC())
	return err
}

// markRotated is rotate_token's update of the old row.
func markRotated(ctx context.Context, tx pgx.Tx, id uuid.UUID, successorJTI string, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE refresh_tokens SET replaced_by_hash = $2, successor_jti = $3, revoked_at = $4 WHERE id = $1::uuid`,
		id, hashToken(successorJTI), successorJTI, now.UTC())
	return err
}

// attemptRow is one login_attempts row.
type attemptRow struct {
	ID             uuid.UUID
	AttemptCount   int64
	FirstAttemptAt *time.Time
	LockedUntil    *time.Time
}

// attemptByEmail is login_attempts._get_attempt.
func attemptByEmail(ctx context.Context, tx pgx.Tx, email string) (*attemptRow, error) {
	rows, err := tx.Query(ctx, `SELECT id, attempt_count, first_attempt_at, locked_until FROM login_attempts
WHERE lower(email) = $1::text`, email)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var found *attemptRow
	for rows.Next() {
		if found != nil {
			return nil, errMultipleRows
		}
		var row attemptRow
		if err := rows.Scan(&row.ID, &row.AttemptCount, &row.FirstAttemptAt, &row.LockedUntil); err != nil {
			return nil, err
		}
		found = &row
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return found, nil
}

func updateAttempt(ctx context.Context, tx pgx.Tx, row attemptRow, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE login_attempts SET attempt_count = $2, first_attempt_at = $3, locked_until = $4, updated_at = $5
WHERE id = $1::uuid`, row.ID, row.AttemptCount, utcPtr(row.FirstAttemptAt), utcPtr(row.LockedUntil), now.UTC())
	return err
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	value := t.UTC()
	return &value
}
