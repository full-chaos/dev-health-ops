package admin

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// fullUser is users.py's User row (the shape _user_response reads).
type fullUser struct {
	ID             uuid.UUID
	Email          string
	Username       *string
	PasswordHash   *string
	FullName       *string
	AvatarURL      *string
	AuthProvider   *string
	AuthProviderID *string
	IsActive       bool
	IsVerified     bool
	IsSuperuser    bool
	TokenVersion   int
	LastLoginAt    *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

const fullUserColumns = `id, email, username, password_hash, full_name, avatar_url, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, last_login_at, created_at, updated_at`

func scanFullUser(row pgx.Row) (*fullUser, error) {
	var u fullUser
	err := row.Scan(&u.ID, &u.Email, &u.Username, &u.PasswordHash, &u.FullName, &u.AvatarURL,
		&u.AuthProvider, &u.AuthProviderID, &u.IsActive, &u.IsVerified, &u.IsSuperuser, &u.TokenVersion,
		&u.LastLoginAt, &u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// fullUserByID is UserService.get_by_id.
func (s pgStore) fullUserByID(ctx context.Context, id uuid.UUID) (*fullUser, error) {
	return scanFullUser(s.Pool.QueryRow(ctx, `SELECT `+fullUserColumns+` FROM users WHERE id = $1`, id))
}

// userByEmail is UserService.get_by_email (case-insensitive).
func (s pgStore) userByEmail(ctx context.Context, email string) (*fullUser, error) {
	return scanFullUser(s.Pool.QueryRow(ctx, `SELECT `+fullUserColumns+` FROM users WHERE lower(email) = lower($1)`, email))
}

// userByUsername is UserService.get_by_username (case-insensitive).
func (s pgStore) userByUsername(ctx context.Context, username string) (*fullUser, error) {
	return scanFullUser(s.Pool.QueryRow(ctx, `SELECT `+fullUserColumns+` FROM users WHERE lower(username) = lower($1)`, username))
}

// listUsersFilter is list_users' shared WHERE clause (active_only + search
// over email/username/full_name), applied by both list_all and list_by_org.
type listUsersFilter struct {
	Limit, Offset int
	ActiveOnly    bool
	Search        *string
}

func (f listUsersFilter) whereActiveAndSearch(nextParam int) (clause string, args []any) {
	if f.ActiveOnly {
		clause += ` AND u.is_active = true`
	}
	if f.Search != nil {
		pattern := "%" + strings.ToLower(*f.Search) + "%"
		clause += ` AND (lower(u.email) LIKE $` + itoa(nextParam) +
			` OR lower(coalesce(u.username, '')) LIKE $` + itoa(nextParam) +
			` OR lower(coalesce(u.full_name, '')) LIKE $` + itoa(nextParam) + `)`
		args = append(args, pattern)
	}
	return clause, args
}

func itoa(n int) string {
	// small, local: avoids importing strconv just for this one call site
	// pattern repeated across two queries.
	digits := "0123456789"
	if n == 0 {
		return "0"
	}
	out := make([]byte, 0, 4)
	for n > 0 {
		out = append([]byte{digits[n%10]}, out...)
		n /= 10
	}
	return string(out)
}

// listAllUsers is UserService.list_all.
func (s pgStore) listAllUsers(ctx context.Context, f listUsersFilter) ([]*fullUser, error) {
	where, args := f.whereActiveAndSearch(3)
	query := `SELECT ` + prefixColumns("u", fullUserColumns) + ` FROM users u WHERE true` + where +
		` ORDER BY u.created_at DESC LIMIT $1 OFFSET $2`
	args = append([]any{f.Limit, f.Offset}, args...)
	return s.queryUsers(ctx, query, args...)
}

// listUsersByOrg is UserService.list_by_org.
func (s pgStore) listUsersByOrg(ctx context.Context, orgID uuid.UUID, f listUsersFilter) ([]*fullUser, error) {
	where, args := f.whereActiveAndSearch(4)
	query := `SELECT ` + prefixColumns("u", fullUserColumns) + ` FROM users u
JOIN memberships m ON m.user_id = u.id
WHERE m.org_id = $3` + where + ` ORDER BY u.created_at DESC LIMIT $1 OFFSET $2`
	args = append([]any{f.Limit, f.Offset, orgID}, args...)
	return s.queryUsers(ctx, query, args...)
}

func (s pgStore) queryUsers(ctx context.Context, query string, args ...any) ([]*fullUser, error) {
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*fullUser
	for rows.Next() {
		u, err := scanFullUser(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func prefixColumns(alias, columns string) string {
	parts := strings.Split(columns, ", ")
	for i, p := range parts {
		parts[i] = alias + "." + p
	}
	return strings.Join(parts, ", ")
}

var errEmailExists = errors.New("email already exists")
var errUsernameExists = errors.New("username already exists")

// userCreateInput is UserCreate's fields, already validated by pybody.
type userCreateInput struct {
	Email, Username, FullName, AuthProvider, AuthProviderID *string
	PasswordHash                                            *string
	IsVerified, IsSuperuser                                 bool
}

// insertUser is UserService.create (password hashing is the caller's --
// bcrypt.GenerateFromPassword before calling this).
func (s pgStore) insertUser(ctx context.Context, in userCreateInput) (*fullUser, error) {
	if in.Email != nil {
		if existing, err := s.userByEmail(ctx, *in.Email); err != nil {
			return nil, err
		} else if existing != nil {
			return nil, errEmailExists
		}
	}
	if in.Username != nil && *in.Username != "" {
		if existing, err := s.userByUsername(ctx, *in.Username); err != nil {
			return nil, err
		} else if existing != nil {
			return nil, errUsernameExists
		}
	}
	now := s.now().UTC()
	u := &fullUser{
		ID: uuid.New(), FullName: in.FullName, PasswordHash: in.PasswordHash,
		AuthProviderID: in.AuthProviderID, IsActive: true, IsVerified: in.IsVerified, IsSuperuser: in.IsSuperuser,
		CreatedAt: now, UpdatedAt: now,
	}
	email := strings.ToLower(strings.TrimSpace(deref(in.Email)))
	u.Email = email
	// UserService.create: `username=username.lower().strip() if username
	// else None` -- Python's truthy check runs on the RAW value, before
	// strip/lower, so a whitespace-only username (falsy check passes: any
	// non-empty string is truthy in Python, including " ") is NOT None --
	// it lowers/strips to "" and is stored/returned as an empty string,
	// never null. Gating on the TRIMMED result's emptiness instead of the
	// raw value's emptiness silently turned that case into a stored NULL.
	if in.Username != nil && *in.Username != "" {
		lowered := strings.ToLower(strings.TrimSpace(*in.Username))
		u.Username = &lowered
	}
	// UserService.create's own signature default (`auth_provider: str =
	// AuthProvider.LOCAL.value`) only ever applies when the caller OMITS
	// the argument -- the router always passes payload.auth_provider
	// (pydantic's own `str = "local"` field), so "local" only reaches here
	// via pydantic's absent-key default, never via a present, explicitly
	// empty string, which pydantic accepts as a valid `str` and passes
	// through verbatim. in.AuthProvider is nil exactly when the key was
	// absent (DefaultedString in the caller), so presence alone decides
	// the fallback -- checking the STRING's own emptiness here collapsed
	// an explicit "" into "local", which is a value Python never stores
	// for that input.
	provider := "local"
	if in.AuthProvider != nil {
		provider = *in.AuthProvider
	}
	u.AuthProvider = &provider
	_, err := s.Pool.Exec(ctx, `
INSERT INTO users (id, email, username, password_hash, full_name, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 0, $11, $12)`,
		u.ID, u.Email, u.Username, u.PasswordHash, u.FullName, u.AuthProvider, u.AuthProviderID,
		u.IsActive, u.IsVerified, u.IsSuperuser, u.CreatedAt, u.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return u, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// userUpdate is UserUpdate: a nil field leaves the column unchanged.
type userUpdate struct {
	Email, Username, FullName, AvatarURL *string
	IsActive, IsVerified, IsSuperuser    *bool
}

// updateUser is UserService.update.
func (s pgStore) updateUser(ctx context.Context, id uuid.UUID, patch userUpdate) (*fullUser, error) {
	existing, err := s.fullUserByID(ctx, id)
	if err != nil || existing == nil {
		return existing, err
	}
	email := existing.Email
	if patch.Email != nil && *patch.Email != "" && !strings.EqualFold(*patch.Email, existing.Email) {
		if other, err := s.userByEmail(ctx, *patch.Email); err != nil {
			return nil, err
		} else if other != nil {
			return nil, errEmailExists
		}
		email = strings.ToLower(strings.TrimSpace(*patch.Email))
	}
	username := existing.Username
	if patch.Username != nil {
		if *patch.Username != "" {
			currentLower := ""
			if existing.Username != nil {
				currentLower = strings.ToLower(*existing.Username)
			}
			if !strings.EqualFold(*patch.Username, currentLower) {
				if other, err := s.userByUsername(ctx, *patch.Username); err != nil {
					return nil, err
				} else if other != nil {
					return nil, errUsernameExists
				}
			}
			lowered := strings.ToLower(strings.TrimSpace(*patch.Username))
			username = &lowered
		} else {
			username = nil
		}
	}
	fullName, avatarURL := existing.FullName, existing.AvatarURL
	if patch.FullName != nil {
		fullName = patch.FullName
	}
	if patch.AvatarURL != nil {
		avatarURL = patch.AvatarURL
	}
	isActive, isVerified, isSuperuser := existing.IsActive, existing.IsVerified, existing.IsSuperuser
	if patch.IsActive != nil {
		isActive = *patch.IsActive
	}
	if patch.IsVerified != nil {
		isVerified = *patch.IsVerified
	}
	if patch.IsSuperuser != nil {
		isSuperuser = *patch.IsSuperuser
	}
	now := s.now().UTC()
	_, err = s.Pool.Exec(ctx, `
UPDATE users SET email = $2, username = $3, full_name = $4, avatar_url = $5, is_active = $6, is_verified = $7, is_superuser = $8, updated_at = $9
WHERE id = $1`, id, email, username, fullName, avatarURL, isActive, isVerified, isSuperuser, now)
	if err != nil {
		return nil, err
	}
	return s.fullUserByID(ctx, id)
}

// setUserPassword is UserService.set_password: bumps token_version (so
// existing access tokens fail TokenVersion comparison) and the caller
// separately revokes all refresh tokens (revokeAllRefreshTokens), matching
// set_password's own two side effects.
// setUserPassword takes tx, not the pool: Python's set_password (the
// password UPDATE), revoke_all_for_user (the refresh_tokens UPDATE), and
// the router's own emit_audit_log all run through the SAME SQLAlchemy
// session and its one implicit commit, so all three of this route's writes
// share one Go transaction too.
func (s pgStore) setUserPassword(ctx context.Context, tx pgx.Tx, id uuid.UUID, passwordHash string) (bool, error) {
	now := s.now().UTC()
	tag, err := tx.Exec(ctx, `
UPDATE users SET password_hash = $2, token_version = token_version + 1, updated_at = $3 WHERE id = $1`,
		id, passwordHash, now)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// revokeAllRefreshTokens is refresh_tokens.py's revoke_all_for_user, over
// the same transaction as setUserPassword -- see its doc comment.
func (s pgStore) revokeAllRefreshTokens(ctx context.Context, tx pgx.Tx, userID uuid.UUID) error {
	_, err := tx.Exec(ctx, `
UPDATE refresh_tokens SET revoked_at = $2 WHERE user_id = $1 AND revoked_at IS NULL`, userID, s.now().UTC())
	return err
}

// deleteUser is UserService.delete.
func (s pgStore) deleteUser(ctx context.Context, id uuid.UUID) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// membershipOrgIDForUser is set_user_password's audit-org fallback: the
// user's first membership org, when the caller had no org_id of its own
// (a superuser calling with no X-Org-Id).
func (s pgStore) membershipOrgIDForUser(ctx context.Context, userID uuid.UUID) (*uuid.UUID, error) {
	var orgID uuid.UUID
	err := s.Pool.QueryRow(ctx, `SELECT org_id FROM memberships WHERE user_id = $1 LIMIT 1`, userID).Scan(&orgID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &orgID, nil
}
