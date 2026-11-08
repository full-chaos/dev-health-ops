package policy

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// AccessUserRow is the users row a user access token is checked against:
// the token carries no profile, superuser or token version (access-token.v1).
type AccessUserRow struct {
	State    UserState
	Email    string
	Username *string
	FullName *string
}

// AccessUserStore is the Store half the user access-token path reads.
type AccessUserStore interface {
	AccessUser(ctx context.Context, id uuid.UUID) (row AccessUserRow, found bool, err error)
}

const principalPrefix = "prn_"

// nonMemberRole is the role of a caller with no membership row in the token
// org: the HS256 token's role when it states none (userFromClaims).
const nonMemberRole = "member"

// orgLessRole is the role of a caller whose token names no org: the HS256
// path's, as go-api's login minted it (internal/api/session/tokens.go).
const orgLessRole = "member"

// parsePrincipalID reverses the session.v1 principal id form: "prn_" and the
// unpadded base64url of the 16 id bytes. The principal id is the users id.
func parsePrincipalID(wire string) (uuid.UUID, bool) {
	text, ok := strings.CutPrefix(wire, principalPrefix)
	if !ok {
		return uuid.UUID{}, false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(text)
	if err != nil || len(raw) != len(uuid.UUID{}) {
		return uuid.UUID{}, false
	}
	return uuid.UUID(raw), true
}

// accessTokenUser is the per-request half of a verified user access token:
// the users row (found, active), the sid (a live session of the subject),
// and the role from the membership in the org claim (Q4); a caller without
// that membership gets the HS256 path's principal. Every failed read is that
// error, never a pass.
func (a *Authenticator) accessTokenUser(ctx context.Context, claims *AccessTokenClaims) (*User, error) {
	id, ok := parsePrincipalID(claims.Subject)
	if !ok {
		a.logger.WarnContext(ctx, "api user access token has an unusable subject")
		return nil, errRejected
	}
	var impersonatedBy *string
	if claims.Actor != nil {
		actor, ok := parsePrincipalID(*claims.Actor)
		if !ok {
			a.logger.WarnContext(ctx, "api user access token has an unusable actor", slog.String("user_id", id.String()))
			return nil, errRejected
		}
		if actor == id {
			a.logger.WarnContext(ctx, "api user access token names its subject as its actor", slog.String("user_id", id.String()))
			return nil, errRejected
		}
		by := actor.String()
		impersonatedBy = &by
	}
	store, ok := a.store.(AccessUserStore)
	if !ok {
		a.logger.WarnContext(ctx, "api user access token with no user store", slog.String("user_id", id.String()))
		return nil, errRejected
	}
	row, found, err := store.AccessUser(ctx, id)
	if err != nil {
		a.logger.WarnContext(ctx, "api user access token user lookup failed",
			slog.String("user_id", id.String()), slog.Bool("unavailable", errors.Is(err, ErrUnavailable)))
		return nil, err
	}
	if !found || !row.State.IsActive {
		a.logger.WarnContext(ctx, "api user access token valid but user not found or deactivated", slog.String("user_id", id.String()))
		return nil, errRejected
	}
	if claims.SessionID == "" {
		a.logger.WarnContext(ctx, "api user access token without a session", slog.String("user_id", id.String()))
		return nil, errRejected
	}
	sessionID, err := a.checkSession(ctx, map[string]any{"sid": claims.SessionID}, id, row.State.TokenVersion)
	if err != nil {
		return nil, err
	}
	user := &User{UserID: id.String(), ID: id, Email: row.Email, Username: row.Username, FullName: row.FullName,
		IsSuperuser: row.State.IsSuperuser, Platform: row.State.Platform, TokenVersion: int(row.State.TokenVersion), accessToken: true, SessionID: sessionID,
		ImpersonatedBy: impersonatedBy, Role: orgLessRole}
	if claims.OrgID != nil && *claims.OrgID != "" {
		role, err := a.tokenOrgRole(ctx, user, *claims.OrgID)
		if err != nil {
			return nil, err
		}
		user.OrgID, user.Role = *claims.OrgID, role
	}
	return user, nil
}

// tokenOrgRole is the user's role in the org claim: the membership's role, or,
// for a caller with no membership row, the role the HS256 path gives a token
// with no role claim (product rule 1, D6773). As on the HS256 path the
// authenticator does not refuse that caller: useTokenOrg and the guard (and
// CheckTokenOrg where a service calls it) decide the token org later, so an
// org without the row is refused there and an org that does not exist or a
// claim that is not a UUID goes on as on main.
func (a *Authenticator) tokenOrgRole(ctx context.Context, user *User, orgID string) (string, error) {
	role, member, err := a.Membership(ctx, user.UserID, orgID)
	if err != nil {
		a.logger.WarnContext(ctx, "api user access token membership lookup failed",
			slog.String("user_id", user.UserID), slog.Bool("unavailable", errors.Is(err, ErrUnavailable)))
		return "", err
	}
	if member {
		return role, nil
	}
	return nonMemberRole, nil
}

// AccessUser reads the users row a user access token is checked against. NULL
// is_active and is_superuser read as false, NULL token_version as 0, NULL
// email as "".
func (s PGStore) AccessUser(ctx context.Context, id uuid.UUID) (AccessUserRow, bool, error) {
	countDirectRead(ctx, kindReadUsers)
	var active, superuser *bool
	var version *int64
	var email *string
	var row AccessUserRow
	query, err := s.accessUserSQL()
	if err != nil {
		return AccessUserRow{}, false, err
	}
	err = s.Pool.QueryRow(ctx, query, id).Scan(&active, &superuser, &version, &email, &row.Username, &row.FullName)
	if errors.Is(err, pgx.ErrNoRows) {
		return AccessUserRow{}, false, nil
	}
	if err != nil {
		return AccessUserRow{}, false, storeError(err, "SELECT on users (id, is_active, is_superuser, token_version, email, username, full_name)"+s.superuserGrant())
	}
	row.State = UserState{IsActive: active != nil && *active, IsSuperuser: superuser != nil && *superuser}
	if version != nil {
		row.State.TokenVersion = *version
	}
	if email != nil {
		row.Email = *email
	}
	return row, true, nil
}
