package policy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// User is api/services/auth.py AuthenticatedUser after
// authenticate_access_token: the token's claims, with IsSuperuser replaced
// by the users row's current value.
type User struct {
	// UserID is the raw "sub" claim; ID is its parsed form.
	UserID string
	ID     uuid.UUID
	Email  string
	// OrgID is the "org_id" claim, "" when absent or null.
	OrgID string
	// Role is the "role" claim ("member" when absent, "" when null).
	Role        string
	IsSuperuser bool
	// Platform is the platform action set the caller holds when it is not
	// the one IsSuperuser means; nil is a caller that carries only the flag.
	Platform       *rule.Platform
	Username       *string
	FullName       *string
	ImpersonatedBy *string
	TokenVersion   int
	// SessionID is the auth-service session the token's sid claim names,
	// checked live.
	SessionID *uuid.UUID
	// accessToken marks a user access token.
	accessToken bool
}

// Holds reports whether the caller holds the platform action now.
func (u *User) Holds(action string) bool {
	return rule.Holds(u.Platform, u.IsSuperuser, action)
}

// IsAdmin is AuthenticatedUser.is_admin.
func (u *User) IsAdmin() bool {
	return u.Role == "owner" || u.Role == "admin" || u.Holds(rule.ActionAdminOrgs)
}

// UserState is the users row authenticate_access_token reads.
type UserState struct {
	IsActive     bool
	IsSuperuser  bool
	Platform     *rule.Platform
	TokenVersion int64
}

// Store is the Postgres surface the policy reads. PGStore is the production
// implementation.
type Store interface {
	// UserState returns the users row for id; found is false when there is
	// none.
	UserState(ctx context.Context, id uuid.UUID) (state UserState, found bool, err error)
	// Membership reads the memberships row linking user and org: the role
	// the user holds in that org, and whether the row exists at all.
	Membership(ctx context.Context, userID, orgID uuid.UUID) (role string, member bool, err error)
	// OrgMembership is Membership's row and whether the org exists, read in
	// one statement: an existing org without the row is told apart from an
	// org that does not exist.
	OrgMembership(ctx context.Context, userID, orgID uuid.UUID) (role string, member, orgFound bool, err error)
	// ActiveImpersonation returns the admin's unexpired, unended session, or
	// nil.
	ActiveImpersonation(ctx context.Context, adminID uuid.UUID) (*Impersonation, error)
	// ReadsPlatformRoles is AUTH_PLATFORM_ROLES_OWNED: UserState's
	// IsSuperuser is the platform role rule, not users.is_superuser.
	ReadsPlatformRoles() bool
}

// ErrUnavailable marks a store failure that get_current_user answers with
// 503 (_db_temporarily_unavailable: a timeout or a lost connection). Any
// other store failure is a 500.
var ErrUnavailable = errors.New("policy: database temporarily unavailable")

// errRejected is an authentication refusal (Python returns None).
var errRejected = errors.New("policy: credential refused")

// Authenticator is AuthService.authenticate_access_token.
type Authenticator struct {
	store  Store
	logger *slog.Logger
	routed *routedAccess
	// delegated is the delegated_bearer lookup DelegatedRows set; nil
	// admits a delegated session's bearer on no row.
	delegated func(profile string) bool
}

// NewAuthenticator builds an Authenticator. Only a routed decision
// (RouteAccess) accepts a token: HS256 access verify is retired.
func NewAuthenticator(store Store, logger *slog.Logger) (*Authenticator, error) {
	if store == nil {
		return nil, errors.New("policy: authenticator needs a store")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Authenticator{store: store, logger: logger}, nil
}

// BearerToken is extract_token_from_header: exactly two whitespace-separated
// parts, the first "bearer" in any case.
func BearerToken(authorization string) (string, bool) {
	if authorization == "" {
		return "", false
	}
	parts := strings.FieldsFunc(Latin1(authorization), isPySpace)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", false
	}
	return parts[1], true
}

// Authenticate verifies token against the request's endpoint-profile row
// (RouteAccess) and checks the users row. It returns (nil, errRejected) for
// every case Python answers with None, and a wrapped store error
// (ErrUnavailable or other) when the lookup fails. Without RouteAccess every
// token is refused.
//
// A request bound to a (ReadOnce) gets the answer of its first
// Authenticate of the same token on the same route row: the token is
// verified and the users row read once for the whole request.
func (a *Authenticator) Authenticate(ctx context.Context, token string) (*User, error) {
	if a.routed == nil {
		a.logger.DebugContext(ctx, "api access token refused", slog.String("reason", "no_access_check"))
		return nil, errRejected
	}
	reads := a.readsFor(ctx)
	if reads == nil {
		return a.authenticateRouted(ctx, token)
	}
	key := RouteProfileFrom(ctx) + "\x00" + token
	reads.mu.Lock()
	got, ok := reads.users[key]
	reads.mu.Unlock()
	if ok {
		return copyUser(got.user), got.err
	}
	// Not under reads.mu: the routed check reads the membership through the
	// same record.
	user, err := a.authenticateRouted(ctx, token)
	reads.mu.Lock()
	defer reads.mu.Unlock()
	if got, ok := reads.users[key]; ok {
		return copyUser(got.user), got.err
	}
	reads.users[key] = userRead{user: copyUser(user), err: err}
	return user, err
}

// isPySpace is str.isspace for the code points a latin-1 header or a JSON
// string can carry in practice.
func isPySpace(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\v', '\f', '\r', 0x1c, 0x1d, 0x1e, 0x1f, 0x85, 0xa0,
		0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000:
		return true
	}
	return r >= 0x2000 && r <= 0x200a
}

// ParsePyUUID is uuid.UUID(str): "urn:" and "uuid:" removed, braces
// stripped from both ends, every hyphen removed, then exactly 32 hex digits.
func ParsePyUUID(value string) (uuid.UUID, bool) {
	text := strings.ReplaceAll(value, "urn:", "")
	text = strings.ReplaceAll(text, "uuid:", "")
	text = strings.Trim(text, "{}")
	text = strings.ReplaceAll(text, "-", "")
	if len(text) != 32 {
		return uuid.UUID{}, false
	}
	parsed, err := uuid.Parse(text)
	if err != nil {
		return uuid.UUID{}, false
	}
	return parsed, true
}

// Credential failure bodies, as get_current_user raises them.
var bearerChallenge = http.Header{"Www-Authenticate": []string{"Bearer"}}

// authFailure writes get_current_user's response for err: 401 for a
// refusal, 503 for ErrUnavailable, 500 for anything else.
func authFailure(w http.ResponseWriter, message string, err error) {
	WriteAuthFailure(w, message, err)
}

// WriteAuthFailure is authFailure for a caller outside this package that
// authenticates with CheckClaims: the same 401/503/500 split, so a store
// failure answers identically on every plane.
func WriteAuthFailure(w http.ResponseWriter, message string, err error) {
	switch {
	case err == nil, errors.Is(err, errRejected):
		WriteDetail(w, http.StatusUnauthorized, ErrorDetail(message), bearerChallenge)
	case errors.Is(err, ErrUnavailable):
		WriteDetail(w, http.StatusServiceUnavailable, ErrorDetail("Database temporarily unavailable"), nil)
	default:
		WriteInternal(w)
	}
}

// Latin1 decodes raw header bytes the way Starlette does (latin-1), so a
// byte such as 0xa0 is the character U+00A0, which str.split() treats as
// whitespace.
func Latin1(raw string) string {
	var builder strings.Builder
	builder.Grow(len(raw))
	for index := 0; index < len(raw); index++ {
		builder.WriteRune(rune(raw[index]))
	}
	return builder.String()
}

func isRefusal(err error) bool { return errors.Is(err, errRejected) }

// IsRefusal reports whether err is Authenticate's refusal (Python's None),
// as opposed to a store failure.
func IsRefusal(err error) bool     { return isRefusal(err) }
func isUnavailable(err error) bool { return errors.Is(err, ErrUnavailable) }
