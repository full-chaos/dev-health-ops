package policy

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
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
	Role           string
	IsSuperuser    bool
	Username       *string
	FullName       *string
	ImpersonatedBy *string
	TokenVersion   int
}

// IsAdmin is AuthenticatedUser.is_admin.
func (u *User) IsAdmin() bool {
	return u.Role == "owner" || u.Role == "admin" || u.IsSuperuser
}

// UserState is the users row authenticate_access_token reads.
type UserState struct {
	IsActive     bool
	IsSuperuser  bool
	TokenVersion int64
}

// Store is the Postgres surface the policy reads. PGStore is the production
// implementation.
type Store interface {
	// UserState returns the users row for id; found is false when there is
	// none.
	UserState(ctx context.Context, id uuid.UUID) (state UserState, found bool, err error)
	// IsMember reports whether a memberships row links user and org.
	IsMember(ctx context.Context, userID, orgID uuid.UUID) (bool, error)
	// ActiveImpersonation returns the admin's unexpired, unended session, or
	// nil.
	ActiveImpersonation(ctx context.Context, adminID uuid.UUID) (*Impersonation, error)
}

// ErrUnavailable marks a store failure that get_current_user answers with
// 503 (_db_temporarily_unavailable: a timeout or a lost connection). Any
// other store failure is a 500.
var ErrUnavailable = errors.New("policy: database temporarily unavailable")

// errRejected is an authentication refusal (Python returns None).
var errRejected = errors.New("policy: credential refused")

// Authenticator is AuthService.authenticate_access_token.
type Authenticator struct {
	verifier *edgetoken.Verifier
	store    Store
	logger   *slog.Logger
}

// NewAuthenticator builds an Authenticator.
func NewAuthenticator(verifier *edgetoken.Verifier, store Store, logger *slog.Logger) (*Authenticator, error) {
	if verifier == nil || store == nil {
		return nil, errors.New("policy: authenticator needs a verifier and a store")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Authenticator{verifier: verifier, store: store, logger: logger}, nil
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

// Authenticate verifies token and checks the users row. It returns
// (nil, errRejected) for every case Python answers with None, and a wrapped
// store error (ErrUnavailable or other) when the lookup fails.
func (a *Authenticator) Authenticate(ctx context.Context, token string) (*User, error) {
	claims, err := a.verifier.Verify(token)
	if err != nil {
		a.logger.DebugContext(ctx, "api access token refused", slog.String("reason", edgetoken.ReasonOf(err)))
		return nil, errRejected
	}
	user, err := userFromClaims(claims)
	if err != nil {
		return nil, err
	}
	id, ok := ParsePyUUID(user.UserID)
	if !ok {
		a.logger.WarnContext(ctx, "api access token has an invalid user id claim")
		return nil, errRejected
	}
	user.ID = id

	state, found, err := a.store.UserState(ctx, id)
	if err != nil {
		a.logger.WarnContext(ctx, "api access token user lookup failed",
			slog.String("user_id", user.UserID), slog.Bool("unavailable", errors.Is(err, ErrUnavailable)))
		return nil, err
	}
	if !found {
		a.logger.WarnContext(ctx, "api access token valid but user not found", slog.String("user_id", user.UserID))
		return nil, errRejected
	}
	if !state.IsActive {
		a.logger.WarnContext(ctx, "api access token valid but user is deactivated", slog.String("user_id", user.UserID))
		return nil, errRejected
	}
	version, ok := tokenVersion(claims)
	if !ok {
		a.logger.WarnContext(ctx, "api access denied: stale-session check unreadable", slog.String("user_id", user.UserID))
		return nil, errRejected
	}
	if version != state.TokenVersion {
		a.logger.WarnContext(ctx, "api access denied: stale session", slog.String("user_id", user.UserID))
		return nil, errRejected
	}
	user.IsSuperuser = state.IsSuperuser
	user.TokenVersion = int(version)
	return user, nil
}

// errClaimType is a claim whose JSON type Python's code path raises on
// (TypeError/AttributeError outside the caught set): a 500, not a 401.
var errClaimType = errors.New("policy: access token claim has an unsupported type")

func userFromClaims(claims jwt.MapClaims) (*User, error) {
	sub, ok := claims["sub"].(string)
	if !ok {
		// uuid.UUID(<non-str>) raises TypeError/AttributeError, which
		// authenticate_access_token does not catch.
		return nil, errClaimType
	}
	user := &User{UserID: sub, Role: "member"}
	user.Email, _ = claims["email"].(string)
	user.OrgID, _ = claims["org_id"].(string)
	if role, present := claims["role"]; present {
		user.Role, _ = role.(string)
	}
	user.Username = optionalString(claims, "username")
	user.FullName = optionalString(claims, "full_name")
	user.ImpersonatedBy = optionalString(claims, "impersonating_user_id")
	return user, nil
}

func optionalString(claims jwt.MapClaims, key string) *string {
	value, ok := claims[key].(string)
	if !ok {
		return nil
	}
	return &value
}

// tokenVersion is `0 if tv is None else int(tv)` with the TypeError and
// ValueError of int() mapped to ok=false.
func tokenVersion(claims jwt.MapClaims) (int64, bool) {
	raw, present := claims["tv"]
	if !present || raw == nil {
		return 0, true
	}
	switch value := raw.(type) {
	case bool:
		if value {
			return 1, true
		}
		return 0, true
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) || math.Abs(value) >= 1<<63 {
			return 0, false
		}
		return int64(value), true
	case string:
		return pyIntString(value)
	default:
		return 0, false
	}
}

// pyIntString is int(str) for base 10: surrounding whitespace, an optional
// sign, and ASCII digits with single underscores between them.
func pyIntString(value string) (int64, bool) {
	text := strings.TrimFunc(value, isPySpace)
	sign := ""
	if strings.HasPrefix(text, "+") || strings.HasPrefix(text, "-") {
		sign, text = text[:1], text[1:]
	}
	if text == "" || strings.HasPrefix(text, "_") || strings.HasSuffix(text, "_") || strings.Contains(text, "__") {
		return 0, false
	}
	// ParseInt refuses anything but ASCII digits after the one sign.
	parsed, err := strconv.ParseInt(sign+strings.ReplaceAll(text, "_", ""), 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
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
