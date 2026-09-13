// Package edgetokenmint mints the product ACCESS TOKEN the Python edge
// accepts, for go-api-prove's dedicated proof service principal.
//
// go-api-prove carries two bearers. The effective-principal envelope is
// minted in the tools pod by internal/envelopemint. This package is its
// sibling for the other bearer: the edge access token that
// AuthService.authenticate_access_token (src/dev_health_ops/api/services/
// auth.py) validates on every request. The same design applies. The tools
// pod holds the signing key the api pod's environment already holds
// (JWT_SECRET_KEY), reached the same way (a Secret key via secretKeyRef),
// and mints locally. It never execs into a running api pod and never calls
// one.
//
// The bearer check on the edge stays exactly as it is. A token minted here
// is an ordinary access token: HS256 over JWT_SECRET_KEY, type "access",
// the same issuer and audience, and a "tv" claim the edge compares with
// users.token_version for the principal row on every request. The edge
// also refuses the token when that row is gone or inactive.
//
// The principal is a DEDICATED service identity, never a human user:
//
//   - its users row has the fixed id ProvePrincipalID;
//   - that row must be marked as a service identity (auth_provider =
//     ServiceAuthProvider and no password hash), so no login path can
//     reach it and no human row can be minted for by mistake;
//   - it must not be a superuser;
//   - it must hold a membership in the requested org, with a role in
//     AllowedRoles (read-level roles only).
//
// Every one of those is checked against the database BEFORE signing, so a
// principal the edge would refuse fails here by name and not as a 401
// fifteen operations into a run.
//
// Lifetime is short (DefaultTTL, capped at MaxTTL). go-api-prove re-runs
// the helper as the token ages, so a token lives for at most one run plus
// one TTL, and only in that process's memory.
package edgetokenmint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	// Algorithm matches auth.py's JWT_ALGORITHM. The edge accepts no other.
	Algorithm = "HS256"

	// TokenType is the "type" claim authenticate_access_token requires.
	TokenType = "access"

	// Env var names match auth.py's os.getenv calls exactly, so a pod
	// wired from the same Secret keys as the api pod needs no new names.
	SigningKeyEnvVar = "JWT_SECRET_KEY"
	IssuerEnvVar     = "JWT_ISSUER"
	AudienceEnvVar   = "JWT_AUDIENCE"

	DefaultIssuer   = "dev-health-ops"
	DefaultAudience = "dev-health-api"

	// MinSigningKeyLength matches _get_jwt_secret's length floor, counted
	// in characters the way Python's len() counts them.
	MinSigningKeyLength = 32

	// DefaultTTL and MaxTTL bound the token's life. A prove run re-mints
	// as the token ages (see go-api-prove's edge credential freshness), so
	// a long run never needs a long-lived token.
	DefaultTTL = 10 * time.Minute
	MaxTTL     = 30 * time.Minute

	// ProvePrincipalID is the users.id of the dedicated proof service
	// principal. Fixed, so the row can be provisioned the same way in every
	// environment and so this minter can never be pointed at another user.
	ProvePrincipalID = "00000000-0000-4000-8000-00000000e0e1"

	// ServiceAuthProvider marks a users row as a service identity.
	ServiceAuthProvider = "service"
)

// AllowedRoles are the membership roles the proof principal may hold.
// go-api-prove only reads; owner and admin are refused.
var AllowedRoles = map[string]bool{"viewer": true, "member": true}

// Refusals. Each names what is wrong with the principal, never a key or a
// token.
var (
	ErrSigningKeyMissing   = fmt.Errorf("%s is not set", SigningKeyEnvVar)
	ErrSigningKeyTooShort  = fmt.Errorf("%s must be at least %d characters", SigningKeyEnvVar, MinSigningKeyLength)
	ErrPrincipalNotFound   = errors.New("edgetokenmint: the proof service principal has no users row")
	ErrPrincipalInactive   = errors.New("edgetokenmint: the proof service principal is not active")
	ErrPrincipalNotService = errors.New("edgetokenmint: the principal row is not marked as a service identity")
	ErrPrincipalSuperuser  = errors.New("edgetokenmint: the proof service principal must not be a superuser")
	ErrNoMembership        = errors.New("edgetokenmint: the proof service principal has no membership in this org")
	ErrRoleNotAllowed      = errors.New("edgetokenmint: the proof service principal's role in this org is not a read-level role")
)

// Claims is the access-token payload auth.py's create_access_token writes,
// field for field. aud is a single string, as PyJWT writes it. username
// and full_name are omitted: create_access_token omits them when empty,
// and a service principal has neither.
type Claims struct {
	Subject      string `json:"sub"`
	Email        string `json:"email"`
	OrgID        string `json:"org_id"`
	Role         string `json:"role"`
	IsSuperuser  bool   `json:"is_superuser"`
	Type         string `json:"type"`
	Issuer       string `json:"iss"`
	Audience     string `json:"aud"`
	ExpiresAt    int64  `json:"exp"`
	IssuedAt     int64  `json:"iat"`
	ID           string `json:"jti"`
	TokenVersion int    `json:"tv"`
}

// The jwt.Claims methods, so a test can parse a minted token with the
// library's own validation (expiry, issuer, audience) switched on.
func (c Claims) GetExpirationTime() (*jwt.NumericDate, error) {
	return jwt.NewNumericDate(time.Unix(c.ExpiresAt, 0)), nil
}
func (c Claims) GetIssuedAt() (*jwt.NumericDate, error) {
	return jwt.NewNumericDate(time.Unix(c.IssuedAt, 0)), nil
}
func (c Claims) GetNotBefore() (*jwt.NumericDate, error) { return nil, nil }
func (c Claims) GetIssuer() (string, error)              { return c.Issuer, nil }
func (c Claims) GetSubject() (string, error)             { return c.Subject, nil }
func (c Claims) GetAudience() (jwt.ClaimStrings, error) {
	return jwt.ClaimStrings{c.Audience}, nil
}

// LoadSigningKey checks raw the way _get_jwt_secret does and returns its
// bytes. The error never carries the value.
func LoadSigningKey(raw string) ([]byte, error) {
	if raw == "" {
		return nil, ErrSigningKeyMissing
	}
	if utf8.RuneCountInString(raw) < MinSigningKeyLength {
		return nil, ErrSigningKeyTooShort
	}
	return []byte(raw), nil
}

// LoadSigningKeyFromEnv reads SigningKeyEnvVar by name. The key is never a
// flag value, so it never reaches argv or the process table.
func LoadSigningKeyFromEnv() ([]byte, error) {
	return LoadSigningKey(os.Getenv(SigningKeyEnvVar))
}

// Principal is what a minted token claims. Build it with LookupPrincipal,
// which checks the database, not by hand.
type Principal struct {
	UserID       string
	Email        string
	OrgID        string
	Role         string
	TokenVersion int
}

// Options configures Mint. Zero values default the way auth.py does:
// issuer and audience from the same env vars and defaults, TTL to
// DefaultTTL, Now to time.Now.
type Options struct {
	Issuer   string
	Audience string
	TTL      time.Duration
	Now      func() time.Time
}

func (o Options) withDefaults() Options {
	if o.Issuer == "" {
		o.Issuer = envOr(IssuerEnvVar, DefaultIssuer)
	}
	if o.Audience == "" {
		o.Audience = envOr(AudienceEnvVar, DefaultAudience)
	}
	if o.TTL == 0 {
		o.TTL = DefaultTTL
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// Mint signs an access token for principal. The result is the raw JWT,
// with no "Bearer " prefix.
func Mint(signingKey []byte, principal Principal, opts Options) (string, error) {
	if utf8.RuneCount(signingKey) < MinSigningKeyLength {
		return "", ErrSigningKeyTooShort
	}
	if _, err := uuid.Parse(principal.UserID); err != nil {
		return "", errors.New("edgetokenmint: principal user id is not a UUID")
	}
	if _, err := uuid.Parse(principal.OrgID); err != nil {
		return "", errors.New("edgetokenmint: org id is not a UUID")
	}
	if strings.TrimSpace(principal.Email) == "" {
		return "", errors.New("edgetokenmint: principal email must not be empty")
	}
	if !AllowedRoles[principal.Role] {
		return "", ErrRoleNotAllowed
	}
	opts = opts.withDefaults()
	if opts.TTL < 0 || opts.TTL > MaxTTL {
		return "", fmt.Errorf("edgetokenmint: TTL must be between 0 and %s", MaxTTL)
	}
	now := opts.Now()
	claims := Claims{
		Subject:      principal.UserID,
		Email:        principal.Email,
		OrgID:        principal.OrgID,
		Role:         principal.Role,
		IsSuperuser:  false,
		Type:         TokenType,
		Issuer:       opts.Issuer,
		Audience:     opts.Audience,
		ExpiresAt:    now.Add(opts.TTL).Unix(),
		IssuedAt:     now.Unix(),
		ID:           uuid.NewString(),
		TokenVersion: principal.TokenVersion,
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(signingKey)
	if err != nil {
		return "", fmt.Errorf("edgetokenmint: sign: %w", err)
	}
	return signed, nil
}

// RowQuerier is the one pgx method LookupPrincipal needs. *pgxpool.Pool
// and pgx.Tx both satisfy it.
type RowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// lookupSQL reads the principal row and its membership in one statement.
// LEFT JOIN, so "no users row" and "no membership" are told apart. Both
// parameters are cast explicitly, so their types are never deduced.
const lookupSQL = `SELECT u.id::text, u.email, u.is_active, u.is_superuser, u.token_version,
       COALESCE(u.auth_provider, ''), u.password_hash IS NOT NULL, m.role
  FROM users u
  LEFT JOIN memberships m ON m.user_id = u.id AND m.org_id = $2::uuid
 WHERE u.id = $1::uuid`

// LookupPrincipal reads the proof service principal for orgID and refuses
// any row the edge must not accept for go-api-prove.
func LookupPrincipal(ctx context.Context, db RowQuerier, orgID string) (Principal, error) {
	if _, err := uuid.Parse(orgID); err != nil {
		return Principal{}, errors.New("edgetokenmint: org id is not a UUID")
	}
	var (
		id, email, authProvider string
		active, superuser       bool
		tokenVersion            int
		hasPassword             bool
		role                    *string
	)
	err := db.QueryRow(ctx, lookupSQL, ProvePrincipalID, orgID).Scan(
		&id, &email, &active, &superuser, &tokenVersion, &authProvider, &hasPassword, &role,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, ErrPrincipalNotFound
	}
	if err != nil {
		return Principal{}, fmt.Errorf("edgetokenmint: read the proof service principal: %w", err)
	}
	switch {
	case authProvider != ServiceAuthProvider || hasPassword:
		return Principal{}, ErrPrincipalNotService
	case !active:
		return Principal{}, ErrPrincipalInactive
	case superuser:
		return Principal{}, ErrPrincipalSuperuser
	case role == nil:
		return Principal{}, ErrNoMembership
	case !AllowedRoles[*role]:
		return Principal{}, ErrRoleNotAllowed
	}
	return Principal{UserID: id, Email: email, OrgID: orgID, Role: *role, TokenVersion: tokenVersion}, nil
}

// MintForProve looks up the proof service principal for orgID and mints
// its token. This is the entry point cmd/mint-edge-token uses.
func MintForProve(ctx context.Context, db RowQuerier, signingKey []byte, orgID string, opts Options) (string, error) {
	principal, err := LookupPrincipal(ctx, db, orgID)
	if err != nil {
		return "", err
	}
	return Mint(signingKey, principal, opts)
}
