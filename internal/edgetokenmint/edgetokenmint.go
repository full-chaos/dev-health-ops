// Package edgetokenmint mints the product ACCESS TOKEN for go-api-prove's
// dedicated proof service principal: an access-token.v1 EdDSA user token
// (eddsa.go). No HS256 access token is minted.
//
// go-api-prove's edge bearer. The signing key files it reads are loaded by
// internal/signingkey. The tools pod holds an Ed25519 key file and
// mints locally. It never execs into a running api pod and never calls one.
//
// The principal is a DEDICATED service identity, never a human user:
//
//   - its users row has the fixed id ProvePrincipalID;
//   - that row must be marked as a service identity (auth_provider =
//     ServiceAuthProvider and no password hash), so no login path can
//     reach it and no human row can be minted for by mistake;
//   - it must not be a superuser;
//   - it must hold a membership in the requested org, with a role
//     RoleAllowedFor its id names (read-level roles only for the proof
//     principal).
//
// A second dedicated principal, AdminProofPrincipalID, exists for proofs of
// org-admin routes. It is a separate row, never a promotion of the first:
// the proof principal stays read-level, so every proof that depends on it is
// unchanged, and the admin role can be minted for that one id only.
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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/platform/pgident"
)

const (
	// Env var names match auth.py's os.getenv calls exactly, so a pod
	// wired from the same Secret keys as the api pod needs no new names.
	IssuerEnvVar   = "JWT_ISSUER"
	AudienceEnvVar = "JWT_AUDIENCE"

	DefaultIssuer   = "dev-health-ops"
	DefaultAudience = "dev-health-api"

	// DefaultTTL and MaxTTL bound the token's life. A prove run re-mints
	// as the token ages (see go-api-prove's edge credential freshness), so
	// a long run never needs a long-lived token.
	DefaultTTL = 10 * time.Minute
	MaxTTL     = 30 * time.Minute

	// ProvePrincipalID is the users.id of the dedicated proof service
	// principal. Fixed, so the row can be provisioned the same way in every
	// environment and so this minter can never be pointed at another user.
	ProvePrincipalID = "00000000-0000-4000-8000-00000000e0e1"

	// AdminProofPrincipalID is the users.id of the dedicated ORG-ADMIN proof
	// service principal (CHAOS-6570). Fixed for the same reason as
	// ProvePrincipalID, and distinct from it: it holds exactly one Admin
	// membership in the proof org and is the only id a token with the admin
	// role can be minted for.
	AdminProofPrincipalID = "00000000-0000-4000-8000-00000000e0e2"

	// ServiceAuthProvider marks a users row as a service identity.
	ServiceAuthProvider = "service"
)

// AllowedRoles are the membership roles the proof principal may hold.
// go-api-prove only reads; owner and admin are refused.
var AllowedRoles = map[string]bool{"viewer": true, "member": true}

// AdminAllowedRoles are the membership roles the org-admin proof principal
// may hold: exactly admin. A viewer, member or owner row for that id is
// refused, so the principal cannot drift into a read-level or an owner role.
var AdminAllowedRoles = map[string]bool{"admin": true}

// RoleAllowedFor reports whether a token with role may be minted for the
// user id. Only AdminProofPrincipalID may carry admin, and only admin; every
// other id keeps the read-level AllowedRoles.
func RoleAllowedFor(userID, role string) bool {
	if userID == AdminProofPrincipalID {
		return AdminAllowedRoles[role]
	}
	return AllowedRoles[role]
}

// Refusals. Each names what is wrong with the principal, never a key or a
// token.
var (
	ErrPrincipalNotFound   = errors.New("edgetokenmint: the proof service principal has no users row")
	ErrPrincipalInactive   = errors.New("edgetokenmint: the proof service principal is not active")
	ErrPrincipalNotService = errors.New("edgetokenmint: the principal row is not marked as a service identity")
	ErrPrincipalSuperuser  = errors.New("edgetokenmint: the proof service principal must not be a superuser")
	ErrNoMembership        = errors.New("edgetokenmint: the proof service principal has no membership in this org")
	ErrRoleNotAllowed      = errors.New("edgetokenmint: the proof service principal's role in this org is not a role it may hold")
)

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
	// PlatformRolesOwned is AUTH_PLATFORM_ROLES_OWNED: the lookup also refuses
	// a principal that holds a platform role in AuthSchema (empty: "auth").
	PlatformRolesOwned bool
	AuthSchema         string
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
func LookupPrincipal(ctx context.Context, db RowQuerier, orgID string, opts Options) (Principal, error) {
	return lookupPrincipal(ctx, db, ProvePrincipalID, orgID, opts)
}

// LookupAdminPrincipal reads the org-admin proof service principal for orgID
// and refuses any row that is not a service identity holding exactly the
// admin role in that org.
func LookupAdminPrincipal(ctx context.Context, db RowQuerier, orgID string, opts Options) (Principal, error) {
	return lookupPrincipal(ctx, db, AdminProofPrincipalID, orgID, opts)
}

// lookupSQLFor is the lookup statement: lookupSQL while the platform roles
// switch is off; while on, the superuser column is the platform role rule over
// the auth schema alone (public is_superuser is a frozen copy then, CHAOS-10993),
// in the same statement, so a failed auth read fails the lookup.
func lookupSQLFor(opts Options) (string, error) {
	if !opts.PlatformRolesOwned {
		return lookupSQL, nil
	}
	schema := opts.AuthSchema
	if schema == "" {
		schema = "auth"
	}
	if pgident.Check(schema) != pgident.OK {
		return "", fmt.Errorf("edgetokenmint: auth schema %q is not a usable schema name", schema)
	}
	holds := rule.HoldsSQL(pgx.Identifier{schema}.Sanitize(), "u.id", "'"+rule.PlatformAdmin+"'")
	return strings.Replace(lookupSQL, "u.is_superuser,", holds+",", 1), nil
}

func lookupPrincipal(ctx context.Context, db RowQuerier, principalID, orgID string, opts Options) (Principal, error) {
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
	query, err := lookupSQLFor(opts)
	if err != nil {
		return Principal{}, err
	}
	err = db.QueryRow(ctx, query, principalID, orgID).Scan(
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
	case !RoleAllowedFor(principalID, *role):
		return Principal{}, ErrRoleNotAllowed
	}
	return Principal{UserID: id, Email: email, OrgID: orgID, Role: *role, TokenVersion: tokenVersion}, nil
}
