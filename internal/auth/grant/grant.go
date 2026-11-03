// Package grant is the auth-service resource grant aggregate: the write
// authority for auth.resource_grants. Every verb commits through audit.Commit,
// so the grant row, its audit row, its provisioning event and its revision
// change together or not at all. A grant is ended by marking it revoked, never
// by deleting it (internal/auth/grantend).
package grant

import (
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
)

// KindRepository is the one resource kind a grant covers (migration 0020).
const KindRepository = "repository"

// AllRepositories is the explicit organization-wide repository key.
const AllRepositories = "*"

// Origin says who made a grant.
type Origin string

const (
	OriginAPI      Origin = "api"
	OriginBackfill Origin = "backfill"
)

// Errors. The lookup and authorization refusals are the membership ones, so an
// Authorize built for the membership verbs refuses the same way here.
var (
	ErrNotOwned          = membership.ErrNotOwned
	ErrDenied            = membership.ErrDenied
	ErrSuperuserRequired = membership.ErrSuperuserRequired
	ErrOrgNotFound       = membership.ErrOrgNotFound
	ErrOrgSuspended      = membership.ErrOrgSuspended
	ErrPrincipalNotFound = membership.ErrPrincipalNotFound
	ErrRevisionConflict  = membership.ErrRevisionConflict

	ErrKeyInvalid     = errors.New("grant: the repository key is not owner/repo or *")
	ErrRoleInvalid    = errors.New("grant: the role is not viewer, member or admin")
	ErrOriginInvalid  = errors.New("grant: the origin is not api or backfill")
	ErrActorRequired  = errors.New("grant: an api grant names the principal that makes it")
	ErrNotMember      = errors.New("grant: the principal is not an active member of the organization")
	ErrAlreadyGranted = errors.New("grant: the principal already holds this grant")
	ErrNotFound       = errors.New("grant: no such grant in the organization")
	ErrRevoked        = errors.New("grant: the grant is revoked")
	ErrExpiryPast     = errors.New("grant: the expiry is not in the future")
	ErrNothingToSet   = errors.New("grant: an update names a role or an expiry")
	ErrStore          = errors.New("grant: store unavailable")
)

// grantRoles are the roles a grant may carry. owner is organization-wide
// authority and is never repository-scoped.
var grantRoles = [...]string{"viewer", "member", "admin"}

// Roles returns a copy of the roles a grant may carry.
func Roles() []string { return append([]string(nil), grantRoles[:]...) }

// ValidRole is true for a role a grant may carry.
func ValidRole(role string) bool {
	for _, r := range grantRoles {
		if r == role {
			return true
		}
	}
	return false
}

// keyPattern is the repository key of authorization_request.v1 and of
// migration 0020's resource_grants_repository_key_canonical; tests hold the
// three equal.
const keyPattern = `^([a-z0-9][a-z0-9._-]{0,99}/[a-z0-9][a-z0-9._-]{0,99}|\*)$`

var keyRE = regexp.MustCompile(keyPattern)

// KeyPattern is the pattern ValidKey holds.
func KeyPattern() string { return keyPattern }

// ValidKey is true for the canonical lowercase owner/repo or the literal *.
func ValidKey(key string) bool { return keyRE.MatchString(key) }

// Actor is who asks for a change; Authorize decides on the locked rows.
type Actor = membership.Actor

// Authorize decides, inside the verb's transaction and after its locks.
type Authorize = membership.Authorize

// Locked is what Authorize sees.
type Locked = membership.Locked

// CreateInput names one new grant.
type CreateInput struct {
	OrgID       uuid.UUID
	PrincipalID uuid.UUID
	Role        string
	Key         string
	ExpiresAt   *time.Time
	Origin      Origin
	Actor       Actor
	Authorize   Authorize
}

// UpdateInput changes the role or the expiry of a live grant. The key and the
// principal never change: that is a revoke and a new grant.
type UpdateInput struct {
	OrgID            uuid.UUID
	GrantID          uuid.UUID
	Role             *string
	ExpiresAt        *time.Time
	ClearExpiry      bool
	ExpectedRevision *int64
	Actor            Actor
	Authorize        Authorize
}

// RevokeInput ends a grant.
type RevokeInput struct {
	OrgID            uuid.UUID
	GrantID          uuid.UUID
	ExpectedRevision *int64
	Actor            Actor
	Authorize        Authorize
}

// Grant is one auth.resource_grants row with its revision.
type Grant struct {
	ID          uuid.UUID
	OrgID       uuid.UUID
	PrincipalID uuid.UUID
	Role        string
	Key         string
	ExpiresAt   *time.Time
	Origin      Origin
	GrantedBy   *uuid.UUID
	RevokedAt   *time.Time
	RevokedBy   *uuid.UUID
	CreatedAt   time.Time
	Revision    int64
}

// Live is true while the grant is not revoked.
func (g Grant) Live() bool { return g.RevokedAt == nil }

// checkCreate is the pure part of Create's validation.
func checkCreate(in CreateInput) (CreateInput, error) {
	if in.Origin == "" {
		in.Origin = OriginAPI
	}
	switch {
	case in.Origin != OriginAPI && in.Origin != OriginBackfill:
		return in, ErrOriginInvalid
	case !ValidRole(in.Role):
		return in, ErrRoleInvalid
	case !ValidKey(in.Key):
		return in, ErrKeyInvalid
	case in.Origin == OriginAPI && in.Actor.PrincipalID == nil:
		return in, ErrActorRequired
	}
	return in, nil
}

// checkUpdate is the pure part of Update's validation.
func checkUpdate(in UpdateInput) error {
	if in.Role == nil && in.ExpiresAt == nil && !in.ClearExpiry {
		return ErrNothingToSet
	}
	if in.ExpiresAt != nil && in.ClearExpiry {
		return ErrNothingToSet
	}
	if in.Role != nil && !ValidRole(*in.Role) {
		return ErrRoleInvalid
	}
	return nil
}

// wireTime is a timestamp as provisioning-event.v1 spells it.
func wireTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.999999Z07:00") }
