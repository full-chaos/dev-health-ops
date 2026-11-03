// Package serviceaccount is the lifecycle of an organization's service
// accounts in the auth schema: create with a named owner, disable, enable and
// change of owner, each one audit row and one provisioning event. It issues no
// credential.
package serviceaccount

import (
	"context"
	"regexp"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

// ValidateName refuses a name outside the service account name pattern.
func ValidateName(name string) error {
	if !namePattern.MatchString(name) {
		return ErrNameInvalid
	}
	return nil
}

// Locked is what Authorize sees: the organization and the account's current owner, read under the verb's locks
// (the zero uuid for a create, which has no account yet).
type Locked struct {
	OrgID uuid.UUID
	Owner uuid.UUID
}

// Authorize decides, inside the verb's transaction after its principal locks and before any write, whether the
// actor may make the change. It must only read: the verbs take no lock on public.users or public.memberships for it. A
// demotion that commits during the verb's transaction therefore counts as after it. A nil Authorize refuses
// (ErrDenied).
type Authorize func(ctx context.Context, tx audit.TxOps, l Locked) error

// Actor is who ran a verb and how it is authorized.
type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
	// Caller is the credential the request came with; the audit row names it.
	Caller    audit.Caller
	Authorize Authorize
}

func (a Actor) authorize(ctx context.Context, tx audit.TxOps, l Locked) error {
	if a.Authorize == nil {
		return ErrDenied
	}
	return a.Authorize(ctx, tx, l)
}

type CreateInput struct {
	OrgID uuid.UUID
	Name  string
	Owner uuid.UUID
}

// Info is one service account. OwnerEnabled and OwnerMember are derived at
// read: a lost owner never disables the account, it blocks Enable.
type Info struct {
	ID           uuid.UUID
	OrgID        uuid.UUID
	Name         string
	Active       bool
	DisabledAt   *time.Time
	Owner        uuid.UUID
	OwnerEnabled bool
	OwnerMember  bool
	CreatedBy    *uuid.UUID
	LastUsedAt   *time.Time
}

// ownerState is what a verb decides on about the owner it names.
type ownerState struct {
	isUser   bool
	disabled bool
	member   bool
}

func (o ownerState) refusal() error {
	switch {
	case !o.isUser:
		return ErrOwnerNotUser
	case o.disabled:
		return ErrOwnerDisabled
	case !o.member:
		return ErrOwnerNotMember
	}
	return nil
}

func decideCreate(orgActive bool, owner ownerState) error {
	if !orgActive {
		return ErrOrgNotActive
	}
	return owner.refusal()
}

func decideEnable(active, orgActive bool, owner ownerState) error {
	switch {
	case active:
		return ErrAlreadyInState
	case !orgActive:
		return ErrOrgNotActive
	}
	return owner.refusal()
}

func decideDisable(active bool) error {
	if !active {
		return ErrAlreadyInState
	}
	return nil
}

func decideSetOwner(current, next uuid.UUID, orgActive bool, owner ownerState) error {
	switch {
	case !orgActive:
		return ErrOrgNotActive
	case current == next:
		return ErrAlreadyInState
	}
	return owner.refusal()
}
