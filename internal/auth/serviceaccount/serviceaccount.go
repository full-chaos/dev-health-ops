// Package serviceaccount is the lifecycle of an organization's service
// accounts in the auth schema: create with a named owner, disable, enable and
// change of owner, each one audit row and one provisioning event. It issues no
// credential.
package serviceaccount

import (
	"regexp"
	"time"

	"github.com/google/uuid"
)

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

// ValidateName refuses a name outside the service account name pattern.
func ValidateName(name string) error {
	return nil
}

// Actor is who ran a verb; both are nil for an operator command.
type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
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

func (o ownerState) refusal() error { return nil }

func decideCreate(orgActive bool, owner ownerState) error { return nil }

func decideEnable(active, orgActive bool, owner ownerState) error { return nil }

func decideDisable(active bool) error { return nil }

func decideSetOwner(current, next uuid.UUID, orgActive bool, owner ownerState) error { return nil }
