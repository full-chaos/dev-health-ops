package session

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// This file is a stub of the cells' commit.

const MaxDelegation = 15 * time.Minute

var (
	errDelegationNotBuilt      = errors.New("session: delegation is not built")
	ErrNotOwned                = errDelegationNotBuilt
	ErrActorNotAuthorized      = errDelegationNotBuilt
	ErrActorUnavailable        = errDelegationNotBuilt
	ErrBaseNotLive             = errDelegationNotBuilt
	ErrNestedDelegation        = errDelegationNotBuilt
	ErrTargetIsActor           = errDelegationNotBuilt
	ErrTargetUnavailable       = errDelegationNotBuilt
	ErrTargetPlatformRole      = errDelegationNotBuilt
	ErrTargetNotMember         = errDelegationNotBuilt
	ErrTargetRoleUnknown       = errDelegationNotBuilt
	ErrOrganizationUnavailable = errDelegationNotBuilt
	ErrInvalidReason           = errDelegationNotBuilt
	ErrNotDelegated            = errDelegationNotBuilt
)

type Delegation struct {
	Actor            uuid.UUID
	Reason           string
	PermittedActions []string
}

type StartInput struct {
	BaseSessionID     uuid.UUID
	TargetPrincipalID uuid.UUID
	OrganizationID    uuid.UUID
	Reason            string
}

func (s *Store) StartDelegation(context.Context, StartInput) (Record, error) {
	return Record{}, errDelegationNotBuilt
}

func (s *Store) EndDelegation(context.Context, uuid.UUID, Actor) (bool, error) {
	return false, errDelegationNotBuilt
}

func delegationReason(string) (string, bool) { return "", false }
