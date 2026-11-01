package saml

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

var errJITNotBuilt = errors.New("saml: PGJIT is not built")

// PGJIT is the MembershipJIT over the identity store.
type PGJIT struct {
	Identities *identity.Store
}

// JoinOnSignIn is the seam's join.
func (j PGJIT) JoinOnSignIn(context.Context, uuid.UUID, Provider) error { return errJITNotBuilt }

// ProvisionOnSignIn is the seam's new account.
func (j PGJIT) ProvisionOnSignIn(context.Context, NewAccount, Provider) (uuid.UUID, error) {
	return uuid.Nil, errJITNotBuilt
}

// mapJITError turns the identity store's refusals into the flow's sentinels.
func mapJITError(err error) error { return err }
