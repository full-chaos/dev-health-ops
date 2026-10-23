package saml

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// PGJIT is the MembershipJIT over the identity store. The membership, and for
// a new account the account and its identity, are one audit.Commit: a failure
// writes none of them. The handler issues the session after the commit. It is
// available only after the users cutover; before it every call is
// ErrSignupNotAvailable and writes nothing.
type PGJIT struct {
	Identities *identity.Store
}

// jitRole is the role a just-in-time member gets.
const jitRole = "member"

// JoinOnSignIn adds an existing account to the provider's organization.
func (j PGJIT) JoinOnSignIn(ctx context.Context, principalID uuid.UUID, p Provider) error {
	_, err := j.Identities.JoinOrg(ctx, principalID, identity.OrgJoin{OrgID: p.OrgID, Role: jitRole}, identity.SelfSignUp(nil), nil)
	return mapJITError(err)
}

// ProvisionOnSignIn creates the account, its identity and its membership.
func (j PGJIT) ProvisionOnSignIn(ctx context.Context, account NewAccount, p Provider) (uuid.UUID, error) {
	id, err := j.Identities.ProvisionAndJoin(ctx, identity.ProvisionRequest{
		Key:   identity.ExternalKey{Issuer: account.Identity.Issuer, Provider: identity.Provider(account.Identity.Provider), Subject: account.Identity.Subject},
		Email: account.Email, FullName: account.FullName, Actor: identity.SelfSignUp(nil),
	}, identity.OrgJoin{OrgID: p.OrgID, Role: jitRole}, nil)
	return id, mapJITError(err)
}

// mapJITError turns the identity store's refusals into the flow's sentinels.
func mapJITError(err error) error {
	switch {
	case errors.Is(err, identity.ErrMembershipNotActive):
		return ErrNotOrgMember
	case errors.Is(err, identity.ErrPrincipalUnavailable):
		return ErrAccountDisabled
	case errors.Is(err, identity.ErrEmailTaken):
		return ErrEmailTaken
	}
	return err
}
