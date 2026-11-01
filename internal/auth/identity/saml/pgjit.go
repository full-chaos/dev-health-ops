package saml

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// PGJIT is the MembershipJIT over the identity store: the membership and a new
// account are one audit.Commit. The caller issues the session after it returns.
// It needs the users cutover: before it every call is ErrSignupNotAvailable and
// writes nothing.
type PGJIT struct {
	Identities *identity.Store
	Logger     *slog.Logger
}

// signInRole is the provider row's role; owner or an unknown value refuses
// before any write.
func (j PGJIT) signInRole(ctx context.Context, p Provider) (string, error) {
	role, ok := identity.SignInRole(p.DefaultRole)
	if ok {
		return role, nil
	}
	j.logRefusal(ctx, p, "default_role_not_granted")
	return "", ErrNotOrgMember
}

func (j PGJIT) logRefusal(ctx context.Context, p Provider, branch string) {
	logger := j.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.WarnContext(ctx, "auth saml: sign-in join refused", "branch", branch, "provider_id", p.ID)
}

func (j PGJIT) refused(ctx context.Context, p Provider, err error) error {
	if branch := identity.JoinRefusalBranch(err); branch != "" {
		j.logRefusal(ctx, p, branch)
	}
	return mapJITError(err)
}

// JoinOnSignIn adds an existing account to the provider's organization. An
// account that became a member since the caller read writes nothing.
func (j PGJIT) JoinOnSignIn(ctx context.Context, principalID uuid.UUID, p Provider) error {
	role, err := j.signInRole(ctx, p)
	if err != nil {
		return err
	}
	err = j.Identities.JoinOnSignIn(ctx, principalID, identity.SSOJoin{OrgID: p.OrgID, Role: role}, identity.SelfSignUp(nil), nil)
	if errors.Is(err, identity.ErrAlreadyMember) {
		return nil
	}
	return j.refused(ctx, p, err)
}

// ProvisionOnSignIn creates the account, its identity and its membership.
func (j PGJIT) ProvisionOnSignIn(ctx context.Context, account NewAccount, p Provider) (uuid.UUID, error) {
	role, err := j.signInRole(ctx, p)
	if err != nil {
		return uuid.Nil, err
	}
	id, err := j.Identities.ProvisionAndJoin(ctx, identity.ProvisionRequest{
		Key:   identity.ExternalKey{Issuer: account.Identity.Issuer, Provider: identity.Provider(account.Identity.Provider), Subject: account.Identity.Subject},
		Email: account.Email, FullName: account.FullName, Actor: identity.SelfSignUp(nil),
		LegacyProvider: "saml", LegacySubject: account.LegacySubject, LegacyFullName: account.LegacyFullName,
	}, identity.SSOJoin{OrgID: p.OrgID, Role: role}, nil)
	return id, j.refused(ctx, p, err)
}

// mapJITError turns the identity store's refusals into the flow's sentinels;
// the cause stays in the chain for the log branch.
func mapJITError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, identity.ErrMemberElsewhere), errors.Is(err, identity.ErrOrganizationSuspended),
		errors.Is(err, identity.ErrMembershipDiverged):
		return fmt.Errorf("%w: %w", ErrNotOrgMember, err)
	case errors.Is(err, identity.ErrAccountInactive):
		return fmt.Errorf("%w: %w", ErrAccountDisabled, err)
	case errors.Is(err, identity.ErrIdentityTaken):
		return fmt.Errorf("%w: %w", ErrIdentityAmbiguous, err)
	}
	return err
}
