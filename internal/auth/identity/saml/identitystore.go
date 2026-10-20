package saml

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// IdentityStore is Store over the auth-service identity store.
type IdentityStore struct {
	Identities *identity.Store
	Logger     *slog.Logger
}

func (s IdentityStore) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// IdentityBySubject looks up the key; the go-api account fallback matches the
// raw NameID and requires a membership in the provider's organization. Two
// go-api accounts with the same NameID match neither.
func (s IdentityStore) IdentityBySubject(ctx context.Context, id Identity, legacy *LegacyMatch) (Principal, bool, error) {
	var match *identity.LegacyMatch
	if legacy != nil {
		org := legacy.MembershipOrg
		match = &identity.LegacyMatch{AuthProvider: legacy.AuthProvider, MembershipOrg: &org, Subject: legacy.Subject}
	}
	p, found, err := s.Identities.IdentityBySubject(ctx, id.Issuer, identity.Provider(id.Provider), id.Subject, match)
	if errors.Is(err, identity.ErrMultipleUsers) {
		s.logger().WarnContext(ctx, "auth saml: more than one go-api account holds the NameID; no account matched", "provider_id", id.Provider)
		return Principal{}, false, nil
	}
	if err != nil || !found {
		return Principal{}, false, err
	}
	out := Principal{ID: p.ID, Disabled: p.DisabledAt != nil || !p.IsActive}
	return out, true, nil
}

// NormalizeEmail is the identity store's email rule.
func (s IdentityStore) NormalizeEmail(raw string) (string, error) {
	return identity.NormalizeEmail(raw)
}

// PrincipalByEmail reports the account holding email and the sign-in method
// to name in the conflict: go-api's auth_provider before the users cutover;
// after it "local" for an account with a password, else the provider of its
// oldest linked identity ("sso" for an enterprise SSO provider row), else "".
// Two accounts holding the email are a conflict too.
func (s IdentityStore) PrincipalByEmail(ctx context.Context, email string) (EmailOwner, bool, error) {
	p, err := s.Identities.ByEmail(ctx, email)
	if errors.Is(err, identity.ErrMultipleUsers) {
		return EmailOwner{}, true, nil
	}
	if err != nil || p == nil {
		return EmailOwner{}, false, err
	}
	owner := EmailOwner{PrincipalID: p.ID}
	if p.AuthProvider != nil {
		owner.Method = *p.AuthProvider
		return owner, true, nil
	}
	hasPassword, err := s.Identities.HasPassword(ctx, p.ID)
	if err != nil {
		return EmailOwner{}, false, err
	}
	if hasPassword {
		owner.Method = "local"
		return owner, true, nil
	}
	linked, err := s.Identities.ListIdentities(ctx, p.ID)
	if err != nil {
		return EmailOwner{}, false, err
	}
	if len(linked) > 0 {
		owner.Method = "sso"
		if provider := linked[0].Provider; provider.IsSocial() {
			owner.Method = string(provider)
		}
	}
	return owner, true, nil
}

// ProvisionFederated creates the account as a self-service sign-up.
func (s IdentityStore) ProvisionFederated(ctx context.Context, u NewFederatedUser) (uuid.UUID, error) {
	return s.Identities.ProvisionFederated(ctx, identity.ProvisionRequest{
		Key:      identity.ExternalKey{Issuer: u.Identity.Issuer, Provider: identity.Provider(u.Identity.Provider), Subject: u.Identity.Subject},
		Email:    u.Email,
		FullName: u.DisplayName,
		Actor:    identity.SelfSignUp(u.AuditRequestID),
	})
}
