package oidc

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// StoreIdentities is the IdentityStore over the shared identity store.
type StoreIdentities struct{ Store *identity.Store }

// IdentityBySubject looks up (issuer, provider row, subject), then the go-api
// fallback bound to the provider row's organization. More than one legacy
// account carrying the subject is a miss, never a pick.
func (a StoreIdentities) IdentityBySubject(ctx context.Context, key ExternalKey, legacy LegacyMatch) (Principal, error) {
	org := legacy.MembershipOrg
	p, found, err := a.Store.IdentityBySubject(ctx, key.Issuer, identity.Provider(key.Provider), key.Subject,
		&identity.LegacyMatch{AuthProvider: legacy.AuthProvider, MembershipOrg: &org})
	switch {
	case errors.Is(err, identity.ErrMultipleUsers):
		return Principal{}, ErrIdentityNotFound
	case err != nil:
		return Principal{}, err
	case !found:
		return Principal{}, ErrIdentityNotFound
	}
	return Principal{ID: p.ID, Active: p.IsActive && p.DisabledAt == nil}, nil
}

// EmailInUse reports whether any account holds the normalized address. It
// only refuses; it never selects an account.
func (a StoreIdentities) EmailInUse(ctx context.Context, email string) (bool, error) {
	normalized, err := identity.NormalizeEmail(email)
	if err != nil {
		return false, err
	}
	p, err := a.Store.ByEmail(ctx, normalized)
	if errors.Is(err, identity.ErrMultipleUsers) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return p != nil, nil
}

// ProvisionFederated creates the account as a self sign-up: the audit row names
// the new principal as its own actor.
func (a StoreIdentities) ProvisionFederated(ctx context.Context, account FederatedAccount) (Principal, error) {
	id, err := a.Store.ProvisionFederated(ctx, identity.ProvisionRequest{
		Key:   identity.ExternalKey{Issuer: account.Key.Issuer, Provider: identity.Provider(account.Key.Provider), Subject: account.Key.Subject},
		Email: account.Email, FullName: account.FullName, Actor: identity.SelfSignUp(account.RequestID),
	})
	switch {
	case errors.Is(err, identity.ErrIdentityTaken), errors.Is(err, identity.ErrEmailTaken):
		return Principal{}, ErrIdentityConflict
	case err != nil:
		return Principal{}, err
	}
	return Principal{ID: id, Active: true}, nil
}
