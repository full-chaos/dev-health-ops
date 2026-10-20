package oidc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// StoreIdentities is the IdentityStore over the shared identity store. Pool
// and Schema read the email stored for a resolved account (auth.users).
type StoreIdentities struct {
	Store  *identity.Store
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
}

// storedEmail is the email auth.users holds for the account: every account
// this flow resolves has that row (projected or provisioned).
func (a StoreIdentities) storedEmail(ctx context.Context, id uuid.UUID) (string, error) {
	var email string
	err := a.Pool.QueryRow(ctx, `SELECT email FROM `+authschema.Quote(a.Schema)+`.users WHERE principal_id = $1`, id).Scan(&email)
	return email, err
}

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
	email, err := a.storedEmail(ctx, p.ID)
	if err != nil {
		return Principal{}, err
	}
	return Principal{ID: p.ID, Email: email, Active: p.IsActive && p.DisabledAt == nil}, nil
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
	email, err := a.storedEmail(ctx, id)
	if err != nil {
		return Principal{}, err
	}
	return Principal{ID: id, Email: email, Active: true}, nil
}
