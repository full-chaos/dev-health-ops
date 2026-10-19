package local

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// IdentityStore is the part of the B2 *identity.Store the flows use. ByID
// returns nil, nil for an unknown principal.
type IdentityStore interface {
	ByEmail(ctx context.Context, normalizedEmail string) (*identity.Principal, error)
	ByID(ctx context.Context, id uuid.UUID) (*identity.Principal, error)
	ProvisionLocal(ctx context.Context, req identity.LocalProvisionRequest) (uuid.UUID, error)
	SetEmailVerified(ctx context.Context, principal uuid.UUID, at time.Time, actor identity.Actor) error
	SetPasswordHash(ctx context.Context, principal uuid.UUID, hash string, actor identity.Actor) error
}

// IdentityAccounts adapts the B2 store to Accounts. Every write is a
// self-service action: the audit row names the subject as its own actor.
type IdentityAccounts struct {
	Store IdentityStore
	// RequestID returns the audit request id of the call (C1's
	// requestid.Audit once it merges); nil means none.
	RequestID func(context.Context) *string
	// Now is the verification time; nil means time.Now.
	Now func() time.Time
}

func (a IdentityAccounts) actor(ctx context.Context) identity.Actor {
	var id *string
	if a.RequestID != nil {
		id = a.RequestID(ctx)
	}
	return identity.SelfSignUp(id)
}

func userOf(p *identity.Principal) *User {
	if p == nil {
		return nil
	}
	return &User{ID: p.ID, IsActive: p.IsActive, IsVerified: p.IsVerified, PasswordHash: p.PasswordHash, AuthProvider: p.AuthProvider}
}

func (a IdentityAccounts) ByEmail(ctx context.Context, email string) (*User, error) {
	p, err := a.Store.ByEmail(ctx, email)
	return userOf(p), err
}

func (a IdentityAccounts) ByID(ctx context.Context, id uuid.UUID) (*User, error) {
	p, err := a.Store.ByID(ctx, id)
	return userOf(p), err
}

func (a IdentityAccounts) ProvisionLocal(ctx context.Context, req ProvisionLocalRequest) (uuid.UUID, error) {
	name := ""
	if req.FullName != nil {
		name = *req.FullName
	}
	hash := req.PasswordHash
	return a.Store.ProvisionLocal(ctx, identity.LocalProvisionRequest{Email: req.Email, FullName: name, PasswordHash: &hash, Actor: a.actor(ctx)})
}

func (a IdentityAccounts) SetEmailVerified(ctx context.Context, id uuid.UUID) error {
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	return a.Store.SetEmailVerified(ctx, id, now(), a.actor(ctx))
}

func (a IdentityAccounts) SetPasswordHash(ctx context.Context, id uuid.UUID, hash string) error {
	return a.Store.SetPasswordHash(ctx, id, hash, a.actor(ctx))
}

var _ Accounts = IdentityAccounts{}
