package local

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// IdentityUsers is Users over the B2 identity store. ByEmail reads
// public.users live until the users cutover (D3).
type IdentityUsers struct{ Store *identity.Store }

func (u IdentityUsers) ByEmail(ctx context.Context, normalizedEmail string) (*User, error) {
	p, err := u.Store.ByEmail(ctx, normalizedEmail)
	if err != nil || p == nil {
		return nil, err
	}
	return &User{ID: p.ID, IsActive: p.IsActive, IsVerified: p.IsVerified, PasswordHash: p.PasswordHash, AuthProvider: p.AuthProvider}, nil
}

func (u IdentityUsers) Project(ctx context.Context, userID uuid.UUID) error {
	return u.Store.Project(ctx, userID)
}
