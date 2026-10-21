package social

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// Store is the part of the identity store (*identity.Store) the social domain
// uses. Every identity write runs in audit.Commit inside it.
type Store interface {
	IdentityBySubject(ctx context.Context, issuer string, provider identity.Provider, subject string, legacy *identity.LegacyMatch) (identity.Principal, bool, error)
	ByEmail(ctx context.Context, normalizedEmail string) (*identity.Principal, error)
	ProvisionFederated(ctx context.Context, req identity.ProvisionRequest) (uuid.UUID, error)
	LinkIdentity(ctx context.Context, key identity.ExternalKey, principal uuid.UUID, actor identity.Actor) (uuid.UUID, error)
	UnlinkIdentity(ctx context.Context, key identity.ExternalKey, principal uuid.UUID, actor identity.Actor) error
	ListIdentities(ctx context.Context, principal uuid.UUID) ([]identity.Identity, error)
	HasPassword(ctx context.Context, principal uuid.UUID) (bool, error)
}

var _ Store = (*identity.Store)(nil)
