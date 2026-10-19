package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var (
	ErrOrganizationNotFound      = errors.New("identity: organization not found")
	ErrOrganizationSlugCollision = errors.New("identity: the organization slug belongs to another organization")
)

// ProjectOrganization is a stub.
func (s *Store) ProjectOrganization(ctx context.Context, orgID uuid.UUID) error {
	return errNotImplemented
}
