package identity

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// EnsureOrganization is ProjectOrganization for a row auth.organizations does
// not hold yet: it projects go-api's organization only when the auth row is
// missing and never changes an existing one, so an organization a deprovision
// suspended stays suspended while go-api's row is still active (until its
// purge). A missing organization in both schemas is ErrOrganizationNotFound.
func (s *Store) EnsureOrganization(ctx context.Context, orgID uuid.UUID) error {
	var exists bool
	err := s.orgDB.QueryRow(ctx, fmt.Sprintf(ownedOrgExistsSQL, s.q), orgID).Scan(&exists)
	switch {
	case err == nil:
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return s.storeErr(ctx, "read auth.organizations", err)
	}
	return s.ProjectOrganization(ctx, orgID)
}
