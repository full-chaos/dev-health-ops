//go:build integration

package identity

import (
	"context"
	"slices"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
)

// storedPlatformAdmin is what the update route decides a platform role change from. Before HeldOn it is the active
// authority (an assignment counts only for an active user), the route's state.Holds.
func storedPlatformAdmin(ctx context.Context, roles *platformrole.Store, q platformrole.RowQuerier, principal uuid.UUID) (bool, error) {
	held, err := roles.ResolveOn(ctx, q, principal)
	return slices.Contains(held.Roles(), platformrole.PlatformAdmin), err
}
