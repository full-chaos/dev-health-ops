//go:build integration

package identity

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
)

// storedPlatformAdmin is what the update route decides a platform role change from: the stored assignment (HeldOn).
func storedPlatformAdmin(ctx context.Context, roles *platformrole.Store, q platformrole.RowQuerier, principal uuid.UUID) (bool, error) {
	return roles.HeldOn(ctx, q, principal, platformrole.PlatformAdmin)
}
