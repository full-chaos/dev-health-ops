package session

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// CreateSupersedingGuarded is the organization switch's supersede with a
// guard run in its transaction. Not built yet: it refuses every call.
func (s *Store) CreateSupersedingGuarded(ctx context.Context, principalID uuid.UUID, organizationID *uuid.UUID, amr []string,
	predecessor uuid.UUID, event audit.AuditEvent, guard Guard) (Issued, error) {
	return Issued{}, fmt.Errorf("%w: a guarded supersede is not built", ErrStore)
}
