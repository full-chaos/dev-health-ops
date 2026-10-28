package local

import (
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func resetRequestedEvent(principal uuid.UUID, known bool, org *string) audit.AuditEvent {
	return audit.AuditEvent{}
}
