package copyproject

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

func TestEveryCopyprojectAuditRowIsAValidAuditEventV1(t *testing.T) {
	id := uuid.NewString()
	for _, row := range []audit.AuditEvent{
		PrincipalProjectedAudit(id),
		OrganizationAudit("auth.organization.projected", "organization_created", id),
	} {
		auditwire.MustValidate(t, row)
		if err := audit.ValidateAttributes(row.Attributes); err != nil {
			t.Fatalf("%s ValidateAttributes: %v", row.EventType, err)
		}
	}
}
