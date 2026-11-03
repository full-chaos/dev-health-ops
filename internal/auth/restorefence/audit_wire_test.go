package restorefence

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

func TestFenceRowsAreValidAuditEventV1(t *testing.T) {
	for _, row := range []audit.AuditEvent{initializedRow(), appliedRow()} {
		auditwire.MustValidate(t, row)
		if err := audit.ValidateAttributes(row.Attributes); err != nil {
			t.Fatalf("%s ValidateAttributes: %v", row.EventType, err)
		}
	}
}
