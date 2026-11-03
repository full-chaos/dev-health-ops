package pushprojection

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

// The four event types the reader writes, with and without a revision.
func TestMutationRowIsAValidAuditEventV1(t *testing.T) {
	for _, eventType := range []string{
		"auth.projection_cursor.created", "auth.projection_cursor.advanced",
		"auth.credential_projection.parked", "auth.credential_projection.released",
	} {
		for _, revision := range []int64{0, 7} {
			row := mutationRow(eventType, revision)
			auditwire.MustValidate(t, row)
			if err := audit.ValidateAttributes(row.Attributes); err != nil {
				t.Fatalf("%s revision %d ValidateAttributes: %v", eventType, revision, err)
			}
		}
	}
}
