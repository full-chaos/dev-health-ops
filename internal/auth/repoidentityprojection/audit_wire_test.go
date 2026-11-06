package repoidentityprojection

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

// Every audit row the reader writes is a valid audit-event.v1 row.
func TestTheReaderAuditRowsAreValidAuditEventV1(t *testing.T) {
	rows := []audit.AuditEvent{observedRow(Event{OrganizationID: uuid.New(), Provider: "github", ProviderRepoID: "1", Path: "a/b", ObservedAt: time.Now(), Revision: 9})}
	for _, eventType := range []string{
		"auth.projection_cursor.created", "auth.projection_cursor.advanced",
		"auth.repository_identity.parked", "auth.repository_identity.released",
	} {
		rows = append(rows, bookkeepingRow(eventType, 0), bookkeepingRow(eventType, 7))
	}
	for _, row := range rows {
		auditwire.MustValidate(t, row)
		if err := audit.ValidateAttributes(row.Attributes); err != nil {
			t.Fatalf("%s ValidateAttributes: %v", row.EventType, err)
		}
	}
}
