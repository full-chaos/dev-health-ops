package delegationend

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

func TestEndedRowIsAValidAuditEventV1(t *testing.T) {
	r := Row{ID: uuid.NewString(), Target: uuid.NewString(), Actor: uuid.NewString(), Organization: uuid.NewString()}
	for _, ctx := range []context.Context{context.Background(), requestid.With(context.Background(), uuid.New())} {
		row := endedEvent(ctx, r)
		if row.EventType != EventEnded {
			t.Fatalf("event type %q", row.EventType)
		}
		auditwire.MustValidate(t, row)
		if err := audit.ValidateAttributes(row.Attributes); err != nil {
			t.Fatalf("ValidateAttributes: %v", err)
		}
	}
}
