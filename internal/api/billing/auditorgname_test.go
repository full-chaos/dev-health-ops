package billing

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func TestAuditPayloadServesTheEntrysOrgName(t *testing.T) {
	own := uuid.New()
	for label, names := range refundOrgNames(own) {
		db := &nameDB{names: names}
		h := handlers{logger: slog.Default()}
		entry := auditEntry{ID: uuid.New(), OrgID: own, ResourceID: uuid.New(), Action: "x", ResourceType: "invoice"}
		payload, err := h.auditJSON(context.Background(), db, entry)
		if err != nil {
			t.Fatal(err)
		}
		checkRefundOrgName(t, "audit", label, pyjson.Value(payload))
		if id, _ := payload.Get("org_id"); id != own.String() {
			t.Errorf("audit %s: org_id = %v, want %v", label, id, own)
		}
		for _, asked := range db.asked {
			if asked != own {
				t.Errorf("audit %s: lookup read org %v, want only %v", label, asked, own)
			}
		}
	}
}
