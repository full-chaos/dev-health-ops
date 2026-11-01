package grantend

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

func TestEventAndAuditRowAreWellFormed(t *testing.T) {
	id, org, principal, actor := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	e := EventFor(id)
	if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
		t.Fatalf("event: %v", err)
	}
	req := uuid.NewString()
	row := AuditFor(org, principal, id, Actor{PrincipalID: &actor, RequestID: &req})
	if row.EventType != EventType {
		t.Fatalf("event type %q", row.EventType)
	}
	if err := audit.ValidateAttributes(row.Attributes); err != nil {
		t.Fatalf("audit attributes: %v", err)
	}
}

func TestPlanEventsAndAuditAreOnePerGrant(t *testing.T) {
	org := uuid.New()
	p := &Plan{org: org, rows: []grantRow{{id: uuid.New(), principal: uuid.New()}, {id: uuid.New(), principal: uuid.New()}}}
	if len(p.Events()) != 2 || len(p.Audit(Actor{})) != 2 || p.Len() != 2 || len(p.IDs()) != 2 {
		t.Fatalf("events %d audit %d len %d", len(p.Events()), len(p.Audit(Actor{})), p.Len())
	}
	if (&Plan{}).Len() != 0 || len((&Plan{}).Events()) != 0 {
		t.Fatal("an empty plan has no events")
	}
}

func TestUUIDArrayLiteral(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	if got, want := uuidArray([]uuid.UUID{a, b}), "{"+a.String()+","+b.String()+"}"; got != want {
		t.Fatalf("uuidArray = %q, want %q", got, want)
	}
	if uuidArray(nil) != "{}" {
		t.Fatal("empty array literal")
	}
}
