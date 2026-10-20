package audit

import (
	"context"
	"errors"
	"testing"
)

// TestMutationRefusesOffContractEvents: an event the provisioning-event.v1
// schema would refuse is refused before any transaction begins, and a
// mutation must name its events or why it has none.
func TestMutationRefusesOffContractEvents(t *testing.T) {
	const id = "0b6f1c3e-8e4f-4b7a-9d2a-3c5e7f9a1b2d"
	org := id
	base := func() Mutation {
		return Mutation{
			Apply: func(context.Context, TxOps) error { return nil },
			Audit: AuditEvent{EventType: "auth.principal.projected", Outcome: OutcomeAllowed},
			Events: []OutboxEvent{{AggregateType: "principal", AggregateID: "prn_" + id, EventType: "principal.created",
				Change: map[string]any{"status": "active"}}},
		}
	}
	if err := base().validate(); err != nil {
		t.Fatalf("a contract event was refused: %v", err)
	}
	cases := map[string]func(*Mutation){
		"unknown aggregate":           func(m *Mutation) { m.Events[0].AggregateType = "session" },
		"raw uuid id":                 func(m *Mutation) { m.Events[0].AggregateID = id },
		"id of another prefix":        func(m *Mutation) { m.Events[0].AggregateID = "org_" + id },
		"verb of another aggregate":   func(m *Mutation) { m.Events[0].EventType = "user.created" },
		"verb outside the vocabulary": func(m *Mutation) { m.Events[0].EventType = "principal.provisioned" },
		"change member not allowed":   func(m *Mutation) { m.Events[0].Change["changed_fields"] = "disabled_at" },
		"status outside vocabulary":   func(m *Mutation) { m.Events[0].Change["status"] = "locked" },
		"events and off-stream":       func(m *Mutation) { m.OffStream = OffStreamSessionLifecycle },
		"neither events nor off-stream": func(m *Mutation) {
			m.Events = nil
		},
		"unknown off-stream reason": func(m *Mutation) { m.Events = nil; m.OffStream = "noise" },
		"organization not wire-able": func(m *Mutation) {
			bad := "not a uuid"
			m.Audit.OrganizationID = &bad
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := base()
			mutate(&m)
			if err := m.validate(); !errors.Is(err, ErrMutationFailed) {
				t.Fatalf("validate = %v, want ErrMutationFailed", err)
			}
		})
	}
	ok := base()
	ok.Audit.OrganizationID = &org
	if err := ok.validate(); err != nil {
		t.Errorf("an organization id the wire can carry was refused: %v", err)
	}
	off := base()
	off.Events = nil
	off.OffStream = OffStreamSessionLifecycle
	if err := off.validate(); err != nil {
		t.Errorf("an off-stream mutation was refused: %v", err)
	}
}
