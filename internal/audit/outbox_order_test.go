package audit

import (
	"fmt"
	"reflect"
	"testing"
)

// A mutation's events are written in (aggregate_type, aggregate_id) order
// whatever the caller's order; events of one aggregate keep the caller's
// order; the caller's slice is not changed.
func TestOutboxOrderSortsAcrossAggregatesAndKeepsOrderWithinOne(t *testing.T) {
	revokedOld := OutboxEvent{AggregateType: "platform_role_assignment", AggregateID: "pra_b", EventType: "platform_role_assignment.revoked"}
	grantedNew := OutboxEvent{AggregateType: "platform_role_assignment", AggregateID: "pra_a", EventType: "platform_role_assignment.granted"}
	principal := OutboxEvent{AggregateType: "principal", AggregateID: "prn_a", EventType: "principal.created"}
	userCreated := OutboxEvent{AggregateType: "user", AggregateID: "usr_a", EventType: "user.created"}
	userVerified := OutboxEvent{AggregateType: "user", AggregateID: "usr_a", EventType: "user.email_verified"}
	in := []OutboxEvent{userCreated, revokedOld, principal, userVerified, grantedNew}
	caller := append([]OutboxEvent(nil), in...)
	want := []OutboxEvent{grantedNew, revokedOld, principal, userCreated, userVerified}
	if got := outboxOrder(in); !reflect.DeepEqual(got, want) {
		t.Errorf("outboxOrder = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(in, caller) {
		t.Error("outboxOrder changed the caller's slice")
	}
	reversed := []OutboxEvent{grantedNew, revokedOld}
	if got := outboxOrder([]OutboxEvent{revokedOld, grantedNew}); !reflect.DeepEqual(got, reversed) {
		t.Errorf("revoked-first caller order = %v, want the aggregate id order %v", got, reversed)
	}
}

// Within one aggregate the caller's order holds for any number of events
// (the sort is stable, not only for the short inputs an insertion sort takes).
func TestOutboxOrderKeepsTheCallersOrderWithinAnAggregateAtSize(t *testing.T) {
	var in []OutboxEvent
	for i := 0; i < 64; i++ {
		aggregate := []string{"usr_c", "usr_a", "usr_b"}[i%3]
		in = append(in, OutboxEvent{AggregateType: "user", AggregateID: aggregate, EventType: fmt.Sprintf("e%02d", i)})
	}
	last := map[string]string{}
	for _, e := range outboxOrder(in) {
		if prev, ok := last[e.AggregateID]; ok && e.EventType < prev {
			t.Fatalf("%s: %s after %s, want the caller's order", e.AggregateID, e.EventType, prev)
		}
		last[e.AggregateID] = e.EventType
	}
}
