package audit

import (
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
