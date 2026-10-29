package membership

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

// A deprovision removes every live membership (active by its public row,
// suspended or invited by its auth row) and leaves the others, keeping each
// role; the plan is in principal order.
func TestRemovalPlansTakeEveryLiveMembership(t *testing.T) {
	str := func(s string) *string { return &s }
	st := func(s Status) *Status { return &s }
	id := func() *uuid.UUID { v := uuid.New(); return &v }
	active, suspended, invited, removed, staleAuth, onlyPublicOwner := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	pairs := map[uuid.UUID]*pairRows{
		active:          {principal: active, publicID: id(), publicRole: str("admin"), authID: id(), authStatus: st(StatusActive), authRole: str("admin")},
		suspended:       {principal: suspended, authID: id(), authStatus: st(StatusSuspended), authRole: str("member")},
		invited:         {principal: invited, authID: id(), authStatus: st(StatusInvited), authRole: str("viewer")},
		removed:         {principal: removed, authID: id(), authStatus: st(StatusRemoved), authRole: str("member")},
		staleAuth:       {principal: staleAuth, authID: id(), authStatus: st(StatusActive), authRole: str("member")},
		onlyPublicOwner: {principal: onlyPublicOwner, publicID: id(), publicRole: str("owner")},
	}
	plans := removalPlans(pairs)
	want := map[uuid.UUID]string{active: "admin", suspended: "member", invited: "viewer", onlyPublicOwner: "owner"}
	if len(plans) != len(want) {
		t.Fatalf("%d plans, want %d", len(plans), len(want))
	}
	for i, pl := range plans {
		if i > 0 && pl.principal.String() <= plans[i-1].principal.String() {
			t.Fatal("plans are not in principal order")
		}
		role, ok := want[pl.principal]
		if !ok || pl.plan.next != (state{status: StatusRemoved, role: role}) {
			t.Fatalf("plan for %v = %+v", pl.principal, pl.plan)
		}
		if pl.plan.rowID == nil || *pl.plan.rowID != pl.plan.id {
			t.Fatalf("a removal keeps the membership's own id: %+v", pl.plan)
		}
	}
	r := &OrgRemoval{orgID: uuid.New(), plans: plans}
	if r.Len() != 4 || len(r.Events()) != 4 {
		t.Fatalf("Len %d, %d events, want 4", r.Len(), len(r.Events()))
	}
	for _, e := range r.Events() {
		if e.EventType != "membership.removed" {
			t.Fatalf("event %s, want membership.removed", e.EventType)
		}
		if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
			t.Fatalf("CheckEvent: %v", err)
		}
	}
}
