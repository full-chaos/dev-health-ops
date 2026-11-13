package goapiproof

import "testing"

func TestHomeUnresolvedScopeDifferenceIsDeclaredWithItsTicket(t *testing.T) {
	d := homeUnresolvedScopeDifference
	if d.Ticket != "CHAOS-6119" || d.Reason == "" {
		t.Fatalf("declaration = %+v, want ticket CHAOS-6119 and a reason", d)
	}
}
