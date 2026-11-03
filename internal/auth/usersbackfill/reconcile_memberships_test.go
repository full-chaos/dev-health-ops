package usersbackfill

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeMemberships struct {
	diffs MembershipDiffs
	err   error
}

func (f fakeMemberships) MembershipDiffs(context.Context) (MembershipDiffs, error) {
	return f.diffs, f.err
}

func reconcileMembers(t *testing.T, d MembershipDiffs) Reconciliation {
	t.Helper()
	rep := reconcile(t, nil, nil, nil)
	if err := ReconcileMemberships(context.Background(), fakeMemberships{diffs: d}, &rep); err != nil {
		t.Fatal(err)
	}
	return rep
}

func hasLine(rep Reconciliation, line string) bool {
	for _, l := range rep.Lines() {
		if l == line {
			return true
		}
	}
	return false
}

// An auth membership that is not active while go-api still holds the member
// fails the gate.
func TestReconcileGatesMembershipInactiveInPublic(t *testing.T) {
	rep := reconcileMembers(t, MembershipDiffs{InactiveInPublic: 2})
	if rep.Diffs[DiffMembershipInactiveInPublic] != 2 || rep.GatedTotal() != 2 {
		t.Fatalf("diffs %+v, gated %d; want 2 and 2", rep.Diffs, rep.GatedTotal())
	}
	if !hasLine(rep, "diff.membership_inactive_in_public=2") || !hasLine(rep, "gated_diffs=2") {
		t.Fatalf("lines:\n%s", strings.Join(rep.Lines(), "\n"))
	}
}

// An active auth membership with no public row (go-api removed the member) is
// printed and never gated.
func TestReconcilePrintsMembershipPublicMissingUngated(t *testing.T) {
	rep := reconcileMembers(t, MembershipDiffs{PublicMissing: 3})
	if rep.GatedTotal() != 0 || !hasLine(rep, "info.membership_public_missing=3") {
		t.Fatalf("gated %d, lines:\n%s", rep.GatedTotal(), strings.Join(rep.Lines(), "\n"))
	}
}

// A role go-api changed in public only is printed and never gated.
func TestReconcilePrintsMembershipRoleDiffUngated(t *testing.T) {
	rep := reconcileMembers(t, MembershipDiffs{RoleDiff: 4})
	if rep.GatedTotal() != 0 || !hasLine(rep, "info.membership_role_diff=4") {
		t.Fatalf("gated %d, lines:\n%s", rep.GatedTotal(), strings.Join(rep.Lines(), "\n"))
	}
}

// A public membership with no auth row (go-api writes, the register owner
// row) is printed and never gated.
func TestReconcilePrintsMembershipAuthMissingUngated(t *testing.T) {
	rep := reconcileMembers(t, MembershipDiffs{AuthMissing: 5})
	if rep.GatedTotal() != 0 || !hasLine(rep, "info.membership_auth_missing=5") {
		t.Fatalf("gated %d, lines:\n%s", rep.GatedTotal(), strings.Join(rep.Lines(), "\n"))
	}
}

// A failed membership read stops the reconciliation with an error that names
// no value.
func TestReconcileMembershipReadFailureStops(t *testing.T) {
	rep := reconcile(t, nil, nil, nil)
	err := ReconcileMemberships(context.Background(), fakeMemberships{err: errors.New("planted-membership-detail")}, &rep)
	if err == nil {
		t.Fatal("a failed membership read was not reported")
	}
	if strings.Contains(err.Error(), "planted-membership-detail") {
		t.Fatalf("error carries the driver text: %v", err)
	}
}
