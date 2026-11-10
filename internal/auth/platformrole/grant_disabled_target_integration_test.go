//go:build integration

package platformrole

import (
	"testing"
)

// go-api's admin update grants is_superuser to an inactive user (pm D6914): the user-admin grant lands on a disabled
// principal, the assignment row is stored, and it gives nothing while the principal is disabled. A grant that does
// not ask for it (the operator CLI) still refuses a disabled target.
func TestGrantToADisabledUserIsStoredAndHoldsNothing(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	disabled := e.user(t, ctx, false, false)
	if _, err := e.store.Grant(ctx, allowDisabledTarget(GrantInput{Actor: admin, Principal: disabled, Role: PlatformAdmin})); err != nil {
		t.Fatalf("the user-admin grant to a disabled user: %v, want it stored", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1 AND role_key = $2`, disabled, PlatformAdmin); n != 1 {
		t.Fatalf("assignments = %d, want 1", n)
	}
	if held, err := e.store.Resolve(ctx, disabled); err != nil || held.Any() {
		t.Fatalf("a disabled principal with the assignment holds %v (err %v), want nothing", held.Roles(), err)
	}
}
