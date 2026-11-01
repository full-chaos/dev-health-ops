package identity

import "testing"

func TestSignInRoleTable(t *testing.T) {
	for raw, want := range map[string]struct {
		role string
		ok   bool
	}{"": {"member", true}, "viewer": {"viewer", true}, "member": {"member", true}, "admin": {"admin", true},
		"owner": {"", false}, "root": {"", false}, "Admin": {"", false}, " member": {"", false}} {
		if role, ok := SignInRole(raw); role != want.role || ok != want.ok {
			t.Errorf("SignInRole(%q) = %q, %v; want %q, %v", raw, role, ok, want.role, want.ok)
		}
	}
}
