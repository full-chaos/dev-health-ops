package policy

import "testing"

// TestMayImpersonateIsTheScopesCallerRule: the exported caller rule is the expression mayImpersonate decides by, term
// by term: a user access token, a legacy token's truthy superuser claim, or a live superuser while the platform roles
// switch is owned.
func TestMayImpersonateIsTheScopesCallerRule(t *testing.T) {
	for _, tc := range []struct {
		name  string
		user  User
		owned bool
		want  bool
	}{
		{"user access token", User{accessToken: true}, false, true},
		{"legacy token, superuser claim", User{superuserClaim: true}, false, true},
		{"legacy token, live superuser, roles owned", User{IsSuperuser: true}, true, true},
		{"legacy token, live superuser, roles not owned", User{IsSuperuser: true}, false, false},
		{"legacy token, no claim, not a superuser, roles owned", User{}, true, false},
		{"legacy token, no claim, not a superuser", User{}, false, false},
	} {
		if got := tc.user.MayImpersonate(tc.owned); got != tc.want {
			t.Errorf("%s: MayImpersonate(%v) = %v, want %v", tc.name, tc.owned, got, tc.want)
		}
	}
}
