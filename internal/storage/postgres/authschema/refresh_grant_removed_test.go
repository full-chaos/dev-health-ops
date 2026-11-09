package authschema

import "testing"

// The auth runtime role holds no privilege on go-api's public.refresh_tokens:
// the one-time legacy refresh exchange window has ended.
func TestRuntimeRoleHoldsNothingOnRefreshTokens(t *testing.T) {
	for _, privilege := range PublicPrivilegeSet() {
		if privilege.Table == "refresh_tokens" {
			t.Errorf("PublicPrivilegeSet grants %s %v on refresh_tokens, want no privilege", privilege.Privilege, privilege.Columns)
		}
	}
}
