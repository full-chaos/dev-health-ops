package authschema

import "testing"

// The privilege set the grants pass may give the runtime role holds no UPDATE on public.organizations.
func TestPublicPrivilegeSetHoldsNoOrganizationsUpdate(t *testing.T) {
	for _, entry := range PublicPrivilegeSet() {
		if entry.Table == "organizations" && entry.Privilege == "UPDATE" {
			t.Fatalf("PublicPrivilegeSet gives UPDATE on public.organizations: %v", entry.Columns)
		}
	}
}
