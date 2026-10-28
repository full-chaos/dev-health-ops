package identity

import (
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// The join inserts exactly the public.memberships columns the users-owned
// grant gives the auth runtime role.
func TestJoinMembershipPublicColumnsAreTheGrantedColumns(t *testing.T) {
	for _, g := range authschema.RegisterOrganizationGrants() {
		if g.Table != "memberships" {
			continue
		}
		if !slices.Equal(joinPublicMembershipColumns, g.Columns) {
			t.Fatalf("join inserts %v, the grant lists %v", joinPublicMembershipColumns, g.Columns)
		}
		return
	}
	t.Fatal("RegisterOrganizationGrants has no memberships entry")
}
