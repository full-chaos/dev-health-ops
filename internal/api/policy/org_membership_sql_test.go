package policy

import (
	"strings"
	"testing"
)

// The token-org read must see the organization's active flag: without it a
// member of a suspended organization (public rows are kept) passes. The
// integration cell TestLegacyGuardRefusesAMemberOfASuspendedOrganization holds
// the behaviour; this holds the statement where no database runs.
func TestOrgMembershipReadsTheOrganizationActiveFlag(t *testing.T) {
	if !strings.Contains(orgMembershipSQL, "is_active") {
		t.Fatalf("orgMembershipSQL does not read organizations.is_active:\n%s", orgMembershipSQL)
	}
}
