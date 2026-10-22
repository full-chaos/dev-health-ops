package authschema

import (
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// The role CHECK on memberships lists exactly the organization roles of the
// closed catalog (roles.v1): a role added to or removed from one and not the
// other fails here.
func TestMembershipRoleCheckListsTheCatalogOrganizationRoles(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var sql string
	for _, m := range migrations {
		if strings.Contains(m.SQL, "memberships_role_closed") {
			sql = m.SQL
		}
	}
	if sql == "" {
		t.Fatal("no migration defines memberships_role_closed")
	}
	match := regexp.MustCompile(`(?s)memberships_role_closed\s+CHECK\s*\(\s*role IN \(([^)]*)\)\s*\)`).FindStringSubmatch(sql)
	if match == nil {
		t.Fatal("memberships_role_closed is not a plain role IN (...) check")
	}
	var got []string
	for _, item := range strings.Split(match[1], ",") {
		got = append(got, strings.Trim(strings.TrimSpace(item), "'"))
	}
	sort.Strings(got)

	var want []string
	for _, role := range policycatalog.Default().Roles() {
		if role.Scope == policycatalog.ScopeOrganization {
			want = append(want, role.ID)
		}
	}
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("memberships role CHECK = %v, catalog organization roles = %v", got, want)
	}
	for _, role := range got {
		if role == "platform_admin" || role == "superuser" {
			t.Errorf("memberships role CHECK admits %s", role)
		}
	}
}
