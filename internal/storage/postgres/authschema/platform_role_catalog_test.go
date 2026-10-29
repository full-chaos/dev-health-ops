package authschema

import (
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// The role CHECK on platform_role_assignments lists exactly the platform roles
// of the closed catalog (roles.v1), and the pre-flight refuses the same list:
// a role added to or removed from one and not the others fails here.
func TestPlatformRoleCheckListsTheCatalogPlatformRoles(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var sql string
	for _, m := range migrations {
		if strings.Contains(m.SQL, "platform_role_assignments_role_closed") {
			sql = m.SQL
		}
	}
	if sql == "" {
		t.Fatal("no migration defines platform_role_assignments_role_closed")
	}
	list := func(pattern string) []string {
		match := regexp.MustCompile(pattern).FindStringSubmatch(sql)
		if match == nil {
			t.Fatalf("no match for %s", pattern)
		}
		var got []string
		for _, item := range strings.Split(match[1], ",") {
			got = append(got, strings.Trim(strings.TrimSpace(item), "'"))
		}
		sort.Strings(got)
		return got
	}
	check := list(`(?s)platform_role_assignments_role_closed\s+CHECK\s*\(\s*role_key IN \(([^)]*)\)\s*\)`)
	preflight := list(`(?s)WHERE role_key NOT IN \(([^)]*)\)`)

	var want []string
	for _, role := range policycatalog.Default().Roles() {
		if role.Scope == policycatalog.ScopePlatform {
			want = append(want, role.ID)
		}
	}
	sort.Strings(want)
	if strings.Join(check, ",") != strings.Join(want, ",") {
		t.Fatalf("platform role CHECK = %v, catalog platform roles = %v", check, want)
	}
	if strings.Join(preflight, ",") != strings.Join(want, ",") {
		t.Fatalf("platform role pre-flight = %v, catalog platform roles = %v", preflight, want)
	}
	for _, role := range policycatalog.Default().Roles() {
		if role.Scope != policycatalog.ScopePlatform {
			for _, held := range check {
				if held == role.ID {
					t.Errorf("platform role CHECK admits the organization role %s", role.ID)
				}
			}
		}
	}
}
