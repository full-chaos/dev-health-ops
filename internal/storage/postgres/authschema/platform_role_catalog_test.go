package authschema

import (
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// The role CHECK on platform_role_assignments, as the newest migration that
// defines it leaves it, lists exactly the platform roles of the closed catalog
// (roles.v1): a role added to or removed from one and not the other fails here.
// Widening needs no pre-flight (no stored row can violate a wider list).
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

// The migration that adds platform_owner to the role CHECK is named, not
// numbered, so a renumber at merge is a file rename only. It widens with NOT
// VALID and a separate VALIDATE, and it is not a pre-flight migration.
func TestPlatformOwnerMigrationWidensTheRoleCheck(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Name != "platform_owner_role" {
			continue
		}
		for _, want := range []string{
			"DROP CONSTRAINT platform_role_assignments_role_closed",
			"CHECK (role_key IN ('platform_admin', 'platform_owner'))",
			"NOT VALID",
			"VALIDATE CONSTRAINT platform_role_assignments_role_closed",
			"CREATE TRIGGER platform_role_assignments_owner_guard",
			"BEFORE INSERT OR UPDATE OR DELETE ON platform_role_assignments",
			"FOR EACH ROW EXECUTE FUNCTION platform_role_assignments_owner_guard()",
			"current_user",
			"relowner",
			"platform_owner rows are written only by auth-migrate",
			"'platform_owner'",
		} {
			if !strings.Contains(m.SQL, want) {
				t.Errorf("migration %04d_%s lacks %q", m.Version, m.Name, want)
			}
		}
		return
	}
	t.Fatal("no migration named platform_owner_role")
}
