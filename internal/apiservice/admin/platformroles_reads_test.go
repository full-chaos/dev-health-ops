package admin

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// The admin users reads take is_superuser by the platform roles switch:
// byte-equal while off, the platform role rule while on.
func TestAdminUsersReadsFollowTheSwitch(t *testing.T) {
	off := pgStore{}
	for alias, want := range map[string]string{"": fullUserColumns, "u": prefixColumns("u", fullUserColumns)} {
		if got, err := off.userColumns(alias); got != want || err != nil {
			t.Errorf("off (%q): %q %v", alias, got, err)
		}
	}
	on := pgStore{PlatformRolesOwned: true}
	for alias, id := range map[string]string{"": "users.id", "u": "u.id"} {
		got, err := on.userColumns(alias)
		if err != nil || !strings.Contains(got, rule.HoldsSQL(`"auth"`, id, "'platform_admin'")) || strings.Contains(got, "is_superuser") {
			t.Errorf("on (%q): %q %v", alias, got, err)
		}
	}
}

func TestAdminTargetAndStatsReadsFollowTheSwitch(t *testing.T) {
	off := pgStore{}
	if got, err := off.targetUserSQL(); got != `SELECT id, email, is_active, is_superuser FROM users WHERE id = $1` || err != nil {
		t.Errorf("off target: %q %v", got, err)
	}
	if got, err := off.superuserCountSQL(); got != `SELECT count(*) FROM users WHERE users.is_superuser IS true` || err != nil {
		t.Errorf("off count: %q %v", got, err)
	}
	if got, err := off.targetsSuperuserSQL(); got != `SELECT is_superuser FROM users WHERE id = ANY($1) ORDER BY id FOR UPDATE` || err != nil {
		t.Errorf("off targets: %q %v", got, err)
	}
	on := pgStore{PlatformRolesOwned: true}
	holds := rule.HoldsSQL(`"auth"`, "users.id", "'platform_admin'")
	for name, build := range map[string]func() (string, error){"target": on.targetUserSQL, "count": on.superuserCountSQL, "targets": on.targetsSuperuserSQL} {
		if got, err := build(); err != nil || !strings.Contains(got, holds) || strings.Contains(got, "is_superuser") {
			t.Errorf("on %s: %q %v", name, got, err)
		}
	}
}
