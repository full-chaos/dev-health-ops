package admin

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// The admin users view takes is_superuser by the platform roles switch: byte-equal while off; while on, the STORED
// holding (rule.StoredHoldsSQL: any principal state, as legacy's stored column), never the authority rule.
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
		if err != nil || !strings.Contains(got, rule.StoredHoldsSQL(`"auth"`, id, "'platform_admin'")) || strings.Contains(got, "is_superuser") ||
			strings.Contains(got, "disabled_at") || strings.Contains(got, ".principals") {
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
	if got, err := off.isSuperuserSQL(); got != `SELECT is_superuser FROM users WHERE id = $1` || err != nil {
		t.Errorf("off transfer target: %q %v", got, err)
	}
	on := pgStore{PlatformRolesOwned: true}
	// A read that asks whether the TARGET is a superuser (a protection) takes the stored holding, as legacy read the
	// target's stored column: a disabled superuser target keeps its protection. The count stays the authority rule.
	stored := rule.StoredHoldsSQL(`"auth"`, "users.id", "'platform_admin'")
	holds := rule.HoldsSQL(`"auth"`, "users.id", "'platform_admin'")
	for name, c := range map[string]struct {
		build func() (string, error)
		want  string
	}{"target": {on.targetUserSQL, stored}, "targets": {on.targetsSuperuserSQL, stored}, "transfer target": {on.isSuperuserSQL, stored},
		"count": {on.superuserCountSQL, holds}} {
		if got, err := c.build(); err != nil || !strings.Contains(got, c.want) || strings.Contains(got, "is_superuser") {
			t.Errorf("on %s: %q %v, want it to hold %q", name, got, err, c.want)
		}
	}
}
