package edgetokenmint

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// Off, the statement is byte-equal to the public-only lookup; on, the
// superuser column is the platform role rule alone (CHAOS-10993 Q5): public
// is_superuser is a frozen copy then and decides nothing.
func TestLookupSQLIsByteEqualWhileNotOwned(t *testing.T) {
	for _, opts := range []Options{{}, {AuthSchema: "acp"}} {
		if got, err := lookupSQLFor(opts); got != lookupSQL || err != nil {
			t.Errorf("off (%q): %q %v", opts.AuthSchema, got, err)
		}
	}
	for schema, quoted := range map[string]string{"": `"auth"`, "acp": `"acp"`} {
		got, err := lookupSQLFor(Options{PlatformRolesOwned: true, AuthSchema: schema})
		want := `SELECT u.id::text, u.email, u.is_active, ` + rule.HoldsSQL(quoted, "u.id", "'platform_admin'") + `, u.token_version,
       COALESCE(u.auth_provider, ''), u.password_hash IS NOT NULL, m.role
  FROM users u
  LEFT JOIN memberships m ON m.user_id = u.id AND m.org_id = $2::uuid
 WHERE u.id = $1::uuid`
		if got != want || err != nil {
			t.Errorf("on (%q): %q %v, want %q", schema, got, err, want)
		}
		if strings.Contains(got, "is_superuser") {
			t.Errorf("on (%q): the owned lookup still reads public is_superuser", schema)
		}
	}
	if _, err := lookupSQLFor(Options{PlatformRolesOwned: true, AuthSchema: "a;b"}); err == nil {
		t.Error("an unusable auth schema must be refused while on")
	}
}
