package edgetokenmint

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
)

// AMENDMENT 1: off, the statement is byte-equal to the public-only lookup; on,
// the principal is refused when public is_superuser or an auth platform role
// says superuser, in the same statement.
func TestLookupSQLIsByteEqualWhileNotOwned(t *testing.T) {
	for _, opts := range []Options{{}, {AuthSchema: "acp"}} {
		if got, err := lookupSQLFor(opts); got != lookupSQL || err != nil {
			t.Errorf("off (%q): %q %v", opts.AuthSchema, got, err)
		}
	}
	for schema, quoted := range map[string]string{"": `"auth"`, "acp": `"acp"`} {
		got, err := lookupSQLFor(Options{PlatformRolesOwned: true, AuthSchema: schema})
		want := `SELECT u.id::text, u.email, u.is_active, (u.is_superuser OR ` + platformrole.HoldsSQL(quoted, "u.id", "'platform_admin'") + `), u.token_version,
       COALESCE(u.auth_provider, ''), u.password_hash IS NOT NULL, m.role
  FROM users u
  LEFT JOIN memberships m ON m.user_id = u.id AND m.org_id = $2::uuid
 WHERE u.id = $1::uuid`
		if got != want || err != nil {
			t.Errorf("on (%q): %q %v, want %q", schema, got, err, want)
		}
	}
	if _, err := lookupSQLFor(Options{PlatformRolesOwned: true, AuthSchema: "a;b"}); err == nil {
		t.Error("an unusable auth schema must be refused while on")
	}
}
