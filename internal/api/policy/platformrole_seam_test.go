package policy

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
)

// Off, the statements are byte-equal to the reads before the switch existed;
// on, is_superuser is the platform role rule over the auth schema, and nothing
// else changes.
func TestUserStateSQLIsByteEqualWhileNotOwned(t *testing.T) {
	const before = `SELECT is_active, is_superuser, token_version FROM users WHERE id = $1`
	for _, s := range []PGStore{{}, {AuthSchema: "acp"}} {
		if got, err := s.userStateSQL(); got != before || err != nil {
			t.Errorf("off (%q): %q %v", s.AuthSchema, got, err)
		}
	}
	for schema, quoted := range map[string]string{"": `"auth"`, "acp": `"acp"`} {
		got, err := PGStore{AuthSchema: schema, PlatformRolesOwned: true}.userStateSQL()
		want := `SELECT is_active, ` + platformrole.HoldsSQL(quoted, "users.id", "'platform_admin'") + `, token_version FROM users WHERE id = $1`
		if got != want || err != nil {
			t.Errorf("on (%q): %q %v, want %q", schema, got, err, want)
		}
	}
	if _, err := (PGStore{AuthSchema: "a;b", PlatformRolesOwned: true}).userStateSQL(); err == nil {
		t.Error("an unusable auth schema must be refused while on")
	}
}

func TestAccessUserSQLIsByteEqualWhileNotOwned(t *testing.T) {
	const before = `SELECT is_active, is_superuser, token_version, email, username, full_name FROM users WHERE id = $1`
	for _, s := range []PGStore{{}, {AuthSchema: "acp"}} {
		if got, err := s.accessUserSQL(); got != before || err != nil {
			t.Errorf("off (%q): %q %v", s.AuthSchema, got, err)
		}
	}
	for schema, quoted := range map[string]string{"": `"auth"`, "acp": `"acp"`} {
		got, err := PGStore{AuthSchema: schema, PlatformRolesOwned: true}.accessUserSQL()
		want := `SELECT is_active, ` + platformrole.HoldsSQL(quoted, "users.id", "'platform_admin'") + `, token_version, email, username, full_name FROM users WHERE id = $1`
		if got != want || err != nil {
			t.Errorf("on (%q): %q %v, want %q", schema, got, err, want)
		}
	}
	if _, err := (PGStore{AuthSchema: "a;b", PlatformRolesOwned: true}).accessUserSQL(); err == nil {
		t.Error("an unusable auth schema must be refused while on")
	}
}
