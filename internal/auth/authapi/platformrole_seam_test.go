package authapi

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func TestStateOnSQLIsByteEqualWhileNotOwned(t *testing.T) {
	const before = `
		SELECT email, username, full_name, COALESCE(is_active, false), COALESCE(is_superuser, false),
		       COALESCE(token_version, 0), auth_provider
		FROM public.users WHERE id = $1`
	schema, err := authschema.NewValidatedIdentifier("acp")
	if err != nil {
		t.Fatal(err)
	}
	if got := (PGUsers{AuthSchema: schema}).stateSQL(); got != before {
		t.Errorf("off: %q", got)
	}
	want := `
		SELECT email, username, full_name, COALESCE(is_active, false), ` + platformrole.HoldsSQL(`"acp"`, "public.users.id", "'platform_admin'") + `,
		       COALESCE(token_version, 0), auth_provider
		FROM public.users WHERE id = $1`
	if got := (PGUsers{AuthSchema: schema, PlatformRolesOwned: true}).stateSQL(); got != want {
		t.Errorf("on: %q, want %q", got, want)
	}
}
