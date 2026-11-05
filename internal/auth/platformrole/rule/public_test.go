package rule

import (
	"strings"
	"testing"
)

func TestPublicActiveRuleReadsPublicUsersNotAuthPrincipals(t *testing.T) {
	got := PublicActiveRule("r", "u")
	for _, want := range []string{"u.is_active IS TRUE", "r.expires_at IS NULL", "r.expires_at > statement_timestamp()"} {
		if !strings.Contains(got, want) {
			t.Errorf("PublicActiveRule = %q, want %q", got, want)
		}
	}
	for _, bad := range []string{"kind", "disabled_at", "principals"} {
		if strings.Contains(got, bad) {
			t.Errorf("PublicActiveRule = %q reads %q", got, bad)
		}
	}
}

func TestPublicHoldsSQLJoinsPublicUsers(t *testing.T) {
	got := PublicHoldsSQL(`"auth"`, "public.users.id", "'platform_admin'")
	for _, want := range []string{
		`FROM "auth".platform_role_assignments pra_held JOIN public.users pra_holder ON pra_holder.id = pra_held.principal_id`,
		"pra_held.principal_id = public.users.id", "pra_held.role_key = 'platform_admin'", PublicActiveRule("pra_held", "pra_holder"),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("PublicHoldsSQL = %q, want %q", got, want)
		}
	}
	if strings.Contains(got, ".principals") {
		t.Errorf("PublicHoldsSQL = %q reads an auth principals table", got)
	}
}
