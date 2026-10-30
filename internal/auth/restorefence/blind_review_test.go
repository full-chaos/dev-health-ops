package restorefence

import (
	"strings"
	"testing"
)

// A blind margin has no source state, so the flag, scope, role and superuser sections change nothing; the apply names
// every row that holds one now (ids only) so the operator can review it.
func TestABlindMarginNamesTheActiveLegacyRowsForReview(t *testing.T) {
	want := map[string]string{
		"public.organizations":              "t.is_active",
		"public.org_ip_allowlist.ids":       "t.is_active",
		"public.integrations":               "t.is_active",
		"public.integration_credentials":    "t.is_active",
		"public.github_app_installations":   "t.suspended_at IS NULL",
		"auth.workloads.scope":              "(cardinality(t.audiences) > 0 OR cardinality(t.allowed_actions) > 0)",
		"auth.credential_registry.scope":    "t.revoked_at IS NULL AND (cardinality(t.scopes) > 0 OR cardinality(t.audiences) > 0)",
		"auth.memberships.role":             "t.role IN ('admin', 'owner')",
		"public.memberships.role":           "t.role IN ('admin', 'owner')",
		"public.users.superuser":            "t.is_superuser",
		"public.provider_oauth_credentials": "true",
		"auth.users.password":               "t.password_hash IS NOT NULL",
		"public.users.password":             "t.password_hash IS NOT NULL",
		"auth.external_identities":          "true",
	}
	for name, pred := range want {
		def := mustSection(t, name)
		sql := blindReviewSQL(def, testSchema(t))
		id := def.idSQL()
		if def.blindIDExpr != "" {
			id = def.blindIDExpr
		}
		if sql == "" || !strings.Contains(sql, "SELECT "+id+" FROM "+def.qualified(testSchema(t))+" t WHERE "+pred) {
			t.Errorf("%s: blind-margin review SQL = %q, want the ids where %s", name, sql, pred)
		}
	}
	for _, s := range sections {
		if _, ok := want[s.name]; !ok && blindReviewSQL(s, testSchema(t)) != "" {
			t.Errorf("%s has a blind-margin review list it was not ruled to have", s.name)
		}
	}
}
