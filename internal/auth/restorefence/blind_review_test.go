package restorefence

import (
	"strings"
	"testing"
)

// A blind margin has no source state, so the flag sections change nothing; the apply names every row that is active now
// (ids only) so the operator can review it.
func TestABlindMarginNamesTheActiveLegacyRowsForReview(t *testing.T) {
	want := map[string]string{
		"public.organizations":            "t.is_active",
		"public.org_ip_allowlist.ids":     "t.is_active",
		"public.integrations":             "t.is_active",
		"public.integration_credentials":  "t.is_active",
		"public.github_app_installations": "t.suspended_at IS NULL",
	}
	for name, pred := range want {
		def := mustSection(t, name)
		sql := blindReviewSQL(def)
		if sql == "" || !strings.Contains(sql, "SELECT t.id::text FROM "+def.qualified(testSchema(t))+" t WHERE "+pred) {
			t.Errorf("%s: blind-margin review SQL = %q, want the ids where %s", name, sql, pred)
		}
	}
	for _, s := range sections {
		if _, ok := want[s.name]; !ok && blindReviewSQL(s) != "" {
			t.Errorf("%s has a blind-margin review list it was not ruled to have", s.name)
		}
	}
}
