package orgs

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// Mix 7: entitlements. Another org's entitlements are read only by admin.billing:
// a caller holding it passes the access check (the id here is no UUID, so it
// answers 404 before any read); any other caller answers 403.
func TestPlatformAction_EntitlementsOfAnotherOrgNeedAdminBilling(t *testing.T) {
	empty := rule.PlatformOf()
	only := rule.PlatformOf(rule.ActionAdminBilling)
	var rest []string
	for _, a := range rule.PlatformActions {
		if a != rule.ActionAdminBilling {
			rest = append(rest, a)
		}
	}
	others := rule.PlatformOf(rest...)
	for _, c := range []struct {
		name      string
		superuser bool
		platform  *rule.Platform
		status    int
	}{
		{"flag only, as before the change", true, nil, http.StatusNotFound},
		{"only admin.billing", false, &only, http.StatusNotFound},
		{"every other action", true, &others, http.StatusForbidden},
		{"an empty set beside the flag", true, &empty, http.StatusForbidden},
		{"no flag, no set", false, nil, http.StatusForbidden},
	} {
		r := httptest.NewRequest(http.MethodGet, "/orgs/not-a-uuid/entitlements", nil)
		r.SetPathValue("org_id", "not-a-uuid")
		r = r.WithContext(policy.WithUser(r.Context(), &policy.User{OrgID: "own", IsSuperuser: c.superuser, Platform: c.platform}))
		w := httptest.NewRecorder()
		handlers{}.entitlements(w, r)
		if w.Code != c.status {
			t.Errorf("%s: status %d, want %d", c.name, w.Code, c.status)
		}
	}
}
