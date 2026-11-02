package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// The impersonation status looks a session up only for a caller that holds
// admin.impersonate; a caller that holds the flag beside every other action is
// answered "not impersonating" without any read (this handler has no store, so
// a read would fail the call).
func TestPlatformAction_ImpersonationStatusNeedsAdminImpersonate(t *testing.T) {
	var others []string
	for _, a := range rule.PlatformActions {
		if a != rule.ActionAdminImpersonate {
			others = append(others, a)
		}
	}
	rest, empty := rule.PlatformOf(others...), rule.PlatformOf()
	for _, c := range []struct {
		name string
		user *policy.User
	}{
		{"the flag beside every other action", &policy.User{IsSuperuser: true, Platform: &rest}},
		{"an empty set beside the flag", &policy.User{IsSuperuser: true, Platform: &empty}},
		{"no power", &policy.User{}},
	} {
		func() {
			defer func() {
				if recover() != nil {
					t.Errorf("%s: the status read a session", c.name)
				}
			}()
			r := httptest.NewRequest(http.MethodGet, "/impersonate/status", nil)
			r = r.WithContext(policy.WithUser(r.Context(), c.user))
			w := httptest.NewRecorder()
			(&handlers{}).impersonationStatus(w, r)
			if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"is_impersonating":false`) && !strings.Contains(w.Body.String(), `"is_impersonating": false`) {
				t.Errorf("%s: %d %s, want 200 is_impersonating false", c.name, w.Code, w.Body.String())
			}
		}()
	}
}
