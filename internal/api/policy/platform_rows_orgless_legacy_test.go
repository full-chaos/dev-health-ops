package policy

import (
	"maps"
	"net/http"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// platformBillingRoutes are the platform rows the billing package mounts (billing/routes.go: WrapPlatform(admin.billing)).
var platformBillingRoutes = map[string]platformAdminRoute{
	"POST /api/v1/billing/plans/pull-stripe": {rule.ActionAdminBilling, false},
}

// profilePlatformRows are the production profile's rows whose action is a platform action, sorted.
func profilePlatformRows(t *testing.T) []string {
	t.Helper()
	var patterns []string
	for pattern, row := range productionGoAPIRows(t) {
		if row.ResourceType == policycatalog.ScopePlatform {
			patterns = append(patterns, pattern)
		}
	}
	slices.Sort(patterns)
	return patterns
}

// platformRowsMux mounts every platform row as production does: the platform wraps, the admin-or /admin/users rows
// and the billing platform wrap.
func platformRowsMux(guard *Guard) *http.ServeMux {
	mux := http.NewServeMux()
	for pattern, route := range platformAdminRoutes {
		if route.bodyFirst {
			mux.Handle(pattern, guard.BodyFirstPlatform(route.action, echo))
		} else {
			mux.Handle(pattern, guard.WrapPlatform(route.action, echo))
		}
	}
	for pattern, route := range adminOrUsersRoutes {
		if route.bodyFirst {
			mux.Handle(pattern, guard.BodyFirstAdminOr(route.action, echo))
		} else {
			mux.Handle(pattern, guard.WrapAdminOr(route.action, echo))
		}
	}
	for pattern, route := range platformBillingRoutes {
		mux.Handle(pattern, guard.WrapPlatform(route.action, echo))
	}
	return mux
}

// The mounted tables cover exactly the production profile's platform rows, each with the action of its mount.
func TestPlatformRowsMuxCoversEveryProfilePlatformRow(t *testing.T) {
	rows := productionGoAPIRows(t)
	got := map[string]string{}
	for _, pattern := range profilePlatformRows(t) {
		got[pattern] = rows[pattern].Action
	}
	want := map[string]string{}
	for _, table := range []map[string]platformAdminRoute{platformAdminRoutes, adminOrUsersRoutes, platformBillingRoutes} {
		for pattern, route := range table {
			want[pattern] = route.action
		}
	}
	if !maps.Equal(got, want) {
		t.Fatalf("production platform rows %v, mounted tables %v", got, want)
	}
}

// Product rule 1, legacy answer, on every platform row of the production profile: the caller gets the same status and
// body with the admin cell on as with it off and as with no Enforcer. A caller with no organization on its token and
// no platform role gets the legacy 403 "Organization context required"; a superuser with no organization, and one
// whose token organization has no membership row, is served.
func TestEveryPlatformRowKeepsTheLegacyOrglessAnswerWithTheAdminCellOn(t *testing.T) {
	orgClaim := func() *RoutedResult { return userClaims(nil) }
	for _, c := range []struct {
		name      string
		superuser bool
		role      string
		claims    func() *RoutedResult
		status    int
		body      string
	}{
		{"orgless, no platform role", false, "", orglessAccessClaims, http.StatusForbidden, `{"detail":"Organization context required"}`},
		{"orgless superuser", true, "", orglessAccessClaims, http.StatusOK, ""},
		{"superuser, org claim, no membership", true, "", orgClaim, http.StatusOK, ""},
		{"org member, no platform role", false, "member", orgClaim, http.StatusForbidden, ""},
	} {
		for _, pattern := range profilePlatformRows(t) {
			t.Run(c.name+"/"+pattern, func(t *testing.T) {
				serve := func(enf *Enforcer) response {
					guard := NewGuard(routed(t, legacyStore(c.superuser, c.role), &fakeValidator{result: c.claims()}), quiet())
					guard.SetEnforcer(enf)
					return serveCell(platformRowsMux(guard), pattern, cellBearer())
				}
				decider := func() pep.Decider {
					if c.superuser {
						return platformAdminFake(t)
					}
					return newDenyingFake(t)
				}
				base := serve(nil)
				off := serve(seamEnforcer(t, decider(), productionGoAPIRows(t), "teams"))
				on := serve(seamEnforcer(t, decider(), productionGoAPIRows(t), "admin"))
				equalToMain(t, off, base)
				equalToMain(t, on, off)
				if on.status != c.status || (c.body != "" && on.body != c.body) {
					t.Fatalf("%d %q, want %d %q", on.status, on.body, c.status, c.body)
				}
			})
		}
	}
}
