package admin_test

import (
	"slices"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// httpapi.AdminRoutes is the one table the no-store middleware and the venue
// oracle read: a route added to admin.Routes must be in it (package apiservice pins the rest).
func TestAdminRoutesTableHoldsTheAdminRouteTable(t *testing.T) {
	var want []string
	for _, route := range admin.Routes(admin.Deps{Guard: policy.NewGuard(nil, nil)}) {
		want = append(want, route.Method+" "+route.Pattern)
	}
	sort.Strings(want)
	got := httpapi.SortedAdminRoutes()
	for _, route := range want {
		if _, found := slices.BinarySearch(got, route); !found {
			t.Errorf("admin.Routes has %q, httpapi.AdminRoutes does not", route)
		}
	}
}
