package admin_test

import (
	"slices"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The venue oracle's "admin no-store" divergence covers exactly the routes of
// admin.Routes: a route added or removed here must change that table too.
func TestVenueOracleNoStoreTableEqualsTheAdminRouteTable(t *testing.T) {
	var want []string
	for _, route := range admin.Routes(admin.Deps{Guard: policy.NewGuard(nil, nil)}) {
		want = append(want, route.Method+" "+route.Pattern)
	}
	sort.Strings(want)
	got := venueoracle.AdminNoStoreRoutes()
	sort.Strings(got)
	if !slices.Equal(got, want) {
		t.Fatalf("venueoracle adminNoStoreRoutes differs from admin.Routes\n got: %v\nwant: %v", got, want)
	}
}
