package admin_test

import (
	"slices"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/adminroutes"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
)

// adminroutes.Table is the one table the no-store middleware and the venue
// oracle read: a route added to or removed from admin.Routes must change it.
func TestAdminRoutesTableEqualsTheAdminRouteTable(t *testing.T) {
	var want []string
	for _, route := range admin.Routes(admin.Deps{Guard: policy.NewGuard(nil, nil)}) {
		want = append(want, route.Method+" "+route.Pattern)
	}
	sort.Strings(want)
	if got := adminroutes.Sorted(); !slices.Equal(got, want) {
		t.Fatalf("adminroutes.Table differs from admin.Routes\n got: %v\nwant: %v", got, want)
	}
}
