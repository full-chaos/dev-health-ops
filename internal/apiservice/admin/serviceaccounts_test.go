package admin

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// unmountedAccounts is a non-nil lifecycle client (the switch on); it is never called.
type unmountedAccounts struct{ authprovision.ServiceAccounts }

// The four lifecycle routes are not in the admin route table yet, even with the switch on (D6617: the mount and
// the profile rows come in the next PR).
func TestServiceAccountRoutesNotMounted(t *testing.T) {
	mounted := map[string]bool{}
	for _, route := range Routes(Deps{Guard: policy.NewGuard(nil, nil), ServiceAccounts: unmountedAccounts{}}) {
		mounted[route.Method+" "+route.Pattern] = true
	}
	for _, want := range []string{
		"POST /api/v1/admin/service-accounts",
		"POST /api/v1/admin/service-accounts/{service_account_id}/disable",
		"POST /api/v1/admin/service-accounts/{service_account_id}/enable",
		"POST /api/v1/admin/service-accounts/{service_account_id}/owner",
	} {
		if mounted[want] {
			t.Errorf("admin.Routes mounts %q before its profile row", want)
		}
	}
}
