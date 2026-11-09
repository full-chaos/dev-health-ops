package admin

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// The four key routes are not in the admin route table yet (D6617: the mount and the profile rows come in the next PR).
func TestServiceAccountKeyRoutesNotMounted(t *testing.T) {
	mounted := map[string]bool{}
	for _, route := range Routes(Deps{Guard: policy.NewGuard(nil, nil)}) {
		mounted[route.Method+" "+route.Pattern] = true
	}
	for _, want := range []string{
		"GET /api/v1/admin/service-accounts/{service_account_id}/keys",
		"POST /api/v1/admin/service-accounts/{service_account_id}/keys",
		"POST /api/v1/admin/service-accounts/{service_account_id}/keys/rotate",
		"POST /api/v1/admin/service-accounts/keys/{credential_id}/revoke",
	} {
		if mounted[want] {
			t.Errorf("admin.Routes mounts %q before its profile row", want)
		}
	}
}
