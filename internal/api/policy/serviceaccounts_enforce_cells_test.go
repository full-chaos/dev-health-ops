package policy

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// serviceAccountLifecycleRoutes are the four service account lifecycle rows: mounted WrapPlatform(admin.users) (with
// AUTH_PROXY_SERVICE_ACCOUNT_KEYS on), action null in the profile, so the admin cell asks no route question; the
// platform gate asks the decision point its own admin.users.
var serviceAccountLifecycleRoutes = []string{
	"POST /api/v1/admin/service-accounts",
	"POST /api/v1/admin/service-accounts/{service_account_id}/disable",
	"POST /api/v1/admin/service-accounts/{service_account_id}/enable",
	"POST /api/v1/admin/service-accounts/{service_account_id}/owner",
}

// Admin cell on, rule 1: a superuser with no membership anywhere whose decision point state is platform_admin is
// allowed admin.users by the production evaluator's platform branch: 200 after one platform question.
func TestServiceAccountLifecycleRowsAdminCellOnSuperuserWithoutMembershipIs200(t *testing.T) {
	for _, pattern := range serviceAccountLifecycleRoutes {
		t.Run(pattern, func(t *testing.T) {
			pdp := newTestPDP(t, evaluatorVerdict(pdpState{status: authorization.StatusActive, role: rule.PlatformAdmin}))
			got := serveCell(cellHandler(t, legacyStore(true, ""), orglessAccessClaims(), productionEnforcer(t, pdp, "admin")), pattern, cellBearer())
			want := pdpSeen{Principal: shadowBearer, Action: rule.ActionAdminUsers, Type: "platform", Key: "platform", Service: "dev-health-ops-api"}
			if calls := pdp.calls(); got.status != http.StatusOK || len(calls) != 1 || calls[0] != want {
				t.Fatalf("%d %q, PDP saw %+v; want 200 after the one platform question %+v", got.status, got.body, calls, want)
			}
		})
	}
}

// Admin cell on: a decision point that does not answer is the 503 with the unavailable reason, and the legacy guard
// does not answer in its place.
func TestServiceAccountLifecycleRowsAdminCellOnUnavailableIs503(t *testing.T) {
	for _, pattern := range serviceAccountLifecycleRoutes {
		t.Run(pattern, func(t *testing.T) {
			dead := newTestPDP(t, allowAll)
			deadEnforcer := productionEnforcer(t, dead, "admin")
			dead.srv.Close()
			got := serveCell(cellHandler(t, legacyStore(true, ""), orglessAccessClaims(), deadEnforcer), pattern, cellBearer())
			if got.status != http.StatusServiceUnavailable || got.body != enforceUnavailableBody || got.header.Get("Retry-After") != enforceRetryAfter {
				t.Fatalf("dead PDP: %d %q Retry-After %q, want 503 %q Retry-After %s", got.status, got.body, got.header.Get("Retry-After"), enforceUnavailableBody, enforceRetryAfter)
			}
		})
	}
}

// Admin cell on: an organization admin who is no superuser is refused 403, the same answer as with the cell off.
func TestServiceAccountLifecycleRowsAdminCellOnOrgAdminIs403AsCellOff(t *testing.T) {
	for _, pattern := range serviceAccountLifecycleRoutes {
		t.Run(pattern, func(t *testing.T) {
			off := serveCell(cellHandler(t, legacyStore(false, "admin"), userClaims(nil), nil), pattern, cellBearer())
			pdp := newTestPDP(t, evaluatorVerdict(pdpState{status: authorization.StatusActive, member: true}))
			got := serveCell(cellHandler(t, legacyStore(false, "admin"), userClaims(nil), productionEnforcer(t, pdp, "admin")), pattern, cellBearer())
			if got.status != http.StatusForbidden || got.body != superuserRefusalBody || off.status != got.status || off.body != got.body {
				t.Fatalf("cell on %d %q, cell off %d %q; want both 403 %q", got.status, got.body, off.status, off.body, superuserRefusalBody)
			}
		})
	}
}
