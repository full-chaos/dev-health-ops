package policy

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// serviceAccountKeyRoutes are the four service account key rows: mounted WrapPlatform(admin.users), action admin.users
// in the profile, so with the admin cell on the decision point decides the route question admin.users.
var serviceAccountKeyRoutes = []string{
	"GET /api/v1/admin/service-accounts/{service_account_id}/keys",
	"POST /api/v1/admin/service-accounts/{service_account_id}/keys",
	"POST /api/v1/admin/service-accounts/{service_account_id}/keys/rotate",
	"POST /api/v1/admin/service-accounts/keys/{credential_id}/revoke",
}

// Admin cell on, rule 1: a superuser with no membership anywhere (an org-less token, no membership row) whose decision
// point state is platform_admin is allowed admin.users by the production evaluator's platform branch: 200 after the one
// route question.
func TestServiceAccountKeyRowsAdminCellOnSuperuserWithoutMembershipIs200(t *testing.T) {
	for _, pattern := range serviceAccountKeyRoutes {
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

// Admin cell on: a decision point that does not answer (dead, or not configured) is the 503, and the legacy guard does
// not answer in its place (a local superuser is not admitted).
func TestServiceAccountKeyRowsAdminCellOnUnavailableIs503(t *testing.T) {
	for _, pattern := range serviceAccountKeyRoutes {
		t.Run(pattern, func(t *testing.T) {
			dead := newTestPDP(t, allowAll)
			deadEnforcer := productionEnforcer(t, dead, "admin")
			dead.srv.Close()
			got := serveCell(cellHandler(t, legacyStore(true, ""), orglessAccessClaims(), deadEnforcer), pattern, cellBearer())
			if got.status != http.StatusServiceUnavailable || got.body != enforceUnavailableBody || got.header.Get("Retry-After") != enforceRetryAfter {
				t.Fatalf("dead PDP: %d %q Retry-After %q, want 503 %q Retry-After %s", got.status, got.body, got.header.Get("Retry-After"), enforceUnavailableBody, enforceRetryAfter)
			}
			unconfigured, err := NewEnforcer(EnforceConfig{Unconfigured: true, Rows: productionGoAPIRows(t), Switch: onFor("admin"), Logger: quiet()})
			if err != nil {
				t.Fatal(err)
			}
			got = serveCell(cellHandler(t, legacyStore(true, ""), orglessAccessClaims(), unconfigured), pattern, cellBearer())
			// The route question answers as on every route-question row: the authz_enforce_no_decision_point reason.
			if got.status != http.StatusServiceUnavailable || got.body != enforceNoDecisionPointBody {
				t.Fatalf("no decision point: %d %q, want 503 %q", got.status, got.body, enforceNoDecisionPointBody)
			}
		})
	}
}

// Admin cell on: an organization admin who is no superuser (decision point state: no platform role) is refused 403,
// the same answer as with the cell off.
func TestServiceAccountKeyRowsAdminCellOnOrgAdminIs403AsCellOff(t *testing.T) {
	for _, pattern := range serviceAccountKeyRoutes {
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
