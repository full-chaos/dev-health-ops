package policy

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// orgEvaluatorVerdict answers an organization question about org as auth-service does: the production evaluator over
// state (authorize.go maps the question to an organization resource of that organization).
func orgEvaluatorVerdict(state pdpState, org string) func(string) (bool, string) {
	evaluator := authorization.NewEvaluator()
	return func(action string) (bool, string) {
		d := evaluator.Evaluate(authorization.Request{Action: action, ResourceKind: "organization", OrganizationID: org}, state)
		return d.Allowed, string(d.Reason)
	}
}

// Product rule 1 (D6773): a platform admin with no membership row is never refused what the legacy guard serves it. Per
// served-organization row (every family: the {org_id} members, invites, transfer and entitlements rows, the billing
// invoice and subscription org_id rows), the enforced answer, decided by the production evaluator on the caller's state
// (platform role admin, no membership row in the token organization), equals the legacy answer byte for byte: status,
// body and headers, for the token organization, another organization, a slug, and no org_id.
func TestServedOrgPlatformAdminWithNoMembershipRowAnswersAsLegacy(t *testing.T) {
	noRow := func() *accessStore { return legacyStore(true, "") }
	state := pdpState{status: authorization.StatusActive, role: rule.PlatformAdmin, member: false}
	served := []string{ownOrg, strings.ToUpper(ownOrg), "{" + ownOrg + "}", strangerOrg, memberOrg, "acme", "not-a-uuid", strings.Repeat("g", 32)}
	for _, row := range servedOrgCensus {
		t.Run(row.pattern, func(t *testing.T) {
			namespace := servedOrgNamespace(t, row.pattern)
			values := served
			if row.query {
				values = append([]string{""}, served...)
			}
			for _, value := range values {
				legacy := servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser, store: noRow}, value)
				pdp := newTestPDP(t, orgEvaluatorVerdict(state, ownOrg))
				got := servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser, store: noRow, enforcer: servedOrgEnforcer(t, pdp, namespace)}, value)
				t.Logf("RULE1-PAIR %s served=%q legacy=%d %q new=%d %q pdp=%+v", row.pattern, value, legacy.status, legacy.body, got.status, got.body, pdp.calls())
				if got.status != legacy.status || got.body != legacy.body || !headersEqual(got.header, legacy.header) {
					t.Errorf("served %q: %d %q %v, want the legacy %d %q %v", value, got.status, got.body, got.header, legacy.status, legacy.body, legacy.header)
				}
				calls := pdp.calls()
				if len(calls) != 1 || calls[0].Org != ownOrg || calls[0].Type != "organization" {
					t.Errorf("served %q: PDP saw %+v, want one organization question on the token organization %s", value, calls, ownOrg)
				}
			}
		})
	}
}
