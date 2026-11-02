package graph

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// Naming another organization in an operation is allowed, and runs against that
// org, only for a non-impersonating caller that holds admin.orgs.
func TestPlatformAction_CrossOrgOperationNeedsAdminOrgs(t *testing.T) {
	const own, other = "org-own", "org-other"
	var others []string
	for _, a := range rule.PlatformActions {
		if a != rule.ActionAdminOrgs {
			others = append(others, a)
		}
	}
	rest, only := rule.PlatformOf(others...), rule.PlatformOf(rule.ActionAdminOrgs)
	document := `mutation M($orgId: String) { doIt(orgId: $orgId) }`
	for _, c := range []struct {
		name   string
		claims authctx.Claims
		rebind string
	}{
		{"the flag only, as before", authctx.Claims{OrgID: own, IsSuperuser: true}, other},
		{"only admin.orgs", authctx.Claims{OrgID: own, Platform: &only}, other},
		{"the flag beside every other action", authctx.Claims{OrgID: own, IsSuperuser: true, Platform: &rest}, ""},
		{"no power", authctx.Claims{OrgID: own}, ""},
		{"a holder while impersonating", authctx.Claims{OrgID: own, IsSuperuser: true, ImpersonationActive: true}, ""},
	} {
		ctx := authctx.WithClaims(context.Background(), c.claims)
		message, rebind := operationOrgDecision(ctx, guardOperation(t, document, map[string]any{"orgId": other}))
		if rebind != c.rebind || (c.rebind == "" && message == "") || (c.rebind != "" && message != "") {
			t.Errorf("%s: message %q, rebind %q; want rebind %q", c.name, message, rebind, c.rebind)
		}
	}
}
