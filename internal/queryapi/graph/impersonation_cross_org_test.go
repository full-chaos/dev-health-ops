package graph

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// TestOperationOrgGuardRefusesAnImpersonatingHoldersCrossOrgRead: a non-superuser holder of admin.impersonate and
// admin.orgs, while its impersonation session is applied, may not name an org other than the target's (D6714:
// cross-org read refused while impersonating); outside a session the same action set may.
func TestOperationOrgGuardRefusesAnImpersonatingHoldersCrossOrgRead(t *testing.T) {
	t.Parallel()
	platform := rule.PlatformOf(rule.ActionAdminImpersonate, rule.ActionAdminOrgs)
	document := `query Q($orgId: String) { doIt(orgId: $orgId) }`
	impersonating := authctx.Claims{OrgID: "org-target", Role: "viewer", ImpersonationActive: true, Platform: &platform}
	if got := operationOrgViolation(authctx.WithClaims(context.Background(), impersonating), guardOperation(t, document, map[string]any{"orgId": "org-other"})); got != "Access denied: cannot query org 'org-other'" {
		t.Fatalf("impersonating: %q, want the cross-org refusal", got)
	}
	if got := operationOrgViolation(authctx.WithClaims(context.Background(), impersonating), guardOperation(t, document, map[string]any{"orgId": "org-target"})); got != "" {
		t.Fatalf("impersonating, the target org: %q, want it served", got)
	}
	own := authctx.Claims{OrgID: "org-own", Role: "admin", Platform: &platform}
	if got := operationOrgViolation(authctx.WithClaims(context.Background(), own), guardOperation(t, document, map[string]any{"orgId": "org-other"})); got != "" {
		t.Fatalf("not impersonating: %q, want admin.orgs to name another org", got)
	}
}
