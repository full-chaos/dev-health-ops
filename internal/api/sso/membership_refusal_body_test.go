package sso

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// TestMembershipRefusalBody pins the 403 body auth-service answers byte for byte
// (internal/auth/identity/oidc and saml carry the same literal).
func TestMembershipRefusalBody(t *testing.T) {
	const want = `{"detail":{"message":"A membership in this organization is required","reason":"membership_required"}}`
	rec := httptest.NewRecorder()
	policy.WriteDetail(rec, http.StatusForbidden, membershipRequiredDetail(), nil)
	if rec.Code != http.StatusForbidden || rec.Body.String() != want {
		t.Fatalf("answer = %d %s, want 403 %s", rec.Code, rec.Body, want)
	}
}
