package policy_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// The web builds its plan-gate sentence from these three keys, so the 402 a
// route writes for an unlicensed feature stays this exact body, written by
// WriteDetail and untouched by the error mapping.
func TestFeatureNotLicensedBodyStaysByteForByte(t *testing.T) {
	required := "team"
	recorder := httptest.NewRecorder()
	policy.WriteDetail(recorder, http.StatusPaymentRequired, licensing.FeatureNotLicensedDetail("sso_saml", &required), nil)
	want := `{"detail":{"error":"feature_not_licensed","feature":"sso_saml","required_tier":"team","current_tier":"community"}}`
	if recorder.Code != http.StatusPaymentRequired || recorder.Body.String() != want {
		t.Fatalf("got %d %s, want 402 %s", recorder.Code, recorder.Body.String(), want)
	}
	nullTier := httptest.NewRecorder()
	policy.WriteDetail(nullTier, http.StatusPaymentRequired, licensing.FeatureNotLicensedDetail("sso_saml", nil), nil)
	wantNull := `{"detail":{"error":"feature_not_licensed","feature":"sso_saml","required_tier":null,"current_tier":"community"}}`
	if nullTier.Body.String() != wantNull {
		t.Fatalf("got %s, want %s", nullTier.Body.String(), wantNull)
	}
}
