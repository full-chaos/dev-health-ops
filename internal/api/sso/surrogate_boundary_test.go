package sso

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// A lone surrogate in an SSO provider body is a 422 at the body boundary, so the
// handler never sends it to auth-service (which would store U+FFFD for it).
func TestSSOProviderBodiesAreRefusedBeforeTheAuthServiceCall(t *testing.T) {
	f := newProxyFixture(t)
	user := &policy.User{ID: f.user, UserID: f.user.String(), OrgID: f.org.String(), Role: "admin"}
	guard := policy.NewGuard(nil, f.h.Logger)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		guard.BodyAfter(http.HandlerFunc(f.h.createProvider)).ServeHTTP(w, r.WithContext(policy.WithUser(r.Context(), user)))
	})
	for _, body := range []string{
		`{"name":"Acme","protocol":"oidc","default_role":"r\ud800","oidc_config":{"client_id":"c","client_secret":"s","issuer":"https://i.example"}}`,
		`{"name":"Acme","protocol":"oidc","allowed_domains":["a\udc00.com"],"oidc_config":{"client_id":"c","client_secret":"s","issuer":"https://i.example"}}`,
		`{"name":"Acme","protocol":"oidc","oidc_config":{"client_id":"c","client_secret":"s","issuer":"https://i.example","claim_mapping":{"\ud800":"x"}}}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), `"string_unicode"`) {
			t.Errorf("%s: %d %s, want 422 string_unicode", body, rec.Code, rec.Body.String())
		}
	}
	if len(f.fake.calls) != 0 {
		t.Errorf("auth-service was called %d time(s) for a refused body", len(f.fake.calls))
	}
}
