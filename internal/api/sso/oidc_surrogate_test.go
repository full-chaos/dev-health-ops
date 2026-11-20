package sso

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// A lone surrogate in redirect_uri, with the OIDC forward off, answers 503 config_missing naming AUTH_PROXY_OIDC before any
// lookup and never echoes the input: go-api builds no authorization_url and seals no state (CHAOS-14131).
func TestOIDCAuthorizeWithALoneSurrogateRedirectURIAnswersConfigMissingBeforeAnyLookup(t *testing.T) {
	for _, escape := range []string{`\ud800`, `\udbff`, `\udc00`, `\udfff`} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/oidc/00000000-0000-4000-8000-000000000001/authorize",
			strings.NewReader(`{"redirect_uri":"https://app.example/cb`+escape+`"}`))
		request.Header.Set("Content-Type", "application/json")
		request.SetPathValue("provider_id", "00000000-0000-4000-8000-000000000001")
		read, outcome, _, err := pybody.Read(request)
		if err != nil || outcome != pybody.Ready {
			t.Fatalf("%s: read: %v %v", escape, outcome, err)
		}
		request = request.WithContext(policy.WithBody(request.Context(), read))
		recorder := httptest.NewRecorder()
		func() {
			defer func() {
				if recover() != nil {
					t.Errorf("%s: reached the provider lookup", escape)
				}
			}()
			newHandlers(Deps{}).refuseOIDCNotForwarded(body(oidcAuthRequest)).ServeHTTP(recorder, request)
		}()
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s: status=%d body=%s", escape, recorder.Code, recorder.Body.String())
		}
		text := recorder.Body.String()
		if !strings.Contains(text, `"AUTH_PROXY_OIDC is not set"`) || strings.Contains(text, "app.example") {
			t.Errorf("%s: body=%s", escape, text)
		}
	}
}
