package sso

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// A lone surrogate in redirect_uri is refused at validation, before the
// provider lookup: otherwise it reaches the authorization_url query and the
// sealed state, which json.Marshal rewrites to U+FFFD.
func TestOIDCAuthorizeRefusesALoneSurrogateRedirectURIBeforeAnyLookup(t *testing.T) {
	for _, escape := range []string{`\ud800`, `\udbff`, `\udc00`, `\udfff`} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/oidc/00000000-0000-4000-8000-000000000001/authorize",
			strings.NewReader(`{"redirect_uri":"https://app.example/cb`+escape+`"}`))
		request.Header.Set("Content-Type", "application/json")
		request.SetPathValue("provider_id", "00000000-0000-4000-8000-000000000001")
		body, outcome, _, err := pybody.Read(request)
		if err != nil || outcome != pybody.Ready {
			t.Fatalf("%s: read: %v %v", escape, outcome, err)
		}
		request = request.WithContext(policy.WithBody(request.Context(), body))
		recorder := httptest.NewRecorder()
		func() {
			defer func() {
				if recover() != nil {
					t.Errorf("%s: reached the provider lookup", escape)
				}
			}()
			handlers{}.initiateOIDCAuth(recorder, request)
		}()
		if recorder.Code != http.StatusUnprocessableEntity {
			t.Fatalf("%s: status=%d body=%s", escape, recorder.Code, recorder.Body.String())
		}
		text := recorder.Body.String()
		if !strings.Contains(text, `"string_unicode"`) || !strings.Contains(text, `"redirect_uri"`) || strings.Contains(text, "app.example") {
			t.Errorf("%s: body=%s", escape, text)
		}
	}
}
