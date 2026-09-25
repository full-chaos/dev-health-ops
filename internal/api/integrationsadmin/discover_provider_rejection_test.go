package integrationsadmin

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestProviderRejection pins which discovery failures are the provider
// refusing the credential: an authentication-class failure with status 401 or
// 403, for any provider, however deeply the service wrapped it. Everything
// else (a provider 5xx, a rate limit, a not-found, an error that is not the
// provider's) stays the 503.
func TestProviderRejection(t *testing.T) {
	provider := func(class providerfoundation.ErrorClass, status int) error {
		return fmt.Errorf("discover github sources: %w", &providerfoundation.ProviderError{Class: class, StatusCode: status})
	}
	for _, c := range []struct {
		name     string
		provider string
		err      error
		want     string
		status   int
		rejected bool
	}{
		{"github 401", "github", provider(providerfoundation.ErrorAuthentication, 401), "github", 401, true},
		{"github 403", "GitHub ", provider(providerfoundation.ErrorAuthentication, 403), "github", 403, true},
		{"gitlab 401", "gitlab", provider(providerfoundation.ErrorAuthentication, 401), "gitlab", 401, true},
		{"jira 401", "Jira", provider(providerfoundation.ErrorAuthentication, 401), "jira", 401, true},
		{"jira 403", "jira", provider(providerfoundation.ErrorAuthentication, 403), "jira", 403, true},
		{"linear 401", "linear", provider(providerfoundation.ErrorAuthentication, 401), "linear", 401, true},
		{"github 503 stays 503", "github", provider(providerfoundation.ErrorTransient, 503), "", 0, false},
		{"github 500 stays 503", "github", provider(providerfoundation.ErrorTransient, 500), "", 0, false},
		{"github not found stays 503", "github", provider(providerfoundation.ErrorNotFound, 404), "", 0, false},
		{"github rate limited stays 503", "github", provider(providerfoundation.ErrorRateLimited, 403), "", 0, false},
		{"authentication class without a 401/403 status", "github", provider(providerfoundation.ErrorAuthentication, 0), "", 0, false},
		{"not a provider error", "github", errors.New("resolve github credential for source discovery: boom"), "", 0, false},
	} {
		name, status, rejected := providerRejection(c.provider, c.err)
		if rejected != c.rejected || name != c.want || status != c.status {
			t.Errorf("%s: providerRejection = (%q, %d, %v), want (%q, %d, %v)", c.name, name, status, rejected, c.want, c.status, c.rejected)
		}
	}
}

// TestWriteProviderRejectionIsAFastAPIShapedDetail pins the 422 body: a detail
// list whose item names the provider and its status and echoes the path value.
func TestWriteProviderRejectionIsAFastAPIShapedDetail(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeProviderRejection(recorder, "0f8fad5b-d9cb-469f-a165-70867728950e", "github", 401)
	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status %d", recorder.Code)
	}
	const want = `{"detail":[{"type":"provider_authentication_failed","loc":["path","integration_id"],"msg":"github rejected the integration's credential (HTTP 401)","input":"0f8fad5b-d9cb-469f-a165-70867728950e"}]}`
	if got := recorder.Body.String(); got != want {
		t.Fatalf("body\n got  %s\n want %s", got, want)
	}
}
