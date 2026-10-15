package credentials

import (
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func TestGitHubBrowseProviderFailuresKeepProviderTextOutOfTheBody(t *testing.T) {
	cases := []struct {
		providerStatus int
		wantStatus     int
		wantDetail     string
	}{
		{http.StatusUnauthorized, http.StatusUnauthorized, "The provider rejected the credential."},
		{http.StatusForbidden, http.StatusUnauthorized, "The provider rejected the credential."},
		{http.StatusTooManyRequests, http.StatusTooManyRequests, "The provider rate limit was reached. Try again later."},
		{http.StatusInternalServerError, http.StatusBadGateway, "The provider request failed."},
	}
	for _, tc := range cases {
		transport := &browseTransport{statuses: map[string]int{"/user/repos": tc.providerStatus}}
		recorder, logs := browseRaw(t, transport, map[string]pyjson.Value{"token": "tok"}, "", nil)
		if recorder.Code != tc.wantStatus {
			t.Fatalf("provider %d: status %d, want %d: %s", tc.providerStatus, recorder.Code, tc.wantStatus, recorder.Body.String())
		}
		if want := `{"detail":"` + tc.wantDetail + `"}`; recorder.Body.String() != want {
			t.Fatalf("provider %d: body %q, want %q", tc.providerStatus, recorder.Body.String(), want)
		}
		if strings.Contains(recorder.Body.String(), "denied") || strings.Contains(recorder.Body.String(), "github ") {
			t.Fatalf("body carries the provider response text: %s", recorder.Body.String())
		}
		if !strings.Contains(logs, "github ") || (tc.providerStatus == http.StatusForbidden && !strings.Contains(logs, "denied")) {
			t.Fatalf("log lacks the provider cause: %s", logs)
		}
	}
}
