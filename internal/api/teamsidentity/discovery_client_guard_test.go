package teamsidentity

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestDiscoveryClientRefusesAnInternalDial: the client discovery uses for a
// GitHub App credential (an installation token exchange against the
// credential's base_url) must not connect to an internal address, whatever
// the URL check resolved earlier.
func TestDiscoveryClientRefusesAnInternalDial(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("the discovery client reached an internal server (Authorization %q)", r.Header.Get("Authorization"))
	}))
	defer server.Close()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/app/installations/1/access_tokens", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer app-jwt")
	response, err := discoveryHTTPClient.Do(request)
	if err == nil {
		response.Body.Close()
		t.Fatalf("the discovery client connected to %s (status %d)", server.URL, response.StatusCode)
	}
	if !strings.Contains(err.Error(), "Connection to private/internal networks is not allowed") {
		t.Errorf("error = %v, want the internal-network refusal", err)
	}
}
