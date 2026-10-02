package atlassianteams

import (
	"net/http"
	"testing"
	"time"

	vendored "atlassian/atlassian"
)

// The vendored module's default client keeps the given timeout and refuses every redirect (CheckRedirect returns
// http.ErrUseLastResponse): the policy the redirect-site table relies on for its row (CHAOS-7910).
func TestTheVendoredDefaultClientRefusesRedirectsOnItsOwn(t *testing.T) {
	client := vendored.NewDefaultHTTPClient(7 * time.Second)
	if client.Timeout != 7*time.Second {
		t.Fatalf("timeout %v", client.Timeout)
	}
	if client.CheckRedirect == nil || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the vendored default client follows redirects")
	}
}
