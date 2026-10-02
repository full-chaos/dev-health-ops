package providersync

import (
	"context"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries the provider credential to another origin:
// the base answers a redirect and the other origin sees no request at all (D4124).
func TestInProcessHTTPDoerNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, probe.Base.URL+"/user", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "token SECRET")
	if response, err := inProcessHTTPDoer(probe.Client()).Do(request); err == nil {
		response.Body.Close()
	}
	probe.Assert(t)
}
