package providersync

import (
	"context"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// With no client (production passes none) the doer follows no redirect either (inprocess.go default branch).
func TestTheDefaultInProcessDoerNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, probe.Base.URL+"/user", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "token SECRET")
	built, ok := inProcessHTTPDoer(nil).(*http.Client)
	if !ok {
		t.Fatal("the default doer is not an *http.Client")
	}
	if response, err := redirectprobe.Reach(built).Do(request); err == nil {
		response.Body.Close()
	}
	probe.Assert(t)
}
