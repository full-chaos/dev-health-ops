package credentials

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries the stored credential to another origin
// (D4124): the probe's request and the repository listing's client both follow nothing.
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	h := newHandlers(Deps{HTTPClient: probe.Client()})
	for _, client := range []*http.Client{h.client, h.repoClient} {
		if response, err := client.Do(probeGet(t, probe.Base.URL+"/user")); err == nil {
			response.Body.Close()
		}
	}
	_, _ = h.send(context.Background(), 5*time.Second, http.MethodGet, probe.Base.URL+"/user", map[string]string{"Authorization": "token SECRET"}, nil)
	probe.Assert(t)
}

// The client production builds (nil: apiservice/service.go:217 passes an unset credentialProbeClient) follows no
// redirect; its SSRF-guarded transport is replaced by the plain one so the probe can be reached.
func TestTheDefaultClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	h := newHandlers(Deps{})
	h.client, h.repoClient = redirectprobe.Reach(h.client), redirectprobe.Reach(h.repoClient)
	_, _ = h.send(context.Background(), 5*time.Second, http.MethodGet, probe.Base.URL+"/user", map[string]string{"PRIVATE-TOKEN": "SECRET"}, nil)
	probe.Assert(t)
}

func probeGet(t *testing.T, url string) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "token SECRET")
	return request
}
