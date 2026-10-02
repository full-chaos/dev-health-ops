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
	h := handlers{client: probeClientFor(probe.Client())}
	_, _ = h.send(context.Background(), 5*time.Second, http.MethodGet, probe.Base.URL+"/user", map[string]string{"Authorization": "token SECRET"}, nil)
	probe.Assert(t)
}

// The client production builds (nil: apiservice/service.go:217 passes an unset credentialProbeClient) follows no
// redirect; its SSRF-guarded transport is replaced by the plain one so the probe can be reached.
func TestTheDefaultClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	h := handlers{client: redirectprobe.Reach(probeClientFor(nil))}
	_, _ = h.send(context.Background(), 5*time.Second, http.MethodGet, probe.Base.URL+"/user", map[string]string{"PRIVATE-TOKEN": "SECRET"}, nil)
	probe.Assert(t)
}
