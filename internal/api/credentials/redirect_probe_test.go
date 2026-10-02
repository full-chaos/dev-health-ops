package credentials

import (
	"context"
	"github.com/full-chaos/dev-health-ops/internal/api/githubcode"
	"github.com/full-chaos/dev-health-ops/internal/api/gitlabcode"
	"math/big"
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
	// the repository listings go through the same client into restcore
	_, _ = githubcode.Client{Token: "SECRET", BaseURL: probe.Base.URL, HTTP: withTimeout(h.repoClient, time.Second)}.ListRepositories(context.Background(), githubcode.ListOptions{Org: "acme"})
	_, _ = gitlabcode.Client{Token: "SECRET", BaseURL: probe.Base.URL, HTTP: withTimeout(h.repoClient, time.Second)}.ListProjects(context.Background(), gitlabcode.ListOptions{MaxProjects: big.NewInt(100)})
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
