package sso

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries the access token (or the client secret) to
// another origin (D4124).
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	h := handlers{Deps{HTTPClient: httpClientFor(probe.Client())}}
	_, _ = h.fetchUserinfo(context.Background(), probe.Base.URL+"/userinfo", "SECRET-TOKEN")
	probe.Assert(t)
}
