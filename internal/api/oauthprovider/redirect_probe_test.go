package oauthprovider

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries the access token to another origin (D4124).
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	client := &Client{HTTP: probe.Client(), Endpoints: DefaultEndpoints}
	_, _ = client.get(context.Background(), probe.Base.URL+"/user", map[string]string{"Authorization": "Bearer SECRET"}, "profile")
	probe.Assert(t)
}
