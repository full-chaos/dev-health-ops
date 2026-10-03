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

// NewClient (production) and a Client with no HTTP follow no redirect (oauthprovider.go:113 and :143).
func TestTheDefaultClientsNeverFollowARedirectToAnotherOrigin(t *testing.T) {
	for name, client := range map[string]*Client{"NewClient": NewClient(), "nil HTTP": {Endpoints: DefaultEndpoints}} {
		t.Run(name, func(t *testing.T) {
			probe := redirectprobe.New(t)
			_, _ = client.get(context.Background(), probe.Base.URL+"/user", map[string]string{"Authorization": "Bearer SECRET"}, "profile")
			probe.Assert(t)
		})
	}
}
