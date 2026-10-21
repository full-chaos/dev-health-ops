package oauthprovider

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestTheDefaultClientsNeverDialTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		endpoints := Endpoints{GitHubUser: proxycapture.Target, GitHubEmails: proxycapture.Target, GitLabBase: proxycapture.Target, GoogleUser: proxycapture.Target}
		built := NewClient()
		built.Endpoints = endpoints
		_, _ = built.FetchUserInfo(ctx, GitHub, proxycapture.Canary)
		_, _ = (&Client{Endpoints: endpoints}).FetchUserInfo(ctx, Google, proxycapture.Canary)
	}).AssertNothingReached(t)
}
