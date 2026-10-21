package oauthprovider

import (
	"context"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestTheDefaultAndSuppliedClientsNeverDialTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		endpoints := Endpoints{GitHubUser: proxycapture.Target, GitHubEmails: proxycapture.Target, GitLabBase: proxycapture.Target, GoogleUser: proxycapture.Target}
		built := NewClient()
		built.Endpoints = endpoints
		_, _ = built.FetchUserInfo(ctx, GitHub, proxycapture.Canary)
		_, _ = (&Client{Endpoints: endpoints}).FetchUserInfo(ctx, Google, proxycapture.Canary)
		_, _ = (&Client{HTTP: &http.Client{}, Endpoints: endpoints}).FetchUserInfo(ctx, GitLab, proxycapture.Canary)
	}).AssertNothingReached(t)
}
