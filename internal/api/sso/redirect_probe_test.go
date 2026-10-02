package sso

import (
	"context"
	"golang.org/x/oauth2"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries the access token (or the client secret) to
// another origin (D4124).
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	h := newHandlers(Deps{HTTPClient: probe.Client()})
	_, _ = h.fetchUserinfo(context.Background(), probe.Base.URL+"/userinfo", "SECRET-TOKEN")
	probe.Assert(t)
}

// The OAuth code exchange carries client_secret in the form BODY, which a 307 would send again to any host.
func TestTheOAuthCodeExchangeNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	h := newHandlers(Deps{HTTPClient: probe.Client()})
	_, _ = h.exchangeOAuthCode(context.Background(), probe.Base.URL+"/token", "id", "SECRET", "code", "https://app.test/cb")
	probe.Assert(t)
}

// The OIDC token exchange (golang.org/x/oauth2 with the handler's client in its context) sends client_secret in the BODY.
func TestTheOIDCTokenExchangeNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	h := newHandlers(Deps{HTTPClient: probe.Client()})
	config := oauth2.Config{ClientID: "id", ClientSecret: "SECRET", Endpoint: oauth2.Endpoint{TokenURL: probe.Base.URL + "/token", AuthStyle: oauth2.AuthStyleInParams}}
	_, _ = config.Exchange(context.WithValue(context.Background(), oauth2.HTTPClient, h.HTTPClient), "code")
	probe.Assert(t)
}

// The client production builds when Deps.HTTPClient is nil (sso.go httpClientFor -> oidc.go defaultOIDCClient) follows
// no redirect; its SSRF-guarded transport is replaced by the plain one so the probe can be reached.
func TestTheDefaultClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	h := newHandlers(Deps{})
	h.HTTPClient = redirectprobe.Reach(h.HTTPClient)
	_, _ = h.exchangeOAuthCode(context.Background(), probe.Base.URL+"/token", "id", "SECRET", "code", "https://app.test/cb")
	probe.Assert(t)
}
