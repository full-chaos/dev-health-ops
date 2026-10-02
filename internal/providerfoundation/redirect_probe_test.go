package providerfoundation

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries a credential to another origin: the base
// answers a redirect and the other origin sees no request at all (D4124).
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	t.Run("the provider HTTP client (origin guard)", func(t *testing.T) {
		probe := redirectprobe.New(t)
		client, err := NewHTTPClient("github", probe.Base.URL, probe.Client(), TokenAuth("Authorization", "token ", secrets.NewValue("SECRET")),
			RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}, LeaseGuardFunc(func(context.Context) error { return nil }))
		if err != nil {
			t.Fatal(err)
		}
		if response, err := client.Do(context.Background(), http.MethodGet, "/user", nil); err == nil && response != nil {
			response.Body.Close()
		}
		probe.Assert(t)
	})
	t.Run("the GitHub App installation token request", func(t *testing.T) {
		probe := redirectprobe.New(t)
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
		credential := NewCredential("github", "probe", nil, map[string]secrets.Value{"app_id": secrets.NewValue("1"), "private_key": secrets.NewValue(pemKey), "installation_id": secrets.NewValue("2")})
		auth, err := NewGitHubAppAuth(credential, probe.Base.URL, probe.Client())
		if err != nil {
			t.Fatal(err)
		}
		_, _ = auth.installationToken(context.Background())
		probe.Assert(t)
	})
	t.Run("the PagerDuty OAuth token exchange", func(t *testing.T) {
		probe := redirectprobe.New(t)
		_, _ = ExchangePagerDutyAuthorizationCode(context.Background(), probe.Client(),
			PagerDutyRevokeConfig{ClientID: "c", ClientSecret: "SECRET", TokenURL: probe.Base.URL + "/oauth/token"}, "code", "verifier", time.Now())
		probe.Assert(t)
	})
	t.Run("the PagerDuty client-credentials auth keeps no redirect policy of the caller (its token URL is fixed)", func(t *testing.T) {
		credential := NewCredential("pagerduty", "probe", nil, map[string]secrets.Value{
			"client_id": secrets.NewValue("c"), "client_secret": secrets.NewValue("SECRET"), "subdomain": secrets.NewValue("acme")})
		auth, err := NewPagerDutyClientCredentialsAuth(credential, &http.Client{})
		if err != nil {
			t.Fatal(err)
		}
		client, ok := auth.doer.(*http.Client)
		if !ok || client.CheckRedirect == nil || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
			t.Fatalf("the supplied client's redirect policy was kept: %#v", auth.doer)
		}
	})
	t.Run("the PagerDuty OAuth token revoke (the token rides the form body)", func(t *testing.T) {
		probe := redirectprobe.New(t)
		_ = RevokePagerDutyOAuthToken(context.Background(), probe.Client(),
			PagerDutyRevokeConfig{ClientID: "c", RevokeURL: probe.Base.URL + "/oauth/revoke"}, "SECRET")
		probe.Assert(t)
	})
	t.Run("the PagerDuty OAuth refresh (client_secret and refresh_token ride the form body)", func(t *testing.T) {
		probe := redirectprobe.New(t)
		base, _ := url.Parse(probe.Base.URL)
		// The token URL is a constant: the supplied client's transport points it at the probe's base.
		supplied := probe.Client()
		supplied.Transport = &rewriteTo{base: base}
		refresh := "REFRESH"
		hydrator := PagerDutyOAuthHydrator{Doer: supplied, AppClientID: secrets.NewValue("c"), AppClientSecret: secrets.NewValue("SECRET")}
		_, _, _ = hydrator.refreshTokens(context.Background(), LeaseGuardFunc(func(context.Context) error { return nil }),
			NewCredential("pagerduty", "probe", nil, nil), PagerDutyOAuthTokenRecord{}, pagerDutyOAuthTokens{RefreshToken: &refresh})
		probe.Assert(t)
	})
	t.Run("the PagerDuty OAuth exchange with no client (pagerDutyClient default, no-follow flavour)", func(t *testing.T) {
		probe := redirectprobe.New(t)
		_, _ = ExchangePagerDutyAuthorizationCode(context.Background(), nil,
			PagerDutyRevokeConfig{ClientID: "c", ClientSecret: "SECRET", TokenURL: probe.Base.URL + "/oauth/token"}, "code", "verifier", time.Now())
		probe.Assert(t)
	})
}

// rewriteTo sends every request to base (a constant URL under test), keeping the client's redirect policy in play.
type rewriteTo struct {
	base *url.URL
	used atomic.Bool
}

func (r *rewriteTo) RoundTrip(request *http.Request) (*http.Response, error) {
	if r.used.CompareAndSwap(false, true) { // only the first (constant-URL) request moves; a redirect's target is left alone
		moved := request.Clone(request.Context())
		moved.URL.Scheme, moved.URL.Host = r.base.Scheme, r.base.Host
		return http.DefaultTransport.RoundTrip(moved)
	}
	return http.DefaultTransport.RoundTrip(request)
}
