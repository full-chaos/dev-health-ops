package teamsidentity

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// The App JWT goes through discoveryAppExchangeClient exactly as discover_credential_prep.go:104 builds it (a following
// client, SSRF-guarded transport); its transport is replaced by the plain one so the probe can be reached. The
// installation-token request must not follow the redirect.
func TestTheDiscoveryAppExchangeClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	credential := providerfoundation.NewCredential("github", "probe", nil, map[string]secrets.Value{
		"app_id": secrets.NewValue("1"), "private_key": secrets.NewValue(pemKey), "installation_id": secrets.NewValue("2")})
	built, ok := discoveryAppExchangeClient.(*http.Client)
	if !ok {
		t.Fatalf("discoveryAppExchangeClient is %T", discoveryAppExchangeClient)
	}
	auth, err := providerfoundation.NewGitHubAppAuth(credential, probe.Base.URL, redirectprobe.Reach(built))
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, probe.Base.URL+"/x", nil)
	_ = auth.Apply(request)
	probe.Assert(t)
}

// Every other discovery call goes through providerfoundation.New*Client with discoveryHTTPClient: the origin guard of
// NewHTTPClient stops the redirect.
func TestTheDiscoveryClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	built, ok := discoveryHTTPClient.(*http.Client)
	if !ok {
		t.Fatalf("discoveryHTTPClient is %T", discoveryHTTPClient)
	}
	client, err := providerfoundation.NewHTTPClient("github", probe.Base.URL, redirectprobe.Reach(built),
		providerfoundation.TokenAuth("Authorization", "token ", secrets.NewValue("SECRET")),
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if response, err := client.Do(context.Background(), http.MethodGet, "/user", nil); err == nil && response != nil {
		response.Body.Close()
	}
	probe.Assert(t)
}
