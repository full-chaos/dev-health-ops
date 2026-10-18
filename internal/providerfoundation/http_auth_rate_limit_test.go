package providerfoundation

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-10095: a rate-limited GitHub App token exchange runs inside c.Auth. It
// must still record the request metric and arm the shared cooldown gate.
func TestHTTPClientAppTokenExchangeRateLimitObservedAndPenalizesGate(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	credential := NewCredential("github", "id", nil, map[string]secrets.Value{"app_id": secrets.NewValue("1"), "private_key": secrets.NewValue(pemKey), "installation_id": secrets.NewValue("2")})
	tokenDoer := HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusForbidden, http.Header{"Retry-After": []string{"60"}}, `{}`), nil
	})
	auth, err := NewGitHubAppAuth(credential, "https://api.github.com", fakehttp.Client(tokenDoer))
	if err != nil {
		t.Fatal(err)
	}
	client := newTestHTTPClient(t, fakehttp.Client(HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		t.Error("provider request sent despite a failed token exchange")
		return testHTTPResponse(request, http.StatusOK, nil, `{}`), nil
	})), RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})
	client.Auth = auth.Apply
	gate := &recordingBackoffGate{}
	client.Gate = gate
	client.Metrics = NewMetrics()

	_, err = client.Do(context.Background(), http.MethodGet, "/items", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited || providerErr.StatusCode != http.StatusForbidden {
		t.Fatalf("err=%v, want rate_limited 403", err)
	}
	if gate.penalty < 59*time.Second || gate.penalty > 60*time.Second {
		t.Fatalf("gate penalty=%s, want the 60s Retry-After", gate.penalty)
	}
	var output bytes.Buffer
	if err := client.Metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `dev_health_provider_requests_total{provider="github",class="rate_limited"} 1`) {
		t.Fatalf("no rate_limited request sample:\n%s", output.String())
	}
}
