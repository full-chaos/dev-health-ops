package providerfoundation

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

const secondaryLimitBody = `{"message":"You have exceeded a secondary rate limit. Please wait a few minutes."}`

// CHAOS-10071: a GitHub secondary limit whose only signal is the body message
// must be rate_limited with its dimension in the App token exchange.
func TestGitHubAppInstallationTokenReadsBodyForSecondaryLimit(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	credential := NewCredential("github", "id", nil, map[string]secrets.Value{"app_id": secrets.NewValue("1"), "private_key": secrets.NewValue(pemKey), "installation_id": secrets.NewValue("2")})
	doer := HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusForbidden, nil, secondaryLimitBody), nil
	})
	auth, err := NewGitHubAppAuth(credential, "https://api.github.com", fakehttp.Client(doer))
	if err != nil {
		t.Fatal(err)
	}
	_, err = auth.installationToken(context.Background())
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("err = %v, want a ProviderError", err)
	}
	if providerErr.Class != ErrorRateLimited || providerErr.Dimension != "secondary_abuse_risk" || providerErr.StatusCode != http.StatusForbidden {
		t.Fatalf("class=%q dimension=%q status=%d, want rate_limited/secondary_abuse_risk/403", providerErr.Class, providerErr.Dimension, providerErr.StatusCode)
	}
}

// CHAOS-10071: the request metric of DoUnauthenticated records the body-only
// limit as rate_limited, and the caller still reads the whole body.
func TestDoUnauthenticatedMetricReadsBodyForSecondaryLimit(t *testing.T) {
	t.Parallel()
	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusForbidden, nil, secondaryLimitBody), nil
	}), RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})
	client.Metrics = NewMetrics()

	response, err := client.DoUnauthenticated(context.Background(), http.MethodGet, "https://objects.example.test/artifact")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != secondaryLimitBody {
		t.Fatalf("body = %q err = %v, want the whole body restored", body, err)
	}
	var rendered bytes.Buffer
	if err := client.Metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	if want := `dev_health_provider_requests_total{provider="github",class="rate_limited"} 1`; !strings.Contains(rendered.String(), want) {
		t.Fatalf("missing %q in:\n%s", want, rendered.String())
	}
	if strings.Contains(rendered.String(), `class="authentication"`) {
		t.Fatalf("body-only limit still recorded as authentication:\n%s", rendered.String())
	}
}
