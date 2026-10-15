package providerfoundation

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func TestPagerDutyExchangeTransportFailureKeepsReason(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	config := PagerDutyRevokeConfig{ClientID: "c", ClientSecret: "hunter2", TokenURL: server.URL + "/oauth/token"}
	for name, call := range map[string]func() error{
		"code": func() error {
			_, err := ExchangePagerDutyAuthorizationCode(context.Background(), &http.Client{}, config, "code", "verifier", time.Now())
			return err
		},
		"client credentials": func() error {
			_, err := RequestPagerDutyClientCredentialsToken(context.Background(), &http.Client{}, config, "acme", "us", time.Now())
			return err
		},
	} {
		err := call()
		var transport *PagerDutyExchangeTransportError
		if !errors.Is(err, ErrPagerDutyExchangeUnavailable) || !errors.As(err, &transport) || transport.Reason != TransportReasonTLSUnknownAuthority {
			t.Fatalf("%s: err = %v, want unavailable with tls_unknown_authority", name, err)
		}
		if err.Error() != ErrPagerDutyExchangeUnavailable.Error() {
			t.Fatalf("%s: Error() changed: %q", name, err.Error())
		}
	}
}

func TestPagerDutyValidationTransportFailureKeepsReason(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	_, err := ValidatePagerDutyCredential(context.Background(), &http.Client{},
		PagerDutyRevokeConfig{APIBaseOverride: server.URL},
		PagerDutyCredentialCandidate{AuthMode: "api_token", APIToken: "tok-secret", Region: "us"}, PagerDutyReadScopeSet())
	var validation *PagerDutyValidationError
	if !errors.As(err, &validation) || validation.Code != "live_read_failed" || validation.Reason != TransportReasonTLSUnknownAuthority {
		t.Fatalf("err = %v, want live_read_failed/tls_unknown_authority", err)
	}
	if err.Error() != "live_read_failed" {
		t.Fatalf("Error() changed: %q", err.Error())
	}
}

func TestGitHubAppInstallationTokenTransportFailureKeepsReason(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	credential := NewCredential("github", "id", nil, map[string]secrets.Value{"app_id": secrets.NewValue("1"), "private_key": secrets.NewValue(pemKey), "installation_id": secrets.NewValue("2")})
	auth, err := NewGitHubAppAuth(credential, "https://api.github.com", fakehttp.Client(errDoer{x509.UnknownAuthorityError{}}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = auth.installationToken(context.Background())
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Reason != TransportReasonTLSUnknownAuthority || providerErr.Class != ErrorTransient {
		t.Fatalf("err = %v, want transient/tls_unknown_authority", err)
	}
}

type truncatedBodyDoer struct{}

func (truncatedBodyDoer) Do(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(io.MultiReader(strings.NewReader("{"), errReader{io.ErrUnexpectedEOF}))}, nil
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

func TestPagerDutyBodyReadFailureKeepsReason(t *testing.T) {
	config := PagerDutyRevokeConfig{ClientID: "c", TokenURL: "https://pd.example.test/oauth/token"}
	for name, err := range map[string]error{
		"code": func() error {
			_, err := ExchangePagerDutyAuthorizationCode(context.Background(), truncatedBodyDoer{}, config, "code", "verifier", time.Now())
			return err
		}(),
		"client credentials": func() error {
			_, err := RequestPagerDutyClientCredentialsToken(context.Background(), truncatedBodyDoer{}, config, "acme", "us", time.Now())
			return err
		}(),
	} {
		var transport *PagerDutyExchangeTransportError
		if !errors.Is(err, ErrPagerDutyExchangeUnavailable) || !errors.As(err, &transport) || transport.Reason != TransportReasonOther {
			t.Fatalf("%s: err = %v, want unavailable with a reason", name, err)
		}
	}
	_, err := ValidatePagerDutyCredential(context.Background(), truncatedBodyDoer{},
		PagerDutyRevokeConfig{APIBaseOverride: "https://pd.example.test"},
		PagerDutyCredentialCandidate{AuthMode: "api_token", APIToken: "tok", Region: "us"}, PagerDutyReadScopeSet())
	var validation *PagerDutyValidationError
	if !errors.As(err, &validation) || validation.Code != "live_read_failed" || validation.Reason != TransportReasonOther {
		t.Fatalf("validation err = %v", err)
	}
}
