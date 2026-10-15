package providerfoundation

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type errDoer struct{ err error }

func (d errDoer) Do(*http.Request) (*http.Response, error) { return nil, d.err }

func transportClient(t *testing.T, doer fakehttp.Doer, base string) *HTTPClient {
	t.Helper()
	client, err := NewHTTPClient("github", base, fakehttp.Client(doer), func(*http.Request) error { return nil },
		RetryPolicy{MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var out bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&out, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &out
}

// A client that does not trust the server's CA (the go-api-tools image without a CA bundle)
// must surface tls_unknown_authority, not a bare transient error.
func TestTransportUnknownAuthorityReachesCaller(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	logs := captureLogs(t)
	client := transportClient(t, &http.Client{}, server.URL)
	_, err := client.Do(context.Background(), http.MethodGet, "/repos/octo/hello/deployments?token=secret-query", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("err = %v, want *ProviderError", err)
	}
	if providerErr.Class != ErrorTransient || providerErr.Reason != TransportReasonTLSUnknownAuthority {
		t.Fatalf("class=%q reason=%q, want transient/%s", providerErr.Class, providerErr.Reason, TransportReasonTLSUnknownAuthority)
	}
	if !strings.Contains(err.Error(), "reason=tls_unknown_authority") || !strings.Contains(err.Error(), "path=/repos/octo/hello/deployments") {
		t.Fatalf("Error() = %q lacks reason or path", err.Error())
	}
	if !strings.Contains(logs.String(), `"reason":"tls_unknown_authority"`) {
		t.Fatalf("no log line with the reason: %s", logs.String())
	}
	if strings.Contains(err.Error(), "secret-query") || strings.Contains(logs.String(), "secret-query") {
		t.Fatal("query string leaked")
	}
	if strings.Contains(logs.String(), "octo/hello") {
		t.Fatalf("path in log line: %s", logs.String())
	}
}

func TestTransportReasonNeverCarriesCauseText(t *testing.T) {
	cause := errors.New(`Get "https://user:hunter2@example.test/x": dial failed`)
	logs := captureLogs(t)
	client := transportClient(t, errDoer{cause}, "https://example.test")
	_, err := client.Do(context.Background(), http.MethodGet, "/x", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Reason != TransportReasonOther {
		t.Fatalf("err = %v, want reason other", err)
	}
	if strings.Contains(err.Error(), "hunter2") || strings.Contains(logs.String(), "hunter2") {
		t.Fatalf("userinfo leaked: %q / %s", err.Error(), logs.String())
	}
}

func TestClassifyTransportReason(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"unknown authority": {x509.UnknownAuthorityError{}, TransportReasonTLSUnknownAuthority},
		"cert invalid":      {x509.CertificateInvalidError{Reason: x509.Expired}, TransportReasonTLSCertInvalid},
		"refused":           {syscall.ECONNREFUSED, TransportReasonConnRefused},
		"deadline":          {context.DeadlineExceeded, TransportReasonTimeout},
		"canceled":          {context.Canceled, TransportReasonCanceled},
		"other":             {errors.New("boom"), TransportReasonOther},
	} {
		if got := classifyTransportReason(tc.err); got != tc.want {
			t.Errorf("%s: got %q want %q", name, got, tc.want)
		}
	}
}
