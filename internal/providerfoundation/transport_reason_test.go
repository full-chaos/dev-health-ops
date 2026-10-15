package providerfoundation

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
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

// lockedBuffer is a bytes.Buffer safe for the test goroutine to read while
// another goroutine (an httptest server's logger) writes through slog.
type lockedBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

// newQuietTLSServer is httptest.NewTLSServer whose own error log (the
// "TLS handshake error" line a rejecting client provokes) is discarded, so it
// never reaches the slog default a test captures.
func newQuietTLSServer(handler http.Handler) *httptest.Server {
	server := httptest.NewUnstartedServer(handler)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	return server
}

func captureLogs(t *testing.T) *lockedBuffer {
	t.Helper()
	out := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(out, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return out
}

// A client that does not trust the server's CA (the go-api-tools image without a CA bundle)
// must surface tls_unknown_authority, not a bare transient error.
func TestTransportUnknownAuthorityReachesCaller(t *testing.T) {
	server := newQuietTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	logs := captureLogs(t)
	client := transportClient(t, &http.Client{}, server.URL)
	_, err := client.Do(context.Background(), http.MethodGet, "/orgs/acme/teams/platform/memberships/octocat-login?token=secret-query", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("err = %v, want *ProviderError", err)
	}
	if providerErr.Class != ErrorTransient || providerErr.Reason != TransportReasonTLSUnknownAuthority {
		t.Fatalf("class=%q reason=%q, want transient/%s", providerErr.Class, providerErr.Reason, TransportReasonTLSUnknownAuthority)
	}
	if !strings.Contains(err.Error(), "reason=tls_unknown_authority") {
		t.Fatalf("Error() = %q lacks the reason", err.Error())
	}
	if !strings.Contains(logs.String(), `"reason":"tls_unknown_authority"`) {
		t.Fatalf("no log line with the reason: %s", logs.String())
	}
	if strings.Contains(err.Error(), "secret-query") || strings.Contains(logs.String(), "secret-query") {
		t.Fatal("query string leaked")
	}
	if strings.Contains(logs.String(), "octocat-login") || strings.Contains(err.Error(), "octocat-login") || providerErr.Path != "" {
		t.Fatalf("request path reached the error or log: %q / %s", err.Error(), logs.String())
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

func TestAuthHelpersSurfaceTransportReason(t *testing.T) {
	cause := x509.UnknownAuthorityError{}
	want := func(name string, err error) {
		t.Helper()
		var providerErr *ProviderError
		if !errors.As(err, &providerErr) || providerErr.Reason != TransportReasonTLSUnknownAuthority || providerErr.Class != ErrorTransient {
			t.Fatalf("%s: %v", name, err)
		}
	}
	credential := NewCredential("pagerduty", "id", map[string]string{"auth_mode": "client_credentials"}, map[string]secrets.Value{
		"client_id": secrets.NewValue("cid"), "client_secret": secrets.NewValue("csecret"), "subdomain": secrets.NewValue("acme")})
	auth, err := NewPagerDutyClientCredentialsAuth(credential, fakehttp.Client(errDoer{cause}))
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodGet, "https://api.pagerduty.com/x", nil)
	want("pagerduty client credentials", auth.Apply(request))
	want("pagerduty revoke", RevokePagerDutyOAuthToken(context.Background(), fakehttp.Client(errDoer{cause}), PagerDutyRevokeConfig{ClientID: "cid"}, "tok"))
	want("github app", transportProviderError("github", cause, 1))
}

// A goroutine the test does not own (an httptest server logging through the
// std log, which slog.SetDefault redirects) writes to the captured buffer
// while the test reads it: run under -race.
func TestCaptureLogsIsSafeWhileAnotherGoroutineLogs(t *testing.T) {
	logs := captureLogs(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			slog.Warn("background line", "n", i)
		}
	}()
	for i := 0; i < 200; i++ {
		_ = logs.String()
	}
	<-done
	if !strings.Contains(logs.String(), `"n":199`) {
		t.Fatal("background lines missing")
	}
}
