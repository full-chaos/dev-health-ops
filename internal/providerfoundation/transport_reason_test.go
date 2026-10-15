package providerfoundation

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
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

// newQuietTLSServer is httptest.NewTLSServer with its error log captured. The "TLS handshake error"
// lines a rejecting client provokes are expected and dropped; any other server error (a handler panic,
// an accept error) fails the test when it ends.
func newQuietTLSServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	server, errs := startCapturedTLSServer(handler)
	t.Cleanup(func() {
		server.Close()
		failUnexpectedServerErrors(t, errs.String())
	})
	return server
}

func startCapturedTLSServer(handler http.Handler) (*httptest.Server, *lockedBuffer) {
	errs := &lockedBuffer{}
	server := httptest.NewUnstartedServer(handler)
	server.Config.ErrorLog = log.New(errs, "", 0)
	server.StartTLS()
	return server, errs
}

type failureRecorder interface {
	Helper()
	Errorf(format string, args ...any)
}

func failUnexpectedServerErrors(t failureRecorder, logged string) {
	t.Helper()
	for _, line := range strings.Split(logged, "\n") {
		if line == "" || strings.Contains(line, "TLS handshake error") {
			continue
		}
		t.Errorf("unexpected server error: %s", line)
	}
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
	server := newQuietTLSServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
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

type recordedFailure struct{ failed bool }

func (r *recordedFailure) Helper()               {}
func (r *recordedFailure) Errorf(string, ...any) { r.failed = true }

func TestQuietTLSServerReportsAHandlerPanic(t *testing.T) {
	server, errs := startCapturedTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
	defer server.Close()
	_, _ = server.Client().Get(server.URL)
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(errs.String(), "http: panic serving") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	var failure recordedFailure
	failUnexpectedServerErrors(&failure, errs.String())
	if !failure.failed {
		t.Fatalf("a handler panic was not reported as an unexpected server error: %s", errs.String())
	}
}

func TestQuietTLSServerDropsOnlyTheHandshakeLines(t *testing.T) {
	var failure recordedFailure
	failUnexpectedServerErrors(&failure, "http: TLS handshake error from 127.0.0.1:1: EOF\n")
	if failure.failed {
		t.Fatal("a TLS handshake error line failed the test")
	}
}
