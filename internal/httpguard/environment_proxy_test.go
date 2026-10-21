package httpguard

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func sendCanary(ctx context.Context, client *http.Client) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxycapture.Target, nil)
	if err != nil {
		panic(err)
	}
	request.Header.Set("Authorization", "Bearer "+proxycapture.Canary)
	if response, err := client.Do(request); err == nil {
		_ = response.Body.Close()
	}
}

func TestNewClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) { sendCanary(ctx, NewClient(5*time.Second)) }).AssertNothingReached(t)
}

func TestTransportAndSharedTransportNeverDialTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		sendCanary(ctx, &http.Client{Transport: Transport()})
		sendCanary(ctx, &http.Client{Transport: SharedTransport()})
	}).AssertNothingReached(t)
}

func TestSharedTransportIsOnePool(t *testing.T) {
	if SharedTransport() != SharedTransport() {
		t.Fatal("SharedTransport returned two transports: every caller would get a pool of its own")
	}
	if Transport() == Transport() {
		t.Fatal("Transport returned the same transport twice: a caller that changes a field would change every client")
	}
	if SharedTransport().Proxy != nil || Transport().Proxy != nil {
		t.Fatal("a transport of this package has a Proxy function")
	}
}

// A supplied client with no Transport would dial through http.DefaultTransport; NoRedirects (and every seam that guards
// a supplied client with it) gives the copy SharedTransport instead.
func TestNoRedirectsGivesASuppliedClientWithoutATransportNoEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		sendCanary(ctx, NoRedirects(&http.Client{Timeout: 5 * time.Second}))
		sendCanary(ctx, NoRedirectsDoer(&http.Client{Timeout: 5 * time.Second}))
	}).AssertNothingReached(t)
	supplied := &http.Client{}
	if NoRedirects(supplied).Transport == nil || supplied.Transport != nil {
		t.Fatal("NoRedirects must give its copy a transport and leave the supplied client unchanged")
	}
}

type replacedDefaultTransport struct{}

func (replacedDefaultTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, http.ErrNotSupported
}

// A dependency may replace http.DefaultTransport with another RoundTripper; Transport must still build a no-proxy
// transport instead of panicking on the type assertion.
func TestTransportSurvivesAReplacedDefaultTransport(t *testing.T) {
	original := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = original })
	http.DefaultTransport = replacedDefaultTransport{}
	var logged bytes.Buffer
	previousLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	transport := Transport()
	line := logged.String()
	if strings.Count(line, "\n") != 1 || !strings.Contains(line, "level=WARN") || !strings.Contains(line, "DefaultTransport") || !strings.Contains(line, "httpguard.replacedDefaultTransport") {
		t.Fatalf("the fallback must log one WARN line naming the replaced DefaultTransport type, got %q", line)
	}
	if transport == nil || transport.Proxy != nil || transport.DialContext == nil || transport.MaxIdleConns != 100 {
		t.Fatalf("Transport() over a replaced DefaultTransport = %+v, want net/http's default settings with no proxy", transport)
	}
}
