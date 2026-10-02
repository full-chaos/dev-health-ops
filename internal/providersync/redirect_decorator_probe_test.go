package providersync

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A request-counting decorator around a supplied client of the default redirect policy still never carries the credential
// to another origin: the provider client's origin guard looks through the decorator (CHAOS-7910 r1 P1).
func TestACountingDecoratorNeverHidesAFollowingClientFromTheOriginGuard(t *testing.T) {
	probe := redirectprobe.New(t)
	client, err := providerfoundation.NewHTTPClient("gitlab", probe.Base.URL, CountRequests(probe.Client()),
		providerfoundation.TokenAuth("PRIVATE-TOKEN", "", secrets.NewValue("SECRET")),
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

// probeDecorator sends one request through the production NewHTTPClient, with a supplied following client and the decorator
// built around client.Doer the way a route builds it: the other origin must see no request.
func probeDecorator(t *testing.T, decorate func(providerfoundation.HTTPDoer, *int) providerfoundation.HTTPDoer) {
	t.Helper()
	probe := redirectprobe.New(t)
	client, err := providerfoundation.NewHTTPClient("gitlab", probe.Base.URL, probe.Client(),
		providerfoundation.TokenAuth("PRIVATE-TOKEN", "", secrets.NewValue("SECRET")),
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	counted := *client
	counted.Doer = decorate(client.Doer, &requests)
	if response, err := counted.Do(context.Background(), http.MethodGet, "/user", nil); err == nil && response != nil {
		response.Body.Close()
	}
	probe.Assert(t)
}

// contextDroppingDoer is a decorator that drops the request context (it sends a copy of the request with a fresh one): the
// guard loses the credential origin the HTTPClient put there (D4219, plant H2).
type contextDroppingDoer struct{ delegate providerfoundation.HTTPDoer }

func (d contextDroppingDoer) Do(request *http.Request) (*http.Response, error) {
	return d.delegate.Do(request.Clone(context.Background()))
}

// With the origin lost, the guard fails closed: the redirect is followed (presigned storage keeps working) and the second host
// sees NO credential header.
func TestAContextDroppingDecoratorStillNeverSendsTheCredentialToAnotherOrigin(t *testing.T) {
	var credentialAtOther string
	hits := 0
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		credentialAtOther = r.Header.Get("PRIVATE-TOKEN") + r.Header.Get("Authorization")
		_, _ = w.Write([]byte("artifact"))
	}))
	defer other.Close()
	base := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/artifact.zip", http.StatusFound)
	}))
	defer base.Close()
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(previous)
	client, err := providerfoundation.NewHTTPClient("gitlab", base.URL, &http.Client{},
		providerfoundation.TokenAuth("PRIVATE-TOKEN", "", secrets.NewValue("SECRET")),
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	dropping := *client
	dropping.Doer = contextDroppingDoer{delegate: client.Doer}
	response, err := dropping.Do(context.Background(), http.MethodGet, "/jobs/1/artifacts", nil)
	if err != nil {
		t.Fatalf("the redirect to presigned storage must keep working: %v", err)
	}
	response.Body.Close()
	if hits != 1 {
		t.Fatalf("the redirect was not followed: the second host saw %d request(s)", hits)
	}
	if credentialAtOther != "" {
		t.Fatalf("a credential header reached the second host")
	}
	if got := logged.String(); strings.Count(got, "provider redirect without a credential origin in the request context: credential headers stripped") != 1 || strings.Contains(got, "SECRET") || strings.Contains(got, other.URL) {
		t.Fatalf("want ONE warn line with the fixed text and no URL or value, got %q", got)
	}
}

// The production construction path: CountRequests around a client (what CompleteRouteExecutor.Execute hands the constructors) is
// accepted; every derived route decorator, handed to a constructor instead of being assigned onto a built client, is refused.
func TestTheProductionConstructionPathIsAcceptedAndEveryRouteDecoratorHandedInIsRefused(t *testing.T) {
	newClient := func(doer providerfoundation.HTTPDoer) error {
		_, err := providerfoundation.NewHTTPClient("gitlab", "https://gitlab.example.test", doer,
			providerfoundation.TokenAuth("PRIVATE-TOKEN", "", secrets.NewValue("SECRET")),
			providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
			providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
		return err
	}
	if err := newClient(CountRequests(&http.Client{})); err != nil {
		t.Fatalf("CountRequests around a client must construct: %v", err)
	}
	if err := newClient(CountRequests(CountRequests(&http.Client{}))); err != nil {
		t.Fatalf("nested counters around a client must construct: %v", err)
	}
	for name, decorate := range generatedDecorators {
		if err := newClient(decorate(&http.Client{}, new(int))); err == nil {
			t.Errorf("%s handed to a constructor must be refused", name)
		}
	}
}
