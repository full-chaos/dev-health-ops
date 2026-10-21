package providerfoundation

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/egressguard"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// countingTransport counts round trips; as an httpguard.TransportWrapper the egress guard reaches the transport inside.
type countingTransport struct {
	inner http.RoundTripper
	trips *atomic.Int32
}

func (c countingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	c.trips.Add(1)
	if c.inner == nil { // what a client with no transport would dial through, unguarded
		return httpguard.SharedTransport().RoundTrip(request)
	}
	return c.inner.RoundTrip(request)
}
func (c countingTransport) UnwrapTransport() http.RoundTripper { return c.inner }
func (c countingTransport) RewrapTransport(inner http.RoundTripper) http.RoundTripper {
	return countingTransport{inner: inner, trips: c.trips}
}

// opaqueTransport sends the request with no dial the guard can see (it stands for a decorator that is not a
// TransportWrapper): the guard must refuse it rather than let it through.
type opaqueTransport struct{ trips *atomic.Int32 }

var opaqueCalls atomic.Int32

func (o opaqueTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	o.trips.Add(1)
	return httpguard.SharedTransport().RoundTrip(request)
}

// A stored provider base URL that is, or resolves to, a non-public address is refused when the sync calls it, for
// every provider family: the server is never reached, the error is the permanent egress_refused class, and the
// request is not retried.
func TestStoredProviderURLThatIsNotPublicIsRefusedForEveryFamily(t *testing.T) {
	t.Cleanup(egressguard.SetAllowForTest())
	t.Cleanup(egressguard.SetUnguardedTransportsForTest(false))
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer server.Close()
	byName := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	appCredential, err := githubAppCredential(t).WithEphemeralSecret("base_url", secrets.NewValue(byName))
	if err != nil {
		t.Fatal(err)
	}
	families := []struct {
		name       string
		credential Credential
		newClient  func(Credential, HTTPDoer, RetryPolicy, LeaseGuard) (*HTTPClient, error)
		path       string
	}{
		{"github token", testCredential("github", map[string]string{"token": "github-token", "base_url": server.URL}), NewGitHubClient, "/repos/acme/api"},
		{"github app (installation token exchange)", appCredential, NewGitHubClient, "/repos/acme/api"},
		{"gitlab", testCredential("gitlab", map[string]string{"token": "gitlab-token", "gitlab_url": byName}), NewGitLabClient, "/api/v4/projects/1"},
		{"jira", testCredential("jira", map[string]string{"email": "dev@acme.test", "api_token": "jira-token", "base_url": server.URL}), NewJiraClient, "/rest/api/3/myself"},
		{"linear", testCredential("linear", map[string]string{"api_key": "linear-key", "base_url": byName}), NewLinearClient, "/graphql"},
		{"launchdarkly", testCredential("launchdarkly", map[string]string{"api_key": "ld-key", "base_url": server.URL}), NewLaunchDarklyClient, "/api/v2/flags"},
		{"pagerduty", testCredential("pagerduty", map[string]string{"auth_mode": "api_token", "api_token": "pd-key", "region": "us", "base_url": byName}), NewPagerDutyClient, "/services"},
	}
	doers := map[string]func(*atomic.Int32) *http.Client{
		"a client with no transport": func(trips *atomic.Int32) *http.Client {
			return &http.Client{Timeout: 5 * time.Second, Transport: countingTransport{inner: nil, trips: trips}}
		},
		"httpguard.NewClient": func(trips *atomic.Int32) *http.Client {
			client := httpguard.NewClient(5 * time.Second)
			client.Transport = countingTransport{inner: client.Transport, trips: trips}
			return client
		},
		"a custom transport the guard cannot reach into": func(trips *atomic.Int32) *http.Client {
			return &http.Client{Timeout: 5 * time.Second, Transport: countingTransport{inner: opaqueTransport{trips: &opaqueCalls}, trips: trips}}
		},
	}
	retry := RetryPolicy{MaxAttempts: 3, InitialWait: time.Millisecond, MaxWait: time.Millisecond}
	lease := LeaseGuardFunc(func(context.Context) error { return nil })
	for _, family := range families {
		for doerName, newDoer := range doers {
			t.Run(family.name+"/"+doerName, func(t *testing.T) {
				var trips atomic.Int32
				client, err := family.newClient(family.credential, newDoer(&trips), retry, lease)
				if err != nil {
					t.Fatal(err)
				}
				response, err := client.Do(context.Background(), http.MethodGet, family.path, nil)
				if err == nil {
					response.Body.Close()
					t.Fatal("the call to a loopback stored URL succeeded")
				}
				var providerErr *ProviderError
				if !IsEgressRefused(err) || !errors.As(err, &providerErr) || providerErr.Class != ErrorPermanent || providerErr.Reason != TransportReasonEgressRefused || providerErr.Retryable() {
					t.Fatalf("err = %v, want the permanent egress_refused class", err)
				}
				if trips.Load() != 1 {
					t.Fatalf("%d round trips, want exactly 1 (no retry of a refused address)", trips.Load())
				}
				if hits.Load() != 0 {
					t.Fatalf("the server received %d requests", hits.Load())
				}
				if opaqueCalls.Load() != 0 {
					t.Fatalf("the transport the guard cannot reach into was called %d times", opaqueCalls.Load())
				}
			})
		}
	}
}

// The allow list keeps local fakes and private self-hosted instances working where an operator lists them.
func TestStoredProviderURLInsideTheAllowListIsCalled(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer server.Close()
	t.Cleanup(egressguard.SetAllowForTest())
	restore := egressguard.SetAllowForTest(netip.MustParsePrefix("127.0.0.0/8"))
	defer restore()
	retry := RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond}
	client, err := NewGitLabClient(testCredential("gitlab", map[string]string{"token": "gitlab-token", "gitlab_url": server.URL}),
		httpguard.NewClient(5*time.Second), retry, LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(context.Background(), http.MethodGet, "/api/v4/projects/1", nil)
	if err != nil {
		t.Fatalf("an allowed address was refused: %v", err)
	}
	response.Body.Close()
	if hits.Load() != 1 {
		t.Fatalf("hits = %d, want 1", hits.Load())
	}
}

// EgressGuardedDoer guards every doer shape a constructor admits and leaves the caller's client unchanged.
func TestEgressGuardedDoerShapes(t *testing.T) {
	supplied := &http.Client{Timeout: time.Second}
	guarded, ok := EgressGuardedDoer(supplied).(*http.Client)
	if !ok || guarded == supplied || guarded.Transport == nil || supplied.Transport != nil {
		t.Fatalf("guarded=%v: want a copy with a transport and the supplied client unchanged", guarded)
	}
	if guarded.Transport != egressguard.Wrap(nil) {
		t.Fatal("a client with no transport must get the shared guarded transport")
	}
	var nilClient *http.Client
	for name, doer := range map[string]HTTPDoer{
		"a nil client": nilClient, "a doer the guard cannot reach into": doerFunc(func(*http.Request) (*http.Response, error) { return nil, nil }),
		"a Wrapper whose rebuild returns nil": nilRewrapper{inner: &http.Client{}}, "a Wrapper whose rebuild returns a typed nil": typedNilRewrapper{inner: &http.Client{}},
	} {
		guardedDoer := EgressGuardedDoer(doer)
		if _, err := guardedDoer.Do(httptest.NewRequest(http.MethodGet, "http://127.0.0.1/", nil)); !IsEgressRefused(err) {
			t.Errorf("%s: Do = %v, want the egress refusal (fail closed)", name, err)
		}
	}
	for name, doer := range map[string]HTTPDoer{
		"nil": nilRewrapper{inner: &http.Client{}}, "typed nil": typedNilRewrapper{inner: &http.Client{}},
	} {
		if _, err := originGuardedAtConstruction(doer).Do(httptest.NewRequest(http.MethodGet, "http://127.0.0.1/", nil)); !IsEgressRefused(err) {
			t.Errorf("originGuardedAtConstruction with a rebuild that returns %s: Do = %v, want the egress refusal", name, err)
		}
	}
}

func TestEgressRefusedIsPermanentAndKeepsItsReason(t *testing.T) {
	err := transportProviderError("gitlab", &egressguard.RefusedError{Class: "private"}, 1)
	if err.Class != ErrorPermanent || err.Reason != TransportReasonEgressRefused || err.Retryable() || !IsEgressRefused(err) {
		t.Fatalf("err = %+v, want permanent egress_refused", err)
	}
	if strings.Contains(err.Error(), "private") {
		t.Fatalf("the error text must carry the reason only: %q", err.Error())
	}
	if IsEgressRefused(errors.New("dial tcp: connection refused")) {
		t.Fatal("an ordinary transport error is not an egress refusal")
	}
}

// nilRewrapper and typedNilRewrapper are decorators whose rebuild fails: the guard must not fall back to the unguarded doer.
type nilRewrapper struct{ inner HTTPDoer }

func (w nilRewrapper) Do(request *http.Request) (*http.Response, error) { return w.inner.Do(request) }
func (w nilRewrapper) Unwrap() interface {
	Do(*http.Request) (*http.Response, error)
} {
	return w.inner
}
func (w nilRewrapper) Rewrap(interface {
	Do(*http.Request) (*http.Response, error)
}) interface {
	Do(*http.Request) (*http.Response, error)
} {
	return nil
}

type typedNilRewrapper struct{ inner HTTPDoer }

func (w typedNilRewrapper) Do(request *http.Request) (*http.Response, error) {
	return w.inner.Do(request)
}
func (w typedNilRewrapper) Unwrap() interface {
	Do(*http.Request) (*http.Response, error)
} {
	return w.inner
}
func (w typedNilRewrapper) Rewrap(interface {
	Do(*http.Request) (*http.Response, error)
}) interface {
	Do(*http.Request) (*http.Response, error)
} {
	var client *http.Client
	return client
}
