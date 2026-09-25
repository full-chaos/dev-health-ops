package providerfoundation

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// CHAOS-6752 review r1 P1: a redirect must never carry the provider credential
// off the credential origin, whatever redirect policy the injected Doer has.

type originPair struct {
	origin, destination *httptest.Server
	destinationHits     atomic.Int32
	destinationToken    atomic.Value // string
}

func newOriginPair(t *testing.T, redirectTo func(destinationURL string) string) *originPair {
	t.Helper()
	p := &originPair{}
	p.destinationToken.Store("")
	p.destination = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.destinationHits.Add(1)
		p.destinationToken.Store(r.Header.Get("PRIVATE-TOKEN"))
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(p.destination.Close)
	p.origin = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/c/ok/same" {
			_, _ = io.WriteString(w, `[]`)
			return
		}
		http.Redirect(w, r, redirectTo(p.destination.URL), http.StatusFound)
	}))
	t.Cleanup(p.origin.Close)
	return p
}

func tokenClient(t *testing.T, base string, doer HTTPDoer) *HTTPClient {
	t.Helper()
	client, err := NewHTTPClient("gitlab", base, doer,
		func(r *http.Request) error { r.Header.Set("PRIVATE-TOKEN", "review-secret"); return nil },
		RetryPolicy{MaxAttempts: 3, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestDoNeverForwardsTheCredentialToAnotherOrigin(t *testing.T) {
	permissive := func(*http.Request, []*http.Request) error { return nil }
	for name, doer := range map[string]HTTPDoer{
		"stock http.Client":                    &http.Client{},
		"http.Client with a permissive policy": &http.Client{CheckRedirect: permissive},
	} {
		t.Run(name, func(t *testing.T) {
			p := newOriginPair(t, func(d string) string { return d + "/captured" })
			response, err := tokenClient(t, p.origin.URL+"/c/ok", doer).Do(context.Background(), http.MethodGet, "/api/v4/projects", nil)
			if !errors.Is(err, ErrCredentialInvalid) || response != nil {
				t.Fatalf("Do = (%v, %v), want (nil, ErrCredentialInvalid)", response, err)
			}
			if hits := p.destinationHits.Load(); hits != 0 {
				t.Fatalf("redirect origin got %d request(s), token %q", hits, p.destinationToken.Load())
			}
		})
	}
}

func TestDoRefusesADoerThatFollowedARedirectOffOrigin(t *testing.T) {
	body := &closeTracker{Reader: strings.NewReader("[]")}
	doer := doerFunc(func(request *http.Request) (*http.Response, error) {
		other, _ := http.NewRequest(http.MethodGet, "https://elsewhere.example.com/captured", nil)
		other.Response = &http.Response{StatusCode: http.StatusFound} // what net/http sets on a request a redirect created
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: body, Request: other}, nil
	})
	response, err := baseURLClient(t, "https://gitlab.example.com/gitlab", doer).Do(context.Background(), http.MethodGet, "/api/v4/projects", nil)
	if !errors.Is(err, ErrCredentialInvalid) || response != nil {
		t.Fatalf("Do = (%v, %v), want (nil, ErrCredentialInvalid)", response, err)
	}
	if !body.closed {
		t.Fatal("refused response body was not closed")
	}
}

func TestDoStillFollowsSameOriginRedirects(t *testing.T) {
	var innerCalls int
	doer := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { innerCalls++; return nil }}
	p := newOriginPair(t, func(string) string { return "/c/ok/same" })
	response, err := tokenClient(t, p.origin.URL+"/c/ok", doer).Do(context.Background(), http.MethodGet, "/api/v4/projects", nil)
	if err != nil {
		t.Fatalf("same-origin redirect refused: %v", err)
	}
	_ = response.Body.Close()
	if innerCalls != 1 {
		t.Fatalf("the Doer's own redirect policy ran %d times, want 1", innerCalls)
	}
	if doer.CheckRedirect == nil {
		t.Fatal("injected http.Client was mutated")
	}
}

func TestDoLeavesTheInjectedClientUntouched(t *testing.T) {
	doer := &http.Client{}
	p := newOriginPair(t, func(d string) string { return d + "/captured" })
	_, _ = tokenClient(t, p.origin.URL+"/c/ok", doer).Do(context.Background(), http.MethodGet, "/x", nil)
	if doer.CheckRedirect != nil {
		t.Fatal("Do installed a CheckRedirect on the caller's http.Client")
	}
}

type doerFunc func(*http.Request) (*http.Response, error)

func (f doerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

type closeTracker struct {
	io.Reader
	closed bool
}

func (c *closeTracker) Close() error { c.closed = true; return nil }

// CHAOS-6778: a transport that rewrites the request's host (a proxy, the venue
// oracles' stub) is not a redirect and must not be refused.
func TestDoAcceptsATransportThatRewritesTheHost(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `[]`) }))
	defer stub.Close()
	target, _ := url.Parse(stub.URL)
	doer := &http.Client{Transport: rewriteHostTo{target}}
	response, err := baseURLClient(t, "https://api.github.com", doer).Do(context.Background(), http.MethodGet, "/user/repos", nil)
	if err != nil {
		t.Fatalf("a host-rewriting transport was refused: %v", err)
	}
	_ = response.Body.Close()
	// The same through a Doer that is not an *http.Client: only response.Request is seen.
	wrapped := doerFunc(func(r *http.Request) (*http.Response, error) { return doer.Do(r) })
	response, err = baseURLClient(t, "https://api.github.com", wrapped).Do(context.Background(), http.MethodGet, "/user/repos", nil)
	if err != nil {
		t.Fatalf("a wrapped host-rewriting transport was refused: %v", err)
	}
	_ = response.Body.Close()
}

type rewriteHostTo struct{ target *url.URL }

func (t rewriteHostTo) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.URL.Scheme, clone.URL.Host, clone.Host = t.target.Scheme, t.target.Host, t.target.Host
	return http.DefaultTransport.RoundTrip(clone)
}
