package githubcode

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A next-page Link on another origin is refused: no request reaches the other host and the error carries no URL,
// userinfo or token (D4037).
func TestCrossOriginLinkIsRefusedAndNothingReachesTheOtherHost(t *testing.T) {
	var mu sync.Mutex
	var received []string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received = append(received, r.Header.Get("Authorization"))
		mu.Unlock()
		_, _ = w.Write([]byte(`[]`))
	}))
	defer other.Close()
	cases := map[string]string{
		"another host":               other.URL + "/orgs/acme/repos?page=2",
		"another host with userinfo": strings.Replace(other.URL, "http://", "http://user:pw@", 1) + "/orgs/acme/repos?page=2",
		"localhost alias":            strings.Replace(other.URL, "127.0.0.1", "localhost", 1) + "/orgs/acme/repos?page=2",
	}
	for name, link := range cases {
		t.Run(name, func(t *testing.T) {
			mu.Lock()
			received = nil
			mu.Unlock()
			base := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Link", "<"+link+`>; rel="next"`)
				_, _ = w.Write([]byte(`[{"id": 1, "name": "a", "full_name": "O/a"}]`))
			}))
			defer base.Close()
			client := Client{Token: "SECRET-TOKEN", BaseURL: base.URL}
			_, err := client.ListRepositories(context.Background(), ListOptions{Org: "acme"})
			var typed *Error
			if !errors.As(err, &typed) || typed.Class != CrossOriginLinkClass {
				t.Fatalf("want an error of class %s, got %v", CrossOriginLinkClass, err)
			}
			for _, secret := range []string{"SECRET-TOKEN", "user", "pw", "127.0.0.1", "localhost", "orgs/acme"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("the error text carries %q: %v", secret, err)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if len(received) != 0 {
				t.Fatalf("the other host received %d request(s): %q", len(received), received)
			}
		})
	}
}

// The same origin is followed whatever its spelling: scheme and host case, an explicit default port, an IDNA host
// written as Unicode or as xn--, and userinfo (not part of an origin).
func TestSameOriginLinkIsFollowedInAnySpelling(t *testing.T) {
	two := page(repoItem(1, "api"))
	for _, tc := range []struct{ name, base, link string }{
		{"explicit default port", "https://ghe.test/api/v3", "https://ghe.test:443/orgs/acme/repos?page=2"},
		{"host case", "https://ghe.test/api/v3", "https://GHE.Test/orgs/acme/repos?page=2"},
		{"scheme case", "https://ghe.test/api/v3", "HTTPS://ghe.test/orgs/acme/repos?page=2"},
		{"default http port", "http://ghe.test", "http://ghe.test:80/orgs/acme/repos?page=2"},
		{"zero-padded default http port", "http://ghe.test", "http://ghe.test:080/orgs/acme/repos?page=2"},
		{"base port named", "https://ghe.test:443/api/v3", "https://ghe.test/orgs/acme/repos?page=2"},
		{"unicode host, xn-- link", "https://ghé.test/api/v3", "https://xn--gh-cja.test/orgs/acme/repos?page=2"},
		{"xn-- base, unicode link", "https://xn--gh-cja.test/api/v3", "https://ghé.test/orgs/acme/repos?page=2"},
		{"relative link", "https://ghe.test/api/v3", "/orgs/acme/repos?page=2"},
		{"IPv6 literal, case", "https://[2001:DB8::1]/api/v3", "https://[2001:db8::1]/orgs/acme/repos?page=2"},
		{"IPv6 literal, expanded", "https://[2001:db8::1]/api/v3", "https://[2001:0db8:0000:0000:0000:0000:0000:0001]/orgs/acme/repos?page=2"},
		{"zero-padded default port", "https://ghe.test/api/v3", "https://ghe.test:0443/orgs/acme/repos?page=2"},
		{"zero-padded explicit port", "https://ghe.test:8443/api/v3", "https://ghe.test:08443/orgs/acme/repos?page=2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &scriptedTransport{responses: []scripted{ok(two, next(tc.link)), ok(page(repoItem(2, "web")))}}
			client := Client{Token: "tok", BaseURL: tc.base, HTTP: &http.Client{Transport: transport}}
			repos, err := client.ListRepositories(context.Background(), ListOptions{Org: "acme"})
			if err != nil || len(repos) != 2 || len(transport.seen) != 2 {
				t.Fatalf("want 2 repos from 2 requests, got %d repos, %d requests, err %v (%v)", len(repos), len(transport.seen), err, transport.seen)
			}
		})
	}
}

// Another scheme, another port or another host is another origin, with the token configured for the base.
func TestAnotherSchemePortOrHostIsAnotherOrigin(t *testing.T) {
	two := page(repoItem(1, "api"))
	for _, tc := range []struct{ name, base, link string }{
		{"scheme", "https://ghe.test", "http://ghe.test/orgs/acme/repos?page=2"},
		{"port", "https://ghe.test", "https://ghe.test:8443/orgs/acme/repos?page=2"},
		{"scheme only, same explicit port", "https://ghe.test:8443", "http://ghe.test:8443/orgs/acme/repos?page=2"},
		{"host only, same explicit port", "https://ghe.test:8443", "https://ghe2.test:8443/orgs/acme/repos?page=2"},
		{"port only", "https://ghe.test:8443", "https://ghe.test:8444/orgs/acme/repos?page=2"},
		{"host", "https://ghe.test", "https://ghe2.test/orgs/acme/repos?page=2"},
		{"subdomain", "https://ghe.test", "https://api.ghe.test/orgs/acme/repos?page=2"},
		{"default base, other host", "", "https://other.test/orgs/acme/repos?page=2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &scriptedTransport{responses: []scripted{ok(two, next(tc.link)), ok(two)}}
			client := Client{Token: "tok", BaseURL: tc.base, HTTP: &http.Client{Transport: transport}}
			_, err := client.ListRepositories(context.Background(), ListOptions{Org: "acme"})
			var typed *Error
			if !errors.As(err, &typed) || typed.Class != CrossOriginLinkClass || len(transport.seen) != 1 {
				t.Fatalf("want %s after one request, got %v after %d (%v)", CrossOriginLinkClass, err, len(transport.seen), transport.seen)
			}
		})
	}
}

// A non-ASCII base host goes on the wire in its IDNA form.
func TestNonASCIIBaseHostIsDialledInIDNAForm(t *testing.T) {
	var dialled []string
	transport := &http.Transport{DialContext: func(_ context.Context, _, addr string) (net.Conn, error) {
		dialled = append(dialled, addr)
		return nil, errors.New("stand-in: not dialled")
	}}
	client := Client{Token: "tok", BaseURL: "https://ghé.test/api/v3", HTTP: &http.Client{Transport: transport}}
	_, _ = client.ListRepositories(context.Background(), ListOptions{Org: "acme"})
	if len(dialled) != 1 || dialled[0] != "xn--gh-cja.test:443" {
		t.Fatalf("dialled %q, want [xn--gh-cja.test:443]", dialled)
	}
}

// An origin that cannot be computed is never trusted: a link host that IDNA cannot encode (a label of 64 characters
// or more) is refused, and so is every next page of a base whose own host cannot be encoded (fail closed, no request
// beyond the first).
func TestAnOriginThatCannotBeComputedIsRefused(t *testing.T) {
	long := strings.Repeat("a", 70)
	for _, tc := range []struct{ name, base, link string }{
		{"link host cannot be encoded", "https://ghe.test", "https://" + long + ".ghe.test/orgs/acme/repos?page=2"},
		{"base host cannot be encoded, relative link", "https://" + long + ".ghe.test", "/orgs/acme/repos?page=2"},
		{"base host cannot be encoded, link to the same host", "https://" + long + ".ghe.test", "https://" + long + ".ghe.test/orgs/acme/repos?page=2"},
		{"base host cannot be encoded, link to another such host", "https://" + long + ".ghe.test", "https://" + long + "b.ghe.test/orgs/acme/repos?page=2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &scriptedTransport{responses: []scripted{ok(page(repoItem(1, "api")), next(tc.link)), ok(`[]`)}}
			client := Client{Token: "tok", BaseURL: tc.base, HTTP: &http.Client{Transport: transport}}
			_, err := client.ListRepositories(context.Background(), ListOptions{Org: "acme"})
			var typed *Error
			if !errors.As(err, &typed) || typed.Class != CrossOriginLinkClass || len(transport.seen) != 1 {
				t.Fatalf("want %s after one request, got %v after %d", CrossOriginLinkClass, err, len(transport.seen))
			}
		})
	}
}

// The origin is checked on every page, not only the first: page 1 links to the same origin, page 2 to another one
// -> refused after two requests, nothing reaches the other host.
func TestACrossOriginLinkOnALaterPageIsRefused(t *testing.T) {
	two := page(repoItem(1, "api"))
	transport := &scriptedTransport{responses: []scripted{
		ok(two, next("https://ghe.test/api/v3/orgs/acme/repos?page=2")),
		ok(two, next("https://other.test/orgs/acme/repos?page=3")),
		ok(two),
	}}
	client := Client{Token: "tok", BaseURL: "https://ghe.test/api/v3", HTTP: &http.Client{Transport: transport}}
	_, err := client.ListRepositories(context.Background(), ListOptions{Org: "acme"})
	var typed *Error
	if !errors.As(err, &typed) || typed.Class != CrossOriginLinkClass || len(transport.seen) != 2 {
		t.Fatalf("want %s after two requests, got %v after %d (%v)", CrossOriginLinkClass, err, len(transport.seen), transport.seen)
	}
	for _, seen := range transport.seen {
		if strings.Contains(seen[0], "other.test") {
			t.Fatalf("a request went to the other host: %v", transport.seen)
		}
	}
}

// A client supplied by the caller (its own redirect policy) never carries the token across a redirect: the listing
// answers a 3xx as a failure, as the default client does, instead of following it (Go forwards Authorization to a
// subdomain of the original host).
func TestASuppliedClientNeverFollowsARedirectWithTheToken(t *testing.T) {
	var seen []string
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		seen = append(seen, request.URL.Host+" "+request.Header.Get("Authorization"))
		if request.URL.Host == "ghe.test" {
			return &http.Response{StatusCode: 302, Header: http.Header{"Location": {"https://api.ghe.test/orgs/acme/repos"}}, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("[]")), Request: request}, nil
	})
	client := Client{Token: "SECRET-TOKEN", BaseURL: "https://ghe.test", HTTP: &http.Client{Transport: transport}}
	_, _ = client.ListRepositories(context.Background(), ListOptions{Org: "acme"})
	for _, entry := range seen {
		if strings.HasPrefix(entry, "api.ghe.test") {
			t.Fatalf("the redirect was followed with the token: %q", seen)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

// The same probe through the shared one (a 307 to another origin; that origin sees no request at all).
func TestASuppliedClientNeverFollowsARedirectToAnotherOriginProbe(t *testing.T) {
	probe := redirectprobe.New(t)
	client := Client{Token: "SECRET-TOKEN", BaseURL: probe.Base.URL, HTTP: probe.Client()}
	_, _ = client.ListRepositories(context.Background(), ListOptions{Org: "acme"})
	probe.Assert(t)
}

// The client restcore builds when none is supplied follows no redirect (restcore.go default branch).
func TestTheDefaultClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	client := Client{Token: "SECRET-TOKEN", BaseURL: probe.Base.URL}
	_, _ = client.ListRepositories(context.Background(), ListOptions{Org: "acme"})
	probe.Assert(t)
}

// The wrapper works on a copy: the caller's own (possibly shared) client keeps its redirect policy.
func TestTheSuppliedClientIsLeftUnchanged(t *testing.T) {
	supplied := &http.Client{Transport: &scriptedTransport{responses: []scripted{ok(page(repoItem(1, "api")))}}}
	client := Client{Token: "tok", HTTP: supplied}
	_, _ = client.ListRepositories(context.Background(), ListOptions{Org: "acme"})
	if supplied.CheckRedirect != nil {
		t.Fatal("the caller's client had its redirect policy changed")
	}
}
