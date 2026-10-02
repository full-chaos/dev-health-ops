// Package redirectprobe is the probe of the "a supplied client never follows a redirect with a credential" class: a base
// server that answers every request with a redirect to a second server (another origin), and the client a caller would
// supply (default redirect policy). A site is closed when, after one call through that client, the second server saw
// NO request at all (not merely no credential).
package redirectprobe

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// Probe is the two servers.
type Probe struct {
	// Base answers every request, whatever its method, with a 307 to the same path and query on Other.
	Base *httptest.Server
	// Other counts the requests it receives.
	Other *httptest.Server
	hits  atomic.Int64
}

// New starts both servers; they stop with the test.
func New(t testing.TB) *Probe {
	t.Helper()
	p := &Probe{}
	p.Other = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(p.Other.Close)
	p.Base = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := p.Other.URL + r.URL.Path
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		http.Redirect(w, r, target, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(p.Base.Close)
	return p
}

// Client is the client a caller supplies: net/http's default redirect policy (it follows up to ten redirects).
func (p *Probe) Client() *http.Client { return &http.Client{} }

// Hits is how many requests the second server has seen.
func (p *Probe) Hits() int { return int(p.hits.Load()) }

// Assert fails the test when the second server saw a request.
func (p *Probe) Assert(t testing.TB) {
	t.Helper()
	if n := p.Hits(); n != 0 {
		t.Fatalf("the redirect was followed: the other origin saw %d request(s)", n)
	}
}
