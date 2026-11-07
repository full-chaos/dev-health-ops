package authapi

import (
	"errors"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
)

// gettingCache counts the Get calls the authorize route makes.
type gettingCache struct {
	DecisionCache
	gets *int
}

func (g gettingCache) Get(k decisioncache.Key) (authorization.Decision, bool) {
	*g.gets++
	return g.DecisionCache.Get(k)
}

// A live cached allow is never served while the decision store is down: the cache is read only inside the store
// transaction, so the outage is a 503 store_unavailable with no decision and no cache read. Once the store is back,
// the same entry is a hit again, so the refusal came from the outage and not from an expiry.
func TestAStoreOutageServesNoCachedAllow(t *testing.T) {
	c := newCacheFixture(t)
	gets := 0
	c.rebuild(t, gettingCache{DecisionCache: c.cache, gets: &gets})
	if d := c.ask(); !d.Allowed {
		t.Fatalf("first ask %+v, want an allow", d)
	}
	if d := c.ask(); !d.Allowed || c.state.snapReads != 1 {
		t.Fatalf("second ask %+v with %d snapshot reads, want a cached allow and 1", d, c.state.snapReads)
	}
	c.cs.beginErr = errors.New("pool exhausted")
	gets = 0
	assertError(t, c.one(c.question(lowAction, "organization", "acme", c.org)), http.StatusServiceUnavailable, reasonStoreUnavailable)
	if gets != 0 {
		t.Fatalf("%d cache reads during the store outage, want 0", gets)
	}
	c.cs.beginErr = nil
	if d := c.ask(); !d.Allowed || c.state.snapReads != 1 || gets != 1 {
		t.Fatalf("after the outage %+v, %d snapshot reads, %d cache reads; want the cached allow, 1 and 1", d, c.state.snapReads, gets)
	}
	if c.cache.Len() != 1 {
		t.Fatalf("%d entries, want the 1 live entry", c.cache.Len())
	}
}
