package revisionfeed

import (
	"context"
	"sync"
	"testing"
)

// The feed is a high-water mark per aggregate: a replica that reconnects reaches the latest revision, and the revisions in
// between are covered by the invalidation at the latest (Invalidate at N covers every revision <= N), not replayed. This
// fake cache holds per-revision entries and honours exactly that contract.
type revisionCache struct {
	mu      sync.Mutex
	entries map[string]map[int64]bool
	calls   []call
}

func newRevisionCache() *revisionCache { return &revisionCache{entries: map[string]map[int64]bool{}} }

func (c *revisionCache) put(typ, id string, revisions ...int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := typ + ":" + id
	if c.entries[key] == nil {
		c.entries[key] = map[int64]bool{}
	}
	for _, r := range revisions {
		c.entries[key][r] = true
	}
}

func (c *revisionCache) Invalidate(_ context.Context, typ, id string, revision int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call{typ: typ, id: id, revision: revision})
	for r := range c.entries[typ+":"+id] {
		if r <= revision {
			delete(c.entries[typ+":"+id], r)
		}
	}
}

func (c *revisionCache) InvalidateAll(context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call{all: true})
	c.entries = map[string]map[int64]bool{}
}

func (c *revisionCache) held(typ, id string) []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int64
	for r := range c.entries[typ+":"+id] {
		out = append(out, r)
	}
	return out
}

func (c *revisionCache) invalidatedAt(typ, id string) []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int64
	for _, k := range c.calls {
		if !k.all && k.typ == typ && k.id == id {
			out = append(out, k.revision)
		}
	}
	return out
}

func consumerWithCache(t *testing.T) (*Consumer, *revisionCache) {
	t.Helper()
	cache := newRevisionCache()
	c, err := NewConsumer(Config{Invalidator: cache})
	if err != nil {
		t.Fatal(err)
	}
	return c, cache
}

func requireNothingAtOrBelow4(t *testing.T, cache *revisionCache) {
	t.Helper()
	if held := cache.held("user", "u1"); len(held) != 0 {
		t.Fatalf("the cache still holds revisions %v of the aggregate after the invalidation at 4: an invalidation at N must cover every revision <= N", held)
	}
	got := cache.invalidatedAt("user", "u1")
	if len(got) == 0 || got[len(got)-1] != 4 {
		t.Fatalf("the invalidations of the aggregate were at %v, want the last one at the latest revision 4", got)
	}
}

func TestACatchUpAtRevision4ClearsEveryCachedRevisionUpTo4(t *testing.T) {
	c, cache := consumerWithCache(t)
	ctx := context.Background()
	c.Handle(ctx, ev("user", "u1", 1))
	cache.put("user", "u1", 1, 2, 3)
	c.CatchUp(ctx, map[string]int64{"user:u1": 4})
	requireNothingAtOrBelow4(t, cache)
	if got, _ := c.Watermarks().Latest("user", "u1"); got != 4 {
		t.Fatalf("the mark is %d, want 4", got)
	}
}

func TestALiveGapAtRevision4ClearsEveryCachedRevisionUpTo4(t *testing.T) {
	c, cache := consumerWithCache(t)
	ctx := context.Background()
	c.Handle(ctx, ev("user", "u1", 1))
	cache.put("user", "u1", 1, 2, 3)
	c.Handle(ctx, ev("user", "u1", 4))
	requireNothingAtOrBelow4(t, cache)
}

func TestACatchUpAfterAnAppliedRevisionDoesNotInvalidateAgain(t *testing.T) {
	c, cache := consumerWithCache(t)
	ctx := context.Background()
	c.Handle(ctx, ev("user", "u1", 1))
	cache.put("user", "u1", 1)
	c.Handle(ctx, ev("user", "u1", 2))
	before := len(cache.invalidatedAt("user", "u1"))
	c.CatchUp(ctx, map[string]int64{"user:u1": 2})
	if after := len(cache.invalidatedAt("user", "u1")); after != before {
		t.Fatalf("a floor at the mark invalidated again (%d calls, was %d)", after, before)
	}
}
