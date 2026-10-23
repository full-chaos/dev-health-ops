package providersync

import (
	"container/list"
	"sync"
	"time"
)

// A walked inventory (paths and sizes of one tree sha) kept in this process so
// the next chunk call of a files unit does not walk the tree again. Per replica,
// never shared; a miss walks as before.
const (
	gitHubTreeWalkCacheMaxEntries = 4
	gitHubTreeWalkCacheMaxPaths   = 400_000
	gitHubTreeWalkCacheTTL        = 30 * time.Minute
)

type gitHubTreeWalkCacheKey struct{ org, root string }

type gitHubTreeWalkCacheEntry struct {
	key      gitHubTreeWalkCacheKey
	sha      string
	paths    []string
	sizes    map[string]*int
	storedAt time.Time
}

// gitHubTreeWalkCache holds one entry per org and repository: a new tree sha
// replaces the old one.
type gitHubTreeWalkCache struct {
	mu      sync.Mutex
	order   *list.List
	entries map[gitHubTreeWalkCacheKey]*list.Element
	total   int
}

var defaultGitHubTreeWalkCache = newGitHubTreeWalkCache()

func newGitHubTreeWalkCache() *gitHubTreeWalkCache {
	return &gitHubTreeWalkCache{order: list.New(), entries: map[gitHubTreeWalkCacheKey]*list.Element{}}
}

func (cache *gitHubTreeWalkCache) len() int {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return len(cache.entries)
}

// get returns a copy of the paths: callers sort them in place. The sizes map is
// read-only for every caller.
func (cache *gitHubTreeWalkCache) get(org, root, sha string, now time.Time) ([]string, map[string]*int, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	element, ok := cache.entries[gitHubTreeWalkCacheKey{org, root}]
	if !ok {
		return nil, nil, false
	}
	entry := element.Value.(*gitHubTreeWalkCacheEntry)
	if now.Sub(entry.storedAt) >= gitHubTreeWalkCacheTTL {
		cache.remove(element)
		return nil, nil, false
	}
	if entry.sha != sha {
		return nil, nil, false
	}
	cache.order.MoveToFront(element)
	return append([]string(nil), entry.paths...), entry.sizes, true
}

func (cache *gitHubTreeWalkCache) put(org, root, sha string, paths []string, sizes map[string]*int, now time.Time) {
	if len(paths) > gitHubTreeWalkCacheMaxPaths {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	key := gitHubTreeWalkCacheKey{org, root}
	if element, ok := cache.entries[key]; ok {
		cache.remove(element)
	}
	entry := &gitHubTreeWalkCacheEntry{
		key: key, sha: sha, paths: append([]string(nil), paths...), sizes: sizes, storedAt: now,
	}
	cache.entries[key] = cache.order.PushFront(entry)
	cache.total += len(entry.paths)
	for cache.order.Len() > 1 && (cache.order.Len() > gitHubTreeWalkCacheMaxEntries || cache.total > gitHubTreeWalkCacheMaxPaths) {
		cache.remove(cache.order.Back())
	}
}

func (cache *gitHubTreeWalkCache) remove(element *list.Element) {
	entry := cache.order.Remove(element).(*gitHubTreeWalkCacheEntry)
	delete(cache.entries, entry.key)
	cache.total -= len(entry.paths)
}
