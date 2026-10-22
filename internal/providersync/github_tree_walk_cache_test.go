package providersync

import (
	"container/list"
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func cacheInventory(t *testing.T, fake *walkTreeFake, cache *gitHubTreeWalkCache, org, root, sha string) ([]string, map[string]*int, bool, error) {
	t.Helper()
	return gitHubFilesInventoryCached(context.Background(), fake.client(t), cache, org, root, sha, "acme/api")
}

func truncatedHybrid() *walkTreeFake {
	fake := hybridTree(30, 4)
	fake.truncateAbove = 20
	return fake
}

func TestGitHubWalkCacheReusesOneWalkAcrossChunkCalls(t *testing.T) {
	logs := captureWalkLog(t)
	cache := newGitHubTreeWalkCache()
	fake := truncatedHybrid()
	first, sizes, walked, err := cacheInventory(t, fake, cache, "org1", "/repos/acme/api", "root")
	if err != nil || !walked || len(first) == 0 || len(sizes) == 0 {
		t.Fatalf("first: paths=%d walked=%v err=%v", len(first), walked, err)
	}
	gets := fake.gets
	second, _, walked, err := cacheInventory(t, fake, cache, "org1", "/repos/acme/api", "root")
	if err != nil || !walked || fake.gets != gets+1 {
		t.Fatalf("second call must cost only the root recursive probe: gets=%d want %d walked=%v err=%v", fake.gets, gets+1, walked, err)
	}
	sort.Strings(first)
	sort.Strings(second)
	if strings.Join(first, "\n") != strings.Join(second, "\n") {
		t.Fatal("cached paths differ from the walked paths")
	}
	if !strings.Contains(logs.String(), "walk_cache=miss") || !strings.Contains(logs.String(), "walk_cache=hit") {
		t.Fatalf("logs=%q, want one miss and one hit", logs.String())
	}
	if strings.Contains(logs.String(), "acme") || strings.Contains(logs.String(), "org1") {
		t.Fatalf("hit/miss log must not name the repository or org: %s", logs.String())
	}
}

func TestGitHubWalkCacheKeyIsOrgRepoAndSha(t *testing.T) {
	cache := newGitHubTreeWalkCache()
	fake := truncatedHybrid()
	for _, key := range [][3]string{{"org1", "/repos/acme/api", "root"}, {"org2", "/repos/acme/api", "root"}, {"org1", "/repos/acme/other", "root"}} {
		before := fake.gets
		if _, _, _, err := gitHubFilesInventoryCached(context.Background(), fake.client(t), cache, key[0], key[1], key[2], "x"); err == nil && fake.gets-before < 2 {
			t.Fatalf("key %v hit another key's entry", key)
		}
	}
}

func TestGitHubWalkCacheNewShaEvictsTheOldShaOfTheSameRepo(t *testing.T) {
	cache := newGitHubTreeWalkCache()
	now := time.Now()
	cache.put("o", "/r", "sha1", []string{"a"}, map[string]*int{}, now)
	cache.put("o", "/r", "sha2", []string{"b"}, map[string]*int{}, now)
	if _, _, ok := cache.get("o", "/r", "sha1", now); ok {
		t.Fatal("old sha must be dropped when the repo moves to a new sha")
	}
	if _, _, ok := cache.get("o", "/r", "sha2", now); !ok {
		t.Fatal("new sha must be cached")
	}
}

func TestGitHubWalkCacheStoresNothingFromAStoppedWalk(t *testing.T) {
	cache := newGitHubTreeWalkCache()
	deep := walkChain(gitHubTreeWalkMaxDepth + 1)
	deep.truncateAbove = 1
	if _, _, _, err := cacheInventory(t, deep, cache, "o", "/repos/acme/api", "root"); err == nil {
		t.Fatal("want a ceiling error")
	}
	if cache.len() != 0 {
		t.Fatalf("entries=%d, a stopped walk must store nothing", cache.len())
	}
}

func TestGitHubWalkCacheGetReturnsACopyOfThePaths(t *testing.T) {
	cache := newGitHubTreeWalkCache()
	now := time.Now()
	cache.put("o", "/r", "s", []string{"b", "a"}, map[string]*int{}, now)
	got, _, _ := cache.get("o", "/r", "s", now)
	sort.Strings(got)
	again, _, _ := cache.get("o", "/r", "s", now)
	if again[0] != "b" {
		t.Fatalf("caller sort leaked into the cache: %v", again)
	}
}

func TestGitHubWalkCacheBounds(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	cache := newGitHubTreeWalkCache()
	for i := 0; i < gitHubTreeWalkCacheMaxEntries+1; i++ {
		cache.put("o", "/r"+string(rune('a'+i)), "s", []string{"p"}, nil, now.Add(time.Duration(i)*time.Second))
	}
	if cache.len() != gitHubTreeWalkCacheMaxEntries {
		t.Fatalf("entries=%d, want LRU cap %d", cache.len(), gitHubTreeWalkCacheMaxEntries)
	}
	if _, _, ok := cache.get("o", "/ra", "s", now.Add(time.Hour)); ok {
		t.Fatal("oldest entry must be evicted")
	}
	big := make([]string, gitHubTreeWalkCacheMaxPaths+1)
	cache.put("o", "/big", "s", big, nil, now)
	if _, _, ok := cache.get("o", "/big", "s", now); ok {
		t.Fatal("a value over the path cap must not be stored")
	}
	fresh := newGitHubTreeWalkCache()
	fresh.put("o", "/r", "s", []string{"p"}, nil, now)
	if _, _, ok := fresh.get("o", "/r", "s", now.Add(gitHubTreeWalkCacheTTL-time.Second)); !ok {
		t.Fatal("entry inside the TTL must hit")
	}
	if _, _, ok := fresh.get("o", "/r", "s", now.Add(gitHubTreeWalkCacheTTL)); ok {
		t.Fatal("entry at the TTL must expire")
	}
}

func TestGitHubWalkCacheTotalPathCapEvictsOldest(t *testing.T) {
	now := time.Now()
	cache := newGitHubTreeWalkCache()
	third := gitHubTreeWalkCacheMaxPaths/3 + 1
	for i := 0; i < 3; i++ {
		cache.put("o", "/r"+string(rune('a'+i)), "s", make([]string, third), nil, now.Add(time.Duration(i)*time.Second))
	}
	if cache.len() != 2 {
		t.Fatalf("entries=%d, want the oldest dropped to stay under the path cap", cache.len())
	}
}

func TestGitHubWalkCacheIsRaceFree(t *testing.T) {
	cache := newGitHubTreeWalkCache()
	var group sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < 200; i++ {
				now := time.Now()
				cache.put("o", "/r"+string(rune('a'+i%6)), "s", []string{"a", "b"}, map[string]*int{}, now)
				if paths, _, ok := cache.get("o", "/r"+string(rune('a'+i%6)), "s", now); ok {
					sort.Strings(paths)
				}
			}
		}()
	}
	group.Wait()
}

func (cache *gitHubTreeWalkCache) clear() {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.order.Init()
	cache.entries = map[gitHubTreeWalkCacheKey]*list.Element{}
	cache.total = 0
}
