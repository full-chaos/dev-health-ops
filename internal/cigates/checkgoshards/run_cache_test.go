package checkgoshards_test

import (
	"sync"
	"sync/atomic"
	"testing"
)

// shardRun is the answer of one fake-go test-leg run: its exit and the `go test` lines it recorded.
type shardRun struct {
	res   cmdResult
	lines []string
}

// runCache runs one function per key and hands the same answer to every later caller of that key.
type runCache struct {
	mu      sync.Mutex
	entries map[string]*runEntry
}

type runEntry struct {
	once sync.Once
	run  shardRun
	done bool
}

// get returns the answer for key, computing it with fn on the first call only. fn may stop its test (t.Fatal): then the
// key is failed for every caller, never a zero answer.
func (c *runCache) get(t *testing.T, key string, fn func() shardRun) shardRun {
	t.Helper()
	c.mu.Lock()
	if c.entries == nil {
		c.entries = map[string]*runEntry{}
	}
	e := c.entries[key]
	if e == nil {
		e = &runEntry{}
		c.entries[key] = e
	}
	c.mu.Unlock()
	e.once.Do(func() {
		e.run = fn()
		e.done = true
	})
	if !e.done {
		t.Fatalf("the shared run for %q failed in another test", key)
	}
	return e.run
}

func TestRunCacheRunsEachKeyOnce(t *testing.T) {
	t.Parallel()
	var c runCache
	var calls atomic.Int32
	fn := func() shardRun {
		calls.Add(1)
		return shardRun{lines: []string{"one"}}
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := c.get(t, "same", fn); len(got.lines) != 1 || got.lines[0] != "one" {
				t.Errorf("answer %v", got.lines)
			}
		}()
	}
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("the same key ran %d times, want once", got)
	}
	c.get(t, "other", fn)
	if got := calls.Load(); got != 2 {
		t.Fatalf("a second key made %d runs in all, want 2", got)
	}
}

// A run that stops its test leaves its key failed for the next caller; the next caller never sees a zero answer.
func TestRunCacheFailsALaterCallerOfAFailedRun(t *testing.T) {
	t.Parallel()
	var c runCache
	var calls atomic.Int32
	failing := func() shardRun {
		calls.Add(1)
		panic("the run stopped")
	}
	func() {
		defer func() { _ = recover() }()
		c.get(t, "k", failing)
	}()
	e := c.entries["k"]
	if e == nil || e.done {
		t.Fatalf("a run that stopped is marked done: %+v", e)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
}
