//go:build !race

package checkgo_test

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

// get returns the answer for key, computing it with fn on the first call only. fn reports its failure by returning an
// error; a failed key is failed for every caller, never a zero answer.
func (c *runCache) get(t *testing.T, key string, fn func() (shardRun, error)) shardRun {
	t.Helper()
	run, err := fn()
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func TestRunCacheRunsEachKeyOnce(t *testing.T) {
	t.Parallel()
	var c runCache
	var calls atomic.Int32
	fn := func() (shardRun, error) {
		calls.Add(1)
		return shardRun{lines: []string{"one"}}, nil
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
