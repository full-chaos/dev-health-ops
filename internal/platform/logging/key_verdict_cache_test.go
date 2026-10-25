package logging

import (
	"fmt"
	"sync"
	"testing"
)

func TestProtectedKeyVerdictCacheStaysWithinItsBoundUnderConcurrentMisses(t *testing.T) {
	clearVerdicts := func() {
		keyVerdicts.Range(func(k, _ any) bool { keyVerdicts.Delete(k); return true })
		keyVerdictsCount.Store(0)
	}
	clearVerdicts()
	t.Cleanup(clearVerdicts)
	var wg sync.WaitGroup
	for worker := 0; worker < 64; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				protectedKeyCached(fmt.Sprintf("verdict_probe_%d_%d", worker, i))
			}
		}()
	}
	wg.Wait()
	stored := 0
	keyVerdicts.Range(func(_, _ any) bool { stored++; return true })
	if stored > maxCachedKeyVerdicts {
		t.Errorf("verdict cache holds %d entries, bound is %d", stored, maxCachedKeyVerdicts)
	}
	if got := int(keyVerdictsCount.Load()); got != stored {
		t.Errorf("count %d disagrees with stored entries %d", got, stored)
	}
}
