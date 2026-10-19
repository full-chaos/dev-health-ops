package gqlgenguard

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	slowMu    sync.Mutex
	slowTimes = map[string]time.Duration{}
)

// trackSlowTest records the wall time of a test that runs the real generator, so the race budget in TestMain can name it.
func trackSlowTest(t *testing.T) {
	t.Helper()
	start := time.Now()
	t.Cleanup(func() {
		slowMu.Lock()
		slowTimes[t.Name()] = time.Since(start)
		slowMu.Unlock()
	})
}

func slowestTests(n int) string {
	slowMu.Lock()
	defer slowMu.Unlock()
	names := make([]string, 0, len(slowTimes))
	for name := range slowTimes {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return slowTimes[names[i]] > slowTimes[names[j]] })
	if len(names) > n {
		names = names[:n]
	}
	rows := make([]string, 0, len(names))
	for _, name := range names {
		rows = append(rows, fmt.Sprintf("%s %.1fs", name, slowTimes[name].Seconds()))
	}
	return strings.Join(rows, "; ")
}
