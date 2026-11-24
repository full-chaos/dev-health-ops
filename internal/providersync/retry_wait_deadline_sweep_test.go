package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// retrySweepRoute is one caller of the client retry arm: it runs one
// Collect with a doer and a client whose retry wait is wait.
type retrySweepRoute struct {
	name    string
	newDoer func() HTTPDoerForSweep
	collect func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error)
}

type HTTPDoerForSweep = providerfoundation.HTTPDoer

var retrySweepRoutes []retrySweepRoute

// sweepStatusMapping loads the real status mapping once: t.Setenv inside
// loadRealStatusMapping must not run from the concurrent rows.
var (
	sweepStatusMappingOnce  sync.Once
	sweepStatusMappingValue *StatusMapping
)

func sweepStatusMapping(t *testing.T) *StatusMapping {
	t.Helper()
	sweepStatusMappingOnce.Do(func() { sweepStatusMappingValue = loadRealStatusMapping(t) })
	return sweepStatusMappingValue
}

func registerRetrySweepRoute(route retrySweepRoute) {
	retrySweepRoutes = append(retrySweepRoutes, route)
}

// sweepClient builds the client every sweep route uses: three attempts and a
// retry wait of wait.
func sweepClient(t *testing.T, provider, baseURL string, doer HTTPDoerForSweep, wait time.Duration) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		provider, baseURL, fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: wait, MaxWait: wait},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// sweepInjectDoer answers request number failAt (1-based) with status and a
// Retry-After far past the deadline; every other request goes to inner.
type sweepInjectDoer struct {
	inner  HTTPDoerForSweep
	failAt int
	status int
	mu     sync.Mutex
	calls  int
	path   string
}

func (d *sweepInjectDoer) Do(request *http.Request) (*http.Response, error) {
	d.mu.Lock()
	d.calls++
	call := d.calls
	d.mu.Unlock()
	if call == d.failAt {
		d.mu.Lock()
		d.path = request.Method + " " + request.URL.Path
		d.mu.Unlock()
		return &http.Response{
			StatusCode: d.status, Status: http.StatusText(d.status), Request: request,
			Header: http.Header{"Retry-After": {"25"}, "Content-Type": {"application/json"}},
			Body:   http.NoBody,
		}, nil
	}
	return d.inner.Do(request)
}

type retrySweepRow struct {
	Path      string `json:"path"`
	Deadline  bool   `json:"deadline"`
	OK        bool   `json:"ok"`
	Watermark bool   `json:"watermark"`
	Effects   int    `json:"effects"`
}

const (
	retrySweepGoldenPath  = "testdata/retrysweep/base_rows.json"
	retrySweepChangedPath = "testdata/retrysweep/tip_changed_rows.json"
)

// TestRetryWaitPastTheDeadlineEqualsBaseForEveryCaller runs every route with
// the k-th request answered 503 or 429 with a wait far past the deadline. A
// row records what the caller turns that into: an error that is a deadline
// error, no error, the watermark, the effects. The golden file holds the rows
// recorded at the base client (the wait slept out to the deadline); the fix
// must not move any of them, except the rows in tip_changed_rows.json: the
// 429 rows where the caller now sees the rate_limited class with a live
// context. A row's Path is the request that was failed.
func TestRetryWaitPastTheDeadlineEqualsBaseForEveryCaller(t *testing.T) {
	if len(retrySweepRoutes) == 0 {
		t.Fatal("no sweep routes registered")
	}
	var mu sync.Mutex
	got := map[string]retrySweepRow{}
	var wg sync.WaitGroup
	for _, route := range retrySweepRoutes {
		route := route
		counter := &sweepInjectDoer{inner: route.newDoer(), failAt: -1}
		if _, err := route.collect(t, context.Background(), counter, time.Millisecond); err != nil {
			t.Fatalf("%s: happy run failed: %v", route.name, err)
		}
		requests := counter.calls
		happy, happyErr := route.collect(t, context.Background(), &sweepInjectDoer{inner: route.newDoer(), failAt: -1}, time.Millisecond)
		mu.Lock()
		got[route.name+"|happy|0"] = retrySweepRow{Path: "(no failure)", OK: happyErr == nil, Watermark: happy.Watermark != nil, Effects: len(happy.Effects)}
		mu.Unlock()
		if requests == 0 {
			t.Fatalf("%s: happy run made no request", route.name)
		}
		for k := 1; k <= requests; k++ {
			for _, status := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests} {
				k, status := k, status
				wg.Add(1)
				go func() {
					defer wg.Done()
					inject := &sweepInjectDoer{inner: route.newDoer(), failAt: k, status: status}
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					batch, err := route.collect(t, ctx, inject, 25*time.Second)
					row := retrySweepRow{
						Path:     inject.path,
						Deadline: errors.Is(err, context.DeadlineExceeded), OK: err == nil,
						Watermark: batch.Watermark != nil, Effects: len(batch.Effects),
					}
					key := fmt.Sprintf("%s|req%02d|%d", route.name, k, status)
					mu.Lock()
					got[key] = row
					mu.Unlock()
				}()
			}
		}
	}
	wg.Wait()

	if path := os.Getenv("RETRY_SWEEP_WRITE"); path != "" {
		data, err := json.MarshalIndent(got, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	raw, err := os.ReadFile(retrySweepGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]retrySweepRow
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(retrySweepChangedPath); err != nil {
		t.Fatal(err)
	} else {
		var changed map[string]retrySweepRow
		if err := json.Unmarshal(raw, &changed); err != nil {
			t.Fatal(err)
		}
		for key, row := range changed {
			if _, ok := want[key]; !ok || want[key] == row {
				t.Errorf("%s: listed as a changed row but base has no different row", key)
			}
			want[key] = row
		}
	}
	var keys []string
	for key := range want {
		keys = append(keys, key)
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	keys = slices.Compact(keys)
	var diffs []string
	for _, key := range keys {
		if got[key] != want[key] {
			diffs = append(diffs, fmt.Sprintf("%s: tip=%+v base=%+v", key, got[key], want[key]))
		}
	}
	if len(diffs) > 0 {
		t.Fatalf("%d caller row(s) differ from the base client:\n%s", len(diffs), strings.Join(diffs, "\n"))
	}
}
