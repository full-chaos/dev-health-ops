package providerfoundation

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// scriptedGate answers Wait from a list of remaining delays, then 0.
type scriptedGate struct {
	remaining []time.Duration
	calls     atomic.Int32
}

func (g *scriptedGate) Wait(context.Context) (time.Duration, error) {
	call := int(g.calls.Add(1)) - 1
	if call < len(g.remaining) {
		return g.remaining[call], nil
	}
	return 0, nil
}

func (g *scriptedGate) Penalize(context.Context, time.Duration) error { return nil }

func gateWaitClient(t *testing.T, gate BackoffGate, requests *atomic.Int32) *HTTPClient {
	t.Helper()
	policy := RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond}
	client := newTestHTTPClient(t, fakehttp.Client(HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		requests.Add(1)
		return testHTTPResponse(request, http.StatusOK, http.Header{}, `{}`), nil
	})), policy)
	client.Gate = gate
	return client
}

// CHAOS-8830: a shared-gate wait is slept out in place and the request goes
// out, as Python's gate_call did; it used to end the try at once, so the unit
// restarted from zero and threw away the pages it had fetched.
func TestAGateWaitWithinTheBoundIsSleptOutInPlace(t *testing.T) {
	t.Parallel()
	gate := &scriptedGate{remaining: []time.Duration{40 * time.Millisecond}}
	var requests atomic.Int32
	client := gateWaitClient(t, gate, &requests)
	client.GateWaitMax = time.Second

	started := time.Now()
	response, err := client.Do(context.Background(), http.MethodGet, "/items", nil)
	if err != nil {
		t.Fatalf("Do ended the try on a gate wait inside the bound: %v", err)
	}
	_ = response.Body.Close()
	if requests.Load() != 1 || time.Since(started) < 30*time.Millisecond {
		t.Fatalf("requests = %d after %s, want one request after the gate's 40ms", requests.Load(), time.Since(started))
	}
}

func TestAGateWaitBeyondTheBoundStillEndsTheTryWithTheWait(t *testing.T) {
	t.Parallel()
	gate := &scriptedGate{remaining: []time.Duration{time.Hour}}
	var requests atomic.Int32
	client := gateWaitClient(t, gate, &requests)
	client.GateWaitMax = time.Second

	_, err := client.Do(context.Background(), http.MethodGet, "/items", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited || providerErr.RetryAfter != time.Hour {
		t.Fatalf("err = %v, want rate_limited carrying the gate's 1h", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("a request went out through a gate that is closed for an hour")
	}
}

// A gate that keeps closing is waited on only up to the bound in total, then the
// try ends with the wait that remains.
func TestInPlaceGateWaitsAreBoundedInTotal(t *testing.T) {
	t.Parallel()
	gate := &scriptedGate{remaining: []time.Duration{30 * time.Millisecond, 30 * time.Millisecond, 30 * time.Millisecond, 30 * time.Millisecond}}
	var requests atomic.Int32
	client := gateWaitClient(t, gate, &requests)
	client.GateWaitMax = 50 * time.Millisecond

	started := time.Now()
	_, err := client.Do(context.Background(), http.MethodGet, "/items", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited {
		t.Fatalf("err = %v, want rate_limited once the bound is spent", err)
	}
	// One 30ms wait fits the 50ms bound and is slept; the second would pass it
	// (30+30 > 50), so the try ends there: two gate reads, one sleep, no request.
	// The gate-read count is the exact proof (a second sleep would make it three
	// or send a request); the elapsed window only guards the sleep itself and is
	// wide enough for a loaded runner.
	elapsed := time.Since(started)
	if elapsed < 25*time.Millisecond || elapsed > 200*time.Millisecond || requests.Load() != 0 || gate.calls.Load() != 2 {
		t.Fatalf("waited %s with %d requests and %d gate reads, want one 30ms sleep inside the 50ms bound, two gate reads and no request", elapsed, requests.Load(), gate.calls.Load())
	}
}

func TestAnInPlaceGateWaitEndsWithTheContext(t *testing.T) {
	t.Parallel()
	gate := &scriptedGate{remaining: []time.Duration{time.Second}}
	var requests atomic.Int32
	client := gateWaitClient(t, gate, &requests)
	client.GateWaitMax = 10 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)

	_, err := client.Do(ctx, http.MethodGet, "/items", nil)
	if !errors.Is(err, context.Canceled) || requests.Load() != 0 {
		t.Fatalf("err = %v requests = %d, want the cancellation and no request", err, requests.Load())
	}
}

// A context whose deadline falls before the wait ends cannot sleep it out: the
// try ends at once with the wait, so the unit defers instead of dying mid-sleep.
func TestAGateWaitLongerThanTheContextDeadlineEndsTheTryAtOnce(t *testing.T) {
	t.Parallel()
	gate := &scriptedGate{remaining: []time.Duration{time.Minute}}
	var requests atomic.Int32
	client := gateWaitClient(t, gate, &requests)
	client.GateWaitMax = 10 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, err := client.Do(ctx, http.MethodGet, "/items", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited || providerErr.RetryAfter != time.Minute {
		t.Fatalf("err = %v, want rate_limited carrying 1m", err)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatalf("slept %s towards a wait the deadline could not outlast", time.Since(started))
	}
}

// reportingGate is a gate that, like InProcessBackoffGate and the Valkey gate,
// reports the penalty owed without sleeping it and can be extended meanwhile.
type reportingGate struct {
	until atomic.Int64 // unix nanos
}

func (g *reportingGate) Wait(ctx context.Context) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if wait := time.Until(time.Unix(0, g.until.Load())); wait > 0 {
		return wait, nil
	}
	return 0, nil
}

func (g *reportingGate) Penalize(_ context.Context, delay time.Duration) error {
	g.until.Store(time.Now().Add(delay).UnixNano())
	return nil
}

// Every guarantee of the in-place wait holds for a gate that only reports: a
// penalty a sibling extends while this request sleeps is read again, the bound
// and the deadline decide before any sleep, and the lease is checked after it.
func TestInPlaceGateWaitRereadsAnExtendedPenalty(t *testing.T) {
	t.Parallel()
	gate := &reportingGate{}
	_ = gate.Penalize(context.Background(), 60*time.Millisecond)
	var requests atomic.Int32
	client := gateWaitClient(t, gate, &requests)
	client.GateWaitMax = time.Second
	time.AfterFunc(20*time.Millisecond, func() { _ = gate.Penalize(context.Background(), 150*time.Millisecond) })

	started := time.Now()
	response, err := client.Do(context.Background(), http.MethodGet, "/items", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if elapsed := time.Since(started); elapsed < 150*time.Millisecond {
		t.Fatalf("request went out after %s, before the extended penalty (>= 150ms from t=20ms) ended", elapsed)
	}
}

func TestInPlaceGateWaitRespectsTheBoundAndTheDeadlineForAReportingGate(t *testing.T) {
	t.Parallel()
	gate := &reportingGate{}
	_ = gate.Penalize(context.Background(), time.Hour)
	var requests atomic.Int32
	client := gateWaitClient(t, gate, &requests)
	client.GateWaitMax = 100 * time.Millisecond
	if _, err := client.Do(context.Background(), http.MethodGet, "/items", nil); err == nil || requests.Load() != 0 {
		t.Fatalf("err = %v requests = %d, want rate_limited and no request past the bound", err, requests.Load())
	}
	client.GateWaitMax = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := client.Do(ctx, http.MethodGet, "/items", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited || requests.Load() != 0 {
		t.Fatalf("err = %v requests = %d, want rate_limited (not a deadline error) and no request", err, requests.Load())
	}
}

type flippingLease struct{ calls atomic.Int32 }

func (l *flippingLease) Assert(context.Context) error {
	if l.calls.Add(1) > 1 {
		return ErrLeaseLost
	}
	return nil
}

func TestDoUnauthenticatedReassertsTheLeaseAfterTheGateSleep(t *testing.T) {
	t.Parallel()
	gate := &reportingGate{}
	_ = gate.Penalize(context.Background(), 40*time.Millisecond)
	var requests atomic.Int32
	client := gateWaitClient(t, gate, &requests)
	client.GateWaitMax = time.Second
	client.Lease = &flippingLease{}
	_, err := client.DoUnauthenticated(context.Background(), http.MethodGet, "https://storage.example/blob")
	if !errors.Is(err, ErrLeaseLost) || requests.Load() != 0 {
		t.Fatalf("err = %v requests = %d, want lease loss and no request after the gate sleep", err, requests.Load())
	}
}

// A retry is a wire attempt like the first: a sibling that extends the shared
// gate while this request sits in a retry wait must be seen before the retry is
// sent, not after (the gate used to be read once, before the retry loop).
func TestARetryRechecksTheGateBeforeItIsSent(t *testing.T) {
	t.Parallel()
	gate := &reportingGate{}
	var calls atomic.Int32
	var secondSentAt atomic.Int64
	policy := RetryPolicy{MaxAttempts: 2, InitialWait: time.Millisecond, MaxWait: time.Millisecond}
	client := newTestHTTPClient(t, fakehttp.Client(HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			// A sibling's rate limit extends the shared gate while this request retries.
			_ = gate.Penalize(context.Background(), 200*time.Millisecond)
			return testHTTPResponse(request, http.StatusServiceUnavailable, http.Header{}, `{}`), nil
		}
		secondSentAt.Store(time.Now().UnixNano())
		return testHTTPResponse(request, http.StatusOK, http.Header{}, `{}`), nil
	})), policy)
	client.Gate = gate
	client.GateWaitMax = time.Second

	started := time.Now()
	response, err := client.Do(context.Background(), http.MethodGet, "/items", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want the failed attempt and the retry", calls.Load())
	}
	if sent := time.Unix(0, secondSentAt.Load()).Sub(started); sent < 180*time.Millisecond {
		t.Fatalf("the retry went out after %s while the shared gate still owed about 200ms", sent)
	}
}

// After this client's own 429 the gate is armed with the same delay the retry
// then waits, so the recheck finds it clear: no second sleep.
func TestARetryAfterOwn429DoesNotSleepTheGateTwice(t *testing.T) {
	t.Parallel()
	gate := &reportingGate{}
	var calls atomic.Int32
	policy := RetryPolicy{MaxAttempts: 2, InitialWait: 60 * time.Millisecond, MaxWait: 60 * time.Millisecond}
	client := newTestHTTPClient(t, fakehttp.Client(HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return testHTTPResponse(request, http.StatusTooManyRequests, http.Header{"Retry-After": {"1"}}, `{"message":"rate limit"}`), nil
		}
		return testHTTPResponse(request, http.StatusOK, http.Header{}, `{}`), nil
	})), policy)
	client.Gate = gate
	client.GateWaitMax = 3 * time.Second

	started := time.Now()
	response, err := client.Do(context.Background(), http.MethodGet, "/items", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if elapsed := time.Since(started); elapsed > 1800*time.Millisecond {
		t.Fatalf("took %s: the retry slept the 1s Retry-After and then the gate it had armed for the same second", elapsed)
	}
}
