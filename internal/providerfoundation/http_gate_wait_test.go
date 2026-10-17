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
	if elapsed := time.Since(started); elapsed > 400*time.Millisecond || requests.Load() != 0 {
		t.Fatalf("waited %s with %d requests, want the bound (50ms) respected", elapsed, requests.Load())
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
