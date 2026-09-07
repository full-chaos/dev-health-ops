package capacityforecast

import (
	"context"
	"math"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// probeEnvVar gates the one test in this file. It is OPT-IN, and off in CI.
const probeEnvVar = "DEV_HEALTH_CAPACITY_OOM_PROBE"

// TestSimulationCapPreventsAProcessKillingAllocation is CHAOS-5349 r2 P1's
// executed pin, kept as a test rather than left in a review transcript.
//
// # What it proves, and why it needs a memory limit to prove it
//
// Before the cap, `simulations: 2147483647` passed the `< 1` guard, reached
// numerical.MonteCarloForecastDays's `make([]int, 0, simulations)` and asked the
// runtime for 8 bytes x (2^31-1) = ~17.2 GB. The observed failure was:
//
//	runtime: out of memory: cannot allocate 17179869184-byte block
//	fatal error: out of memory
//
// `fatal error` is the load-bearing word. A Go PANIC unwinds one request and the
// server keeps serving; a fatal OOM is unrecoverable and takes the whole process
// with it, so one GraphQL variable was an outage for every org on the instance.
//
// A test cannot assert that from inside the process it would have killed. What
// it CAN do is run the guarded path under a hard address-space ceiling: with the
// cap, the resolver rejects the request and allocates nothing, so the test
// passes; without it, the runtime aborts and the test binary dies, which is a
// loud failure rather than a silent regression.
//
// # Why it is opt-in
//
// The ceiling has to come from OUTSIDE the process -- runtime/debug's
// SetMemoryLimit is a SOFT limit that makes the GC work harder and does not stop
// a single oversized make() from reaching mmap, so it cannot stand in for
// prlimit. Requiring an external limit means requiring a specific way of
// invoking the test, and CI should not have to allocate anything to run the
// normal suite. So this skips unless DEV_HEALTH_CAPACITY_OOM_PROBE=1, and the
// intended invocation is recorded here:
//
//	go test -c -o /tmp/capacity-oom.test ./cmd/query-api/internal/capacityforecast
//	DEV_HEALTH_CAPACITY_OOM_PROBE=1 prlimit --as=2147483648 -- \
//	  /tmp/capacity-oom.test -test.run '^TestSimulationCapPreventsAProcessKillingAllocation$' -test.v
//
// A 2 GiB address-space ceiling is comfortably below the 17.2 GB the unguarded
// path requests and comfortably above anything the guarded path touches.
func TestSimulationCapPreventsAProcessKillingAllocation(t *testing.T) {
	if os.Getenv(probeEnvVar) != "1" {
		t.Skipf("set %s=1 and run under an address-space limit (see this test's doc "+
			"comment for the exact prlimit invocation) -- a soft GOMEMLIMIT cannot "+
			"substitute, and CI should not allocate to run the normal suite", probeEnvVar)
	}

	// A WORKING fake client, deliberately -- not the nil one the other guard
	// tests use.
	//
	// This was wrong in the first draft and the red run caught it: with a nil
	// client, removing the cap made the probe die of SIGSEGV in loadThroughput
	// long before it reached the Monte Carlo, so the test went red for a reason
	// that had nothing to do with the allocation it claims to pin. A test whose
	// failure mode does not match its own doc comment is worse than no test --
	// it would have kept passing as "proof" while proving something else.
	//
	// With real rows, removing the cap carries the request all the way to
	// MonteCarloForecastDays and its 17.2 GB make(), which is the thing under
	// test.
	client := &fakeClient{responses: []*fakeRowScanner{
		throughputRows(t, 2, 4, 6, 8, 10),
		{rows: [][]any{{uint64(30)}}},
	}}
	input := &model.CapacityForecastInput{
		HistoryDays: 90,
		Simulations: math.MaxInt32,
	}

	result, err := ResolveForecast(
		context.Background(), client, "org-1", input, day(t, "2026-09-01"))

	// Reaching this line AT ALL under the ceiling is the proof: the unguarded
	// path does not return, it aborts the process.
	if err == nil {
		t.Fatalf("simulations=%d was accepted; the draw slice alone is %d bytes and the "+
			"runtime aborts on it", math.MaxInt32, int64(math.MaxInt32)*8)
	}
	if result != nil {
		t.Errorf("got a forecast for simulations=%d, want a rejection", math.MaxInt32)
	}
	// Rejected BEFORE the reads, not after them: an oversized request must not
	// cost the org a ClickHouse round trip either.
	if client.calls != 0 {
		t.Errorf("issued %d queries before rejecting; the guard runs before any read",
			client.calls)
	}
	t.Logf("rejected without allocating or querying: %v", err)
}
