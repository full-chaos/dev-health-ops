//go:build !race

package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// This allocation measurement is not compiled into the -race leg: testing.AllocsPerRun is
// not stable under the race detector (it adds allocations of its own: 64 vs 63 allocs for
// the same two requests), so the comparison cannot be asserted there. It runs in the
// non-race unit leg of internal/auth/httpapi (CI job "go-quality-leg (test)"). The
// property that recording a cause allocates nothing is ALSO pinned race-safely by
// TestRecordingACauseAllocatesNothing, which stays in the regular file.

// A whole request through each observer: a handler that records a cause costs
// no more allocations than one that does not.
func TestARequestThatRecordsACauseAllocatesNoMoreThanOneThatDoesNot(t *testing.T) {
	_ = tracedEnv(t, "0") // sampled out: the exporter's own allocations are not the subject
	count := func(handler http.Handler) float64 {
		return testing.AllocsPerRun(100, func() {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/gone", nil))
		})
	}
	plain := routeAnswering("/v1/gone", http.StatusNotFound, nil)
	recording := routeAnswering("/v1/gone", http.StatusNotFound, func(r *http.Request) { RecordNotFoundCause(r.Context(), NotFoundIDEOff) })
	withoutAccess := count(spanHandler(t, "public", false, plain))
	withAccess := count(spanHandler(t, "public", false, recording))
	if withAccess > withoutAccess {
		t.Errorf("access observer: recording costs %v allocs vs %v without", withAccess, withoutAccess)
	}
	traceOf := func(route Route) http.Handler {
		mux := http.NewServeMux()
		mux.Handle("GET /v1/gone", route.Handler)
		return TraceHandler(mux, TraceOptions{Listener: "public"})
	}
	if a, b := count(traceOf(recording)), count(traceOf(plain)); a > b {
		t.Errorf("TraceHandler: recording costs %v allocs vs %v without", a, b)
	}
}
