package investment

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/refusedaddr"
)

func servedRefusedRun(t *testing.T, baseURL string) *servedFailure {
	t.Helper()
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	served, err := newServedDecision(servedTestClient(t, baseURL, 10*time.Second, logs), debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	return servedRunTo(t, served, collector, shadowTestEntries(t, "u-refused"))["u-refused"]
}

// The refused address of the served tests stays refused while other listeners try for it and for fresh ports: its port cannot
// be handed to another test, so the unit is always classed refused and counts no call.
func TestTheRefusedAddressOfTheServedTestsStaysRefusedWhileOthersListen(t *testing.T) {
	addr := refusedaddr.Addr(t)
	if listener, err := net.Listen("tcp", addr); err == nil {
		_ = listener.Close()
		t.Fatalf("listen on the refused address %s succeeded", addr)
	} else if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("listen on %s: %v, want EADDRINUSE", addr, err)
	}
	for range 200 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
	}
	failure := servedRefusedRun(t, "http://"+addr)
	if failure == nil || failure.class != servedOutcomeRefused || failure.calls != 0 {
		t.Fatalf("failure %+v, want class refused and 0 calls", failure)
	}
}

// The cause, executed: a port that was bound, closed and dialled can be taken by another server in between. Here the freed port
// is re-bound at once by another server whose reply is not a decision, and the served unit then reads that reply: it is
// classed invalid_answer, not refused. The bind is retried a bounded number of times and the test FAILS (never skips) if the port
// cannot be re-bound.
func TestAFreedPortTakenByAnotherServerIsNotAReusedRefusedConnection(t *testing.T) {
	freed := httptest.NewServer(http.NotFoundHandler())
	addr := freed.Listener.Addr().String()
	freed.Close()
	var taken net.Listener
	for attempt := 0; attempt < 50 && taken == nil; attempt++ {
		listener, err := net.Listen("tcp", addr)
		if err == nil {
			taken = listener
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if taken == nil {
		t.Fatalf("the freed port %s could not be re-bound in 50 attempts: the cell cannot show the cause", addr)
	}
	other := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"unrelated":"another test's reply"}`))
	}))
	_ = other.Listener.Close()
	other.Listener = taken
	other.Start()
	t.Cleanup(other.Close)
	failure := servedRefusedRun(t, "http://"+addr)
	if failure == nil || failure.class != servedOutcomeInvalidAnswer {
		t.Fatalf("failure %+v, want class invalid_answer: a taken port is not a refused connection", failure)
	}
}
