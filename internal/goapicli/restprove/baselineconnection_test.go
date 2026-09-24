package restprove

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// countingListener counts the TCP connections Accept hands out, so a test
// can tell a fresh connection from a reused pooled one without depending on
// timing-sensitive "kill the connection and see what breaks" behaviour.
type countingListener struct {
	net.Listener
	accepts atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
	}
	return conn, err
}

// startCountingServer serves ok 200 responses (the connection-reuse
// question is independent of status: CHAOS-6580's root cause was a Python
// error path, but the fix must not depend on noticing a 5xx to be correct)
// and returns its URL plus the listener whose Accept count is the number of
// distinct TCP connections the server has handed out.
func startCountingServer(t *testing.T) (string, *countingListener) {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &countingListener{Listener: raw}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return "http://" + raw.Addr().String(), listener
}

// TestDoRESTBaselineNeverReusesAConnection is CHAOS-6580's guard: the
// Python api's unhandled-error path answers a real response but drops the
// TCP connection without `Connection: close`, so a reused pooled connection
// fails the NEXT baseline request with a transport error indistinguishable
// from a real outage. Two consecutive baseline requests to the same host
// must therefore open two separate connections -- not because either
// answered an error here (both answer 200; the fix must not depend on
// noticing one), but because doREST(baseline=true) always closes.
func TestDoRESTBaselineNeverReusesAConnection(t *testing.T) {
	url, listener := startCountingServer(t)
	client := goapiproof.NewLegClient(0)
	for i := 0; i < 2; i++ {
		if _, err := doREST(context.Background(), client, url, http.MethodGet, "/x", nil, nil, nil, true, 5*time.Second); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	if got := listener.accepts.Load(); got != 2 {
		t.Fatalf("baseline accepted %d connection(s) for 2 requests, want 2 (no reuse)", got)
	}
}

// TestDoRESTCandidateReusesAConnection pins the other half: the candidate
// (Go) leg is not part of CHAOS-6580's class, so it keeps ordinary
// keep-alive reuse -- the fix must be scoped to the baseline leg only, never
// a blanket "never reuse anything".
func TestDoRESTCandidateReusesAConnection(t *testing.T) {
	url, listener := startCountingServer(t)
	client := goapiproof.NewLegClient(0)
	for i := 0; i < 2; i++ {
		if _, err := doREST(context.Background(), client, url, http.MethodGet, "/x", nil, nil, nil, false, 5*time.Second); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	client.CloseIdleConnections()
	if got := listener.accepts.Load(); got != 1 {
		t.Fatalf("candidate accepted %d connection(s) for 2 requests, want 1 (reused)", got)
	}
}
