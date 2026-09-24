package server

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// lookupOf serves settings from a map only, like a process whose
// environment holds exactly these keys.
func lookupOf(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// freeAddr returns a loopback address nothing listens on right now.
func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// runInBackground starts Run and returns the channel its exit code arrives on.
func runInBackground(ctx context.Context, args []string, lookup func(string) (string, bool)) (<-chan int, *bytes.Buffer) {
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- Run(ctx, args, lookup, &stdout, &stderr) }()
	return done, &stderr
}

func waitForHealthz(t *testing.T, addr string, done <-chan int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case code := <-done:
			t.Fatalf("Run returned %d before serving /healthz on %s", code, addr)
		default:
		}
		response, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("query-api never served /healthz on %s", addr)
}

func TestRunRefusesArgumentsWithAUsageExit(t *testing.T) {
	// Bounded, on a free address: a Run that ignored its arguments would
	// serve until this deadline and return 0, not hang the test.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	code := Run(ctx, []string{"--port=1"}, lookupOf(map[string]string{"QUERY_API_ADDR": freeAddr(t)}), &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("Run with an argument returned %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "takes none") {
		t.Fatalf("stderr does not name the usage error: %q", stderr.String())
	}
}

func TestRunShutsDownWithExitZeroWhenTheContextEnds(t *testing.T) {
	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runInBackground(ctx, nil, lookupOf(map[string]string{"QUERY_API_ADDR": addr}))
	waitForHealthz(t, addr, done)
	cancel()
	select {
	case code := <-done:
		if code != exitOK {
			t.Fatalf("Run returned %d after a clean shutdown, want %d", code, exitOK)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after its context ended")
	}
}

func TestRunReturnsFailureWhenTheListenerCannotBind(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	done, _ := runInBackground(context.Background(), nil, lookupOf(map[string]string{"QUERY_API_ADDR": occupied.Addr().String()}))
	select {
	case code := <-done:
		if code != exitFailure {
			t.Fatalf("Run returned %d when its address was taken, want %d", code, exitFailure)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return when its listener could not bind")
	}
}

// The injected lookup, not the process environment, decides the address:
// the process env points at a taken port, the lookup at a free one, and Run
// serves on the free one.
func TestRunReadsItsSettingsFromTheLookupNotTheProcessEnvironment(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	t.Setenv("QUERY_API_ADDR", occupied.Addr().String())

	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runInBackground(ctx, nil, lookupOf(map[string]string{"QUERY_API_ADDR": addr}))
	waitForHealthz(t, addr, done)
	cancel()
	if code := <-done; code != exitOK {
		t.Fatalf("Run returned %d, want %d", code, exitOK)
	}
}
