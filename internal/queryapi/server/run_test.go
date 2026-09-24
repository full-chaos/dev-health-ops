package server

import (
	"bytes"
	"context"
	"log/slog"
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

// The binary always ignored its arguments; Run keeps that, and says so in
// its log so an unexpected argument is visible.
func TestRunIgnoresArgumentsAndLogsThem(t *testing.T) {
	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, []string{"extra"}, lookupOf(map[string]string{"QUERY_API_ADDR": addr}), &stdout, &stderr)
	}()
	waitForHealthz(t, addr, done)
	cancel()
	if code := <-done; code != exitOK {
		t.Fatalf("Run with an argument returned %d, want %d", code, exitOK)
	}
	if !strings.Contains(stdout.String(), `"msg":"query-api takes no arguments; ignoring them"`) ||
		!strings.Contains(stdout.String(), `"extra"`) {
		t.Fatalf("the ignored argument is not logged: %s", stdout.String())
	}
}

// Run installs its logger as the process default only for the run: a caller
// that continues after Run logs through its own handler again.
func TestRunRestoresTheDefaultLogger(t *testing.T) {
	before := slog.Default()
	var callerOutput bytes.Buffer
	callerLogger := slog.New(slog.NewTextHandler(&callerOutput, nil))
	slog.SetDefault(callerLogger)
	t.Cleanup(func() { slog.SetDefault(before) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	Run(ctx, nil, lookupOf(map[string]string{"QUERY_API_ADDR": freeAddr(t)}), &stdout, &stderr)
	if slog.Default() != callerLogger {
		t.Fatal("Run left its own logger as the process default")
	}
	slog.Info("after run")
	if strings.Contains(stdout.String(), "after run") || !strings.Contains(callerOutput.String(), "after run") {
		t.Fatalf("a log after Run went to query-api's stream: query-api %q, caller %q", stdout.String(), callerOutput.String())
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
