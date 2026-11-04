package workerservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
)

const startMaskSentinel = "dependency unreachable: cause=unreachable"

type failingChild struct{ err error }

func (failingChild) Name() string                      { return "failing-child" }
func (child failingChild) Start(context.Context) error { return child.err }
func (failingChild) Shutdown(context.Context) error    { return nil }

func dialErrorNamingAddress() error {
	return fmt.Errorf("failed to connect to host=10.1.2.3:6432 user=app database=db: %w", syscall.ECONNREFUSED)
}

// runAndLog runs the component the way the shell does and returns the exit log line.
func runAndLog(t *testing.T, component workerProcessComponent) (error, string) {
	t.Helper()
	runtime, err := lifecycle.New(lifecycle.Options{ShutdownTimeout: time.Second, Components: []lifecycle.Component{component}})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	runErr := runtime.Run(context.Background())
	slog.New(slog.NewTextHandler(&out, nil)).Error("worker exited", "error", runErr)
	return runErr, out.String()
}

func assertMasked(t *testing.T, err error, line string) {
	t.Helper()
	if err == nil {
		t.Fatal("Run() = nil, want the start error")
	}
	if !strings.Contains(line, startMaskSentinel) {
		t.Fatalf("exit log line lacks the sentinel: %s", line)
	}
	for _, leak := range []string{"10.1.2.3", "6432", "user=app"} {
		if strings.Contains(line, leak) {
			t.Fatalf("exit log line holds %q: %s", leak, line)
		}
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("errors.Is lost the dial cause: %v", err)
	}
}

func TestWorkerStartGiveUpErrorHoldsSentinelNotAddress(t *testing.T) {
	t.Parallel()
	presence := &presenceFake{errs: []error{dialErrorNamingAddress()}}
	err, line := runAndLog(t, workerProcessComponent{presence: presence})
	assertMasked(t, err, line)
}

func TestWorkerStartBudgetSpentErrorHoldsSentinelNotAddress(t *testing.T) {
	t.Parallel()
	failures := make([]error, 8)
	for index := range failures {
		failures[index] = dialErrorNamingAddress()
	}
	clock := &preclaimFakeClock{}
	component := workerProcessComponent{
		presence: &presenceFake{errs: failures}, startBudget: 10 * time.Second, now: clock.now, sleep: clock.sleep,
	}
	err, line := runAndLog(t, component)
	assertMasked(t, err, line)
	if got := len(clock.recordedWaits()); got < 2 {
		t.Fatalf("waits = %d, want retries before the give-up", got)
	}
}

func TestWorkerStartChildFailureHoldsSentinelNotAddress(t *testing.T) {
	t.Parallel()
	component := workerProcessComponent{components: []lifecycle.Component{failingChild{err: dialErrorNamingAddress()}}}
	err, line := runAndLog(t, component)
	assertMasked(t, err, line)
}

func TestWorkerStartFinalErrorIsUnchanged(t *testing.T) {
	t.Parallel()
	final := &pgconn.PgError{Code: "28P01", Message: "password authentication failed"}
	presence := &presenceFake{errs: []error{final}}
	err, line := runAndLog(t, workerProcessComponent{presence: presence})
	var got *pgconn.PgError
	if !errors.As(err, &got) || got != final || strings.Contains(line, startMaskSentinel) || !strings.Contains(line, "password authentication failed") {
		t.Fatalf("final error changed: err=%v line=%s", err, line)
	}
}
