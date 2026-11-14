package workerservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/refusedaddr"
)

// dialCheck is a required check that dials addr and reads one byte, the way a
// pooler answers once it is up. A refused dial or a peer that accepts and
// closes surfaces the raw network error.
func dialCheck(addr string) health.CheckFunc {
	return func(ctx context.Context) error {
		var dialer net.Dialer
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return dependencyCheckFailed(err)
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		one := make([]byte, 1)
		if _, err := io.ReadFull(conn, one); err != nil {
			return dependencyCheckFailed(err)
		}
		return nil
	}
}

func serveOnce(t *testing.T, addr string, answer bool) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			if answer {
				_, _ = conn.Write([]byte{1})
			}
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func TestPreclaimReadinessRetriesRefusedDialUntilEndpointReturns(t *testing.T) {
	t.Parallel()
	registry := health.NewRegistry(2 * time.Second)
	var attempts atomic.Int32
	// The refusals come from the check, not from a port that has to stay free for two attempts: a listener another parallel test
	// bound on a freed port answered an early dial and the count was the port's (CHAOS-13696).
	if err := registry.RegisterRequired("queue_postgres", func(context.Context) error {
		if attempts.Add(1) < 3 {
			return dependencyCheckFailed(refusedDial())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock := &preclaimFakeClock{}
	var logs bytes.Buffer
	component := preclaimReadinessComponent{
		registry: registry, logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		budget: time.Minute, now: clock.now, sleep: clock.sleep,
	}
	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil once the endpoint returns", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("check ran %d times, want 3", got)
	}
	want := []time.Duration{2 * time.Second, 4 * time.Second}
	if got := clock.recordedWaits(); !slices.Equal(got, want) {
		t.Fatalf("waits = %v, want %v", got, want)
	}
	if strings.Contains(logs.String(), "preclaim readiness refused") {
		t.Fatalf("recovered start must not log a refusal: %s", logs.String())
	}
}

func TestPreclaimReadinessRetriesAcceptThenCloseUntilEndpointAnswers(t *testing.T) {
	t.Parallel()
	// Both endpoints are listening from the start (held for the test), so no attempt depends on a port that must stay free: the
	// first two attempts reach a peer that accepts and closes, the third a peer that answers.
	closing := serveOnce(t, "127.0.0.1:0", false)
	answering := serveOnce(t, "127.0.0.1:0", true)
	registry := health.NewRegistry(2 * time.Second)
	var attempts atomic.Int32
	closingCheck, answeringCheck := dialCheck(closing.Addr().String()), dialCheck(answering.Addr().String())
	if err := registry.RegisterRequired("domain_postgres", func(ctx context.Context) error {
		if attempts.Add(1) == 3 {
			return answeringCheck(ctx)
		}
		return closingCheck(ctx)
	}); err != nil {
		t.Fatal(err)
	}
	clock := &preclaimFakeClock{}
	component := preclaimReadinessComponent{registry: registry, budget: time.Minute, now: clock.now, sleep: clock.sleep}
	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("check ran %d times, want 3", got)
	}
}

func TestPreclaimReadinessRefusedDialExitsAfterBudget(t *testing.T) {
	t.Parallel()
	addr := refusedaddr.Addr(t)
	registry := health.NewRegistry(2 * time.Second)
	var attempts atomic.Int32
	check := dialCheck(addr)
	if err := registry.RegisterRequired("queue_postgres", func(ctx context.Context) error {
		attempts.Add(1)
		return check(ctx)
	}); err != nil {
		t.Fatal(err)
	}
	clock := &preclaimFakeClock{}
	var logs bytes.Buffer
	component := preclaimReadinessComponent{
		registry: registry, logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		budget: 10 * time.Second, now: clock.now, sleep: clock.sleep,
	}
	if err := component.Start(context.Background()); !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("Start() = %v, want errWorkerDependencyUnavailable", err)
	}
	if attempts.Load() < 2 {
		t.Fatalf("check ran %d times, want a retry before giving up", attempts.Load())
	}
	if !strings.Contains(logs.String(), "retry_budget_exhausted") {
		t.Fatalf("want reason retry_budget_exhausted: %s", logs.String())
	}
	if strings.Contains(logs.String(), addr) {
		t.Fatalf("log must not carry the dial address: %s", logs.String())
	}
}

// refusedDial is the error a dial to a port nothing listens on returns: an *net.OpError over an *os.SyscallError over
// ECONNREFUSED, the shape the retry classification reads, with no port and no network in it.
func refusedDial() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
}

// The retry count is a function of the controlled clock and the refusals the hook returns, never of a port: the first form of
// this test dialled a real closed port, and a listener another parallel test bound on that freed port answered the second
// attempt, so Start succeeded after 2 attempts (CHAOS-13696; same class as CHAOS-6446: a count that depended on the machine).
func TestRiverWorkerProcessRetriesRefusedDialUntilSuccess(t *testing.T) {
	t.Parallel()
	process := newTestRiverWorkerProcess(t)
	process.budget = time.Minute
	clock := newFakeRetryClock()
	process.now, process.sleep = clock.Now, clock.Sleep
	begin := clock.Now()
	var attempts atomic.Int32
	process.startClient = func(context.Context) error {
		if attempts.Add(1) < 3 {
			return fmt.Errorf("start producer: %w", refusedDial())
		}
		return nil
	}
	if err := process.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil once the endpoint returns", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("startClient ran %d times, want 3", got)
	}
	if waited := clock.Now().Sub(begin); waited != 6*time.Second {
		t.Fatalf("waited %v on the controlled clock, want 2s + 4s", waited)
	}
}

// The hook above returns the error shape of a refused dial; this pins that the retry classifier of the producer start reads that
// shape as retryable and a dial that failed for another reason (permission denied) as final.
func TestProducerStartRetryClassifierReadsARefusedDialAsRetryableAndADeniedOneAsFinal(t *testing.T) {
	t.Parallel()
	if !isRiverProducerStartTimeout(fmt.Errorf("start producer: %w", refusedDial())) {
		t.Fatal("a refused dial is not retryable")
	}
	denied := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.EACCES)}
	if isRiverProducerStartTimeout(fmt.Errorf("start producer: %w", denied)) {
		t.Fatal("a dial refused for permission (EACCES) is retryable, want final")
	}
}

func TestPreclaimReadinessPermanentErrorStillExitsAtOnce(t *testing.T) {
	t.Parallel()
	for name, failure := range map[string]error{
		"bad_config": errors.New("parse dsn: invalid"),
		"posture":    errors.New("posture manifest mismatch"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			registry := health.NewRegistry(2 * time.Second)
			var attempts atomic.Int32
			if err := registry.RegisterRequired("domain_postgres", func(context.Context) error {
				attempts.Add(1)
				return dependencyCheckFailed(failure)
			}); err != nil {
				t.Fatal(err)
			}
			clock := &preclaimFakeClock{}
			component := preclaimReadinessComponent{registry: registry, budget: time.Minute, now: clock.now, sleep: clock.sleep}
			if err := component.Start(context.Background()); err == nil {
				t.Fatal("Start() = nil, want an exit")
			}
			if attempts.Load() != 1 || len(clock.recordedWaits()) != 0 {
				t.Fatalf("attempts=%d waits=%v, want 1 and none", attempts.Load(), clock.recordedWaits())
			}
		})
	}
}

func TestWorkerStartupRetryDoesNotAttemptAtOrAfterBudget(t *testing.T) {
	t.Parallel()
	process := newTestRiverWorkerProcess(t)
	process.budget = 20 * time.Second
	clock := newFakeRetryClock()
	process.now, process.sleep = clock.Now, clock.Sleep
	begin := clock.Now()
	var starts []time.Duration
	process.startClient = func(context.Context) error {
		starts = append(starts, clock.Now().Sub(begin))
		return fmt.Errorf("start producer: %w", syscall.ECONNREFUSED)
	}
	if err := process.Start(context.Background()); !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("Start() = %v, want the dial cause", err)
	}
	want := []time.Duration{0, 2 * time.Second, 6 * time.Second, 14 * time.Second}
	if !slices.Equal(starts, want) {
		t.Fatalf("attempt starts = %v, want %v (none at or after the 20s budget)", starts, want)
	}
}
