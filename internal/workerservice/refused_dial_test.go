package workerservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// closedPortAddr returns an address nothing listens on: dials get ECONNREFUSED.
func closedPortAddr(t *testing.T) string {
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
	addr := closedPortAddr(t)
	registry := health.NewRegistry(2 * time.Second)
	var attempts atomic.Int32
	check := dialCheck(addr)
	if err := registry.RegisterRequired("queue_postgres", func(ctx context.Context) error {
		if attempts.Add(1) == 3 {
			serveOnce(t, addr, true)
		}
		return check(ctx)
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
	want := []time.Duration{preclaimReadinessInitialBackoff, 2 * preclaimReadinessInitialBackoff}
	if got := clock.recordedWaits(); !slices.Equal(got, want) {
		t.Fatalf("waits = %v, want %v", got, want)
	}
	if strings.Contains(logs.String(), "preclaim readiness refused") {
		t.Fatalf("recovered start must not log a refusal: %s", logs.String())
	}
}

func TestPreclaimReadinessRetriesAcceptThenCloseUntilEndpointAnswers(t *testing.T) {
	t.Parallel()
	addr := closedPortAddr(t)
	serveOnce(t, addr, false)
	addr2 := closedPortAddr(t)
	registry := health.NewRegistry(2 * time.Second)
	var attempts atomic.Int32
	check := dialCheck(addr)
	if err := registry.RegisterRequired("domain_postgres", func(ctx context.Context) error {
		if attempts.Add(1) == 3 {
			serveOnce(t, addr2, true)
			return dialCheck(addr2)(ctx)
		}
		return check(ctx)
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
	addr := closedPortAddr(t)
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

func TestRiverWorkerProcessRetriesRefusedDialUntilSuccess(t *testing.T) {
	t.Parallel()
	addr := closedPortAddr(t)
	process := newTestRiverWorkerProcess(t)
	process.budget = time.Minute
	process.sleep = riverStartSleepAfterCheckTimeout(time.Millisecond)
	var attempts atomic.Int32
	process.startClient = func(ctx context.Context) error {
		if attempts.Add(1) == 3 {
			serveOnce(t, addr, true)
		}
		var dialer net.Dialer
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("start producer: %w", err)
		}
		return conn.Close()
	}
	if err := process.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil once the endpoint returns", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("startClient ran %d times, want 3", got)
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
