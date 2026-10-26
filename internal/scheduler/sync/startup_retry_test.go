package sync

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// refusingEndpoint returns the address of a listener that refuses the dial
// ("closed") or accepts and drops it ("drop").
func refusingEndpoint(t *testing.T, mode string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if mode == "closed" {
		_ = listener.Close()
		return address
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	return address
}

func dialEndpoint(ctx context.Context, address string) error {
	config, err := pgconn.ParseConfig("postgres://u:p@" + address + "/db?sslmode=disable&connect_timeout=2")
	if err != nil {
		return err
	}
	connection, err := pgconn.ConnectConfig(ctx, config)
	if err == nil {
		_ = connection.Close(ctx)
	}
	return err
}

type waitRecorder struct {
	clock *testLoopClock
	waits []time.Duration
}

func (recorder *waitRecorder) sleep(_ context.Context, wait time.Duration) {
	recorder.waits = append(recorder.waits, wait)
	recorder.clock.mu.Lock()
	recorder.clock.now = recorder.clock.now.Add(wait)
	recorder.clock.mu.Unlock()
}

func (recorder *waitRecorder) total() time.Duration {
	var sum time.Duration
	for _, wait := range recorder.waits {
		sum += wait
	}
	return sum
}

func TestStartRetriesARefusedFirstDialAndEndsReady(t *testing.T) {
	for _, mode := range []string{"closed", "drop"} {
		t.Run(mode, func(t *testing.T) {
			address := refusingEndpoint(t, mode)
			clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
			waits := &waitRecorder{clock: clock}
			calls := 0
			loop, registry, logs := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int, _ Coordinator) (HandoffResult, error) {
				calls++
				if calls <= 3 {
					return HandoffResult{}, dialEndpoint(ctx, address)
				}
				return HandoffResult{}, nil
			}), clock, 5*time.Minute, waits.sleep)
			openLoopReadiness(t, registry)
			if err := loop.Start(context.Background()); err != nil {
				t.Fatalf("Start = %v, want nil after the endpoint returned", err)
			}
			defer loop.Shutdown(context.Background())
			if calls != 4 || len(waits.waits) != 3 || waits.waits[0] != 2*time.Second || waits.waits[1] != 4*time.Second {
				t.Fatalf("calls=%d waits=%v, want 4 calls and 2s,4s,8s waits", calls, waits.waits)
			}
			if status := registry.Readiness(context.Background()); !status.Ready {
				t.Fatalf("readiness after recovery = %#v", status)
			}
			text := logs.String()
			if !strings.Contains(text, "cause=unreachable") || strings.Contains(text, "127.0.0.1") || strings.Contains(text, address) {
				t.Fatalf("retry logs must carry cause=unreachable and no dial address:\n%s", text)
			}
		})
	}
}

func TestStartExitsAtOnceOnAPermanentError(t *testing.T) {
	for name, permanent := range map[string]error{
		"auth":       &pgconn.PgError{Code: "28P01"},
		"permission": &pgconn.PgError{Code: "42501"},
		"config":     errors.New("invalid configuration"),
	} {
		t.Run(name, func(t *testing.T) {
			clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
			waits := &waitRecorder{clock: clock}
			calls := 0
			loop, registry, _ := newStartupRetryLoop(t, loopStepFunc(func(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
				calls++
				return HandoffResult{}, permanent
			}), clock, 5*time.Minute, waits.sleep)
			openLoopReadiness(t, registry)
			if err := loop.Start(context.Background()); !errors.Is(err, permanent) {
				t.Fatalf("Start = %v, want the permanent error", err)
			}
			if calls != 1 || len(waits.waits) != 0 {
				t.Fatalf("calls=%d waits=%v, want one attempt and no wait", calls, waits.waits)
			}
		})
	}
}

func TestStartExitsWhenTheRetryBudgetIsSpent(t *testing.T) {
	address := refusingEndpoint(t, "closed")
	clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
	waits := &waitRecorder{clock: clock}
	budget := 20 * time.Second
	loop, registry, logs := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int, _ Coordinator) (HandoffResult, error) {
		return HandoffResult{}, dialEndpoint(ctx, address)
	}), clock, budget, waits.sleep)
	openLoopReadiness(t, registry)
	err := loop.Start(context.Background())
	if err == nil {
		t.Fatal("Start = nil, want the refused dial after the budget")
	}
	assertExitErrorHasNoAddress(t, err, address)
	if waits.total() != budget || len(waits.waits) < 2 {
		t.Fatalf("waits=%v total=%v, want waits summing to the %v budget", waits.waits, waits.total(), budget)
	}
	if text := logs.String(); !strings.Contains(text, "cause=unreachable") || strings.Contains(text, address) {
		t.Fatalf("give-up logs must carry the cause and no dial address:\n%s", text)
	}
}

func TestStartWithoutABudgetExitsOnTheFirstRefusedDial(t *testing.T) {
	address := refusingEndpoint(t, "closed")
	clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
	waits := &waitRecorder{clock: clock}
	calls := 0
	loop, registry, _ := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int, _ Coordinator) (HandoffResult, error) {
		calls++
		return HandoffResult{}, dialEndpoint(ctx, address)
	}), clock, 0, waits.sleep)
	openLoopReadiness(t, registry)
	if err := loop.Start(context.Background()); err == nil || calls != 1 || len(waits.waits) != 0 {
		t.Fatalf("Start=%v calls=%d waits=%v, want one attempt and an error", err, calls, waits.waits)
	}
}

func TestHealthzAnswersWhileTheFirstDialRetries(t *testing.T) {
	address := refusingEndpoint(t, "closed")
	clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
	waiting := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	loop, registry, _ := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int, _ Coordinator) (HandoffResult, error) {
		calls++
		if calls == 1 {
			return HandoffResult{}, dialEndpoint(ctx, address)
		}
		return HandoffResult{}, nil
	}), clock, 5*time.Minute, func(context.Context, time.Duration) {
		close(waiting)
		<-release
	})
	openLoopReadiness(t, registry)
	server, err := health.NewServer(health.ServerOptions{Registry: registry, Address: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- loop.Start(context.Background()) }()
	select {
	case <-waiting:
	case err := <-started:
		t.Fatalf("Start = %v, want it to be retrying", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start never retried")
	}
	for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable} {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != want {
			t.Fatalf("%s while retrying = %d, want %d", path, recorder.Code, want)
		}
	}
	close(release)
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	defer loop.Shutdown(context.Background())
}

// assertExitErrorHasNoAddress checks the error as the lifecycle runtime logs it
// ("start component %s: %w"): it names the unreachable cause, never the dial
// address, and still unwraps to the refused dial.
func assertExitErrorHasNoAddress(t *testing.T, err error, address string) {
	t.Helper()
	text := fmt.Errorf("start component x: %w", err).Error()
	if strings.Contains(text, address) || strings.Contains(text, "127.0.0.1") || !strings.Contains(text, "cause=unreachable") {
		t.Fatalf("exit error text = %q, want the unreachable cause and no dial address", text)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("exit error %q must still unwrap to the refused dial", text)
	}
}
