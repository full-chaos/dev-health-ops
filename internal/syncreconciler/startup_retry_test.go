package syncreconciler

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
	clock *testClock
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

func startClock() *testClock {
	return &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
}

func TestStartRetriesARefusedFirstStepAndEndsReady(t *testing.T) {
	for _, mode := range []string{"closed", "drop"} {
		t.Run(mode, func(t *testing.T) {
			address := refusingEndpoint(t, mode)
			clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
			waits := &waitRecorder{clock: clock}
			calls := 0
			loop, registry, logs := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
				calls++
				if calls <= 3 {
					return Observation{}, dialEndpoint(ctx, address)
				}
				return Observation{}, nil
			}), clock, 5*time.Minute, waits.sleep)
			openReadinessGate(t, registry)
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
		"config":     ErrInvalidConfiguration,
	} {
		t.Run(name, func(t *testing.T) {
			clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
			waits := &waitRecorder{clock: clock}
			calls := 0
			loop, registry, _ := newStartupRetryLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
				calls++
				return Observation{}, permanent
			}), clock, 5*time.Minute, waits.sleep)
			openReadinessGate(t, registry)
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
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	waits := &waitRecorder{clock: clock}
	budget := 20 * time.Second
	loop, registry, logs := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
		return Observation{}, dialEndpoint(ctx, address)
	}), clock, budget, waits.sleep)
	openReadinessGate(t, registry)
	err := loop.Start(context.Background())
	if err == nil {
		t.Fatal("Start = nil, want the refused dial after the budget")
	}
	assertExitErrorHasNoAddress(t, err, address)
	if waits.total() != budget || len(waits.waits) < 2 {
		t.Fatalf("waits=%v total=%v, want waits summing to the %v budget", waits.waits, waits.total(), budget)
	}
	if text := logs.String(); !strings.Contains(text, "cause=unreachable") || strings.Contains(text, address) {
		t.Fatalf("logs must carry the cause and no dial address:\n%s", text)
	}
}

func TestStartWithoutABudgetExitsOnTheFirstRefusedDial(t *testing.T) {
	address := refusingEndpoint(t, "closed")
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	waits := &waitRecorder{clock: clock}
	calls := 0
	loop, registry, _ := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
		calls++
		return Observation{}, dialEndpoint(ctx, address)
	}), clock, 0, waits.sleep)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err == nil || calls != 1 || len(waits.waits) != 0 {
		t.Fatalf("Start=%v calls=%d waits=%v, want one attempt and an error", err, calls, waits.waits)
	}
}

func TestHealthzAnswersWhileTheFirstStepRetries(t *testing.T) {
	address := refusingEndpoint(t, "closed")
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	waiting := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	loop, registry, _ := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
		calls++
		if calls == 1 {
			return Observation{}, dialEndpoint(ctx, address)
		}
		return Observation{}, nil
	}), clock, 5*time.Minute, func(context.Context, time.Duration) {
		close(waiting)
		<-release
	})
	openReadinessGate(t, registry)
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

func TestStartKeepsADegradedFirstStepOutOfTheRetry(t *testing.T) {
	clock := startClock()
	waits := &waitRecorder{clock: clock}
	calls := 0
	loop, registry, _ := newStartupRetryLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		calls++
		return Observation{}, fmt.Errorf("observer stage: %w: %w", ErrDegradedStage, syscall.ECONNREFUSED)
	}), clock, 5*time.Minute, waits.sleep)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatalf("Start = %v, want a degraded first step absorbed as before", err)
	}
	defer loop.Shutdown(context.Background())
	if calls != 1 || len(waits.waits) != 0 {
		t.Fatalf("calls=%d waits=%v, want no retry for a degraded stage", calls, waits.waits)
	}
}

type tickRun struct {
	loop     *Loop
	ticker   *testTicker
	observed chan struct{}
}

func startTickRun(t *testing.T, address string, budget time.Duration, outcomes []string) *tickRun {
	t.Helper()
	clock := startClock()
	call := 0
	loop, registry, _ := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
		index := call
		call++
		switch {
		case index == 0 || outcomes[index-1] == "ok":
			return testObservation(), nil
		case outcomes[index-1] == "dial":
			return Observation{}, dialEndpoint(ctx, address)
		default:
			return Observation{}, &pgconn.PgError{Code: "28P01"}
		}
	}), clock, budget, nil)
	run := &tickRun{loop: loop, observed: make(chan struct{}, 8)}
	loop.stepObserved = func() { run.observed <- struct{}{} }
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	run.ticker = clock.ticker
	clock.mu.Unlock()
	t.Cleanup(func() { _ = loop.Shutdown(context.Background()) })
	return run
}

func (run *tickRun) tick(at time.Time) { run.ticker.ticks <- at }

func (run *tickRun) waitObserved(t *testing.T) {
	t.Helper()
	select {
	case <-run.observed:
	case err := <-run.loop.Errors():
		t.Fatalf("loop went fatal: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("tick was not observed")
	}
}

func TestTickKeepsRunningThroughTransientDialFailuresInsideTheBudget(t *testing.T) {
	address := refusingEndpoint(t, "closed")
	base := startClock().now
	run := startTickRun(t, address, 5*time.Minute, []string{"dial", "dial", "ok", "dial"})
	run.tick(base.Add(10 * time.Second))
	run.waitObserved(t)
	run.tick(base.Add(200 * time.Second))
	run.waitObserved(t)
	run.tick(base.Add(220 * time.Second))
	run.waitObserved(t)
	run.tick(base.Add(900 * time.Second))
	run.waitObserved(t)
}

func TestTickExitsOnceTransientDialFailuresOutlastTheBudget(t *testing.T) {
	address := refusingEndpoint(t, "closed")
	base := startClock().now
	run := startTickRun(t, address, 5*time.Minute, []string{"dial", "dial", "dial"})
	run.tick(base.Add(10 * time.Second))
	run.waitObserved(t)
	run.tick(base.Add(320 * time.Second))
	select {
	case err := <-run.loop.Errors():
		assertExitErrorHasNoAddress(t, err, address)
	case <-time.After(5 * time.Second):
		t.Fatal("a dead database did not surface after the budget")
	}
}

func TestTickPermanentErrorIsFatalAtOnce(t *testing.T) {
	address := refusingEndpoint(t, "closed")
	base := startClock().now
	run := startTickRun(t, address, 5*time.Minute, []string{"auth"})
	run.tick(base.Add(10 * time.Second))
	select {
	case err := <-run.loop.Errors():
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "28P01" {
			t.Fatalf("fatal error = %v, want the auth error", err)
		}
	case <-run.observed:
		t.Fatal("a permanent error was absorbed")
	case <-time.After(5 * time.Second):
		t.Fatal("a permanent error did not end the loop")
	}
}

// assertExitErrorHasNoAddress checks the error as the lifecycle runtime logs it
// ("start component %s: %w"): it names the unreachable cause, never the dial
// address, and still unwraps to the refused dial.
func assertExitErrorHasNoAddress(t *testing.T, err error, address string) {
	t.Helper()
	text := fmt.Errorf("component x failed: %w", err).Error()
	if strings.Contains(text, address) || strings.Contains(text, "127.0.0.1") || !strings.Contains(text, "cause=unreachable") {
		t.Fatalf("exit error text = %q, want the unreachable cause and no dial address", text)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("exit error %q must still unwrap to the refused dial", text)
	}
}
