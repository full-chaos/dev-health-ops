package joboutbox

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

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

func endpointDSN(address string) string {
	return "postgres://u:p@" + address + "/db?sslmode=disable&connect_timeout=2"
}

// dialEndpoint dials like the repository does and classifies the failure the
// way the repository does.
func dialEndpoint(ctx context.Context, address string) error {
	pool, err := pgxpool.New(ctx, endpointDSN(address))
	if err != nil {
		return err
	}
	defer pool.Close()
	repository, err := NewRepository(pool)
	if err != nil {
		return err
	}
	_, err = repository.ClaimDue(ctx, time.Now(), 1, time.Minute)
	return err
}

type waitRecorder struct {
	clock *testReconcilerClock
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

func TestRepositoryKeepsATransientDialCauseButNotItsText(t *testing.T) {
	for _, mode := range []string{"closed", "drop"} {
		t.Run(mode, func(t *testing.T) {
			address := refusingEndpoint(t, mode)
			err := dialEndpoint(context.Background(), address)
			if !errors.Is(err, ErrUnavailable) || err.Error() != ErrUnavailable.Error() {
				t.Fatalf("err = %v, want the bare ErrUnavailable text", err)
			}
			if !postgresTransient(err) {
				t.Fatalf("a refused dial must stay classifiable as transient: %v", err)
			}
		})
	}
}

func TestClassifyStrandErrorKeepsPermanentCausesFinal(t *testing.T) {
	for name, permanent := range map[string]error{
		"auth":       &pgconn.PgError{Code: "28P01"},
		"permission": &pgconn.PgError{Code: "42501"},
		"decode":     errors.New("scan failed"),
	} {
		t.Run(name, func(t *testing.T) {
			err := classifyStrandError(permanent)
			if postgresTransient(err) {
				t.Fatalf("%s must not be transient: %v", name, err)
			}
			if name == "permission" && !errors.Is(err, ErrNotAuthorized) {
				t.Fatalf("permission = %v, want ErrNotAuthorized", err)
			}
		})
	}
}

func TestStartRetriesARefusedFirstStepAndEndsReady(t *testing.T) {
	for _, mode := range []string{"closed", "drop"} {
		t.Run(mode, func(t *testing.T) {
			address := refusingEndpoint(t, mode)
			clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
			waits := &waitRecorder{clock: clock}
			calls := 0
			loop, registry, logs := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (StepResult, error) {
				calls++
				if calls <= 3 {
					return StepResult{}, dialEndpoint(ctx, address)
				}
				return StepResult{}, nil
			}), clock, 5*time.Minute, waits.sleep)
			openReconcilerReadiness(t, registry)
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
		"permission": ErrNotAuthorized,
		"config":     ErrInvalidConfiguration,
		"decode":     ErrUnavailable,
	} {
		t.Run(name, func(t *testing.T) {
			clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
			waits := &waitRecorder{clock: clock}
			calls := 0
			loop, registry, _ := newStartupRetryLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
				calls++
				return StepResult{}, permanent
			}), clock, 5*time.Minute, waits.sleep)
			openReconcilerReadiness(t, registry)
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
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	waits := &waitRecorder{clock: clock}
	budget := 20 * time.Second
	loop, registry, logs := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (StepResult, error) {
		return StepResult{}, dialEndpoint(ctx, address)
	}), clock, budget, waits.sleep)
	openReconcilerReadiness(t, registry)
	if err := loop.Start(context.Background()); err == nil {
		t.Fatal("Start = nil, want the refused dial after the budget")
	}
	if waits.total() != budget || len(waits.waits) < 2 {
		t.Fatalf("waits=%v total=%v, want waits summing to the %v budget", waits.waits, waits.total(), budget)
	}
	if text := logs.String(); !strings.Contains(text, "cause=unreachable") || strings.Contains(text, address) {
		t.Fatalf("logs must carry the cause and no dial address:\n%s", text)
	}
}

func TestStartWithoutABudgetExitsOnTheFirstRefusedDial(t *testing.T) {
	address := refusingEndpoint(t, "closed")
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	waits := &waitRecorder{clock: clock}
	calls := 0
	loop, registry, _ := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (StepResult, error) {
		calls++
		return StepResult{}, dialEndpoint(ctx, address)
	}), clock, 0, waits.sleep)
	openReconcilerReadiness(t, registry)
	if err := loop.Start(context.Background()); err == nil || calls != 1 || len(waits.waits) != 0 {
		t.Fatalf("Start=%v calls=%d waits=%v, want one attempt and an error", err, calls, waits.waits)
	}
}

func TestHealthzAnswersWhileTheFirstStepRetries(t *testing.T) {
	address := refusingEndpoint(t, "closed")
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	waiting := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	loop, registry, _ := newStartupRetryLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (StepResult, error) {
		calls++
		if calls == 1 {
			return StepResult{}, dialEndpoint(ctx, address)
		}
		return StepResult{}, nil
	}), clock, 5*time.Minute, func(context.Context, time.Duration) {
		close(waiting)
		<-release
	})
	openReconcilerReadiness(t, registry)
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
