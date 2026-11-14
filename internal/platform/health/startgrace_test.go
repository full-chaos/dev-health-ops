package health

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// graceFixture is a registry on an injected clock with one required check
// that, while failing, records the level DependencyRefusalLevel gives it.
type graceFixture struct {
	registry *Registry
	start    time.Time
	elapsed  atomic.Int64
	failing  atomic.Bool

	mu     sync.Mutex
	levels []slog.Level
}

func newGraceFixture(t *testing.T) *graceFixture {
	t.Helper()
	fixture := &graceFixture{start: time.Date(2026, 11, 14, 8, 0, 0, 0, time.UTC)}
	registry := NewRegistry(time.Second)
	registry.startedAt = fixture.start
	registry.now = func() time.Time { return fixture.start.Add(time.Duration(fixture.elapsed.Load())) }
	if err := registry.RegisterRequired("queue_postgres", func(ctx context.Context) error {
		if !fixture.failing.Load() {
			return nil
		}
		level := DependencyRefusalLevel(ctx)
		fixture.mu.Lock()
		fixture.levels = append(fixture.levels, level)
		fixture.mu.Unlock()
		return errors.New("queue pool not open yet")
	}); err != nil {
		t.Fatal(err)
	}
	fixture.registry = registry
	return fixture
}

func (fixture *graceFixture) at(elapsed time.Duration) { fixture.elapsed.Store(int64(elapsed)) }

// refuse runs one probe answer with the check failing and returns the level
// the check logged at.
func (fixture *graceFixture) refuse(t *testing.T, probe func(context.Context) Readiness) slog.Level {
	t.Helper()
	fixture.failing.Store(true)
	fixture.mu.Lock()
	fixture.levels = nil
	fixture.mu.Unlock()
	if readiness := probe(context.Background()); readiness.Ready {
		t.Fatalf("a failing check answered Ready: %+v", readiness)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.levels) != 1 {
		t.Fatalf("the check ran %d times, want 1", len(fixture.levels))
	}
	return fixture.levels[0]
}

func (fixture *graceFixture) answerReady(t *testing.T) {
	t.Helper()
	fixture.failing.Store(false)
	if readiness := fixture.registry.Readiness(context.Background()); !readiness.Ready {
		t.Fatalf("a passing check did not answer Ready: %+v", readiness)
	}
}

func TestNewRegistryUsesReadinessStartGrace(t *testing.T) {
	t.Parallel()
	if got := NewRegistry(time.Second).startGrace; got != ReadinessStartGrace {
		t.Fatalf("startGrace = %s, want ReadinessStartGrace %s", got, ReadinessStartGrace)
	}
}

func TestDependencyRefusalIsWarnInsideStartGraceBeforeFirstReady(t *testing.T) {
	t.Parallel()
	fixture := newGraceFixture(t)
	fixture.at(3 * time.Second)
	if got := fixture.refuse(t, fixture.registry.CheckRequired); got != slog.LevelWarn {
		t.Errorf("preclaim-path refusal 3s after start, never Ready: level %s, want WARN", got)
	}
	fixture.registry.SetReady(true)
	fixture.at(14 * time.Second)
	if got := fixture.refuse(t, fixture.registry.Readiness); got != slog.LevelWarn {
		t.Errorf("/readyz refusal 14s after start, never Ready: level %s, want WARN", got)
	}
}

func TestDependencyRefusalIsErrorAfterFirstReady(t *testing.T) {
	t.Parallel()
	fixture := newGraceFixture(t)
	fixture.registry.SetReady(true)
	fixture.at(5 * time.Second)
	fixture.answerReady(t)
	fixture.at(6 * time.Second)
	if got := fixture.refuse(t, fixture.registry.Readiness); got != slog.LevelError {
		t.Errorf("refusal inside the grace after first Ready: level %s, want ERROR", got)
	}
	if got := fixture.refuse(t, fixture.registry.CheckRequired); got != slog.LevelError {
		t.Errorf("CheckRequired refusal after first Ready: level %s, want ERROR", got)
	}
}

func TestDependencyRefusalAfterStartGraceIsErrorWhenNeverReady(t *testing.T) {
	t.Parallel()
	fixture := newGraceFixture(t)
	fixture.registry.SetReady(true)
	for _, step := range []struct {
		elapsed time.Duration
		want    slog.Level
	}{
		{ReadinessStartGrace - time.Nanosecond, slog.LevelWarn},
		{ReadinessStartGrace, slog.LevelError},
		{ReadinessStartGrace + time.Hour, slog.LevelError},
	} {
		fixture.at(step.elapsed)
		if got := fixture.refuse(t, fixture.registry.Readiness); got != step.want {
			t.Errorf("never Ready, refusal %s after start: level %s, want %s", step.elapsed, got, step.want)
		}
	}
}

func TestFirstReadyLatchSurvivesAGateClose(t *testing.T) {
	t.Parallel()
	fixture := newGraceFixture(t)
	fixture.registry.SetReady(true)
	fixture.at(2 * time.Second)
	fixture.answerReady(t)
	fixture.registry.SetReady(false)
	fixture.registry.SetReady(true)
	fixture.at(3 * time.Second)
	if got := fixture.refuse(t, fixture.registry.Readiness); got != slog.LevelError {
		t.Errorf("refusal after a gate close and reopen that followed first Ready: level %s, want ERROR", got)
	}
}

func TestDependencyRefusalLevelWithoutARegistryIsError(t *testing.T) {
	t.Parallel()
	if got := DependencyRefusalLevel(context.Background()); got != slog.LevelError {
		t.Fatalf("level for a check not run by a registry = %s, want ERROR", got)
	}
}

func TestReadyzAnswerIsTheSameInsideAndAfterStartGrace(t *testing.T) {
	t.Parallel()
	fixture := newGraceFixture(t)
	fixture.registry.SetReady(true)
	fixture.failing.Store(true)
	server, err := NewServer(ServerOptions{Address: "127.0.0.1:0", Registry: fixture.registry})
	if err != nil {
		t.Fatal(err)
	}
	answer := func(elapsed time.Duration) (int, string, Readiness) {
		fixture.at(elapsed)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return response.Code, response.Body.String(), fixture.registry.Readiness(context.Background())
	}
	insideCode, insideBody, insideReadiness := answer(time.Second)
	afterCode, afterBody, afterReadiness := answer(ReadinessStartGrace)
	if insideCode != http.StatusServiceUnavailable || afterCode != insideCode {
		t.Errorf("/readyz status inside grace %d, after %d; want 503 both", insideCode, afterCode)
	}
	if insideBody != afterBody {
		t.Errorf("/readyz body differs:\ninside: %q\nafter:  %q", insideBody, afterBody)
	}
	if !reflect.DeepEqual(insideReadiness, afterReadiness) {
		t.Errorf("Readiness differs:\ninside: %+v\nafter:  %+v", insideReadiness, afterReadiness)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if want := []slog.Level{slog.LevelWarn, slog.LevelWarn, slog.LevelError, slog.LevelError}; !reflect.DeepEqual(fixture.levels, want) {
		t.Errorf("levels = %v, want %v (only the log level changes)", fixture.levels, want)
	}
}
