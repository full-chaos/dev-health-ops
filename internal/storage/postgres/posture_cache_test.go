package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type postureClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *postureClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *postureClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newTestCachedPostureCheck(run func(context.Context) error, options PostureCheckOptions) (*CachedPostureCheck, *postureClock) {
	clock := &postureClock{now: time.Unix(1_700_000_000, 0)}
	check := newCachedPostureCheck("api_role", run, options)
	check.now = clock.Now
	return check, clock
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCachedPostureCheckServesAPassingAnswerWithoutRerunning(t *testing.T) {
	var runs atomic.Int32
	check, clock := newTestCachedPostureCheck(func(context.Context) error {
		runs.Add(1)
		return nil
	}, PostureCheckOptions{})
	for i := 0; i < 5; i++ {
		if err := check.Check(context.Background()); err != nil {
			t.Fatalf("Check() = %v, want nil", err)
		}
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("runs = %d within the TTL, want 1", got)
	}
	// Past TTL, still inside MaxStale: the probe answers at once from the
	// stale pass and exactly one background refresh runs.
	clock.Advance(defaultPostureTTL + time.Second)
	if err := check.Check(context.Background()); err != nil {
		t.Fatalf("stale Check() = %v, want nil", err)
	}
	waitFor(t, "background refresh", func() bool { return runs.Load() == 2 })
}

// The probe's deadline must not cancel the query: the incident was a 1.4-1.9 s
// query cancelled at the registry's 2 s bound, which cached nothing and so
// flapped forever.
func TestCachedPostureCheckSlowRunFinishesAfterTheProbeGivesUp(t *testing.T) {
	release := make(chan struct{})
	var runs atomic.Int32
	var sawCancel atomic.Bool
	check, _ := newTestCachedPostureCheck(func(ctx context.Context) error {
		runs.Add(1)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			sawCancel.Store(true)
			return ctx.Err()
		}
	}, PostureCheckOptions{})

	probe, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := check.Check(probe)
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Check() = %v, want ErrUnavailable wrapping the probe deadline", err)
	}
	close(release)
	waitFor(t, "the detached run to fill the cache", func() bool {
		check.mu.Lock()
		defer check.mu.Unlock()
		return !check.okAt.IsZero()
	})
	if sawCancel.Load() {
		t.Fatal("the run saw its context cancelled by the probe's deadline")
	}
	if err := check.Check(context.Background()); err != nil {
		t.Fatalf("Check() after the run finished = %v, want nil", err)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("runs = %d, want 1", got)
	}
}

func TestCachedPostureCheckIsSingleFlight(t *testing.T) {
	release := make(chan struct{})
	var runs atomic.Int32
	check, _ := newTestCachedPostureCheck(func(context.Context) error {
		runs.Add(1)
		<-release
		return nil
	}, PostureCheckOptions{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := check.Check(context.Background()); err != nil {
				t.Errorf("Check() = %v", err)
			}
		}()
	}
	waitFor(t, "the single run to start", func() bool { return runs.Load() >= 1 })
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	if got := runs.Load(); got != 1 {
		t.Fatalf("runs = %d for 8 concurrent probes, want 1", got)
	}
}

func TestCachedPostureCheckRefusalInvalidatesAPassingAnswer(t *testing.T) {
	refusal := errors.Join(ErrUnavailable, ErrPostureRefused)
	var runs atomic.Int32
	var refuse atomic.Bool
	check, clock := newTestCachedPostureCheck(func(context.Context) error {
		runs.Add(1)
		if refuse.Load() {
			return refusal
		}
		return nil
	}, PostureCheckOptions{})
	if err := check.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	refuse.Store(true)
	clock.Advance(defaultPostureTTL + time.Second)
	// Stale pass served, background refresh discovers the refusal ...
	if err := check.Check(context.Background()); err != nil {
		t.Fatalf("stale Check() = %v, want nil", err)
	}
	waitFor(t, "the refusal to land", func() bool {
		check.mu.Lock()
		defer check.mu.Unlock()
		return check.refused != nil
	})
	// ... and from then on the probe fails, without re-running per probe.
	before := runs.Load()
	for i := 0; i < 3; i++ {
		if err := check.Check(context.Background()); !errors.Is(err, ErrPostureRefused) {
			t.Fatalf("Check() = %v, want ErrPostureRefused", err)
		}
	}
	if got := runs.Load(); got != before {
		t.Fatalf("runs grew %d -> %d within RefusalTTL", before, got)
	}
	// After RefusalTTL a fixed grant is seen.
	refuse.Store(false)
	clock.Advance(defaultPostureRefusalTTL + time.Second)
	if err := check.Check(context.Background()); err != nil {
		t.Fatalf("Check() after the fix = %v, want nil", err)
	}
}

func TestCachedPostureCheckStalePassExpiresAtMaxStale(t *testing.T) {
	unanswered := errors.Join(ErrUnavailable, errors.New("connection refused"))
	var fail atomic.Bool
	check, clock := newTestCachedPostureCheck(func(context.Context) error {
		if fail.Load() {
			return unanswered
		}
		return nil
	}, PostureCheckOptions{})
	if err := check.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	// An unanswered refresh is not an answer: the pass survives until MaxStale.
	clock.Advance(defaultPostureTTL + time.Second)
	if err := check.Check(context.Background()); err != nil {
		t.Fatalf("stale Check() = %v, want nil", err)
	}
	waitFor(t, "the failed refresh to finish", func() bool {
		check.mu.Lock()
		defer check.mu.Unlock()
		return check.flight == nil
	})
	clock.Advance(defaultPostureMaxStale)
	if err := check.Check(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Check() past MaxStale = %v, want ErrUnavailable", err)
	}
}

type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func TestCachedPostureCheckLogsDurationAndOutcome(t *testing.T) {
	var buffer syncBuffer
	logger := slog.New(slog.NewJSONHandler(&buffer, nil))
	check, _ := newTestCachedPostureCheck(func(context.Context) error { return nil },
		PostureCheckOptions{Logger: logger})
	if err := check.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the log line", func() bool { return strings.Contains(buffer.String(), "\n") })
	var record map[string]any
	if err := json.Unmarshal([]byte(buffer.String()), &record); err != nil {
		t.Fatalf("log line %q is not one JSON record: %v", buffer.String(), err)
	}
	if record["msg"] != "role posture check" || record["role"] != "api_role" || record["outcome"] != "ok" {
		t.Fatalf("log record = %v, want msg, role and outcome set", record)
	}
	if _, ok := record["duration_ms"].(float64); !ok {
		t.Fatalf("log record = %v, want a numeric duration_ms", record)
	}
}

func TestCachedPostureCheckPanicReleasesTheFlight(t *testing.T) {
	check, _ := newTestCachedPostureCheck(func(context.Context) error { panic("boom") }, PostureCheckOptions{})
	if err := check.Check(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Check() = %v, want ErrUnavailable", err)
	}
}

// The refusal window must stay shorter than the shortest readiness probe
// period (deploy/values.prod.yaml goApi readinessProbe periodSeconds: 5), so
// a failing answer is never served across two probes.
func TestPostureRefusalTTLIsShorterThanTheReadinessProbePeriod(t *testing.T) {
	const shortestProbePeriod = 5 * time.Second
	if defaultPostureRefusalTTL >= shortestProbePeriod {
		t.Fatalf("defaultPostureRefusalTTL = %s, want < %s", defaultPostureRefusalTTL, shortestProbePeriod)
	}
}
