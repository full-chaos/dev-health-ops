package workerservice

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

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// newTestRiverWorkerProcess builds a riverWorkerProcess around a river.Client
// that never actually dials a database: riverpgxv5.New wraps whatever pool
// pointer it is given (nil here) without touching it, and river.NewClient
// does no I/O at construction time. process.startClient is always overridden
// in these tests, so the real (unusable) client.Start is never invoked --
// only Start's own retry loop around it is under test.
func newTestRiverWorkerProcess(t *testing.T) riverWorkerProcess {
	t.Helper()
	client, err := river.NewClient(riverpgxv5.New(nil), &river.Config{})
	if err != nil {
		t.Fatalf("construct test river client: %v", err)
	}
	return riverWorkerProcess{client: client}
}

// riverStartSleepAfterCheckTimeout mirrors preclaimSleepAfterCheckTimeout: it
// waits long enough between retries that the test does not depend on real
// wall-clock backoff timing.
func riverStartSleepAfterCheckTimeout(step time.Duration) func(context.Context, time.Duration) {
	return func(context.Context, time.Duration) {
		time.Sleep(step)
	}
}

// fakeRetryClock drives startupRetryBudget's loop without the wall clock: sleep
// advances it by exactly the requested wait, so a test's retry count is a
// function of the budget and the backoff, never of how long the runner takes to
// schedule a goroutine (CHAOS-6446, same class as CHAOS-6397). Under -race with
// a starved CPU a 20 ms real-time budget could be spent before the second
// attempt began: 1 failure in 200 runs pinned to one core beside 12 busy loops.
type fakeRetryClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeRetryClock() *fakeRetryClock {
	return &fakeRetryClock{now: time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)}
}

func (clock *fakeRetryClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeRetryClock) Sleep(_ context.Context, wait time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(wait)
}

// A fleet-wide roll restarts every worker group's Deployments at once, and
// River's producer.StartWorkContext gives fetching a queue's settings a
// fixed 10 second budget with no retry of its own -- go-ops, go-sync (x2),
// and sync-provider all exited on exactly this during today's rolls. Start
// must retry through the deadline and become ready the moment a later
// attempt succeeds, without ever returning an error.
func TestRiverWorkerProcessRetriesStartTimeoutUntilSuccess(t *testing.T) {
	t.Parallel()
	const wantSuccessAttempt = 3

	process := newTestRiverWorkerProcess(t)
	var logs bytes.Buffer
	process.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	process.budget = time.Second
	process.sleep = riverStartSleepAfterCheckTimeout(time.Millisecond)

	var attempts atomic.Int32
	process.startClient = func(context.Context) error {
		if attempts.Add(1) < wantSuccessAttempt {
			return context.DeadlineExceeded
		}
		return nil
	}

	if err := process.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v, want nil once a later attempt succeeds", err)
	}
	if got := attempts.Load(); got != wantSuccessAttempt {
		t.Fatalf("startClient ran %d times, want exactly %d", got, wantSuccessAttempt)
	}
	if count := strings.Count(logs.String(), "river workers start timed out, retrying"); count != wantSuccessAttempt-1 {
		t.Fatalf("retry log count = %d, want %d (one per timed-out attempt)", count, wantSuccessAttempt-1)
	}
	if strings.Contains(logs.String(), "river workers start refused") {
		t.Fatalf("a recovered start must never log a refusal: %s", logs.String())
	}
}

// A genuine start error -- a rejected DSN, a misconfigured queue -- is never
// River's internal StartWorkContext deadline, so it must never be confused
// for the roll-storm case: Start exits on the very first attempt.
func TestRiverWorkerProcessExitsImmediatelyOnGenuineStartError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("queue and workers must be configured")

	process := newTestRiverWorkerProcess(t)
	var logs bytes.Buffer
	process.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	process.budget = time.Minute
	process.sleep = func(context.Context, time.Duration) {
		t.Fatal("a genuine start error must not be retried")
	}

	var attempts atomic.Int32
	process.startClient = func(context.Context) error {
		attempts.Add(1)
		return wantErr
	}

	if err := process.Start(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Start() error = %v, want %v", err, wantErr)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("startClient ran %d times, want exactly 1 (no retry on a genuine error)", got)
	}

	var record struct {
		Attempts int    `json:"attempts"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &record); err != nil {
		t.Fatalf("decode log record: %v (raw %q)", err, logs.String())
	}
	if record.Reason != "river_producer_start_failed" || record.Attempts != 1 {
		t.Fatalf("log record = %#v, want reason river_producer_start_failed at attempt 1", record)
	}
}

// A producer start that never clears within the configured retry budget must
// still end the process -- retrying survives a roll storm, it does not hang
// forever -- and must log how many attempts it took.
func TestRiverWorkerProcessExitsAfterBudgetExhaustedOnPersistentStartTimeout(t *testing.T) {
	t.Parallel()
	const budget = 20 * time.Millisecond

	process := newTestRiverWorkerProcess(t)
	var logs bytes.Buffer
	process.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	process.budget = budget
	clock := newFakeRetryClock()
	process.now = clock.Now
	process.sleep = clock.Sleep

	var attempts atomic.Int32
	process.startClient = func(context.Context) error {
		attempts.Add(1)
		return context.DeadlineExceeded
	}

	if err := process.Start(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v, want context.DeadlineExceeded once the budget is spent", err)
	}
	// The first backoff (2 s) is clamped to the remaining budget, so the fake
	// clock lands exactly on the budget after one wait: two attempts, always.
	finalAttempts := attempts.Load()
	if finalAttempts != 2 {
		t.Fatalf("startClient ran %d times, want exactly 2 (one retry, then the budget is spent)", finalAttempts)
	}
	if count := strings.Count(logs.String(), "river workers start timed out, retrying"); count < 1 {
		t.Fatalf("retry log count = %d, want at least 1 warn-level retry before giving up", count)
	}

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	var record struct {
		Attempts int    `json:"attempts"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &record); err != nil {
		t.Fatalf("decode final log record: %v (raw %q)", err, lines[len(lines)-1])
	}
	if record.Reason != "retry_budget_exhausted" {
		t.Fatalf("final log reason = %q, want retry_budget_exhausted", record.Reason)
	}
	if record.Attempts != int(finalAttempts) {
		t.Fatalf("logged attempts = %d, want %d (matching how many times startClient actually ran)", record.Attempts, finalAttempts)
	}
}

// Start must still refuse a missing client exactly as before: the retry
// budget is meaningless without a client to retry against.
func TestRiverWorkerProcessStartRefusesNilClient(t *testing.T) {
	t.Parallel()
	var process riverWorkerProcess
	if err := process.Start(context.Background()); !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("Start() error = %v, want errWorkerDependencyUnavailable", err)
	}
}
