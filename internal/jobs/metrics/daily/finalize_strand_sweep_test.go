package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type sweepingStore struct {
	fakeStore
	calls  int
	sawOrg string
	err    error
}

func (store *sweepingStore) SweepStrandedFinalizeForOrg(
	_ context.Context, _ redriveFinalizePublisher, orgID string,
) (int, error) {
	store.calls++
	store.sawOrg = orgID
	return 0, store.err
}

type redrivingPublisher struct{ fakePublisher }

func (redrivingPublisher) PublishRedriveFinalizeTx(context.Context, pgx.Tx, Run, string) error {
	return nil
}

func captureDefaultLog(t *testing.T, body func()) []map[string]any {
	t.Helper()
	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	body()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(captured.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

// The nightly dispatch sweeps its own organization, and a failed sweep is a
// WARN line that never changes the dispatch result.
func TestDispatchSweepsStrandedFinalizeFailOpenWithAWarnLine(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		err      error
		wantWarn int
	}{
		{name: "sweep succeeds", wantWarn: 0},
		{name: "sweep fails", err: errors.New("sweep refused"), wantWarn: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := &sweepingStore{fakeStore: fakeStore{run: runningRun()}, err: testCase.err}
			handler, err := NewDispatcher(store, redrivingPublisher{}, &fakeRepositoryDiscoverer{})
			if err != nil {
				t.Fatal(err)
			}
			var workErr error
			records := captureDefaultLog(t, func() {
				workErr = handler.Work(context.Background(), blockedDispatchExecution())
			})
			if workErr != nil {
				t.Fatalf("Work = %v, want success whatever the sweep did", workErr)
			}
			if store.calls != 1 || store.sawOrg != testOrgID {
				t.Fatalf("sweep calls = %d for org %q, want 1 for %q", store.calls, store.sawOrg, testOrgID)
			}
			warns := 0
			for _, record := range records {
				if record["msg"] == StrandedFinalizeSweepFailedLogMessage {
					if record["level"] != "WARN" {
						t.Fatalf("sweep failure logged at %v, want WARN", record["level"])
					}
					warns++
				}
			}
			if warns != testCase.wantWarn {
				t.Fatalf("sweep failure lines = %d, want %d", warns, testCase.wantWarn)
			}
		})
	}
}

type countingFinalizeSweepObserver struct{ counts map[string]int }

func (observer *countingFinalizeSweepObserver) ObserveDailyMetricsFinalizeSweep(outcome string, count int) error {
	observer.counts[outcome] += count
	return nil
}

// A sweep that cannot read its candidates counts one failed pass.
func TestSweepStrandedFinalizeCountsAFailedPass(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://sweep@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC) }
	observer := &countingFinalizeSweepObserver{counts: map[string]int{}}
	store.SetFinalizeSweepObserver(observer)
	if _, err := store.SweepStrandedFinalizeForOrg(context.Background(), redrivingPublisher{}, testOrgID); err == nil {
		t.Fatal("a sweep on a closed pool returned no error")
	}
	if observer.counts["failed"] != 1 {
		t.Fatalf("failed passes counted = %d, want 1 (%v)", observer.counts["failed"], observer.counts)
	}
}

type orderedSweepStore struct {
	sweepingStore
	order         *[]string
	sweepDeadline time.Time
	reconcileCtx  context.Context
}

func (store *orderedSweepStore) SweepStrandedFinalizeForOrg(
	ctx context.Context, publisher redriveFinalizePublisher, orgID string,
) (int, error) {
	*store.order = append(*store.order, "sweep")
	store.sweepDeadline, _ = ctx.Deadline()
	return store.sweepingStore.SweepStrandedFinalizeForOrg(ctx, publisher, orgID)
}

func (store *orderedSweepStore) ReconcileBlockedRuns(ctx context.Context, _ string) (BlockedReconcileOutcome, error) {
	*store.order = append(*store.order, "reconcile")
	store.reconcileCtx = ctx
	return BlockedReconcileOutcome{}, nil
}

// The sweep runs after the existing hooks and gets an earlier deadline than the
// dispatch, so a slow sweep never starves the reconcile or the adapter's
// classification of the dispatch result.
func TestDispatchSweepRunsLastWithAnEarlierDeadline(t *testing.T) {
	var order []string
	store := &orderedSweepStore{sweepingStore: sweepingStore{fakeStore: fakeStore{run: runningRun()}}, order: &order}
	handler, err := NewDispatcher(store, redrivingPublisher{}, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	dispatchDeadline, _ := ctx.Deadline()
	if err := handler.Work(ctx, blockedDispatchExecution()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "reconcile,sweep" {
		t.Fatalf("hook order = %v, want reconcile before sweep", order)
	}
	if reconcileDeadline, _ := store.reconcileCtx.Deadline(); !reconcileDeadline.Equal(dispatchDeadline) {
		t.Fatal("the reconcile did not keep the dispatch deadline")
	}
	if !store.sweepDeadline.Before(dispatchDeadline) {
		t.Fatalf("sweep deadline %s is not before the dispatch deadline %s", store.sweepDeadline, dispatchDeadline)
	}
}
