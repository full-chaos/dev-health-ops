package syncdispatchruntime

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/riverqueue/river/rivertype"
)

// CHAOS-6883: the bare sync workers feed the claim-liveness taps through River
// worker middleware, once per job, balanced on every exit path.

type tapObserver struct {
	jobruntime.Observer
	mu              sync.Mutex
	events          []string
	panicOnInvoked  bool
	panicOnReturned bool
}

func (o *tapObserver) HandlerInvoked(_ context.Context, labels jobruntime.JobLabels) {
	o.mu.Lock()
	o.events = append(o.events, "invoked:"+labels.Queue+"/"+labels.Kind)
	o.mu.Unlock()
	if o.panicOnInvoked {
		panic("tap fault")
	}
}

func (o *tapObserver) HandlerReturned(_ context.Context, labels jobruntime.JobLabels) {
	o.mu.Lock()
	o.events = append(o.events, "returned:"+labels.Queue+"/"+labels.Kind)
	o.mu.Unlock()
	if o.panicOnReturned {
		panic("tap fault")
	}
}

func syncJobRow(kind string) *rivertype.JobRow {
	return &rivertype.JobRow{Kind: kind, Queue: "sync"}
}

// runMiddleware drives a worker's Middleware exactly as River does.
func runMiddleware(t *testing.T, middlewares []rivertype.WorkerMiddleware, row *rivertype.JobRow, inner func(context.Context) error) error {
	t.Helper()
	if len(middlewares) != 1 {
		t.Fatalf("want exactly one tap middleware, got %d", len(middlewares))
	}
	return middlewares[0].Work(context.Background(), row, inner)
}

func TestEveryBareSyncWorkerBracketsItsWorkWithTheHandlerTaps(t *testing.T) {
	t.Parallel()
	observer := &tapObserver{}
	taps := handlerTaps{observer: observer}
	for name, middleware := range map[string]func(*rivertype.JobRow) []rivertype.WorkerMiddleware{
		"dispatch":             (&dispatchWorker{taps: taps}).Middleware,
		"finalize":             (&finalizeWorker{taps: taps}).Middleware,
		"post_sync":            (&postSyncWorker{taps: taps}).Middleware,
		"reference_discovery":  (&referenceDiscoveryWorker{taps: taps}).Middleware,
		"team_autoimport":      (&teamAutoimportWorker{taps: taps}).Middleware,
		"ownership_derivation": (&teamRepoOwnershipDerivationWorker{taps: taps}).Middleware,
	} {
		observer.events = nil
		row := syncJobRow("k." + name)
		var during []string
		err := runMiddleware(t, middleware(row), row, func(context.Context) error {
			during = append([]string(nil), observer.events...)
			return nil
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want := []string{"invoked:sync/k." + name, "returned:sync/k." + name}
		if len(during) != 1 || during[0] != want[0] {
			t.Errorf("%s: the work must run AFTER HandlerInvoked and BEFORE HandlerReturned, saw %v while it ran", name, during)
		}
		if len(observer.events) != 2 || observer.events[1] != want[1] {
			t.Errorf("%s: events %v, want %v", name, observer.events, want)
		}
	}
}

func TestBareWorkerTapsBalanceOnErrorAndPanicAndSurviveTapFaults(t *testing.T) {
	t.Parallel()
	row := syncJobRow("dispatch_sync_run")

	observer := &tapObserver{}
	worker := &dispatchWorker{taps: handlerTaps{observer: observer}}
	sentinel := errors.New("work failed")
	if err := runMiddleware(t, worker.Middleware(row), row, func(context.Context) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("the work's own error must pass through untouched, got %v", err)
	}
	if len(observer.events) != 2 {
		t.Fatalf("a failing job must still return its tap: %v", observer.events)
	}

	observer = &tapObserver{}
	worker = &dispatchWorker{taps: handlerTaps{observer: observer}}
	func() {
		defer func() { _ = recover() }() // River recovers a worker panic; the tap must already have balanced.
		_ = runMiddleware(t, worker.Middleware(row), row, func(context.Context) error { panic("boom") })
	}()
	if len(observer.events) != 2 || observer.events[1] != "returned:sync/dispatch_sync_run" {
		t.Fatalf("a panicking job must still return its tap: %v", observer.events)
	}

	// A fault inside the telemetry call must never fail (or panic) the job.
	observer = &tapObserver{panicOnInvoked: true, panicOnReturned: true}
	worker = &dispatchWorker{taps: handlerTaps{observer: observer}}
	ran := false
	if err := runMiddleware(t, worker.Middleware(row), row, func(context.Context) error { ran = true; return nil }); err != nil || !ran {
		t.Fatalf("a tap fault must not affect the job: ran=%v err=%v", ran, err)
	}
}

func TestBareSyncWorkersWithoutAnObserverAddNoMiddleware(t *testing.T) {
	t.Parallel()
	row := syncJobRow("dispatch_sync_run")
	for name, middlewares := range map[string][]rivertype.WorkerMiddleware{
		"dispatch":  (&dispatchWorker{}).Middleware(row),
		"finalize":  (&finalizeWorker{}).Middleware(row),
		"post_sync": (&postSyncWorker{}).Middleware(row),
		"ref":       (&referenceDiscoveryWorker{}).Middleware(row),
		"autoimp":   (&teamAutoimportWorker{}).Middleware(row),
		"derive":    (&teamRepoOwnershipDerivationWorker{}).Middleware(row),
	} {
		if len(middlewares) != 0 {
			t.Errorf("%s: no observer must mean no middleware, got %d", name, len(middlewares))
		}
	}
	// A nil worker (River never passes one) must not panic.
	if got := (*dispatchWorker)(nil).Middleware(row); got != nil {
		t.Fatal("a nil worker adds no middleware")
	}
}

func TestResolveWorkerOptionsIgnoresNilOptions(t *testing.T) {
	t.Parallel()
	observer := &tapObserver{}
	got := resolveWorkerOptions([]WorkerOption{nil, WithHandlerObserver(observer), nil})
	if got.observer != observer {
		t.Fatal("the observer option was lost")
	}
	if resolveWorkerOptions(nil).observer != nil {
		t.Fatal("no options means no observer")
	}
}
