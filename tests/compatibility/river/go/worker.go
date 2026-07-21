package rivercompat

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/riverqueue/river"
)

// Scenario controls only the behavior of jobs inserted by this probe. It is
// deliberately out-of-band so the cross-language JobArgs envelope stays exact.
type Scenario string

const (
	ScenarioBlockFirst Scenario = "block-first"
	ScenarioExecute    Scenario = "execute"
	ScenarioCancel     Scenario = "cancel"
	ScenarioRecover    Scenario = "recover"
)

var (
	ErrIntentionalCancel   = errors.New("intentional compatibility cancellation")
	ErrIntentionalRecovery = errors.New("intentional first-attempt compatibility failure")
	ErrProbeRelease        = errors.New("test-only release after cancellation propagation probe")
)

// Start records the observable point at which Go began executing a River job.
type Start struct {
	Args    JobArgs
	Attempt int
	JobID   int64
	Time    time.Time
}

// Finish records the context-driven exit of a blocking scenario. It lets the
// probe distinguish real running-job cancellation propagation from a row that
// was merely cancelled before a worker claimed it.
type Finish struct {
	Attempt int
	Cause   error
	JobID   int64
	Marker  string
}

// Worker executes the shared payload and exposes starts to the compatibility
// harness. Scenario selection is registered by marker rather than encoded in
// JobArgs, preserving the Python/Go contract under test.
type Worker struct {
	river.WorkerDefaults[JobArgs]

	mu        sync.RWMutex
	finishes  chan Finish
	releases  map[string]chan struct{}
	scenarios map[string]Scenario
	starts    chan Start
}

func NewWorker() *Worker {
	return &Worker{
		finishes:  make(chan Finish, 32),
		releases:  make(map[string]chan struct{}),
		scenarios: make(map[string]Scenario),
		starts:    make(chan Start, 32),
	}
}

func (w *Worker) Register(marker string, scenario Scenario) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.scenarios[marker] = scenario
	if scenario == ScenarioBlockFirst {
		w.releases[marker] = make(chan struct{}, 1)
	}
}

// Release unblocks a compatibility-only blocking scenario after the probe has
// shown that River did not propagate a running-job cancellation. The worker
// returns river.JobCancel so the row reaches a terminal state, but the result
// records this path separately and never mistakes it for context propagation.
func (w *Worker) Release(marker string) error {
	w.mu.RLock()
	release, ok := w.releases[marker]
	w.mu.RUnlock()
	if !ok {
		return errors.New("blocking scenario is not registered")
	}
	select {
	case release <- struct{}{}:
		return nil
	default:
		return errors.New("blocking scenario is already released")
	}
}

func (w *Worker) Starts() <-chan Start    { return w.starts }
func (w *Worker) Finishes() <-chan Finish { return w.finishes }

func (w *Worker) Work(ctx context.Context, job *river.Job[JobArgs]) error {
	if err := job.Args.Validate(); err != nil {
		return err
	}

	start := Start{
		Args:    job.Args,
		Attempt: job.Attempt,
		JobID:   job.ID,
		Time:    time.Now().UTC(),
	}
	select {
	case w.starts <- start:
	case <-ctx.Done():
		return context.Cause(ctx)
	}

	switch w.scenario(job.Args.Marker) {
	case ScenarioBlockFirst:
		if job.Attempt == 1 {
			cause := w.waitForCancellationOrRelease(ctx, job.Args.Marker)
			w.finishes <- Finish{
				Attempt: job.Attempt,
				Cause:   cause,
				JobID:   job.ID,
				Marker:  job.Args.Marker,
			}
			if errors.Is(cause, ErrProbeRelease) {
				return river.JobCancel(cause)
			}
			return cause
		}
	case ScenarioCancel:
		return river.JobCancel(ErrIntentionalCancel)
	case ScenarioRecover:
		if job.Attempt == 1 {
			return ErrIntentionalRecovery
		}
	}
	return nil
}

func (w *Worker) waitForCancellationOrRelease(ctx context.Context, marker string) error {
	w.mu.RLock()
	release := w.releases[marker]
	w.mu.RUnlock()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-release:
		return ErrProbeRelease
	}
}

// NextRetry makes the recovery scenario observable without waiting for the
// production retry policy. River keeps sub-scheduler-interval retries available.
func (w *Worker) NextRetry(job *river.Job[JobArgs]) time.Time {
	if w.scenario(job.Args.Marker) == ScenarioRecover {
		return time.Now().UTC().Add(50 * time.Millisecond)
	}
	return time.Time{}
}

func (w *Worker) scenario(marker string) Scenario {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if scenario, ok := w.scenarios[marker]; ok {
		return scenario
	}
	return ScenarioExecute
}
