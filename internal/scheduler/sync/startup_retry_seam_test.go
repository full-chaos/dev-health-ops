package sync

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// newStartupRetryLoop builds a loop with a startup retry budget, a fake wait
// and no jitter, logging into the returned buffer.
func newStartupRetryLoop(
	t *testing.T,
	stepper HandoffStepper,
	clock *testLoopClock,
	budget time.Duration,
	sleep func(context.Context, time.Duration),
) (*Loop, *health.Registry, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	registry := health.NewRegistry(time.Second)
	loop, err := newLoop(stepper, CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
		return OccurrenceMinted, nil
	}), LoopConfig{
		PollInterval: minLoopPollInterval,
		StepTimeout:  time.Second,
		MaxBackoff:   80 * time.Millisecond,
		Limit:        3,
		Registry:     registry,
		Occurrences:  &stubOccurrences{},
	}.WithLogger(slog.New(slog.NewTextHandler(logs, nil))).WithStartupRetryBudget(budget), clock)
	if err != nil {
		t.Fatal(err)
	}
	loop.sleep = sleep
	loop.jitter = nil
	return loop, registry, logs
}
