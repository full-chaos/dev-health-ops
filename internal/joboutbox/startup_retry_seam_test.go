package joboutbox

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// newStartupRetryLoop builds a loop with a startup retry budget, a fake wait
// and no jitter, logging into the returned buffer.
func newStartupRetryLoop(
	t *testing.T,
	stepper RelayStepper,
	clock *testReconcilerClock,
	budget time.Duration,
	sleep func(context.Context, time.Duration),
) (*ReconcilerLoop, *health.Registry, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	registry := health.NewRegistry(time.Second)
	loop, err := newReconcilerLoop(stepper, ReconcilerLoopConfig{
		PollInterval:       minReconcilerPollInterval,
		Limit:              7,
		Registry:           registry,
		Logger:             slog.New(slog.NewTextHandler(logs, nil)),
		StartupRetryBudget: budget,
		StartupRetry:       testStartupRetry,
		IsTransientDial:    postgres.IsTransientDialError,
		MaskUnreachable:    postgres.MaskUnreachable,
		StartupJitter:      postgres.DialRetryJitter,
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	loop.sleep = sleep
	loop.jitter = nil
	return loop, registry, logs
}

func postgresTransient(err error) bool { return postgres.IsTransientDialError(err) }

func testStartupRetry(
	ctx context.Context,
	budget time.Duration,
	seams StartupRetrySeams,
	step func(context.Context) error,
) error {
	return postgres.DialRetry{
		Budget:  budget,
		Now:     seams.Now,
		Sleep:   seams.Sleep,
		Jitter:  seams.Jitter,
		OnRetry: seams.OnRetry,
	}.Do(ctx, step)
}

// strandCause returns the database cause a strand classification keeps for
// the startup retry.
func strandCause(err error) error {
	var withCause unavailableWithCause
	if errors.As(err, &withCause) {
		return withCause.cause
	}
	return err
}
