package workerservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// CHAOS-6771: a saturated work pool is busy, not broken -- but only in exactly
// the shape the readiness contract can defend. Each row plants one way the
// probe could be wrongly tolerated or wrongly refused.
func TestIdempotencyBackendBusyToleranceIsExactlyBusyNotBroken(t *testing.T) {
	deadline := fmt.Errorf("acquire: %w", context.DeadlineExceeded)
	refused := errors.New("connection refused")
	progressing := func(context.Context) error { return nil }
	wedged := func(context.Context) error { return errors.New("no claim in the staleness window") }
	for _, tc := range []struct {
		name       string
		beginErr   error
		saturation float64
		progress   func(context.Context) error
		wantBusy   bool
	}{
		{"healthy begin", nil, 0, progressing, false},
		{"acquire deadline, pool saturated, work progressing = busy", deadline, 1, progressing, true},
		{"acquire deadline, pool NOT saturated = broken (slow database)", deadline, 0.75, progressing, false},
		{"acquire deadline, pool saturated, claim liveness red = broken (wedged pool)", deadline, 1, wedged, false},
		{"connection error while saturated = broken (the transaction path failed)", refused, 1, progressing, false},
		{"no progress guard wired = broken", deadline, 1, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database := &fakeWorkerDatabase{domainSaturation: tc.saturation}
			database.setTxOpenerErr(tc.beginErr)
			var logs bytes.Buffer
			dependencies := &workerDependencies{
				database: database,
				logger:   slog.New(slog.NewJSONHandler(&logs, nil)),
				busy:     newReadinessBusy(slog.New(slog.NewJSONHandler(&logs, nil))),
			}
			if tc.progress != nil {
				dependencies.progressGuard = tc.progress
			}
			err := dependencies.idempotencyBackendReady(context.Background())
			wantPass := tc.beginErr == nil || tc.wantBusy
			if wantPass != (err == nil) {
				t.Fatalf("idempotencyBackendReady = %v, want pass=%v", err, wantPass)
			}
			var metrics bytes.Buffer
			if err := dependencies.busy.WritePrometheus(&metrics); err != nil {
				t.Fatal(err)
			}
			wantCount := 0
			if tc.wantBusy {
				wantCount = 1
			}
			want := fmt.Sprintf(`worker_readiness_busy_total{check="idempotency_backend"} %d`, wantCount)
			if !strings.Contains(metrics.String(), want) {
				t.Fatalf("metrics %q lack %q", metrics.String(), want)
			}
			if tc.wantBusy && !strings.Contains(logs.String(), "passed as busy") {
				t.Fatalf("a busy pass left no log line: %q", logs.String())
			}
		})
	}
}

// The ticking execution-liveness monitor samples the same pool and is
// tolerated by the same rule (its own check name on the counter).
func TestExecutionLivenessSampleIsToleratedWhenBusyToo(t *testing.T) {
	database := &fakeWorkerDatabase{domainSaturation: 1}
	database.setTxOpenerErr(context.DeadlineExceeded)
	dependencies := &workerDependencies{
		database:      database,
		busy:          newReadinessBusy(nil),
		progressGuard: func(context.Context) error { return nil },
	}
	tx, err := dependencies.busyTolerantDomainTxOpener("execution_liveness").Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin = %v, want a busy pass", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	var metrics bytes.Buffer
	_ = dependencies.busy.WritePrometheus(&metrics)
	if !strings.Contains(metrics.String(), `worker_readiness_busy_total{check="execution_liveness"} 1`) {
		t.Fatalf("metrics = %q", metrics.String())
	}
}

func TestBusyLogIsRateLimitedButTheCounterCountsEveryProbe(t *testing.T) {
	var logs bytes.Buffer
	busy := newReadinessBusy(slog.New(slog.NewJSONHandler(&logs, nil)))
	clock := time.Unix(1_700_000_000, 0)
	busy.now = func() time.Time { return clock }
	for i := 0; i < 5; i++ {
		busy.record(context.Background(), "idempotency_backend")
	}
	if got := strings.Count(logs.String(), "passed as busy"); got != 1 {
		t.Fatalf("%d log lines within the interval, want 1", got)
	}
	clock = clock.Add(busyLogInterval + time.Second)
	busy.record(context.Background(), "idempotency_backend")
	if got := strings.Count(logs.String(), "passed as busy"); got != 2 {
		t.Fatalf("%d log lines after the interval, want 2", got)
	}
	var metrics bytes.Buffer
	_ = busy.WritePrometheus(&metrics)
	if !strings.Contains(metrics.String(), `worker_readiness_busy_total{check="idempotency_backend"} 6`) {
		t.Fatalf("metrics = %q, want the counter at 6", metrics.String())
	}
}
