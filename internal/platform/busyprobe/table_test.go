package busyprobe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
)

// The worker's busy-tolerance rule as a state table over the Opener's inputs
// (the scheduler and reconciler no longer use it: CHAOS-6800).
//
// INVARIANT: a probe passes as BUSY if and only if (a) the pool is fully
// acquired right now, AND (b) the probe failed WAITING FOR A POOL CONNECTION and
// hit its own deadline (an AcquireError wrapping context.DeadlineExceeded), AND
// (c) the caller's progress guard is green. Any other non-success is FAILED; a
// successful Begin is READY and never counted as busy.

type acquireOutcome string

const (
	outcomeOK              acquireOutcome = "ok"
	outcomeAcquireDeadline acquireOutcome = "acquire-deadline"
	outcomeAcquireOther    acquireOutcome = "acquire-other-error"
	outcomeBeginDeadline   acquireOutcome = "begin-deadline"
	outcomeOtherError      acquireOutcome = "other-error"
)

type tableInner struct{ outcome acquireOutcome }

func (i tableInner) Begin(context.Context) (selfprobe.Tx, error) {
	switch i.outcome {
	case outcomeOK:
		return fakeTx{}, nil
	case outcomeAcquireDeadline:
		return nil, &selfprobe.AcquireError{Err: fmt.Errorf("acquire: %w", context.DeadlineExceeded)}
	case outcomeAcquireOther:
		return nil, &selfprobe.AcquireError{Err: errors.New("connection refused")}
	case outcomeBeginDeadline:
		return nil, fmt.Errorf("begin: %w", context.DeadlineExceeded)
	default:
		return nil, errors.New("connection refused")
	}
}

func TestBusyToleranceStateTable(t *testing.T) {
	outcomes := []acquireOutcome{outcomeOK, outcomeAcquireDeadline, outcomeAcquireOther, outcomeBeginDeadline, outcomeOtherError}
	cells := 0
	for _, outcome := range outcomes {
		for _, saturated := range []bool{true, false} {
			for _, progressGreen := range []bool{true, false} {
				cells++
				name := fmt.Sprintf("%s/saturated=%v/progress-green=%v", outcome, saturated, progressGreen)
				t.Run(name, func(t *testing.T) {
					counter := NewCounter("probe_busy_total", []string{"check"}, nil)
					opener := Opener{
						Inner:     tableInner{outcome},
						Check:     "check",
						Saturated: func() bool { return saturated },
						Progress: func(context.Context) error {
							if progressGreen {
								return nil
							}
							return errors.New("no progress")
						},
						Counter: counter,
					}
					tx, err := opener.Begin(context.Background())
					var metrics bytes.Buffer
					_ = counter.WritePrometheus(&metrics)
					busy := strings.Contains(metrics.String(), `probe_busy_total{check="check"} 1`)
					got := "failed"
					switch {
					case err == nil && busy:
						got = "busy"
					case err == nil:
						got = "ready"
					}
					want := "failed"
					switch {
					case outcome == outcomeOK:
						want = "ready"
					case saturated && progressGreen && outcome == outcomeAcquireDeadline:
						want = "busy"
					}
					if got != want {
						t.Fatalf("%s = %s, want %s (err=%v)", name, got, want, err)
					}
					if err == nil {
						if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
							t.Fatal(rollbackErr)
						}
					}
				})
			}
		}
	}
	if cells != 20 {
		t.Fatalf("table has %d cells, want 20 (5 outcomes x 2 x 2)", cells)
	}
}
