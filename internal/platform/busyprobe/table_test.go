package busyprobe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
)

// The busy-tolerance rule as a state table, generated from its three inputs
// (CHAOS-6800: two review rounds each found one cell by hand; this enumerates them).
//
// INVARIANT: a probe passes as BUSY if and only if
//
//	(a) the pool is fully acquired right now, AND
//	(b) the probe failed WAITING FOR A POOL CONNECTION and hit its own deadline
//	    (an AcquireError wrapping context.DeadlineExceeded), AND
//	(c) the pool's work acquired a connection AFTER the guard's baseline, within
//	    the progress window (a new guard gets one grace window, as a new process
//	    does).
//
// Any other outcome that is not a success is FAILED: a deadline on the BEGIN
// itself, any other error, a saturated pool with no or stale progress, a pool that
// is not saturated. A successful Begin is READY and never counted as busy.

type acquireOutcome string

const (
	outcomeOK              acquireOutcome = "ok"
	outcomeAcquireDeadline acquireOutcome = "acquire-deadline"
	outcomeAcquireOther    acquireOutcome = "acquire-other-error" // failed waiting for a connection, but not by deadline
	outcomeBeginDeadline   acquireOutcome = "begin-deadline"
	outcomeOtherError      acquireOutcome = "other-error"
)

type progressEvidence string

const (
	evidenceFresh        progressEvidence = "fresh"         // work acquired after the baseline, seen inside the window
	evidenceStale        progressEvidence = "stale"         // work acquired after the baseline, but longer ago than the window
	evidenceAtWindow     progressEvidence = "at-window"     // the movement was seen exactly one window ago (the window is strict)
	evidenceNone         progressEvidence = "none"          // no work acquire since the baseline, window elapsed
	evidenceBaselineOnly progressEvidence = "baseline-only" // a large count from BEFORE the guard existed, no movement since, window elapsed
	evidenceGrace        progressEvidence = "grace"         // no movement yet, still inside the first window after construction
)

type poolState string

const (
	poolSaturated poolState = "saturated"
	poolFree      poolState = "free"
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

// guardFor builds a PoolProgress whose state, at the moment the opener consults
// it, is the requested evidence, using a fake count and clock.
func guardFor(evidence progressEvidence) *PoolProgress {
	const window = 40 * time.Second
	start := time.Unix(1_700_000_000, 0)
	clock := start
	count := int64(0)
	if evidence == evidenceBaselineOnly {
		count = 500 // a long-running process: the baseline is large
	}
	progress := newPoolProgress(func() int64 { return count }, window, func() time.Time { return clock })
	switch evidence {
	case evidenceFresh:
		count++
		clock = start.Add(10 * time.Second)
	case evidenceStale:
		count++
		clock = start.Add(5 * time.Second)
		_ = progress.Ready(context.Background()) // the movement is seen at +5 s
		clock = start.Add(5*time.Second + window + time.Second)
	case evidenceAtWindow:
		count++
		clock = start.Add(5 * time.Second)
		_ = progress.Ready(context.Background()) // the movement is seen at +5 s
		clock = start.Add(5*time.Second + window)
	case evidenceNone, evidenceBaselineOnly:
		clock = start.Add(window + time.Second)
	case evidenceGrace:
		clock = start.Add(10 * time.Second)
	}
	return progress
}

func expectedTableResult(outcome acquireOutcome, evidence progressEvidence, state poolState) string {
	if outcome == outcomeOK {
		return "ready"
	}
	freshEnough := evidence == evidenceFresh || evidence == evidenceGrace
	if state == poolSaturated && outcome == outcomeAcquireDeadline && freshEnough {
		return "busy"
	}
	return "failed"
}

func TestBusyToleranceStateTable(t *testing.T) {
	outcomes := []acquireOutcome{outcomeOK, outcomeAcquireDeadline, outcomeAcquireOther, outcomeBeginDeadline, outcomeOtherError}
	evidences := []progressEvidence{evidenceFresh, evidenceAtWindow, evidenceStale, evidenceNone, evidenceBaselineOnly, evidenceGrace}
	states := []poolState{poolSaturated, poolFree}
	cells := 0
	for _, outcome := range outcomes {
		for _, evidence := range evidences {
			for _, state := range states {
				cells++
				name := fmt.Sprintf("%s/%s/%s", outcome, evidence, state)
				t.Run(name, func(t *testing.T) {
					counter := NewCounter("probe_busy_total", []string{"check"}, nil)
					opener := Opener{
						Inner:     tableInner{outcome},
						Check:     "check",
						Saturated: func() bool { return state == poolSaturated },
						Progress:  guardFor(evidence).Ready,
						Counter:   counter,
					}
					tx, err := opener.Begin(context.Background())
					var metrics bytes.Buffer
					_ = counter.WritePrometheus(&metrics)
					busyCount := 0
					if strings.Contains(metrics.String(), `probe_busy_total{check="check"} 1`) {
						busyCount = 1
					}
					got := "failed"
					switch {
					case err == nil && busyCount == 1:
						got = "busy"
					case err == nil:
						got = "ready"
					}
					if want := expectedTableResult(outcome, evidence, state); got != want {
						t.Fatalf("%s = %s, want %s (err=%v)", name, got, want, err)
					}
					if err == nil {
						if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
							t.Fatal(rollbackErr)
						}
					}
					if got != "busy" && busyCount != 0 {
						t.Fatalf("%s counted a busy pass but read %s", name, got)
					}
				})
			}
		}
	}
	if want := len(outcomes) * len(evidences) * len(states); cells != want || cells != 60 {
		t.Fatalf("table has %d cells, want %d (5 outcomes x 6 evidence x 2 pool states)", cells, want)
	}
}

// CHAOS-6800 r3 P1: the readiness GATE. A busy pass is a success to the monitor,
// which then grants its own staleness allowance: readiness must not be the
// monitor's freshness alone when its latest success was tolerated rather than
// earned. Generated over monitor state x latest-success kind x progress evidence:
//
//	ready  iff  the monitor is fresh  AND  (the latest success was a real
//	transaction  OR  the work evidence is fresh right now).
func TestReadinessGateStateTable(t *testing.T) {
	type monitorState string
	monitors := []monitorState{"fresh", "stale"}
	kinds := []string{"real", "busy"}
	evidences := []progressEvidence{evidenceFresh, evidenceAtWindow, evidenceStale, evidenceNone, evidenceBaselineOnly, evidenceGrace}
	cells := 0
	for _, monitor := range monitors {
		for _, kind := range kinds {
			for _, evidence := range evidences {
				cells++
				name := fmt.Sprintf("monitor-%s/last-%s/%s", monitor, kind, evidence)
				t.Run(name, func(t *testing.T) {
					liveness := &Liveness{Progress: guardFor(evidence)}
					liveness.lastBusy.Store(kind == "busy")
					base := func(context.Context) error {
						if monitor == "stale" {
							return errors.New("monitor stale")
						}
						return nil
					}
					err := liveness.Gate(base)(context.Background())
					freshEvidence := evidence == evidenceFresh || evidence == evidenceGrace
					wantReady := monitor == "fresh" && (kind == "real" || freshEvidence)
					if (err == nil) != wantReady {
						t.Fatalf("%s: ready=%v (err=%v), want ready=%v", name, err == nil, err, wantReady)
					}
				})
			}
		}
	}
	if cells != 24 {
		t.Fatalf("gate table has %d cells, want 24 (2 monitor x 2 kinds x 6 evidence)", cells)
	}
}

// The opener reports how each Begin ended, so the gate knows whether the
// monitor's latest success was earned or tolerated.
func TestOpenerReportsWhetherTheSuccessWasBusy(t *testing.T) {
	var last []bool
	opener := Opener{
		Inner:     tableInner{outcomeAcquireDeadline},
		Check:     "check",
		Saturated: func() bool { return true },
		Progress:  func(context.Context) error { return nil },
		Counter:   NewCounter("m", nil, nil),
		OnOutcome: func(busy bool) { last = append(last, busy) },
	}
	if _, err := opener.Begin(context.Background()); err != nil {
		t.Fatal(err)
	}
	opener.Inner = tableInner{outcomeOK}
	if _, err := opener.Begin(context.Background()); err != nil {
		t.Fatal(err)
	}
	opener.Inner = tableInner{outcomeOtherError}
	if _, err := opener.Begin(context.Background()); err == nil {
		t.Fatal("a plain error passed")
	}
	if fmt.Sprint(last) != "[true false]" {
		t.Fatalf("outcomes = %v, want [true false] (busy, real; a failure reports nothing)", last)
	}
}
