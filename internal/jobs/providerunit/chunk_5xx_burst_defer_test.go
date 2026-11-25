package providerunit

import (
	"net/http"
	"testing"
	"time"
)

// A provider 5xx / gateway burst that outlasts the five ordinary attempts must
// defer the chunked unit without spending an attempt, so the unit keeps its
// chunk checkpoint and resumes from its cursor. Today the unit spends all five
// attempts in about 75 s, fails, and failTxOnce deletes the checkpoint.
//
// Harness: driveFlakyWalk (see chunk_progress_exhaustion_repro_test.go). One
// unit attempt of 503s is exhaustReproHTTPTries answers.

const (
	burstPage        = 64
	burstAttempts    = 8
	burstDeferCapMax = 65 * time.Minute
)

func TestChunkedUnitGatewayBurstDefersAndKeepsItsCheckpoint(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			walk := driveFlakyWalk(t, flakyWalk{
				pages:  exhaustReproPages,
				status: map[int]int{burstPage: status},
				tries:  map[int]int{burstPage: burstAttempts * exhaustReproHTTPTries},
			})
			if walk.terminal || walk.repository.status != "success" {
				t.Fatalf("unit ended %q category=%q cause_class=%q at River attempt %d after %d ordinary failures; want a deferred burst and success",
					walk.repository.status, walk.repository.lastFailCategory, walk.repository.causeClass, walk.attempt, walk.failures)
			}
			if walk.failures != 0 || walk.attempt != 1 {
				t.Fatalf("failures=%d River attempt=%d, want an attempt-neutral defer: 0 ordinary failures, attempt 1", walk.failures, walk.attempt)
			}
			if walk.snoozes < burstAttempts {
				t.Fatalf("snoozes=%d, want at least %d (one per burst attempt)", walk.snoozes, burstAttempts)
			}
			for index, ordinal := range walk.ordinalAtSnooze {
				if ordinal < burstPage {
					t.Fatalf("checkpoint next_ordinal after snooze %d = %d, want the checkpoint kept at %d or later", index+1, ordinal, burstPage)
				}
			}
			if len(walk.accounting) != 0 {
				t.Fatalf("attempts arithmetic: %v", walk.accounting)
			}
		})
	}
}

func TestChunkedUnitGatewayBurstFailsAfterTheDeferCap(t *testing.T) {
	walk := driveFlakyWalk(t, flakyWalk{
		pages:  exhaustReproPages,
		status: map[int]int{burstPage: http.StatusServiceUnavailable},
		tries:  map[int]int{burstPage: 1000 * exhaustReproHTTPTries},
	})
	if walk.attempt != 1 || walk.failures != 1 {
		t.Fatalf("River attempt=%d ordinary failures=%d, want the burst deferred attempt-neutral and one final failure on attempt 1", walk.attempt, walk.failures)
	}
	if !walk.terminal {
		t.Fatalf("unit ended %q, want a terminal failure once the defer cap is spent", walk.repository.status)
	}
	//abstime:allow simulated clock of the harness (the handler's Now), never wall time
	if walk.elapsed > burstDeferCapMax {
		t.Fatalf("unit failed after %v of deferral, want the total snooze bounded by about %v", walk.elapsed, burstDeferCapMax)
	}
	//abstime:allow simulated clock of the harness (the handler's Now), never wall time
	if walk.elapsed < 25*time.Minute {
		t.Fatalf("unit failed after %v of deferral, want a defer of about 30-60 minutes", walk.elapsed)
	}
}

// CONTROL (passes today): a 429 burst defers through the rate-limit arm.
func TestChunkedUnitRateLimitBurstDefersControl(t *testing.T) {
	walk := driveFlakyWalk(t, flakyWalk{
		pages:  exhaustReproPages,
		status: map[int]int{burstPage: http.StatusTooManyRequests},
		tries:  map[int]int{burstPage: burstAttempts * exhaustReproHTTPTries},
	})
	if walk.terminal || walk.repository.status != "success" || walk.failures != 0 || walk.attempt != 1 || walk.snoozes == 0 {
		t.Fatalf("control: ended %q failures=%d attempt=%d snoozes=%d, want success by attempt-neutral defers",
			walk.repository.status, walk.failures, walk.attempt, walk.snoozes)
	}
}

// CONTROL (passes today): a deterministic provider answer is not a burst; it
// fails on its attempt cap with no defer.
func TestChunkedUnitDeterministicErrorStillFailsOnAttemptsControl(t *testing.T) {
	walk := driveFlakyWalk(t, flakyWalk{
		pages:  exhaustReproPages,
		status: map[int]int{burstPage: http.StatusUnprocessableEntity},
		tries:  map[int]int{burstPage: burstAttempts * exhaustReproHTTPTries},
	})
	if !walk.terminal || walk.attempt != 1 || walk.failures != 1 {
		t.Fatalf("control: terminal=%v attempt=%d failures=%d, want one terminal failure on attempt 1",
			walk.terminal, walk.attempt, walk.failures)
	}
}
