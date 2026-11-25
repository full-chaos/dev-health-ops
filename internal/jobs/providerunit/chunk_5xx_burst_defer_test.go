package providerunit

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
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
			if len(walk.ordinalAtSnooze) < burstAttempts {
				t.Fatalf("checkpoint read after %d snoozes, want at least %d (one per burst attempt)", len(walk.ordinalAtSnooze), burstAttempts)
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
	if !walk.terminal || walk.repository.lastFailCategory != "provider_unit_exhausted" {
		t.Fatalf("unit ended %q category=%q, want a terminal failure once the defer cap is spent", walk.repository.status, walk.repository.lastFailCategory)
	}
	if len(walk.runAgeAtFail) == 0 {
		t.Fatal("no ordinary failure recorded")
	}
	// The first ordinary failure marks the end of the deferred window; the
	// harness adds 30 minutes per ordinary failure after it.
	deferred := walk.runAgeAtFail[0]
	//abstime:allow simulated clock of the harness (the handler's Now), never wall time
	if deferred > burstDeferCapMax {
		t.Fatalf("first ordinary failure after %v of deferral, want the total snooze bounded by about %v", deferred, burstDeferCapMax)
	}
	//abstime:allow simulated clock of the harness (the handler's Now), never wall time
	if deferred < 25*time.Minute {
		t.Fatalf("first ordinary failure after %v of deferral, want a defer of about 30-60 minutes", deferred)
	}
	for index, ordinal := range walk.ordinalAtFail {
		if ordinal < burstPage {
			t.Fatalf("checkpoint next_ordinal at failure %d = %d, want the checkpoint kept at %d or later", index+1, ordinal, burstPage)
		}
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

func gatewayUnitHandler(unit providersync.Unit, now time.Time, failure error) (*Handler, *memoryUnitRepository) {
	repository := newMemoryUnitRepository(unit)
	return &Handler{
		Repository:    repository,
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, failure
		},
	}, repository
}

// PIN: a gateway burst defer feeds no provider rate-limit observation, so the
// dispatch cooldown gate that a 429 feeds stays open for the provider's other
// units, even when the error carries a rate-limit dimension.
func TestGatewayBurstDeferWritesNoRateLimitObservation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	unit.Provider, unit.Dataset = "github", "cicd"
	failure := &providerfoundation.ProviderError{
		Class: providerfoundation.ErrorTransient, StatusCode: http.StatusServiceUnavailable,
		Dimension: "core", RetryAfter: time.Minute,
	}
	handler, repository := gatewayUnitHandler(unit, now, failure)
	execution := providerExecution(unit, now, 1)
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)
	if _, snoozed := jobruntime.SnoozeDelay(err); !snoozed {
		t.Fatalf("Work() = %v; want an attempt-neutral snooze", err)
	}
	if repository.rateLimitDeferrals != 1 || repository.failures != 0 || repository.releaseCalls != 0 {
		t.Fatalf("deferrals=%d failures=%d releases=%d, want one defer and no attempt used",
			repository.rateLimitDeferrals, repository.failures, repository.releaseCalls)
	}
	for _, observation := range repository.rateLimitObservations {
		if observation.Dimension != "" {
			t.Fatalf("gateway burst wrote a rate-limit observation %+v; it must not feed the cooldown gate", observation)
		}
	}
}

// CONTROL: a 503 on a unit that is not chunked, and a transport error (no
// status) on a chunked one, keep the ordinary retry arm.
func TestGatewayBurstDeferLeavesUnchunkedAndTransportErrorsAlone(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name          string
		provider, set string
		status        int
	}{
		{"unchunked 503", "launchdarkly", "feature-flags", http.StatusServiceUnavailable},
		{"chunked transport error", "github", "cicd", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			unit := providerUnit()
			unit.Provider, unit.Dataset = test.provider, test.set
			handler, repository := gatewayUnitHandler(unit, now, &providerfoundation.ProviderError{
				Class: providerfoundation.ErrorTransient, StatusCode: test.status,
			})
			execution := providerExecution(unit, now, 1)
			execution.Definition.MaxAttempts = 5
			if _, snoozed := jobruntime.SnoozeDelay(handler.Work(context.Background(), execution)); snoozed {
				t.Fatal("deferred; want the ordinary retry")
			}
			if repository.rateLimitDeferrals != 0 {
				t.Fatalf("rate_limit_deferrals=%d, want 0", repository.rateLimitDeferrals)
			}
		})
	}
}

// CONTROL: the gateway arm is chunked-only. launchdarkly/feature-flags is a
// descriptor with Chunked=false, so a 5xx there keeps the ordinary retry and
// cap: a 503 or 500 retries before try 5 and ends provider_unit_exhausted /
// provider_transient on try 5. (TestExhaustedUnitStoresTheClassOfItsLastAttempt
// and TestCappedClassRetriesBeforeItsCap... run on the chunked github/files
// unit for the files inventory category, so they use a transport error.)
func TestUnchunkedUnitFiveXXKeepsTheOrdinaryRetryAndCap(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	if descriptor, ok := providersync.Descriptor("launchdarkly", "feature-flags"); !ok || descriptor.Chunked {
		t.Fatalf("launchdarkly/feature-flags chunked=%v ok=%v, want an unchunked descriptor", descriptor.Chunked, ok)
	}
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusInternalServerError} {
		for _, test := range []struct {
			attempt    int
			wantFailed bool
		}{{4, false}, {5, true}} {
			unit := providerUnit()
			repository := &classRepository{memoryUnitRepository: newMemoryUnitRepository(unit)}
			handler := &Handler{
				Repository:    repository,
				LeaseDuration: time.Minute,
				Heartbeat:     10 * time.Second,
				Now:           func() time.Time { return now },
				BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
					return providersync.CompleteRouteExecutor{}, &providerfoundation.ProviderError{
						Class: providerfoundation.ErrorTransient, StatusCode: status,
					}
				},
			}
			execution := providerExecution(unit, now, test.attempt)
			execution.Definition.MaxAttempts = 5
			err := handler.Work(context.Background(), execution)
			if err == nil {
				t.Fatalf("status %d try %d: Work() = nil, want a retryable or exhausted error", status, test.attempt)
			}
			if _, snoozed := jobruntime.SnoozeDelay(err); snoozed || repository.rateLimitDeferrals != 0 {
				t.Fatalf("status %d try %d: deferred on an unchunked unit", status, test.attempt)
			}
			if failed := repository.status == "failed"; failed != test.wantFailed {
				t.Fatalf("status %d try %d: status=%q, want failed=%v", status, test.attempt, repository.status, test.wantFailed)
			}
			if test.wantFailed && (repository.lastFailCategory != "provider_unit_exhausted" || repository.causeClass != "provider_transient") {
				t.Fatalf("status %d: category=%q class=%q, want provider_unit_exhausted/provider_transient",
					status, repository.lastFailCategory, repository.causeClass)
			}
		}
	}
}
