package syncreconciler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/jackc/pgx/v5"
)

type contextT = context.Context

type txT = pgx.Tx

func testContext() context.Context { return context.Background() }

func absentSweep(t *testing.T, switches providersync.CompleteRouteSwitches) *UnreclaimableSweep {
	t.Helper()
	sweep, err := newUnreclaimableSweep(
		func(ctx contextT) (txT, error) { return nil, nil },
		UnreclaimableSweepConfig{
			Age: time.Hour, Idle: 15 * time.Minute, Mode: SweepModeActive,
			Switches: switches, Presence: CeleryAbsent,
		},
	)
	if err != nil {
		t.Fatalf("construct sweep: %v", err)
	}
	return sweep
}

func TestPresenceFromExpectedWorkerGroups(t *testing.T) {
	for _, testCase := range []struct {
		name string
		raw  string
		want CeleryPresence
	}{
		{"declared", "heavy,ops,sync,sync-provider", CeleryAbsent},
		{"single group", "sync", CeleryAbsent},
		{"unset", "", CeleryUnknown},
		// A malformed value must never be read as "no Celery": that would turn
		// a typo into permission to destroy queued work.
		{"whitespace only", "   ", CeleryUnknown},
		{"commas only", ",,,", CeleryUnknown},
		{"padded", " , sync , ", CeleryAbsent},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := PresenceFromExpectedWorkerGroups(testCase.raw); got != testCase.want {
				t.Fatalf("presence for %q = %q, want %q", testCase.raw, got, testCase.want)
			}
		})
	}
}

func TestUnroutableGate(t *testing.T) {
	enabled := providersync.CompleteRouteSwitches{GithubTests: true, GithubRepoMetadata: true}
	disabled := providersync.CompleteRouteSwitches{}

	for _, testCase := range []struct {
		name     string
		switches providersync.CompleteRouteSwitches
		presence CeleryPresence
		provider string
		dataset  string
		want     bool
	}{
		// The production strand: River declines the pair and no Celery exists.
		{"disabled pair, celery absent", disabled, CeleryAbsent, "github", "tests", true},
		// The CUT-19 rollback: consumers are live, so this is queued work.
		{"disabled pair, celery present", disabled, CeleryPresent, "github", "tests", false},
		// An undecidable broker must never authorize destruction.
		{"disabled pair, celery unknown", disabled, CeleryUnknown, "github", "tests", false},
		// A pair a live runtime can execute is never swept, whatever its age.
		{"enabled pair", enabled, CeleryAbsent, "github", "tests", false},
		{"enabled repo-metadata", enabled, CeleryAbsent, "github", "repo-metadata", false},
		// An unknown pair is not proof of anything.
		{"unknown pair", disabled, CeleryAbsent, "nosuch", "nosuch", false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			sweep := absentSweep(t, testCase.switches)
			sweep.config.Presence = testCase.presence
			candidate := unreclaimableCandidate{
				provider: testCase.provider, datasetKey: testCase.dataset,
			}
			if got := sweep.unroutable(candidate); got != testCase.want {
				t.Fatalf("unroutable(%s/%s) = %v, want %v",
					testCase.provider, testCase.dataset, got, testCase.want)
			}
		})
	}
}

// A non-canonical member of an atomic family is never executable on its own,
// so it must remain sweepable even when the family switch is on.
func TestUnroutableAtomicFamilyAlias(t *testing.T) {
	switches := providersync.CompleteRouteSwitches{GithubWorkItems: true}
	sweep := absentSweep(t, switches)

	canonical := unreclaimableCandidate{provider: "github", datasetKey: "work-items"}
	if sweep.unroutable(canonical) {
		t.Fatal("canonical work-items claim is executable and must not be swept")
	}
	alias := unreclaimableCandidate{provider: "github", datasetKey: "work-item-labels"}
	if !sweep.unroutable(alias) {
		t.Fatal("non-canonical family alias cannot execute and must be sweepable")
	}
}

func TestSweepConfigValidation(t *testing.T) {
	valid := UnreclaimableSweepConfig{
		Age: time.Hour, Idle: time.Minute, Mode: SweepModeShadow, Presence: CeleryAbsent,
	}
	if !valid.valid() {
		t.Fatal("well-formed config rejected")
	}
	for _, testCase := range []struct {
		name   string
		mutate func(*UnreclaimableSweepConfig)
	}{
		{"zero age", func(c *UnreclaimableSweepConfig) { c.Age = 0 }},
		{"negative age", func(c *UnreclaimableSweepConfig) { c.Age = -time.Hour }},
		{"zero idle", func(c *UnreclaimableSweepConfig) { c.Idle = 0 }},
		{"empty mode", func(c *UnreclaimableSweepConfig) { c.Mode = "" }},
		{"unknown mode", func(c *UnreclaimableSweepConfig) { c.Mode = "sometimes" }},
		{"empty presence", func(c *UnreclaimableSweepConfig) { c.Presence = "" }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			config := valid
			testCase.mutate(&config)
			if config.valid() {
				t.Fatalf("%s accepted", testCase.name)
			}
		})
	}
}

func TestUnreclaimableReasonNamesThePairAndTheCondition(t *testing.T) {
	reason := unreclaimableReason(unreclaimableCandidate{
		provider: "github", datasetKey: "tests",
	})
	for _, want := range []string{
		"github/tests", "no lease", "no worker_job_outbox row",
	} {
		if !strings.Contains(reason, want) {
			t.Fatalf("reason %q is missing %q", reason, want)
		}
	}
}

func TestUnreclaimableDedupeKeyMatchesTheProducer(t *testing.T) {
	// The producer writes sync.provider_unit:<unit id>; a drift here would
	// silently stop the outbox filter from ever matching, and the sweep would
	// start terminalizing units River owns.
	if got := unreclaimableDedupeKey("abc"); got != "sync.provider_unit:abc" {
		t.Fatalf("dedupe key = %q", got)
	}
}

func TestStepDefersWhenPresenceIsUnknown(t *testing.T) {
	// Fail-safe: an undeclared deployment must sweep nothing WITHOUT opening a
	// transaction, so an unreadable disposition can never abort the reconcile
	// pass it shares with lease repair.
	sweep, err := newUnreclaimableSweep(
		func(ctx contextT) (txT, error) {
			t.Fatal("unknown presence must not open a transaction")
			return nil, nil
		},
		UnreclaimableSweepConfig{
			Age: time.Hour, Idle: time.Minute, Mode: SweepModeActive,
			Presence: CeleryUnknown,
		},
	)
	if err != nil {
		t.Fatalf("construct sweep: %v", err)
	}
	result, err := sweep.Step(testContext(), time.Now().UTC(), 100)
	if err != nil {
		t.Fatalf("Step returned %v, want nil", err)
	}
	if result.Candidates != 0 || result.Terminalized != 0 {
		t.Fatalf("deferred pass reported %+v", result)
	}
}

func TestStepRejectsInvalidLimits(t *testing.T) {
	sweep := absentSweep(t, providersync.CompleteRouteSwitches{})
	for _, limit := range []int{0, -1, unreclaimableMaximumLimit + 1} {
		if _, err := sweep.Step(testContext(), time.Now().UTC(), limit); err != ErrInvalidConfiguration {
			t.Fatalf("limit %d returned %v, want ErrInvalidConfiguration", limit, err)
		}
	}
}

type recordingSweep struct{ calls int }

func (sweep *recordingSweep) Step(
	ctx contextT, now time.Time, limit int,
) (UnreclaimableSweepResult, error) {
	sweep.calls++
	return UnreclaimableSweepResult{Mode: SweepModeShadow}, nil
}

type failingSweep struct{ err error }

func (sweep failingSweep) Step(
	ctx contextT, now time.Time, limit int,
) (UnreclaimableSweepResult, error) {
	return UnreclaimableSweepResult{}, sweep.err
}

// The sweep must actually be reached by the pipeline. CHAOS-3990's whole
// second layer existed in the tree and was never executed, so "is it wired?"
// gets its own assertion rather than being inferred from construction.
func TestPipelineStepInvokesTheSweep(t *testing.T) {
	sweep := &recordingSweep{}
	pipeline := pipelineWithSweep(t, sweep)
	if _, err := pipeline.Step(testContext(), time.Now().UTC(), 10); err != nil {
		t.Fatalf("pipeline step: %v", err)
	}
	if sweep.calls != 1 {
		t.Fatalf("sweep invoked %d times, want 1", sweep.calls)
	}
}

// A failing safety net must not take lease repair and wakeup materialization
// down with it: that would trade a bounded strand for an unbounded one.
func TestPipelineStepSurvivesSweepFailure(t *testing.T) {
	pipeline := pipelineWithSweep(t, failingSweep{err: ErrUnavailable})
	if _, err := pipeline.Step(testContext(), time.Now().UTC(), 10); err != nil {
		t.Fatalf("sweep failure propagated out of the pass: %v", err)
	}
}

func pipelineWithSweep(t *testing.T, sweep UnreclaimableSweepStepper) *MutationPipeline {
	t.Helper()
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(contextT, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(contextT, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(contextT, time.Time, time.Time, int) (MaterializerResult, error) {
			return MaterializerResult{}, nil
		}),
		pipelineKernelFunc(func(contextT, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(contextT, time.Time, int) (Observation, error) {
			return Observation{}, nil
		}),
		AtLeastOncePublisher(func(contextT, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(contextT, TransportClaim) error { return nil }),
		sweep,
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}
	return pipeline
}
