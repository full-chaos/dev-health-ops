package syncreconciler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type contextT = context.Context

type txT = pgx.Tx

func testContext() context.Context { return context.Background() }

// unreadableRoutes is the coordinator-pool seam for tests that must never
// reach it. A sweep that opens the route read when it should have declined
// earlier fails loudly rather than passing on a nil result.
type unreadableRoutes struct {
	t      *testing.T
	reason string
}

func (routes unreadableRoutes) Query(
	ctx contextT, sql string, args ...any,
) (pgx.Rows, error) {
	routes.t.Helper()
	routes.t.Fatal(routes.reason)
	return nil, nil
}

func (routes unreadableRoutes) Begin(ctx contextT) (txT, error) {
	routes.t.Helper()
	routes.t.Fatal(routes.reason + " (and must not open a fence transaction)")
	return nil, nil
}

// failingRoutes reproduces the CHAOS-4035 production fault: the coordinator
// read answering 42501.
type failingRoutes struct{ err error }

func (routes failingRoutes) Query(
	ctx contextT, sql string, args ...any,
) (pgx.Rows, error) {
	return nil, routes.err
}

// The opening read fails, so the fence is never reached. Saying so out loud
// beats returning a nil transaction that would panic somewhere less obvious.
func (routes failingRoutes) Begin(ctx contextT) (txT, error) {
	return nil, routes.err
}

func absentSweep(t *testing.T, switches providersync.CompleteRouteSwitches) *UnreclaimableSweep {
	t.Helper()
	sweep, err := newUnreclaimableSweep(
		unreadableRoutes{t: t, reason: "this sweep must not read the durable route"},
		func(ctx contextT) (txT, error) { return nil, nil },
		UnreclaimableSweepConfig{
			Age: time.Hour, Idle: 15 * time.Minute, Mode: SweepModeActive,
			Switches: switches,
		},
	)
	if err != nil {
		t.Fatalf("construct sweep: %v", err)
	}
	return sweep
}

func TestParseSweepMode(t *testing.T) {
	for _, testCase := range []struct {
		name string
		raw  string
		want SweepMode
	}{
		{"unset defaults to shadow", "", SweepModeShadow},
		{"whitespace defaults to shadow", "   ", SweepModeShadow},
		{"off", "off", SweepModeOff},
		{"shadow", "shadow", SweepModeShadow},
		{"active", "active", SweepModeActive},
		{"case insensitive", "ACTIVE", SweepModeActive},
		{"padded", "  active  ", SweepModeActive},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := ParseSweepMode(testCase.raw)
			if err != nil {
				t.Fatalf("ParseSweepMode(%q): %v", testCase.raw, err)
			}
			if got != testCase.want {
				t.Fatalf("ParseSweepMode(%q) = %q, want %q", testCase.raw, got, testCase.want)
			}
		})
	}
}

// A typo must not silently become an assertion about the deployment, nor
// silently disable the safety net.
func TestParseSweepModeRejectsUnknownValues(t *testing.T) {
	for _, raw := range []string{"actve", "on", "true", "enabled", "yes"} {
		if _, err := ParseSweepMode(raw); err != ErrInvalidConfiguration {
			t.Fatalf("ParseSweepMode(%q) error = %v, want ErrInvalidConfiguration", raw, err)
		}
	}
}

func TestUnroutableGate(t *testing.T) {
	enabled := providersync.CompleteRouteSwitches{GithubTests: true, GithubRepoMetadata: true}
	disabled := providersync.CompleteRouteSwitches{}

	for _, testCase := range []struct {
		name     string
		switches providersync.CompleteRouteSwitches
		provider string
		dataset  string
		want     bool
	}{
		// The production strand: River declines the pair.
		{"disabled pair", disabled, "github", "tests", true},
		// A pair a live River runtime can execute is never swept.
		{"enabled pair", enabled, "github", "tests", false},
		{"enabled repo-metadata", enabled, "github", "repo-metadata", false},
		// An unknown pair is not proof of anything.
		{"unknown pair", disabled, "nosuch", "nosuch", false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			sweep := absentSweep(t, testCase.switches)
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
		Age: time.Hour, Idle: time.Minute, Mode: SweepModeShadow,
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

func TestStepDefersWhenModeIsOff(t *testing.T) {
	// Fail-safe: a disabled sweep must do nothing WITHOUT opening a
	// transaction, so it can never disturb the pass it shares with lease
	// repair.
	sweep, err := newUnreclaimableSweep(
		unreadableRoutes{t: t, reason: "mode=off must not read the durable route"},
		func(ctx contextT) (txT, error) {
			t.Fatal("mode=off must not open a transaction")
			return nil, nil
		},
		UnreclaimableSweepConfig{
			Age: time.Hour, Idle: time.Minute, Mode: SweepModeOff,
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
		t.Fatalf("disabled pass reported %+v", result)
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

// CHAOS-4035 acceptance criterion 4. The sweep returned the identical
// ErrUnavailable from fourteen sites, so the only production log line for a
// 42501 on the route read named the observer -- a component that was healthy
// and had nothing to do with it.
//
// Two properties are asserted together because either alone is a defect: the
// error must NAME its step, and it must STILL classify as ErrUnavailable, so
// the pipeline's non-fatal branch and the lifecycle's readiness branch keep
// working rather than silently stopping matching.
func TestSweepRouteReadFailureNamesItselfAndStaysClassified(t *testing.T) {
	denied := &pgconn.PgError{
		Code:    "42501",
		Message: "permission denied for table worker_job_routes",
	}
	sweep, err := newUnreclaimableSweep(
		failingRoutes{err: denied},
		func(ctx contextT) (txT, error) {
			t.Fatal("a failed route read must not open the domain transaction")
			return nil, nil
		},
		UnreclaimableSweepConfig{
			Age: time.Hour, Idle: 15 * time.Minute, Mode: SweepModeActive,
		},
	)
	if err != nil {
		t.Fatalf("construct sweep: %v", err)
	}
	_, stepErr := sweep.Step(testContext(), time.Now().UTC(), 100)
	if stepErr == nil {
		t.Fatal("a denied route read reported success")
	}
	if !errors.Is(stepErr, ErrUnavailable) {
		t.Fatalf("error %v no longer classifies as ErrUnavailable; every caller "+
			"branching on unavailability has silently stopped matching", stepErr)
	}
	if got := SweepStepIdentity(stepErr); got != sweepStepRouteQuery {
		t.Fatalf("step identity = %q, want %q", got, sweepStepRouteQuery)
	}
	message := stepErr.Error()
	for _, want := range []string{"worker_job_routes", "coordinator pool", "42501"} {
		if !strings.Contains(message, want) {
			t.Errorf("error %q does not mention %q, so the next occurrence is "+
				"no more diagnosable than the one that cost the CHAOS-4035 "+
				"investigation", message, want)
		}
	}
}

// The safety property the single error was protecting must survive being made
// informative: a SQLSTATE is actionable, a DSN or a row value is a leak.
func TestSweepStepErrorsCarryNoConnectionMaterial(t *testing.T) {
	leak := &pgconn.PgError{
		Code:       "28P01",
		Message:    "password authentication failed for user \"devhealth_domain\"",
		Detail:     "connection to postgres://devhealth_domain:hunter2@db.internal:5432/ops",
		Hint:       "check the password",
		SchemaName: "public",
	}
	forbidden := []string{"hunter2", "db.internal", "postgres://", "5432", "password"}
	for _, step := range sweepStepIdentities() {
		message := sweepUnavailable(step, leak).Error()
		for _, banned := range forbidden {
			if strings.Contains(strings.ToLower(message), strings.ToLower(banned)) {
				t.Errorf("step %q message %q leaks %q", step, message, banned)
			}
		}
		if !strings.Contains(message, "28P01") {
			t.Errorf("step %q message %q dropped the SQLSTATE", step, message)
		}
	}
}

// Every step must be distinguishable from every other. A duplicated constant
// would quietly re-create the exact ambiguity this ticket removed.
func TestSweepStepIdentitiesAreDistinct(t *testing.T) {
	steps := sweepStepIdentities()
	if len(steps) != 17 {
		t.Fatalf("declared %d steps, want the 17 failure paths this file has "+
			"(the original 14, plus the three the closing route fence adds)", len(steps))
	}
	seen := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		if step == "" {
			t.Fatal("an empty step name is no identity at all")
		}
		if _, duplicate := seen[step]; duplicate {
			t.Fatalf("step %q is declared twice", step)
		}
		seen[step] = struct{}{}
		err := sweepUnavailable(step, nil)
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("step %q does not classify as ErrUnavailable", step)
		}
		if !strings.Contains(err.Error(), step) {
			t.Fatalf("step %q is missing from its own message %q", step, err.Error())
		}
	}
}

func sweepStepIdentities() []string {
	return []string{
		sweepStepBegin,
		sweepStepRouteQuery, sweepStepRouteFence,
		sweepStepRouteFenceBegin, sweepStepRouteFenceTimeout,
		sweepStepRouteScan, sweepStepRouteRows,
		sweepStepCandidateQuery, sweepStepCandidateScan, sweepStepCandidateRows,
		sweepStepOutboxQuery, sweepStepOutboxScan, sweepStepOutboxRows,
		sweepStepTerminalizePayload, sweepStepTerminalizeExec,
		sweepStepTerminalizeRows, sweepStepCommit,
	}
}

type decliningSweep struct{ candidates int }

func (sweep decliningSweep) Step(
	ctx contextT, now time.Time, limit int,
) (UnreclaimableSweepResult, error) {
	return UnreclaimableSweepResult{
		Mode:                SweepModeActive,
		Candidates:          sweep.candidates,
		DeclinedRouteChange: true,
	}, nil
}

// A fenced decline returns a nil error, so the pipeline's existing failure
// branch never sees it. Without its own log line an operator watching a
// route-churning deployment sees healthy passes while every selected strand is
// abandoned -- a quieter version of the invisibility that let CHAOS-3990 sit
// unnoticed and CHAOS-4035 ship.
func TestPipelineReportsAFencedSweepDecline(t *testing.T) {
	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&captured, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	pipeline := pipelineWithSweep(t, decliningSweep{candidates: 3})
	if _, err := pipeline.Step(testContext(), time.Now().UTC(), 10); err != nil {
		t.Fatalf("a fenced decline must not fail the pass: %v", err)
	}
	logged := captured.String()
	if !strings.Contains(logged, "unreclaimable_sweep_declined_route_change") {
		t.Fatalf("the pass logged %q, which never mentions the decline", logged)
	}
	// The count is what tells an operator whether anything was actually
	// abandoned, so a line without it is only marginally better than silence.
	if !strings.Contains(logged, "candidates=3") {
		t.Errorf("the decline line %q does not report how many strands it abandoned", logged)
	}
}

// The other direction: an ordinary pass must not emit the decline line, or the
// signal is worthless.
func TestPipelineDoesNotReportADeclineOnAnOrdinaryPass(t *testing.T) {
	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&captured, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	pipeline := pipelineWithSweep(t, &recordingSweep{})
	if _, err := pipeline.Step(testContext(), time.Now().UTC(), 10); err != nil {
		t.Fatalf("pipeline step: %v", err)
	}
	if strings.Contains(captured.String(), "unreclaimable_sweep_declined_route_change") {
		t.Fatalf("a clean pass reported a route-change decline: %q", captured.String())
	}
}
