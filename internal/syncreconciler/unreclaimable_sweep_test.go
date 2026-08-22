package syncreconciler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

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

// unreadableJobs is the queue-control seam for a case that must never reach
// the delivery-liveness read. It fails the test loudly rather than returning
// an empty result, because an empty result would let a sweep that wrongly
// consulted River still pass.
type unreadableJobs struct {
	t      *testing.T
	reason string
}

func (jobs unreadableJobs) Query(
	ctx contextT, sql string, args ...any,
) (pgx.Rows, error) {
	jobs.t.Helper()
	jobs.t.Fatal(jobs.reason)
	return nil, nil
}

func absentSweep(t *testing.T) *UnreclaimableSweep {
	t.Helper()
	sweep, err := newUnreclaimableSweep(
		unreadableRoutes{t: t, reason: "this sweep must not read the durable route"},
		func(ctx contextT) (txT, error) { return nil, nil },
		unreadableJobs{t: t, reason: "this sweep must not read river_job"},
		"river",
		UnreclaimableSweepConfig{
			Age: time.Hour, Idle: 15 * time.Minute, Mode: SweepModeActive,
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

// TestUnroutableGate is the CHAOS-4054 successor: github/repo-metadata used
// to be swept whenever its route switch was off, and github/tests used to
// stay unswept once GithubTests was flipped on. There is no route switch
// left. github/repo-metadata is now RouteReady && Plannable unconditionally,
// so it is NEVER unroutable; github/tests is RouteReady but aliases onto the
// canonical `cicd` writer, so it is ALWAYS unroutable. Neither depends on any
// configuration any more.
func TestUnroutableGate(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		provider string
		dataset  string
		want     bool
	}{
		// An alias identity is RouteReady but never independently Plannable,
		// so it is always sweepable.
		{"alias identity is always unroutable", "github", "tests", true},
		// A canonical, shipped pair is never swept.
		{"routable pair is never unroutable", "github", "repo-metadata", false},
		// An unknown pair is not proof of anything.
		{"unknown pair", "nosuch", "nosuch", false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			sweep := absentSweep(t)
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
	sweep := absentSweep(t)

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
		unreadableJobs{t: t, reason: "mode=off must not read river_job"},
		"river",
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
	sweep := absentSweep(t)
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
		unreadableJobs{t: t, reason: "a failed route read must not reach river_job"},
		"river",
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

// staticSweep returns one fixed result, so a test can assert what the pipeline
// does with a selection rather than only that it made the call.
type staticSweep struct{ result UnreclaimableSweepResult }

func (sweep staticSweep) Step(
	ctx contextT, now time.Time, limit int,
) (UnreclaimableSweepResult, error) {
	return sweep.result, nil
}

// SHADOW MODE MUST PRODUCE A RECORD (adversarial review finding).
//
// Shadow is the default, and the sweep justifies that default by claiming it
// gives "would-terminalize observability at zero write risk". Nothing
// implemented the observability half: the pipeline consumed only
// DeclinedRouteChange, so a shadow deployment could select the same strand on
// every tick forever and emit not one line. In shadow mode that log line is
// the ONLY record that exists -- there is no row to read afterwards -- which
// is why this asserts on the emitted record rather than on the result value.
func TestPipelineStepReportsWhatTheShadowSweepSelected(t *testing.T) {
	units := make([]string, 0, sweepReportSample+5)
	for index := 0; index < sweepReportSample+5; index++ {
		units = append(units, fmt.Sprintf("unit-%02d", index))
	}
	captured, restore := captureSlogRecords(t)
	defer restore()

	pipeline := pipelineWithSweep(t, staticSweep{result: UnreclaimableSweepResult{
		Mode:       SweepModeShadow,
		Candidates: len(units),
		RunIDs:     []string{"run-a", "run-b"},
		Pairs:      []string{"github/repo-metadata"},
		UnitIDs:    units,
	}})
	if _, err := pipeline.Step(testContext(), time.Now().UTC(), 10); err != nil {
		t.Fatalf("pipeline step: %v", err)
	}

	record, found := findSlogRecord(*captured, "syncreconciler.unreclaimable_sweep_selected")
	if !found {
		t.Fatalf("a shadow pass selected %d units and emitted no selection record; "+
			"shadow mode is the default, so this is the whole of its output", len(units))
	}
	if record["mode"] != string(SweepModeShadow) {
		t.Fatalf("record mode = %v, want the shadow mode named", record["mode"])
	}
	// The COUNT is the truth and must never be the sampled length. slog
	// widens an int attribute to int64, so the comparison is numeric rather
	// than an interface equality that would silently never match.
	if countedInt(t, record, "candidates") != int64(len(units)) {
		t.Fatalf("record candidates = %v, want the exact %d selected", record["candidates"], len(units))
	}
	if countedInt(t, record, "terminalized") != 0 {
		t.Fatalf("record terminalized = %v, want zero: shadow writes nothing", record["terminalized"])
	}
	sample, ok := record["unit_id_sample"].([]string)
	if !ok || len(sample) != sweepReportSample {
		t.Fatalf("record unit_id_sample = %#v, want %d entries", record["unit_id_sample"], sweepReportSample)
	}
	// A capped sample that does not say it was capped reads as "these are all
	// of them", which is the under-reporting this ticket is about.
	if record["unit_id_sample_truncated"] != true {
		t.Fatalf("record did not declare the sample truncated: %#v", record)
	}
}

// NON-VACUITY. A pass that selected nothing must stay silent, or the line
// becomes noise on every tick of a healthy deployment and gets muted -- which
// would leave shadow mode exactly as invisible as it was before.
func TestPipelineStepStaysSilentWhenTheSweepSelectedNothing(t *testing.T) {
	captured, restore := captureSlogRecords(t)
	defer restore()

	pipeline := pipelineWithSweep(t, staticSweep{result: UnreclaimableSweepResult{
		Mode: SweepModeShadow,
	}})
	if _, err := pipeline.Step(testContext(), time.Now().UTC(), 10); err != nil {
		t.Fatalf("pipeline step: %v", err)
	}
	if _, found := findSlogRecord(*captured, "syncreconciler.unreclaimable_sweep_selected"); found {
		t.Fatal("an empty pass emitted a selection record")
	}
}

// countedInt reads a numeric attribute without caring which width slog chose
// for it. An interface comparison against an untyped constant is the trap
// here: it compiles, always fails, and looks like a real assertion.
func countedInt(t *testing.T, record map[string]any, key string) int64 {
	t.Helper()
	switch value := record[key].(type) {
	case int64:
		return value
	case int:
		return int64(value)
	default:
		t.Fatalf("record %q = %#v, want a numeric attribute", key, record[key])
		return 0
	}
}

// captureSlogRecords swaps the default slog handler for one that keeps every
// record's attributes, and restores it. The pipeline logs through the package
// default, so this is the seam that exists rather than one invented for the
// test.
func captureSlogRecords(t *testing.T) (*[]map[string]any, func()) {
	t.Helper()
	records := make([]map[string]any, 0, 4)
	previous := slog.Default()
	slog.SetDefault(slog.New(&capturingHandler{records: &records}))
	return &records, func() { slog.SetDefault(previous) }
}

func findSlogRecord(records []map[string]any, message string) (map[string]any, bool) {
	for _, record := range records {
		if record["msg"] == message {
			return record, true
		}
	}
	return nil, false
}

type capturingHandler struct{ records *[]map[string]any }

func (handler *capturingHandler) Enabled(contextT, slog.Level) bool { return true }

func (handler *capturingHandler) Handle(_ contextT, record slog.Record) error {
	captured := map[string]any{"msg": record.Message}
	record.Attrs(func(attr slog.Attr) bool {
		captured[attr.Key] = attr.Value.Any()
		return true
	})
	*handler.records = append(*handler.records, captured)
	return nil
}

func (handler *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }

func (handler *capturingHandler) WithGroup(string) slog.Handler { return handler }

// The pipeline must SAY the detector broke. The materializer carrying the step
// out on its result is only half the fix -- a field nobody reads is the same
// silence in a different place.
func TestPipelineStepReportsAnUnavailableRunawayReport(t *testing.T) {
	captured, restore := captureSlogRecords(t)
	defer restore()

	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(contextT, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(contextT, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(contextT, time.Time, time.Time, int) (MaterializerResult, error) {
			// A pass that materialized fine and could not take the report.
			// The empty Runaway is the point: without the step field this is
			// byte-identical to a healthy, quiet system.
			return MaterializerResult{
				Dispatch:          1,
				RunawayReportStep: runawayReportStepQuery,
			}, nil
		}),
		pipelineKernelFunc(func(contextT, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(contextT, time.Time, int) (Observation, error) {
			return Observation{}, nil
		}),
		AtLeastOncePublisher(func(contextT, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(contextT, TransportClaim) error { return nil }),
		nil,
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}
	if _, err := pipeline.Step(testContext(), time.Now().UTC(), 10); err != nil {
		t.Fatalf("a broken report took the pass down with it: %v", err)
	}

	record, found := findSlogRecord(*captured, "syncreconciler.dispatch_wakeup_report_unavailable")
	if !found {
		t.Fatal("the runaway detector failed and the pass reported nothing; " +
			"an empty report that does not admit it failed is indistinguishable " +
			"from a healthy one, which is the silence this report exists to end")
	}
	if record["step"] != runawayReportStepQuery {
		t.Fatalf("record step = %v, want the failing statement named", record["step"])
	}
}

// NON-VACUITY: a working detector must not emit the broken-detector line, or
// it fires on every tick and stops meaning anything.
func TestPipelineStepStaysSilentWhenTheRunawayReportWorked(t *testing.T) {
	captured, restore := captureSlogRecords(t)
	defer restore()

	pipeline := pipelineWithSweep(t, staticSweep{result: UnreclaimableSweepResult{Mode: SweepModeShadow}})
	if _, err := pipeline.Step(testContext(), time.Now().UTC(), 10); err != nil {
		t.Fatal(err)
	}
	if _, found := findSlogRecord(*captured, "syncreconciler.dispatch_wakeup_report_unavailable"); found {
		t.Fatal("a healthy pass claimed the runaway detector was unavailable")
	}
}

// The metric is only real if the number reaches the loop. A field the pipeline
// populates but never carries out is the same silence in a different place --
// the exact shape of the shadow-mode gap this ticket already had to fix once.
func TestPipelineStepCarriesTheSweepAndRunawayFiguresOntoTheObservation(t *testing.T) {
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(contextT, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(contextT, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(contextT, time.Time, time.Time, int) (MaterializerResult, error) {
			// Sample deliberately SMALLER than the true total, so a
			// regression to len(Runaway) cannot pass here either.
			return MaterializerResult{
				Runaway: []RunawayDispatchWakeup{
					{SyncRunID: "run-a", Attempts: 5000},
					{SyncRunID: "run-b", Attempts: 4000},
				},
				RunawayTotal:      9,
				RunawayReportStep: "",
			}, nil
		}),
		pipelineKernelFunc(func(contextT, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		// The read-only Observer authors none of these fields, so a non-empty
		// observation here proves the pipeline copied rather than got lucky.
		pipelineObserverFunc(func(contextT, time.Time, int) (Observation, error) {
			return Observation{CeleryDuePending: 42}, nil
		}),
		AtLeastOncePublisher(func(contextT, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(contextT, TransportClaim) error { return nil }),
		staticSweep{result: UnreclaimableSweepResult{
			Mode: SweepModeActive, Candidates: 6, Terminalized: 4,
		}},
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}
	observation, err := pipeline.Step(testContext(), time.Now().UTC(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if observation.UnreclaimableCandidates != 6 || observation.UnreclaimableTerminalized != 4 {
		t.Fatalf("sweep figures did not reach the observation: %+v", observation)
	}
	if observation.RunawayDispatchWakeups != 9 {
		t.Fatalf("runaway gauge = %d, want the exact 9 over the threshold rather "+
			"than the 2-row sample", observation.RunawayDispatchWakeups)
	}
	if observation.WakeupReportFailures != 0 {
		t.Fatalf("a working report was counted as a failure: %+v", observation)
	}
	// The observer's own snapshot must survive the copy: a struct-level
	// assignment would have silently erased it.
	if observation.CeleryDuePending != 42 {
		t.Fatalf("the observer's own fields were clobbered: %+v", observation)
	}
}

// A broken detector must reach the counter, and it must NOT publish a
// reassuring zero gauge on the same pass -- "nothing looked" and "nothing
// found" are the two readings this whole series exists to separate.
func TestPipelineStepCountsAnUnavailableRunawayReportOnTheObservation(t *testing.T) {
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(contextT, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(contextT, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(contextT, time.Time, time.Time, int) (MaterializerResult, error) {
			return MaterializerResult{RunawayReportStep: runawayReportStepQuery}, nil
		}),
		pipelineKernelFunc(func(contextT, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(contextT, time.Time, int) (Observation, error) {
			return Observation{}, nil
		}),
		AtLeastOncePublisher(func(contextT, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(contextT, TransportClaim) error { return nil }),
		nil,
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}
	observation, err := pipeline.Step(testContext(), time.Now().UTC(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if observation.WakeupReportFailures != 1 {
		t.Fatalf("WakeupReportFailures = %d, want exactly one per failed pass",
			observation.WakeupReportFailures)
	}
}

// THE DURABLE-WORK RULE, extended to the sweep (the ExhaustedDeliveriesRecovered
// comment states it for the repair). The sweep commits before the stages after
// it run, so a later failure does not un-destroy the units it already
// terminalized. An observation reporting zero after a real terminalization
// would put destroyed work under its own alert threshold -- and the sweep is
// the one component here whose mistakes are unrecoverable.
func TestPipelineStepCarriesSweepFiguresOutOfAFailedPass(t *testing.T) {
	failure := errors.New("scripted terminal-delivery repair failure")
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(contextT, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(contextT, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, failure
		}),
		pipelineMaterializerFunc(func(contextT, time.Time, time.Time, int) (MaterializerResult, error) {
			t.Fatal("the materializer must not run after the repair failed")
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
		staticSweep{result: UnreclaimableSweepResult{
			Mode: SweepModeActive, Candidates: 3, Terminalized: 3,
		}},
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}
	observation, stepErr := pipeline.Step(testContext(), time.Now().UTC(), 10)
	if !errors.Is(stepErr, failure) {
		t.Fatalf("Step() error = %v, want the scripted failure", stepErr)
	}
	if observation.UnreclaimableTerminalized != 3 || observation.UnreclaimableCandidates != 3 {
		t.Fatalf("a failed pass dropped the sweep's already-committed work: %+v", observation)
	}
}

// "THE REPORT DID NOT RUN" IS WIDER THAN "THE REPORT FAILED" (adversarial
// review finding), and the gap was a blind spot in the very counter added to
// remove blind spots.
//
// A transaction that will not begin, an earlier materialization statement that
// faults, a failed commit — all return an empty MaterializerResult with
// RunawayReportStep unset. The detector demonstrably did not look, and the
// counter advertised as separating "nothing found" from "nothing looked" would
// have said it did, publishing a reassuring zero gauge beside it.
//
// This drives the upstream shape specifically: the materializer errors WITHOUT
// naming a report step.
func TestPipelineStepCountsAMaterializerFailureAsAReportThatDidNotRun(t *testing.T) {
	captured, restore := captureSlogRecords(t)
	defer restore()

	failure := errors.New("scripted materializer transaction failure")
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(contextT, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(contextT, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(contextT, time.Time, time.Time, int) (MaterializerResult, error) {
			// Exactly what a begin/commit/earlier-SQL failure produces: an
			// empty result and an error, with no report step named because
			// the report never executed.
			return MaterializerResult{}, failure
		}),
		pipelineKernelFunc(func(contextT, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			t.Fatal("the kernel must not run after the materializer failed")
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(contextT, time.Time, int) (Observation, error) {
			return Observation{}, nil
		}),
		AtLeastOncePublisher(func(contextT, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(contextT, TransportClaim) error { return nil }),
		nil,
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}
	observation, stepErr := pipeline.Step(testContext(), time.Now().UTC(), 10)
	if !errors.Is(stepErr, failure) {
		t.Fatalf("Step() error = %v, want the scripted failure", stepErr)
	}
	if observation.WakeupReportFailures != 1 {
		t.Fatalf("WakeupReportFailures = %d: the detector did not look, and the "+
			"counter that exists to say so stayed silent — the gauge's zero on "+
			"this pass is then indistinguishable from a real measurement",
			observation.WakeupReportFailures)
	}
	record, found := findSlogRecord(*captured, "syncreconciler.dispatch_wakeup_report_unavailable")
	if !found {
		t.Fatal("a pass that never reached the detector logged nothing about it")
	}
	// The two causes want different first questions from an operator, so the
	// log distinguishes them even though the counter merges them.
	if record["step"] != runawayReportStepUpstream {
		t.Fatalf("record step = %v, want the upstream cause named rather than a "+
			"report-statement failure it was not", record["step"])
	}
}

// A FAILING SWEEP MUST REACH THE COUNTER (adversarial review finding).
//
// TestPipelineStepSurvivesSweepFailure above pins that a sweep failure does
// not fail the pass, and that is correct: taking lease repair down with the
// safety net would trade a bounded strand for an unbounded one. But the
// consequence is that a sweep which has stopped working entirely leaves a
// healthy pass behind it and a candidate gauge of zero — indistinguishable
// from a system with nothing to sweep.
//
// CHAOS-4035 is what that looks like in production: this component answered
// 42501 once a second from its first deploy, and survived because nothing but
// a log line could see it.
func TestPipelineStepCountsASweepFailureOntoTheObservation(t *testing.T) {
	pipeline := pipelineWithSweep(t, failingSweep{err: ErrUnavailable})
	observation, err := pipeline.Step(testContext(), time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("a sweep failure took the pass down with it: %v", err)
	}
	if observation.UnreclaimableSweepFailures != 1 {
		t.Fatalf("UnreclaimableSweepFailures = %d: the safety net failed and the "+
			"only metric that could say so stayed at zero, which is what a "+
			"healthy idle system reports", observation.UnreclaimableSweepFailures)
	}
	// ...and the gauges must not fabricate a selection for a pass that failed.
	if observation.UnreclaimableCandidates != 0 || observation.UnreclaimableTerminalized != 0 {
		t.Fatalf("a failed sweep pass published figures it never measured: %+v", observation)
	}
}

// NON-VACUITY: a working sweep must leave the failure counter alone, or it
// climbs on every tick and stops distinguishing anything.
func TestPipelineStepLeavesTheSweepFailureCounterAloneOnAHealthyPass(t *testing.T) {
	pipeline := pipelineWithSweep(t, staticSweep{result: UnreclaimableSweepResult{
		Mode: SweepModeActive, Candidates: 2, Terminalized: 2,
	}})
	observation, err := pipeline.Step(testContext(), time.Now().UTC(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if observation.UnreclaimableSweepFailures != 0 {
		t.Fatalf("a successful sweep was counted as a failure: %+v", observation)
	}
}

// EVERY EXIT THAT PREVENTS THE REPORT MUST COUNT (adversarial review, third
// time this class appeared).
//
// The counter's contract is "this pass did not deliver a report". Three
// successive rounds each found another exit satisfying that description while
// leaving it at zero — the materializer's own error, then the swallowed
// non-fatal failures, then repair failure and terminal-repair failure. Every
// one was the same bug; every fix patched the site in front of it.
//
// The accounting is now deferred on the named return, so it covers paths this
// table does not list and paths added later. This table exists to prove the
// ones that already exist, and to fail loudly if someone replaces the defer
// with per-site assignments again.
func TestPipelineStepCountsEveryExitThatPreventsTheReport(t *testing.T) {
	failure := errors.New("scripted stage failure")
	okRepair := func(contextT, time.Time, int) (LeaseRepairResult, error) {
		return LeaseRepairResult{}, nil
	}
	okTerminal := func(contextT, time.Time, int) (TerminalDeliveryRepairResult, error) {
		return TerminalDeliveryRepairResult{}, nil
	}
	for _, testCase := range []struct {
		name     string
		repair   func(contextT, time.Time, int) (LeaseRepairResult, error)
		terminal func(contextT, time.Time, int) (TerminalDeliveryRepairResult, error)
	}{
		{
			name: "lease repair fails before anything else runs",
			repair: func(contextT, time.Time, int) (LeaseRepairResult, error) {
				return LeaseRepairResult{}, failure
			},
			terminal: okTerminal,
		},
		{
			name:   "terminal-delivery repair fails before the materializer",
			repair: okRepair,
			terminal: func(contextT, time.Time, int) (TerminalDeliveryRepairResult, error) {
				return TerminalDeliveryRepairResult{}, failure
			},
		},
		{
			name:   "the repair returns counts it cannot stand behind",
			repair: okRepair,
			// Recovered is out of range against its own subtotals, which the
			// pipeline treats as a failed step returning ErrUnavailable.
			terminal: func(contextT, time.Time, int) (TerminalDeliveryRepairResult, error) {
				return TerminalDeliveryRepairResult{Recovered: 1, ExhaustedRecovered: 5}, nil
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pipeline, err := NewMutationPipeline(
				pipelineLeaseRepairFunc(testCase.repair),
				pipelineTerminalDeliveryRepairFunc(testCase.terminal),
				pipelineMaterializerFunc(func(contextT, time.Time, time.Time, int) (MaterializerResult, error) {
					t.Fatal("the materializer must not run on this path")
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
				nil,
				DefaultMutationPipelineConfig(),
			)
			if err != nil {
				t.Fatalf("construct pipeline: %v", err)
			}
			observation, stepErr := pipeline.Step(testContext(), time.Now().UTC(), 10)
			if stepErr == nil {
				t.Fatal("Step() = nil, want the scripted failure")
			}
			if observation.WakeupReportFailures != 1 {
				t.Fatalf("WakeupReportFailures = %d on a pass that never reached the "+
					"report: the detector reads as continuously healthy through a "+
					"real upstream outage", observation.WakeupReportFailures)
			}
			// ...and the gauge must not claim to have measured anything.
			if observation.RunawayMeasured {
				t.Fatalf("a pass that never ran the report claimed to have measured it: %+v",
					observation)
			}
		})
	}
}

// NON-VACUITY for the defer: a pass that DID deliver a report must leave the
// counter alone, or it increments on every healthy tick and distinguishes
// nothing. This is the case the deferred accounting has to actively suppress.
func TestPipelineStepLeavesTheReportCounterAloneWhenTheReportRan(t *testing.T) {
	pipeline := pipelineWithSweep(t, staticSweep{result: UnreclaimableSweepResult{Mode: SweepModeShadow}})
	observation, err := pipeline.Step(testContext(), time.Now().UTC(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if observation.WakeupReportFailures != 0 || !observation.RunawayMeasured {
		t.Fatalf("a delivered report was counted as a failure: %+v", observation)
	}
}
