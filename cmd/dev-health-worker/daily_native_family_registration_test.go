package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// fakeDailyStoreForRegistrationTest is the daily.Store analog of
// githubWorkItemsBuildExecutorConn below: a non-nil value that satisfies the
// interface for construction-time nil checks (CHAOS-5194:
// NewBenchmarkingFinalizeExecutor requires store != nil) without ever having
// a method called -- ClickHouse/Postgres I/O happens later, when the handler
// executes a run, never at construction.
type fakeDailyStoreForRegistrationTest struct{ daily.Store }

// CHAOS-4292 rebase-gate finding (codex, 2026-09-01, two rounds): the
// pre-existing drift checks this metrics.daily cutover wave relied on --
// families_test.go's families.json validation and internal/jobruntime's
// TestDailyMetricsNativeFamiliesCoverEveryPortedFamily -- both read a
// declared source of truth, never buildDailyWorker's ACTUAL dispatch
// registration. A first fix (parsing daily.go's source with go/ast) closed
// the "assignment is missing entirely" case but codex then proved even
// that insufficient: inserting `delete(nativeFamilies, "incident")`
// immediately before SetNativeFamilies left the AST-based test green too,
// since the assignment statement was still present in source -- the AST
// walk could not see that a LATER statement undid it before the setter
// call ever ran.
//
// The real fix (team-lead ruling): dailyNativeFamilyRegistrations is now a
// PURE FUNCTION returning the native/postBridge maps, and buildDailyWorker
// passes its return values straight to the two setter calls with no
// intermediate variable a stray statement could mutate in between (see
// that function's own doc comment). This test calls it directly with a
// connection stub that makes every executor constructor succeed (each one
// only checks conn != nil at construction time; ClickHouse I/O happens
// later, when the handler executes a partition) and asserts SET EQUALITY,
// both directions, between the ACTUAL returned map keys and families.json's
// own "port":"go" set. There is no longer any source text between
// construction and assertion for an adversarial edit to hide in.
//
// CHAOS-4278's post_bridge phase adds a THIRD way this pair can drift,
// caught by a follow-up scoped round (codex, 2026-09-01): merging native
// and postBridge into one "is it registered anywhere" key set (as an
// earlier revision of this test did) cannot see a family registered in the
// WRONG map -- moving work_item_state from postBridge to native inside
// dailyNativeFamilyRegistrations left every drift test green, including
// this one, because "registered, and port=go" was still true. That
// mutation is a real regression: work_item_state reads
// work_item_team_attributions, written by the still-Python-bridged
// work_item_attribution family in the SAME partition, so running it
// pre_bridge means it observes stale (or absent, for a newly-attributed
// item) data -- the exact bug CHAOS-4278's own post_bridge mechanism
// exists to prevent (see WorkItemStateExecutor's doc comment). This test
// now checks PHASE too: a family registered in native must be
// families.json phase ""/"pre_bridge"; a family in postBridge must be
// families.json phase "post_bridge". Either mismatch is a drift the two
// maps' mere key-set union could never see.
func TestDailyNativeFamilyRegistrationsMatchesFamiliesJSONPortGo(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	native, postBridge, finalize, _ := dailyNativeFamilyRegistrations(
		fakeDailyStoreForRegistrationTest{}, githubWorkItemsBuildExecutorConn{}, nil, logger,
	)

	registeredPhase := make(map[string]string, len(native)+len(postBridge))
	for family := range native {
		registeredPhase[family] = "pre_bridge"
	}
	for family := range postBridge {
		if _, alreadyRegistered := registeredPhase[family]; alreadyRegistered {
			t.Fatalf("family %q is registered in BOTH native and postBridge maps -- "+
				"SetNativeFamilies/SetPostBridgeNativeFamilies each replace their own "+
				"map wholesale, so a family present in both is dispatched twice, not once", family)
		}
		registeredPhase[family] = "post_bridge"
	}
	for family := range finalize {
		if _, alreadyRegistered := registeredPhase[family]; alreadyRegistered {
			t.Fatalf("family %q is registered as a finalize family AND in a partition "+
				"map -- it would run once per partition AND once per run, writing the "+
				"same rows repeatedly", family)
		}
		registeredPhase[family] = "finalize"
	}

	goFamilyPhase := readFamiliesJSONPortGoPhases(t)

	var missingFromRegistration []string
	for family := range goFamilyPhase {
		if _, ok := registeredPhase[family]; !ok {
			missingFromRegistration = append(missingFromRegistration, family)
		}
	}
	sort.Strings(missingFromRegistration)
	if len(missingFromRegistration) > 0 {
		t.Errorf(
			"families.json marks %v as port=\"go\" but dailyNativeFamilyRegistrations "+
				"(cmd/dev-health-worker/daily.go) does not return them in its native or "+
				"postBridge map -- every partition for this family silently falls "+
				"through to the Python compatibility bridge with no refusal log and no "+
				"native-family telemetry",
			missingFromRegistration,
		)
	}

	var registeredButNotGo []string
	var wrongPhase []string
	for family, gotPhase := range registeredPhase {
		wantPhase, isGo := goFamilyPhase[family]
		if !isGo {
			registeredButNotGo = append(registeredButNotGo, family)
			continue
		}
		if gotPhase != wantPhase {
			wrongPhase = append(wrongPhase, family+": registered "+gotPhase+", families.json wants "+wantPhase)
		}
	}
	sort.Strings(registeredButNotGo)
	if len(registeredButNotGo) > 0 {
		t.Errorf(
			"dailyNativeFamilyRegistrations returns %v but families.json does not "+
				"mark them port=\"go\" -- either families.json is stale (a family's "+
				"cutover flag was reverted or never flipped) or the registration is "+
				"dead code; both call sites must agree",
			registeredButNotGo,
		)
	}
	sort.Strings(wrongPhase)
	if len(wrongPhase) > 0 {
		t.Errorf(
			"registration phase mismatch: %v -- a family in the WRONG map (native "+
				"vs postBridge) runs at the wrong point relative to the Python "+
				"compatibility bridge call for its partition; for a family with a "+
				"real post_bridge dependency (like work_item_state's read of "+
				"work_item_team_attributions) this is silent stale-data corruption, "+
				"not a crash",
			wrongPhase,
		)
	}
}

// readFamiliesJSONPortGoPhases reads the drift-gated families.json (the
// same file families_test.go validates) and returns, for every family
// currently marked "port":"go", the phase dailyNativeFamilyRegistrations
// must register it under: "post_bridge" families.json entries map to
// "post_bridge"; everything else (an omitted phase, or the explicit
// "pre_bridge" default) maps to "pre_bridge".
func readFamiliesJSONPortGoPhases(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "jobs", "metrics", "daily", "families.json"))
	if err != nil {
		t.Fatalf("read families.json: %v", err)
	}
	var registry struct {
		Families []struct {
			Name  string `json:"name"`
			Port  string `json:"port"`
			Phase string `json:"phase"`
		} `json:"families"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		t.Fatalf("decode families.json: %v", err)
	}
	goFamilyPhase := make(map[string]string, len(registry.Families))
	for _, family := range registry.Families {
		if family.Port != "go" {
			continue
		}
		switch family.Phase {
		case "post_bridge":
			goFamilyPhase[family.Name] = "post_bridge"
		case "finalize":
			// CHAOS-4290: a RUN-scoped family, registered through
			// FinalizeHandler.SetNativeFinalizeFamilies rather than either
			// partition map. A third bucket, not a variant of pre_bridge --
			// a finalize family in a partition map would run once per
			// PARTITION instead of once per run, writing the same rows
			// repeatedly.
			goFamilyPhase[family.Name] = "finalize"
		default:
			goFamilyPhase[family.Name] = "pre_bridge"
		}
	}
	return goFamilyPhase
}

// recordingNativeFamilyObserver captures every ObserveDailyMetricsNativeFamily
// call, mirroring internal/jobs/metrics/daily's own test double of the same
// name (daily_test.go) -- that one lives in a different package, so this is
// a separate copy rather than an import, matching this file's existing
// pattern of duplicating small fakes per package rather than exporting them.
// Embeds jobruntime.Observer (nil), same shape as this file's own
// fakeObserverWithNoCollector (daily_test.go), to satisfy
// dailyNativeFamilyRegistrations' wide observer parameter without stubbing
// every unrelated method by hand.
type recordingNativeFamilyObserver struct {
	jobruntime.Observer
	mu    sync.Mutex
	calls []recordingNativeFamilyObservation
}

type recordingNativeFamilyObservation struct {
	family      string
	outcome     jobruntime.DailyMetricsNativeFamilyOutcome
	rowsWritten int
}

func (observer *recordingNativeFamilyObserver) ObserveDailyMetricsNativeFamily(
	family string, outcome jobruntime.DailyMetricsNativeFamilyOutcome, rowsWritten int, _ time.Duration,
) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.calls = append(observer.calls, recordingNativeFamilyObservation{family: family, outcome: outcome, rowsWritten: rowsWritten})
	return nil
}

// TestDailyNativeFamilyRegistrationsReturnsEveryRefusalForFailFast is
// CHAOS-3092 (PR-A)'s startup fail-fast proof.
//
// Before this change, a partition-scope family whose executor could not be
// constructed was logged and left off the map, because "the Python
// compatibility bridge still computes that family for every partition."
// That bridge is deleted. An unregistered family now means its rows are
// never written by ANYONE, silently, for the worker's whole process
// lifetime -- so every refusal must instead reach buildDailyWorker as a
// startup error naming the family and its cause.
//
// This asserts the mechanism that makes that possible: with a nil ClickHouse
// connection every constructor refuses, and EVERY refusal (not a sampled
// four, not only the ones with an observer hook) is returned in `refusals`
// carrying both a family name and a non-nil error. It also asserts the
// refusal set and the native/postBridge maps are exact complements, which is
// what stops a family from being silently dropped on the floor -- neither
// registered nor reported.
func TestDailyNativeFamilyRegistrationsReturnsEveryRefusalForFailFast(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	observer := &recordingNativeFamilyObserver{}

	// nil connection fails every native executor's own `conn != nil` check.
	native, postBridge, _, refusals := dailyNativeFamilyRegistrations(
		fakeDailyStoreForRegistrationTest{}, nil, observer, logger,
	)

	if len(refusals) == 0 {
		t.Fatal("no refusals returned with a nil ClickHouse connection -- " +
			"buildDailyWorker would start a worker that computes nothing")
	}
	if len(native) != 0 || len(postBridge) != 0 {
		t.Fatalf("native=%d postBridge=%d families registered with a nil connection, want 0/0",
			len(native), len(postBridge))
	}
	seen := make(map[string]struct{}, len(refusals))
	for _, refusal := range refusals {
		if refusal.family == "" {
			t.Errorf("refusal %#v has no family name -- the startup error could not name it", refusal)
		}
		if refusal.err == nil {
			t.Errorf("refusal for %q carries a nil error -- the startup error could not say why", refusal.family)
		}
		if _, duplicate := seen[refusal.family]; duplicate {
			t.Errorf("family %q reported refused twice", refusal.family)
		}
		seen[refusal.family] = struct{}{}
	}

	// Every families.json family this wiring is responsible for must be
	// accounted for exactly once: registered, or reported refused. A family
	// that is neither is the silent gap this ticket exists to close.
	for _, family := range []string{
		"team_wellbeing", "repo_user_commit", "incident", "deploy", "cicd",
		"file_hotspots", "file_risk_hotspots", "ai_governance", "ai_impact",
		"work_graph_edges", "ai_workflow", "testops_risk",
		"work_item_attribution", "testops_pipeline", "testops_test",
		"testops_coverage", "review_edges", "compounding_risk",
		"work_item_state", "work_item", "work_item_estimate",
	} {
		if _, refused := seen[family]; !refused {
			t.Errorf(
				"family %q was neither registered nor reported refused with a nil "+
					"connection -- it would be silently absent from a started worker "+
					"with no Python bridge behind it (CHAOS-3092)",
				family,
			)
		}
	}
}

// TestDailyNativeFamilyRegistrationsObservesRefusalForDeletedPythonFamilies is
// CHAOS-5342 (found during #2317's r1 review, delete-unowned-daily-families):
// CHAOS-3092/CHAOS-5311/CHAOS-5312/CHAOS-5313/CHAOS-5309 deleted
// team_wellbeing/cicd/incident/deploy's Python compute ENTIRELY, so a
// construction-time refusal for these four specifically has no fallback
// left at all -- unlike every OTHER family in
// dailyNativeFamilyRegistrations, where a refusal genuinely does leave the
// family on a still-live Python compatibility bridge (fail-open by design,
// one family degrading must never take another down). deploy was folded in
// alongside the original three (team_wellbeing/cicd/incident): identical
// gap, same fix, one PR rather than a second near-duplicate ticket.
//
// Before this fix, a refusal for these four was logged (with the FALSE
// claim that the family "stays on the Python compatibility bridge") and
// otherwise produced nothing observable: no counter, nothing a dashboard
// could alert on -- the family's rows simply stop being written for the rest
// of the worker's process lifetime, indistinguishable from a quiet day. This
// test forces every constructor to refuse (nil connection) and asserts the
// four deleted-Python families are recorded as
// DailyMetricsNativeFamilyOutcomeRefused via the SAME per-partition
// telemetry channel PartitionHandler already uses
// (jobruntime.DailyMetricsNativeFamilyObserver) -- reusing existing
// observability plumbing rather than inventing a new metric. Other,
// still-Python-fallback families' refusal behavior is intentionally
// untouched and out of this ticket's scope.
func TestDailyNativeFamilyRegistrationsObservesRefusalForDeletedPythonFamilies(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	observer := &recordingNativeFamilyObserver{}

	// nil connection fails every native executor's own `conn != nil` check,
	// refusing construction for the whole registry.
	native, _, _, _ := dailyNativeFamilyRegistrations(fakeDailyStoreForRegistrationTest{}, nil, observer, logger)

	for _, family := range []string{"team_wellbeing", "cicd", "incident", "deploy"} {
		if _, stillRegistered := native[family]; stillRegistered {
			t.Fatalf("family %q registered natively with a nil connection -- "+
				"the refusal path this test exercises never ran", family)
		}
	}

	observer.mu.Lock()
	calls := append([]recordingNativeFamilyObservation(nil), observer.calls...)
	observer.mu.Unlock()

	observed := make(map[string]jobruntime.DailyMetricsNativeFamilyOutcome, len(calls))
	for _, call := range calls {
		observed[call.family] = call.outcome
	}
	for _, family := range []string{"team_wellbeing", "cicd", "incident", "deploy"} {
		outcome, ok := observed[family]
		if !ok {
			t.Errorf(
				"family %q refused construction with no Python fallback left "+
					"(CHAOS-3092/CHAOS-5311/CHAOS-5312/CHAOS-5313/CHAOS-5309) but "+
					"dailyNativeFamilyRegistrations recorded NO "+
					"ObserveDailyMetricsNativeFamily call for it -- the refusal "+
					"is invisible to the same telemetry every partition-level "+
					"refusal already uses",
				family,
			)
			continue
		}
		if outcome != jobruntime.DailyMetricsNativeFamilyOutcomeRefused {
			t.Errorf("family %q recorded outcome %q, want %q", family, outcome, jobruntime.DailyMetricsNativeFamilyOutcomeRefused)
		}
	}
}
