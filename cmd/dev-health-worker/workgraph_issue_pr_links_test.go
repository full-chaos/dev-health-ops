package main

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func frozenPreStep() *issuePRLinksPreStep {
	frozen := time.Date(2026, 9, 1, 12, 30, 45, 500_000_000, time.UTC)
	return &issuePRLinksPreStep{now: func() time.Time { return frozen }}
}

// TestBuildWindowDefaultsToThirtyDaysEndingNow pins the default that is easiest
// to get wrong by assuming an absent window means "unbounded".
//
// `run_work_graph_build` (workers/work_graph_tasks.py:121-132) sets
// `to = now` and `from = to - 30 days` when the scope omits them, so Python's
// build window is NEVER unbounded. A pre-step reading with no window would
// consider pull requests the Python producer never looks at and write mapping
// rows for them.
func TestBuildWindowDefaultsToThirtyDaysEndingNow(t *testing.T) {
	step := frozenPreStep()

	for _, scope := range [][]byte{nil, []byte(``), []byte(`{}`)} {
		window, err := step.windowFor(scope)
		if err != nil {
			t.Fatalf("windowFor(%q): %v", scope, err)
		}
		if window.To == nil || window.From == nil {
			t.Fatalf("windowFor(%q) left a bound unset: %+v", scope, window)
		}
		wantTo := time.Date(2026, 9, 1, 12, 30, 45, 500_000_000, time.UTC)
		if !window.To.Equal(wantTo) {
			t.Errorf("to = %s, want now (%s)", window.To, wantTo)
		}
		if want := wantTo.AddDate(0, 0, -30); !window.From.Equal(want) {
			t.Errorf("from = %s, want to-30d (%s)", window.From, want)
		}
		if window.RepoID != nil {
			t.Errorf("repo_id = %v, want unset", window.RepoID)
		}
	}
}

// TestBuildWindowReadsScopeDates covers the shape the bridge actually sends:
// bare ISO dates, which Python's fromisoformat turns into NAIVE midnight
// datetimes that its strftime then renders as a UTC literal.
func TestBuildWindowReadsScopeDates(t *testing.T) {
	step := frozenPreStep()
	window, err := step.windowFor([]byte(`{"from_date":"2026-07-01","to_date":"2026-08-15"}`))
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC); !window.From.Equal(want) {
		t.Errorf("from = %s, want %s", window.From, want)
	}
	if want := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC); !window.To.Equal(want) {
		t.Errorf("to = %s, want %s", window.To, want)
	}
}

// TestBuildWindowUsesToDateForTheFromDefault: `from` defaults to
// `to - 30 days`, not `now - 30 days`. With an explicit to_date those differ,
// and taking `now` would widen the window past what Python reads.
func TestBuildWindowUsesToDateForTheFromDefault(t *testing.T) {
	step := frozenPreStep()
	window, err := step.windowFor([]byte(`{"to_date":"2026-08-15"}`))
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC); !window.From.Equal(want) {
		t.Errorf("from = %s, want to-30d (%s), not now-30d", window.From, want)
	}
}

// TestBuildWindowAcceptsAZeroOffsetBound keeps the refusal from being
// over-strict, and keeps the two layers consistent: "Z" and "+00:00" denote the
// same wall clock either reading would produce, and
// issueprlinks.truncateBoundToSecond accepts a zero-offset bound too.
func TestBuildWindowAcceptsAZeroOffsetBound(t *testing.T) {
	step := frozenPreStep()
	for _, value := range []string{
		`{"to_date":"2026-08-15T06:07:08Z"}`,
		`{"to_date":"2026-08-15T06:07:08+00:00"}`,
	} {
		window, err := step.windowFor([]byte(value))
		if err != nil {
			t.Fatalf("windowFor(%s): %v", value, err)
		}
		if want := time.Date(2026, 8, 15, 6, 7, 8, 0, time.UTC); !window.To.Equal(want) {
			t.Errorf("windowFor(%s) to = %s, want %s", value, window.To, want)
		}
	}
}

func TestBuildWindowReadsRepoID(t *testing.T) {
	step := frozenPreStep()
	id := "11111111-1111-4111-8111-111111111111"
	window, err := step.windowFor([]byte(`{"repo_id":"` + id + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	if window.RepoID == nil || *window.RepoID != uuid.MustParse(id) {
		t.Fatalf("repo_id = %v, want %s", window.RepoID, id)
	}
}

func TestBuildWindowRejectsMalformedScopeValues(t *testing.T) {
	step := frozenPreStep()
	for _, scope := range []string{
		`{"repo_id":"not-a-uuid"}`,
		`{"to_date":"15/08/2026"}`,
		`not json`,
	} {
		if _, err := step.windowFor([]byte(scope)); err == nil {
			t.Errorf("windowFor(%s) accepted a malformed scope", scope)
		}
	}
}

// TestBuildWindowIgnoresTheHeuristicScopeKeys: the bridge's allowed set for
// workgraph.build also carries heuristic_window and heuristic_confidence
// (worker_workgraph.py:74-80). They belong to the heuristic edge builder, which
// this step does not implement, and their presence must not fail the step.
func TestBuildWindowIgnoresTheHeuristicScopeKeys(t *testing.T) {
	step := frozenPreStep()
	if _, err := step.windowFor([]byte(`{"heuristic_window":7,"heuristic_confidence":0.3}`)); err != nil {
		t.Fatalf("windowFor rejected a scope carrying heuristic keys: %v", err)
	}
}

func TestPreStepNameIsStable(t *testing.T) {
	// The name is a ledger evidence key; changing it orphans historical rows.
	if got := (&issuePRLinksPreStep{}).Name(); got != "issue_pr_links" {
		t.Fatalf("Name() = %q", got)
	}
}

// TestBuildPreStepOrderIsPinned makes appending a pre-step a DECISION.
//
// Order is load-bearing (see the ordering invariant on workgraph.NativePreStep):
// Python's build() runs the edge builder, then the mapping, then the fast-path
// edge builder that READS the mapping. A step registered on the wrong side of
// the mapping does not fail — it reads the previous run's rows and produces a
// plausible, stale result. Nothing else in the process would notice.
//
// So this test exists to FAIL when the list changes. That is the point: whoever
// appends has to come here, read the invariant, and state where their step
// belongs relative to "issue_pr_links". lane-4752-go's edge producer straddles
// it — but only one of its halves lands in THIS list; see the note below and
// buildPostStepOrder for why the other went to the post-step seam.
func TestBuildPreStepOrderIsPinned(t *testing.T) {
	want := []string{
		"issue_pr_links",
		"issue_pr_edges_fast_path", "issue_pr_edges_text_parse", "issue_pr_edges_heuristic",
		"pr_commit_links", "pr_commit_edges",
		"flag_guards_edges", "operational_incident_edges",
	}
	got := buildPreStepOrder()

	if len(got) != len(want) {
		t.Fatalf(
			"build pre-step order is %v, want %v.\n"+
				"If you are ADDING a step, read the ordering invariant on workgraph.NativePreStep "+
				"first, then place it by this rule: a step that READS a table an earlier step WRITES "+
				"goes after it.\n"+
				"NOTE (CHAOS-4766, lane-4766-go): `issue_issue_edges` does NOT appear here. It "+
				"was expected to register before `issue_pr_links`, and it does not read the "+
				"mapping, so that placement was correct by this rule — but Python's build() "+
				"OVERWRITES what it writes (confidence=1.0 at builder.py:905, and "+
				"work_graph_edges is ReplacingMergeTree(last_synced)), so it runs as a POST-step "+
				"instead; see buildPostStepOrder. A later PR of that lane ports "+
				"`_build_issue_pr_edges_from_fast_path`, which DOES read work_graph_issue_pr and "+
				"therefore registers HERE, after `issue_pr_links`.",
			got, want,
		)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("build pre-step order is %v, want %v", got, want)
		}
	}
}

// TestBuildPreStepOrderNamesAreUnique guards the evidence merge: fragments are
// keyed by step name, so two steps sharing one would silently drop a fragment.
func TestBuildPreStepOrderNamesAreUnique(t *testing.T) {
	seen := map[string]struct{}{}
	for _, name := range buildPreStepOrder() {
		if name == "" {
			t.Fatal("a pre-step has an empty name; it is a ledger evidence key")
		}
		if _, duplicate := seen[name]; duplicate {
			t.Fatalf("pre-step name %q appears twice; evidence fragments are keyed by name", name)
		}
		seen[name] = struct{}{}
	}
}

// TestBuildWindowTreatsAnEmptyScopeValueAsAbsent is codex round-1 F2 (EXECUTED).
//
// Python gates on TRUTHINESS -- `if to_date:` (work_graph_tasks.py:124,129) --
// so an empty string is "absent" and the default window applies. Go saw a
// non-nil pointer to "" and failed the whole build. An empty string is a
// perfectly ordinary serialisation of "no value", so this turned a normal
// Python default into a Go build failure.
//
// Worse than the divergence: the previous version of
// TestBuildWindowRejectsMalformedScopeValues asserted that rejection as
// DESIRABLE. A test can pin a bug as a requirement, which is why "the tests
// pass" is not the same claim as "the behaviour matches the reference".
func TestBuildWindowTreatsAnEmptyScopeValueAsAbsent(t *testing.T) {
	step := frozenPreStep()
	wantTo := time.Date(2026, 9, 1, 12, 30, 45, 500_000_000, time.UTC)

	// Only FALSY values. A whitespace-only string is NON-EMPTY and therefore
	// TRUTHY in Python — it reaches fromisoformat and raises — so it does NOT
	// belong here. The first version of this test listed it, which made the
	// test a false oracle: it asserted that Go should default where Python
	// rejects. See TestBuildWindowRejectsWhitespaceLikePython.
	for _, scope := range []string{
		`{"from_date":""}`,
		`{"to_date":""}`,
		`{"repo_id":""}`,
		`{"to_date":null}`,
		`{"to_date":false}`,
		`{"to_date":0}`,
	} {
		window, err := step.windowFor([]byte(scope))
		if err != nil {
			t.Fatalf("windowFor(%s) failed; Python treats an empty value as absent: %v", scope, err)
		}
		if !window.To.Equal(wantTo) {
			t.Errorf("windowFor(%s) to = %s, want the default now (%s)", scope, window.To, wantTo)
		}
		if want := wantTo.AddDate(0, 0, -30); !window.From.Equal(want) {
			t.Errorf("windowFor(%s) from = %s, want the default to-30d (%s)", scope, window.From, want)
		}
		if window.RepoID != nil {
			t.Errorf("windowFor(%s) repo_id = %v, want unset", scope, window.RepoID)
		}
	}
}

// TestBuildWindowRejectsWhitespaceLikePython is codex round-2, and it is the
// SECOND time this one gate caught me guessing.
//
// Round 1 found that Python treats "" as absent (falsy) while I failed the
// build. I fixed that — and then EXTENDED it to whitespace by assumption,
// trimming before the emptiness test, and wrote a test asserting the extension
// as correct. Measured against the deployed interpreter:
//
//	''    -> FALSY, default window
//	'\t'  -> TRUTHY -> ValueError: Invalid isoformat string
//	'   ' -> TRUTHY -> ValueError: Invalid isoformat string
//
// A whitespace-only string is non-empty, so Python parses it and RAISES. My
// trimming turned a request Python REJECTS into a default 30-day window — and
// this step writes mapping rows before the bridge ever gets to reject it.
//
// The lesson is not "handle whitespace". It is that the fix for a measured
// divergence must itself be measured: I had the interpreter open when I fixed
// the empty-string case and did not spend one more line on the neighbouring
// input.
func TestBuildWindowRejectsWhitespaceLikePython(t *testing.T) {
	step := frozenPreStep()
	for _, scope := range []string{
		`{"to_date":"\t"}`,
		`{"from_date":"   "}`,
		`{"repo_id":" "}`,
	} {
		if _, err := step.windowFor([]byte(scope)); err == nil {
			t.Errorf(
				"windowFor(%s) defaulted; a whitespace-only value is TRUTHY in Python and "+
					"raises in fromisoformat, so this must fail too", scope,
			)
		}
	}
}

// TestBuildWindowRejectsATruthyNonString: Python's scope filter checks field
// NAMES, not types, so a truthy non-string reaches fromisoformat and raises
// TypeError. Go must fail rather than defaulting or silently unmarshalling.
func TestBuildWindowRejectsATruthyNonString(t *testing.T) {
	step := frozenPreStep()
	for _, scope := range []string{
		`{"to_date":123}`,
		`{"to_date":true}`,
		`{"from_date":["2026-08-15"]}`,
	} {
		if _, err := step.windowFor([]byte(scope)); err == nil {
			t.Errorf("windowFor(%s) accepted a truthy non-string; the reference raises TypeError", scope)
		}
	}
}

// TestBuildWindowAcceptsExactlyTheCanonicalShapes replaces four tests that each
// asserted a WIDER accept set, and it is worth recording what they were,
// because their history is the argument for this one.
//
//	TestBuildWindowAcceptsADateTimeWithoutAnOffset      round 2, F2
//	TestBuildWindowAcceptsTheOtherISOFormsPythonAccepts round 2, basic dates + minute precision
//	TestBuildWindowAcceptsAColonLessZeroOffset          round 4, "+0000"
//	TestBuildWindowRefusesAnOffsetBearingDate           round 1, F1
//
// Each was written to close a real gap, each widened the parser toward
// `datetime.fromisoformat`, and rounds 7, 8 and 9 then each found a
// DANGEROUS-DIRECTION defect in the widened surface -- every one introduced by
// the previous round's fix. The surface was the problem, not any of the fixes.
//
// So the accept set is now exactly the three shapes the producers emit, and
// everything those four tests asserted is refused fail-closed. The forms are
// still measured against the reference in the 1,492-case corpus; they are
// enumerated there under one class rather than accepted here.
func TestBuildWindowAcceptsExactlyTheCanonicalShapes(t *testing.T) {
	step := frozenPreStep()

	for _, testCase := range []struct {
		scope string
		want  time.Time
	}{
		// The live Go emitter's shape: sync_dispatch.go formats with
		// time.RFC3339, and all 744 dated values on the proof org look like this.
		{`{"to_date":"2026-08-15T06:07:08Z"}`, time.Date(2026, 8, 15, 6, 7, 8, 0, time.UTC)},
		// The Python plane's shape.
		{`{"to_date":"2026-08-15T06:07:08+00:00"}`, time.Date(2026, 8, 15, 6, 7, 8, 0, time.UTC)},
		// The date-only fallback.
		{`{"to_date":"2026-08-15"}`, time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)},
	} {
		window, err := step.windowFor([]byte(testCase.scope))
		if err != nil {
			t.Fatalf("windowFor(%s) refused a shape a producer emits: %v", testCase.scope, err)
		}
		if !window.To.Equal(testCase.want) {
			t.Errorf("windowFor(%s) to = %s, want %s", testCase.scope, window.To, testCase.want)
		}
	}

	for _, scope := range []string{
		// What the four replaced tests used to require be ACCEPTED.
		`{"to_date":"2026-08-15T06:07:08"}`,
		`{"to_date":"20260815"}`,
		`{"to_date":"2026-08-15T06:07"}`,
		`{"to_date":"2026-08-15T06:07:08+0000"}`,
		// And what one of them required be refused, which still is.
		`{"to_date":"2026-01-01T00:00:00+05:00"}`,
		`{"from_date":"2026-01-01T00:00:00-08:00"}`,
		// Value checks behind the shape gate.
		`{"to_date":"0000-01-01T00:00:00Z"}`,
		`{"to_date":"2026-02-30"}`,
		`{"to_date":"2026-13-01"}`,
	} {
		if _, err := step.windowFor([]byte(scope)); err == nil {
			t.Errorf("windowFor(%s) accepted a non-canonical scope", scope)
		}
	}

	// The DERIVED lower bound is range-checked too: this parses, and then
	// `to - 30 days` leaves the reference's 1..9999 year range.
	if _, err := step.windowFor([]byte(`{"to_date":"0001-01-01"}`)); err == nil {
		t.Error("windowFor accepted a to_date whose default lower bound underflows year 1")
	}
}
