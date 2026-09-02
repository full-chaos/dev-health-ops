package main

import (
	"strings"
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

func TestBuildWindowAcceptsADateTimeWithoutAnOffset(t *testing.T) {
	step := frozenPreStep()
	window, err := step.windowFor([]byte(`{"to_date":"2026-08-15T06:07:08"}`))
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 8, 15, 6, 7, 8, 0, time.UTC); !window.To.Equal(want) {
		t.Errorf("to = %s, want %s", window.To, want)
	}
}

// TestBuildWindowRefusesAnOffsetBearingDate is the scope-level half of
// issueprlinks.ErrNonUTCWindowBound. Python's strftime keeps the wall-clock
// fields and DISCARDS the offset, so `2026-01-01T00:00:00+05:00` means
// `2026-01-01 00:00:00 UTC` there and `2025-12-31T19:00:00Z` under any
// instant-preserving reading. Different queries; nothing produces such a scope
// today; guessing between them is what the refusal exists to prevent.
func TestBuildWindowRefusesAnOffsetBearingDate(t *testing.T) {
	step := frozenPreStep()
	for _, value := range []string{
		`{"to_date":"2026-01-01T00:00:00+05:00"}`,
		`{"from_date":"2026-01-01T00:00:00-08:00"}`,
	} {
		if _, err := step.windowFor([]byte(value)); err == nil {
			t.Errorf("windowFor(%s) accepted a shifted bound", value)
		} else if !strings.Contains(err.Error(), "offset") {
			t.Errorf("windowFor(%s) error = %v, want it to name the offset", value, err)
		}
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
// it and therefore needs at least two entries, one on each side.
func TestBuildPreStepOrderIsPinned(t *testing.T) {
	want := []string{"issue_pr_links"}
	got := buildPreStepOrder()

	if len(got) != len(want) {
		t.Fatalf(
			"build pre-step order is %v, want %v.\n"+
				"If you are ADDING a step, read the ordering invariant on workgraph.NativePreStep "+
				"first, then place it by this rule: a step that READS a table an earlier step WRITES "+
				"goes after it.\n"+
				"KNOWN PENDING (CHAOS-4766, lane-4766-go): `issue_issue_edges` registers BEFORE "+
				"`issue_pr_links` — it does not read the mapping — giving "+
				"[issue_issue_edges issue_pr_links]. A later PR of that lane ports "+
				"`_build_issue_pr_edges_from_fast_path`, which DOES read work_graph_issue_pr and "+
				"therefore registers AFTER `issue_pr_links`.",
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

	for _, scope := range []string{
		`{"from_date":""}`,
		`{"to_date":""}`,
		`{"from_date":"   ","to_date":"\t"}`,
		`{"repo_id":""}`,
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

// TestBuildWindowAcceptsTheOtherISOFormsPythonAccepts covers the rest of F2:
// `datetime.fromisoformat` (3.11+) accepts basic dates and minute precision,
// verified against the deployed interpreter.
func TestBuildWindowAcceptsTheOtherISOFormsPythonAccepts(t *testing.T) {
	step := frozenPreStep()
	for _, testCase := range []struct {
		scope string
		want  time.Time
	}{
		{`{"to_date":"20260815"}`, time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)},
		{`{"to_date":"2026-08-15T06:07"}`, time.Date(2026, 8, 15, 6, 7, 0, 0, time.UTC)},
	} {
		window, err := step.windowFor([]byte(testCase.scope))
		if err != nil {
			t.Fatalf("windowFor(%s): %v", testCase.scope, err)
		}
		if !window.To.Equal(testCase.want) {
			t.Errorf("windowFor(%s) to = %s, want %s", testCase.scope, window.To, testCase.want)
		}
	}
}
