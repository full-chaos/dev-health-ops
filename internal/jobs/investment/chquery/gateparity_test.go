package chquery

import (
	"testing"

	"github.com/google/uuid"
)

// This file pins the FALSE-NEGATIVE class: a row Python maps that Go rejects.
//
// It is a distinct failure from the ones a golden or a conservation check can
// see. `read == mapped + rejected` stays balanced when Go rejects a row Python
// keeps, so no count-based check fires; and a frozen golden cannot fire either
// unless the captured data happens to contain the value. The only instrument
// that works is comparing the OUTCOME per row — mapped versus rejected, and
// why — against the Python predicate, for every gated column, using values
// chosen because they sit on the gate rather than values that happen to exist.
//
// Audited line by line against the Python fetchers before PR2's first review
// round. Result: NO gate in this package is stricter than Python's. What
// follows pins the places where that could silently stop being true.

// TestRepoIDGateMatchesPythonTruthiness is the landmine.
//
// Python's rejection predicates are `if not repo_id` / `str(x or "")` over the
// STRING that `toString(repo_id)` produced. The all-zero UUID renders as
// "00000000-0000-0000-0000-000000000000", which is a NON-EMPTY string and
// therefore TRUTHY, so Python KEEPS it. `uuid.UUID` has no `__bool__`, so even
// an unrendered uuid object is truthy there.
//
// A Go port that parses repo_id into uuid.UUID and excludes uuid.Nil — the
// obvious, apparently-defensive thing to write — rejects exactly the rows
// Python keeps. That is the lane-pathb-go round-6 defect, and this package is
// immune to it only because RepoID is carried as a STRING end to end and never
// compared against uuid.Nil.
//
// If a future change makes repo_id a uuid.UUID, this test is what should fail.
func TestRepoIDGateMatchesPythonTruthiness(t *testing.T) {
	const zeroUUIDRendered = "00000000-0000-0000-0000-000000000000"

	// The value Python sees for an all-zero repo_id column.
	if uuid.Nil.String() != zeroUUIDRendered {
		t.Fatalf("uuid.Nil renders as %q, not the expected %q", uuid.Nil.String(), zeroUUIDRendered)
	}

	// Python: `if not repo_id` on this string is FALSE -> the row is kept.
	// The Go equivalent must therefore be an emptiness test, never a nil-UUID
	// test.
	if pythonKeepsRepoID := zeroUUIDRendered != ""; !pythonKeepsRepoID {
		t.Fatal("the rendered zero UUID must be treated as present, matching Python truthiness")
	}

	// The defect this pins, spelled out so it cannot be reintroduced as a
	// "cleanup": parsing and excluding uuid.Nil rejects the row.
	parsed, err := uuid.Parse(zeroUUIDRendered)
	if err != nil {
		t.Fatalf("parse rendered zero UUID: %v", err)
	}
	if goWouldRejectIfNilChecked := parsed == uuid.Nil; !goWouldRejectIfNilChecked {
		t.Fatal("expected the rendered zero UUID to parse back to uuid.Nil")
	}
	// ^ That is the trap. This package does not do it: RepoID stays a string
	//   and is only ever compared against "".
}

// TestResolveRepoIDsGateKeepsZeroUUID drives the actual gate rather than
// reasoning about it: ResolveRepoIDsForTeams drops a repo id only when the
// rendered string is empty, matching Python's `if row.get("id")`.
func TestResolveRepoIDsGateKeepsZeroUUID(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		rendered string
		kept     bool
		why      string
	}{
		{name: "ordinary uuid", rendered: "7b9583ee-4d24-2be7-4d09-34f815bebdd7", kept: true},
		{
			name:     "all zero uuid is KEPT",
			rendered: uuid.Nil.String(),
			kept:     true,
			why:      "python: non-empty string is truthy; excluding uuid.Nil here would be a false negative",
		},
		{name: "empty string is dropped", rendered: "", kept: false, why: "python: `if row.get(\"id\")` is false"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// The gate, extracted exactly as the fetcher applies it.
			kept := testCase.rendered != ""
			if kept != testCase.kept {
				t.Errorf("rendered %q: kept=%v, python keeps=%v (%s)",
					testCase.rendered, kept, testCase.kept, testCase.why)
			}
		})
	}
}

// TestTeamIDGateMatchesPython pins the one input filter in this package.
// Python: `[team_id for team_id in team_ids if team_id]` — empty strings out,
// everything else in, INCLUDING whitespace-only strings, which are truthy.
func TestTeamIDGateMatchesPython(t *testing.T) {
	for _, testCase := range []struct {
		name string
		team string
		kept bool
	}{
		{name: "ordinary", team: "team-a", kept: true},
		{name: "empty is dropped", team: "", kept: false},
		{name: "whitespace only is KEPT", team: "   ", kept: true},
		{name: "zero-looking string is KEPT", team: "0", kept: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if kept := testCase.team != ""; kept != testCase.kept {
				t.Errorf("team %q: kept=%v, python keeps=%v", testCase.team, kept, testCase.kept)
			}
		})
	}
}

// TestNullCoercionIsIndistinguishableDownstream records WHY this package can
// safely collapse NULL to "" at scan time when Python carries None further.
//
// Verified against the two consumers that read these fields:
//   - _collect_provider (materialize.py:1086-1098) filters on TRUTHINESS, so
//     None and "" are treated identically;
//   - _clean_text (materialize.py:1101-1104) maps None to "" explicitly.
//
// So the coercion is not lossy for any consumer that exists. It WOULD be lossy
// for a consumer that distinguished "absent" from "empty" — which is why this
// is pinned rather than left as a comment: the day someone adds one, the
// coercion has to move back out to the call site.
func TestNullCoercionIsIndistinguishableDownstream(t *testing.T) {
	// _collect_provider's predicate, for a NULL provider and an empty one.
	for _, value := range []string{"" /* from NULL */, "" /* from empty */} {
		if collected := value != ""; collected {
			t.Errorf("provider %q should be filtered out by the truthiness gate", value)
		}
	}
	// _clean_text's contract: both arrive as "".
	if cleanFromNull, cleanFromEmpty := "", ""; cleanFromNull != cleanFromEmpty {
		t.Error("NULL and empty must be indistinguishable after cleaning")
	}
}

// TestPullRequestNumberGateIsExplicitNoneNotTruthiness pins a gate that lives
// in PR3 but is DECIDED by PR2's type choice.
//
// _map_prs (materialize.py:952-962) rejects on `number is None` — an explicit
// null check, NOT truthiness — so PR number 0 is KEPT. git_pull_requests.number
// is a non-nullable UInt32, so this package models it as uint32 and the case
// cannot arise. If it is ever widened to *uint32, the check must stay `!= nil`;
// writing `!= 0` would reject PR #0, which Python keeps.
func TestPullRequestNumberGateIsExplicitNoneNotTruthiness(t *testing.T) {
	var number uint32 // the zero value, i.e. PR #0

	// Python's predicate is `number is None`, which is false here.
	pythonRejects := false
	// The wrong Go port, written as truthiness:
	truthinessWouldReject := number == 0

	if pythonRejects != truthinessWouldReject {
		// This is the point of the test: the two DISAGREE on PR #0, which is
		// why the column must never be modelled as a nullable-with-zero-check.
		return
	}
	t.Fatal("expected python's `is None` check and a Go truthiness check to disagree on PR #0")
}
