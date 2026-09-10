package goapiproof

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The whole input DOMAIN of every guard this change adds or modifies,
// executed in one pass -- each field crossed with every shape a value can
// take, not a sample of the shapes its author thought of.
//
// This is the shape dimension. The vocabulary dimension lives in
// TestTheEnablementRuleOverItsWholeInputSurface, which crosses every
// stage x terminal state x route x binding x outside-count x citation
// shape x target mode against real PostgreSQL. That test asserts its own
// size from the slices it iterates, so no count is written down here --
// a hand-copied number is what went stale three times (opus r5, P3).
//
// WHY SOME CELLS ARE ABSENT RATHER THAN FAILING. Go's type system makes
// several cells UNREPRESENTABLE, and a test asserting them would not
// compile. They are recorded here so "we tested every cell" is checkable
// rather than asserted:
//
//   - wrong scalar type   -- `Receipt.MeasurementRoute` is a `string`;
//     passing an int is a compile error, never a
//     runtime input. Same for every other field.
//   - wrong container type -- `BaselineDefects` is `[]string`; a map or a
//     scalar cannot be assigned to it.
//   - fractional where integral -- `DifferencesOutsideBaselineDefect` is
//     an `int`; 0.5 is a compile error.
//   - null vs absent      -- a Go string has no null distinct from "".
//     The DISTINCTION is real one layer down, in
//     the database, and is executed there: the
//     surface test writes a genuine SQL NULL into
//     measurement_route, build_binding and
//     baseline_defect and asserts the predicate's
//     answer for each.
//
// So the cells below are the ones a Go caller can actually construct.
// Everything unrepresentable is named above with the reason.
func TestWriteRefusesEveryShapeOutsideItsContract(t *testing.T) {
	canonical := func() Receipt {
		r := wellFormedReceipt()
		return r
	}

	cases := []struct {
		field  string
		shape  string
		mutate func(*Receipt)
		accept bool // true = the contract ACCEPTS this shape
		names  string
	}{
		// --- CandidateBuild -------------------------------------------
		{"CandidateBuild", "canonical", func(r *Receipt) {}, true, ""},
		{"CandidateBuild", "absent/empty", func(r *Receipt) { r.CandidateBuild = "" }, false, "candidate build"},
		{"CandidateBuild", "whitespace only", func(r *Receipt) { r.CandidateBuild = "   " }, false, "candidate build"},

		// --- MeasurementRoute -----------------------------------------
		{"MeasurementRoute", "canonical edge", func(r *Receipt) { r.MeasurementRoute = RouteEdge }, true, ""},
		{"MeasurementRoute", "canonical proof", func(r *Receipt) { r.MeasurementRoute = RouteProof }, true, ""},
		{"MeasurementRoute", "absent/empty", func(r *Receipt) { r.MeasurementRoute = "" }, false, "measurement route"},
		{"MeasurementRoute", "out of vocabulary", func(r *Receipt) { r.MeasurementRoute = "served" }, false, "measurement route"},
		{"MeasurementRoute", "wrong case", func(r *Receipt) { r.MeasurementRoute = "Edge" }, false, "measurement route"},
		{"MeasurementRoute", "padded canonical", func(r *Receipt) { r.MeasurementRoute = " edge " }, false, "measurement route"},

		// --- BuildBinding ---------------------------------------------
		{"BuildBinding", "canonical present", func(r *Receipt) { r.BuildBinding = EdgeBuildPresent }, true, ""},
		{"BuildBinding", "canonical absent", func(r *Receipt) { r.BuildBinding = EdgeBuildAbsent }, true, ""},
		{"BuildBinding", "absent/empty", func(r *Receipt) { r.BuildBinding = "" }, false, "build binding"},
		{"BuildBinding", "out of vocabulary (retired value)", func(r *Receipt) { r.BuildBinding = "run_level" }, false, "build binding"},
		{"BuildBinding", "wrong case", func(r *Receipt) { r.BuildBinding = "Per_Request" }, false, "build binding"},

		// --- Stage ----------------------------------------------------
		{"Stage", "canonical", func(r *Receipt) { r.Stage = EnablementProofStage }, true, ""},
		{"Stage", "absent/empty", func(r *Receipt) { r.Stage = "" }, false, "stage"},
		{"Stage", "out of vocabulary", func(r *Receipt) { r.Stage = "promoted" }, false, "stage"},

		// --- TerminalState --------------------------------------------
		{"TerminalState", "canonical", func(r *Receipt) { r.TerminalState = "match" }, true, ""},
		{"TerminalState", "absent/empty", func(r *Receipt) { r.TerminalState = "" }, false, "terminal_state"},
		{"TerminalState", "out of vocabulary", func(r *Receipt) { r.TerminalState = "sort_of_matched" }, false, "terminal_state"},

		// --- BaselineDefects (container shapes) -----------------------
		{"BaselineDefects", "nil container", func(r *Receipt) { r.BaselineDefects = nil }, true, ""},
		{"BaselineDefects", "empty container", func(r *Receipt) { r.BaselineDefects = []string{} }, true, ""},
		{"BaselineDefects", "duplicate entries", func(r *Receipt) { r.BaselineDefects = []string{"CH-1", "CH-1"} }, true, ""},
		{"BaselineDefects", "empty string entry", func(r *Receipt) { r.BaselineDefects = []string{""} }, false, "empty citation"},
		{"BaselineDefects", "whitespace-only entry", func(r *Receipt) { r.BaselineDefects = []string{"  "} }, false, "empty citation"},
		{"BaselineDefects", "one real, one empty", func(r *Receipt) { r.BaselineDefects = []string{"CH-1", ""} }, false, "empty citation"},

		// --- DifferencesOutsideBaselineDefect (numeric boundaries) ----
		{"DifferencesOutside", "zero (boundary)", func(r *Receipt) { r.DifferencesOutsideBaselineDefect = 0 }, true, ""},
		{"DifferencesOutside", "boundary+1", func(r *Receipt) { r.DifferencesOutsideBaselineDefect = 1 }, true, ""},
		{"DifferencesOutside", "boundary-1 (negative)", func(r *Receipt) { r.DifferencesOutsideBaselineDefect = -1 }, true, ""},

		// --- DataWatermark, conditional on stage ----------------------
		{"DataWatermark", "absent on a shadow stage", func(r *Receipt) { r.Stage = "shadow"; r.DataWatermark = "" }, false, "watermark"},
		{"DataWatermark", "present on a shadow stage", func(r *Receipt) { r.Stage = "shadow"; r.DataWatermark = "2026-09-01" }, true, ""},
	}

	var unrefused []string
	for _, c := range cases {
		c := c
		t.Run(c.field+"/"+c.shape, func(t *testing.T) {
			receipt := canonical()
			c.mutate(&receipt)

			// db is nil: every guard under test runs BEFORE any statement
			// is sent, so an ACCEPTED shape panics on the nil Querier and
			// a REFUSED one returns before touching it. That is the
			// discriminator, and it needs no database.
			var err error
			var reached bool
			func() {
				defer func() {
					if recover() != nil {
						reached = true
					}
				}()
				_, err = Write(context.Background(), nil, receipt)
			}()

			if c.accept {
				if !reached && err != nil && !strings.Contains(err.Error(), "no such host") {
					t.Fatalf("contract ACCEPTS %s=%s, but Write refused: %v", c.field, c.shape, err)
				}
				return
			}
			if reached {
				unrefused = append(unrefused, c.field+"/"+c.shape)
				t.Fatalf("contract REFUSES %s=%s, but Write reached the database", c.field, c.shape)
			}
			if err == nil {
				unrefused = append(unrefused, c.field+"/"+c.shape)
				t.Fatalf("contract REFUSES %s=%s, but Write returned no error", c.field, c.shape)
			}
			if c.names != "" && !strings.Contains(err.Error(), c.names) {
				t.Fatalf("%s=%s was refused, but the message does not name %q -- an operator cannot act on it: %v",
					c.field, c.shape, c.names, err)
			}
		})
	}
	if len(unrefused) > 0 {
		t.Fatalf("cells the contract says are refused and Write did not refuse: %v", unrefused)
	}
}

// The unbound-binding logic, over the shapes its own input can take.
// Its input is the SERVING BUILD HEADER as received, which unlike a Go
// struct field genuinely can be absent, empty, padded or unexpected.
func TestTheUnboundDowngradeOverEveryHeaderShape(t *testing.T) {
	const realBuild = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`

	for _, c := range []struct {
		shape   string
		header  string
		bound   bool // the contract says this header BINDS the measurement
		refused bool // ...or is refused outright as a wrong build
	}{
		{"canonical", realBuild, true, false},
		{"absent", "", false, false},
		{"whitespace only", "   ", false, false},
		{"padded canonical", "  " + realBuild + "  ", true, false},
		{"out of vocabulary (another build)", "0000000000000000000000000000000000000000", false, true},
		{"truncated (boundary-1)", realBuild[:len(realBuild)-1], false, true},
		{"extended (boundary+1)", realBuild + "0", false, true},
	} {
		c := c
		t.Run(c.shape, func(t *testing.T) {
			edge := &fakeEdge{goBody: body, pythonBody: body, goBuild: c.header}
			runner := newRunner(t, edge, "canary")
			outcomes, _, err := runner.Run(context.Background())

			if c.refused {
				if err == nil {
					t.Fatalf("header shape %q must be REFUSED: it names a build the receipt would not", c.shape)
				}
				if outcomes[0].RefusalReason != RefusalBuildMismatch {
					t.Fatalf("refused for %q, not build mismatch", outcomes[0].RefusalReason)
				}
				return
			}
			if err != nil {
				t.Fatalf("header shape %q: Run: %v", c.shape, err)
			}
			outcome := outcomes[0]
			if c.bound {
				if outcome.EdgeBuildBinding != EdgeBuildPresent {
					t.Fatalf("header shape %q must BIND, got %q", c.shape, outcome.EdgeBuildBinding)
				}
				if outcome.TerminalState != TerminalStateMatch {
					t.Fatalf("a bound match must stay a match, got %q", outcome.TerminalState)
				}
				return
			}
			if outcome.EdgeBuildBinding != EdgeBuildAbsent {
				t.Fatalf("header shape %q must NOT bind, got %q", c.shape, outcome.EdgeBuildBinding)
			}
			if outcome.TerminalState != TerminalStateUnsupported {
				t.Fatalf("an UNBOUND match must be downgraded to unsupported, got %q -- otherwise it authorizes an enablement tied to no replica", outcome.TerminalState)
			}
		})
	}
}

var _ = time.Now
