package goapiproof

import (
	"context"
	"errors"
	"testing"
)

// An intermittent baseline defect is visible only while the baseline's
// source table still holds unmerged row versions. A comparison taken
// after the merge legitimately shows no difference under its paths, so
// that comparison records the entry as idle rather than stale. A plain
// entry covering nothing stays stale, exactly as before.
func TestCompareIntermittentBaselineDefectCoveringNothingIsIdleNotStale(t *testing.T) {
	body := `{"data":{"workItems":{"cycleTimeDays":4}}}`
	for _, c := range []struct {
		name      string
		defect    BaselineDefect
		wantStale []string
		wantIdle  []string
	}{
		{
			name:      "plain entry",
			defect:    BaselineDefect{Ticket: "CHAOS-0001", Reason: "r", Paths: []string{"data.workItems.cycleTimeDays"}},
			wantStale: []string{"CHAOS-0001"},
		},
		{
			name: "intermittent entry",
			defect: BaselineDefect{
				Ticket: "CHAOS-0002", Reason: "r", Paths: []string{"data.workItems.cycleTimeDays"},
				Intermittent: true, IntermittentReason: "visible only while the source table has unmerged versions",
			},
			wantIdle: []string{"CHAOS-0002"},
		},
		{
			name: "intermittent entry with a blank reason",
			defect: BaselineDefect{
				Ticket: "CHAOS-0003", Reason: "r", Paths: []string{"data.workItems.cycleTimeDays"},
				Intermittent: true, IntermittentReason: "  ",
			},
			wantStale: []string{"CHAOS-0003"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			result := Compare(snapshotFromJSON(t, body), snapshotFromJSON(t, body), Options{BaselineDefects: []BaselineDefect{c.defect}})
			if result.TerminalState != TerminalStateMatch {
				t.Fatalf("no differences means match, got %s", result.TerminalState)
			}
			if !equalStrings(result.StaleBaselineDefects, c.wantStale) {
				t.Fatalf("stale = %v, want %v", result.StaleBaselineDefects, c.wantStale)
			}
			if !equalStrings(result.IdleIntermittentBaselineDefects, c.wantIdle) {
				t.Fatalf("idle = %v, want %v", result.IdleIntermittentBaselineDefects, c.wantIdle)
			}
			if len(result.BaselineDefectsMatched) != 0 {
				t.Fatalf("nothing differed, so nothing matched: %v", result.BaselineDefectsMatched)
			}
		})
	}
}

// The flag relaxes the stale rule and nothing else: a covered value
// difference keeps the mismatch terminal state, and a structural difference
// under the cited path stays outside the citation.
func TestCompareIntermittentBaselineDefectCoversOnlyLeafDifferences(t *testing.T) {
	defect := BaselineDefect{
		Ticket: "CHAOS-0002", Reason: "r", Paths: []string{"data.cycles"},
		Intermittent: true, IntermittentReason: "visible only while the source table has unmerged versions",
	}
	opts := Options{BaselineDefects: []BaselineDefect{defect}}
	for _, c := range []struct {
		name                string
		baseline, candidate string
		outside             int
	}{
		{"leaf value", `{"data":{"cycles":{"a":2,"b":[1,2]}}}`, `{"data":{"cycles":{"a":1,"b":[1,2]}}}`, 0},
		{"list length", `{"data":{"cycles":{"a":1,"b":[1,2]}}}`, `{"data":{"cycles":{"a":1,"b":[1]}}}`, 1},
		{"key absent", `{"data":{"cycles":{"a":1,"b":[1,2]}}}`, `{"data":{"cycles":{"b":[1,2]}}}`, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			result := Compare(snapshotFromJSON(t, c.baseline), snapshotFromJSON(t, c.candidate), opts)
			if result.TerminalState != TerminalStateMismatch {
				t.Fatalf("an intermittent defect must never convert a mismatch, got %s", result.TerminalState)
			}
			if result.DifferencesOutsideBaselineDefect != c.outside {
				t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.outside, result.Findings)
			}
			if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-0002"}) {
				t.Fatalf("a difference beneath the cited path makes the entry live, got matched=%v", result.BaselineDefectsMatched)
			}
			if len(result.StaleBaselineDefects) != 0 || len(result.IdleIntermittentBaselineDefects) != 0 {
				t.Fatalf("a live entry is neither stale nor idle: stale=%v idle=%v", result.StaleBaselineDefects, result.IdleIntermittentBaselineDefects)
			}
		})
	}
}

func TestValidateBaselineDefectsRequiresAnIntermittentReason(t *testing.T) {
	for _, c := range []struct {
		name    string
		defect  BaselineDefect
		wantErr bool
	}{
		{"plain entry", BaselineDefect{Ticket: "CHAOS-0001", Paths: []string{"data.x"}}, false},
		{"intermittent with a reason", BaselineDefect{Ticket: "CHAOS-0001", Paths: []string{"data.x"}, Intermittent: true, IntermittentReason: "unmerged versions"}, false},
		{"intermittent with no reason", BaselineDefect{Ticket: "CHAOS-0001", Paths: []string{"data.x"}, Intermittent: true}, true},
		{"intermittent with a blank reason", BaselineDefect{Ticket: "CHAOS-0001", Paths: []string{"data.x"}, Intermittent: true, IntermittentReason: " \t "}, true},
		{"a reason without the flag", BaselineDefect{Ticket: "CHAOS-0001", Paths: []string{"data.x"}, IntermittentReason: "unmerged versions"}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := validateBaselineDefects([]BaselineDefect{c.defect})
			if (err != nil) != c.wantErr {
				t.Fatalf("validateBaselineDefects err = %v, wantErr %v", err, c.wantErr)
			}
		})
	}
}

// Through the real runner: identical legs under a plain entry still refuse
// as stale; under a valid intermittent entry they record a match; under an
// intermittent entry with no reason the declaration itself is refused
// before any request is sent.
func TestRunHonoursTheIntermittentFlagOnlyForTheStaleRefusal(t *testing.T) {
	const build = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	cited := []string{"data.featureFlags.key"}
	for _, c := range []struct {
		name        string
		defect      BaselineDefect
		wantRefusal string
		wantSent    bool
	}{
		{"plain entry", BaselineDefect{Ticket: "CHAOS-0001", Reason: "r", Paths: cited}, RefusalStaleBaselineDefect, true},
		{"intermittent entry", BaselineDefect{Ticket: "CHAOS-0002", Reason: "r", Paths: cited, Intermittent: true, IntermittentReason: "unmerged versions"}, "", true},
		{"intermittent entry with no reason", BaselineDefect{Ticket: "CHAOS-0003", Reason: "r", Paths: cited, Intermittent: true}, RefusalInvalidBaselineDefect, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			withOverriddenParity(t, "featureFlags", Options{BaselineDefects: []BaselineDefect{c.defect}})
			edge := &fakeEdge{goBody: body, pythonBody: body, goBuild: build}
			runner := newRunner(t, edge, "canary")

			outcomes, _, err := runner.Run(context.Background())
			if (len(edge.seen) > 0) != c.wantSent {
				t.Fatalf("requests sent = %d, want sent=%v", len(edge.seen), c.wantSent)
			}
			if c.wantRefusal == "" {
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				if outcomes[0].RefusalReason != "" || outcomes[0].TerminalState != TerminalStateMatch {
					t.Fatalf("want an executed match, got refusal=%q (%s) terminal=%q", outcomes[0].RefusalReason, outcomes[0].RefusalDetail, outcomes[0].TerminalState)
				}
				return
			}
			if !errors.Is(err, ErrNothingMeasured) {
				t.Fatalf("a refused declaration must leave nothing measured, got %v", err)
			}
			if outcomes[0].RefusalReason != c.wantRefusal {
				t.Fatalf("refusal = %q (%s), want %q", outcomes[0].RefusalReason, outcomes[0].RefusalDetail, c.wantRefusal)
			}
		})
	}
}

// A SHAPED defect's own cited paths carrying a real leaf-level difference
// this run -- the mechanism is LIVE, not absent -- that its shape does not
// admit must refuse the run (LiveBaselineDefectsUnexplained), never read as
// idle: "idle" is for a citation whose Paths carried nothing at all, and
// conflating the two is exactly the silent-pass failure class this package
// exists to close.
func TestCompareShapedBaselineDefectLiveButUnexplainedRefusesEvenIntermittent(t *testing.T) {
	// Only ONE coverage leaf moves in the required (downward) direction;
	// the other moves the WRONG way, so SupersessionSkewShape's rule 2
	// fails and admits nothing -- but both leaves are still under the
	// defect's own Paths, and a leaf finding genuinely exists there.
	baseline := `{"data":{"analytics":{"sankey":{"coverage":{"teamCoverage":0.85,"repoCoverage":0.9}}}}}`
	candidate := `{"data":{"analytics":{"sankey":{"coverage":{"teamCoverage":0.9,"repoCoverage":0.85}}}}}`
	defect := BaselineDefect{
		Ticket: "ABC-123", Reason: "test fixture",
		Paths: []string{
			"data.analytics.sankey.coverage.teamCoverage",
			"data.analytics.sankey.coverage.repoCoverage",
		},
		Intermittent:       true,
		IntermittentReason: "test fixture",
		SupersessionSkewShape: &SupersessionSkewShape{
			TeamCoveragePath: "data.analytics.sankey.coverage.teamCoverage",
			RepoCoveragePath: "data.analytics.sankey.coverage.repoCoverage",
		},
	}
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), Options{BaselineDefects: []BaselineDefect{defect}})
	if len(result.BaselineDefectsMatched) != 0 {
		t.Fatalf("the shape admits nothing here, so nothing matched: %v", result.BaselineDefectsMatched)
	}
	if len(result.IdleIntermittentBaselineDefects) != 0 {
		t.Fatalf("the citation's own paths carried real findings -- it must not read as idle: %v", result.IdleIntermittentBaselineDefects)
	}
	if len(result.StaleBaselineDefects) != 0 {
		t.Fatalf("a shaped, live-but-unexplained entry is its own bucket, not stale: %v", result.StaleBaselineDefects)
	}
	if !equalStrings(result.LiveBaselineDefectsUnexplained, []string{"ABC-123"}) {
		t.Fatalf("liveBaselineDefectsUnexplained = %v, want [ABC-123]", result.LiveBaselineDefectsUnexplained)
	}
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 -- neither coverage leaf is covered: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// The SAME live-but-unexplained shaped defect must NOT be flagged when a
// SIBLING declaration -- covering the identical two paths through a
// DIFFERENT, unshaped mechanism -- explains the finding instead. The shaped
// defect's own mechanism genuinely did not fire; that is an honest idle,
// not a masked failure, and the two-pass classification must tell them
// apart regardless of which of the two defects happens to run first.
func TestCompareShapedBaselineDefectIsIdleWhenASiblingExplainsTheSameFinding(t *testing.T) {
	baseline := `{"data":{"analytics":{"sankey":{"coverage":{"teamCoverage":0.85,"repoCoverage":0.9}}}}}`
	candidate := `{"data":{"analytics":{"sankey":{"coverage":{"teamCoverage":0.9,"repoCoverage":0.85}}}}}`
	shaped := BaselineDefect{
		Ticket: "ABC-123", Reason: "test fixture",
		Paths: []string{
			"data.analytics.sankey.coverage.teamCoverage",
			"data.analytics.sankey.coverage.repoCoverage",
		},
		Intermittent:       true,
		IntermittentReason: "test fixture",
		SupersessionSkewShape: &SupersessionSkewShape{
			TeamCoveragePath: "data.analytics.sankey.coverage.teamCoverage",
			RepoCoveragePath: "data.analytics.sankey.coverage.repoCoverage",
		},
	}
	blanket := BaselineDefect{
		Ticket: "DEF-456", Reason: "test fixture",
		Paths: []string{
			"data.analytics.sankey.coverage.teamCoverage",
			"data.analytics.sankey.coverage.repoCoverage",
		},
		Intermittent:       true,
		IntermittentReason: "test fixture",
	}
	for _, order := range [][]BaselineDefect{{shaped, blanket}, {blanket, shaped}} {
		result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), Options{BaselineDefects: order})
		if !equalStrings(result.BaselineDefectsMatched, []string{"DEF-456"}) {
			t.Fatalf("matched = %v, want [DEF-456]", result.BaselineDefectsMatched)
		}
		if !equalStrings(result.IdleIntermittentBaselineDefects, []string{"ABC-123"}) {
			t.Fatalf("idle = %v, want [ABC-123] -- the shaped defect's own mechanism genuinely did not fire", result.IdleIntermittentBaselineDefects)
		}
		if len(result.LiveBaselineDefectsUnexplained) != 0 {
			t.Fatalf("liveBaselineDefectsUnexplained = %v, want none -- a sibling covered the finding", result.LiveBaselineDefectsUnexplained)
		}
		if result.DifferencesOutsideBaselineDefect != 0 {
			t.Fatalf("outside = %d, want 0 -- the blanket sibling covers both leaves: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
		}
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
