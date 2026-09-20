package goapiproof

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSummaryCountsKnownRefusalsApartFromUnexplainedGaps(t *testing.T) {
	known := &KnownRefusal{Ticket: "CHAOS-0000", Reason: "differs by design"}
	cases := []struct {
		name                string
		o                   Outcome
		wantKnown, wantNotP int
	}{
		{"known refusal that mismatches outside its declaration", Outcome{Executed: true, TerminalState: "mismatch", DifferencesOutsideBaselineDefect: 1, KnownRefusal: known}, 1, 0},
		{"known refusal that is refused", Outcome{TerminalState: "proof_failed", RefusalReason: RefusalLegsDoNotOverlap, KnownRefusal: known}, 1, 0},
		{"known refusal that happens to match", Outcome{Executed: true, TerminalState: "match", KnownRefusal: known}, 1, 0},
		{"ordinary mismatch outside its declaration", Outcome{Executed: true, TerminalState: "mismatch", DifferencesOutsideBaselineDefect: 1, BaselineDefects: []string{"CHAOS-1"}}, 0, 1},
		{"ordinary refusal", Outcome{TerminalState: "proof_failed", RefusalReason: RefusalVacuousEmptyLegs}, 0, 1},
		{"ordinary mismatch without any declaration", Outcome{Executed: true, TerminalState: "mismatch"}, 0, 1},
		{"ordinary match", Outcome{Executed: true, TerminalState: "match"}, 0, 0},
		{"a match state on an outcome that never executed does not prove", Outcome{TerminalState: "match"}, 0, 1},
		{"cited mismatch fully inside its declaration", Outcome{Executed: true, TerminalState: "mismatch", BaselineDefects: []string{"CHAOS-1"}}, 0, 0},
	}
	for _, c := range cases {
		var s Summary
		s.countProofState("op", c.o)
		if s.KnownRefusals != c.wantKnown || s.NotProving != c.wantNotP {
			t.Errorf("%s: known=%d notProving=%d want %d/%d", c.name, s.KnownRefusals, s.NotProving, c.wantKnown, c.wantNotP)
		}
	}
}

// A run's summary separates the known refusals of compoundingRisk from every
// other case, and the outcome carries the ticket and reason.
func TestRunReportsKnownRefusalVariantsApart(t *testing.T) {
	runner, _ := variantRunner(t, `{"data":{"compoundingRisk":{"rows":[{"scope":"REPO","scopeId":"r1","computedAt":"2026-01-01T00:00:00Z"}]}}}`, nil)
	runner.Documents = map[string]string{"compoundingRisk": "query CompoundingRisk { compoundingRisk { rows { scope } } }"}
	runner.Registry.DocumentDigest = map[string]string{"compoundingRisk": "06ca28a0"}
	runner.Routing = map[string]RoutingRow{"compoundingRisk": {Mode: "canary", CandidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36"}}
	outcomes, summary, err := runner.Run(context.Background())
	if err != nil && !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("Run: %v", err)
	}
	flagged := map[string]bool{}
	for _, o := range outcomes {
		if o.KnownRefusal != nil {
			flagged[o.Variant] = true
			if o.KnownRefusal.Ticket == "" || !strings.Contains(o.KnownRefusal.Reason, "team ownership") {
				t.Errorf("%s: %#v", o.Variant, o.KnownRefusal)
			}
		}
	}
	if len(flagged) != 2 || !flagged["TEAM_IDS_EMPTY"] || !flagged["TEAM_STORED"] {
		t.Fatalf("flagged variants %v", flagged)
	}
	if summary.KnownRefusals != 2 {
		t.Errorf("KnownRefusals = %d", summary.KnownRefusals)
	}
}

// The verdict is per operation: another operation's gaps never make this one
// incomplete, a known refusal never makes it incomplete, and a vacuous or
// unexecuted required case does.
func TestByOperationVerdicts(t *testing.T) {
	known := &KnownRefusal{Ticket: "CHAOS-0000", Reason: "differs by design"}
	proving := Outcome{Executed: true, TerminalState: "match"}
	vacuous := Outcome{TerminalState: "proof_failed", RefusalReason: RefusalVacuousEmptyLegs}
	var s Summary
	for _, x := range []struct {
		op string
		o  Outcome
	}{
		{"complete", proving}, {"complete", proving},
		{"knownOnly", Outcome{TerminalState: "proof_failed", KnownRefusal: known}}, {"knownOnly", proving},
		{"knownOnly", Outcome{Executed: true, TerminalState: "mismatch", DifferencesOutsideBaselineDefect: 2, KnownRefusal: known}},
		{"gap", proving}, {"gap", vacuous},
		{"onlyRefused", vacuous},
		{"onlyKnown", Outcome{TerminalState: "proof_failed", KnownRefusal: known}},
	} {
		s.countProofState(x.op, x.o)
	}
	want := map[string]OperationVerdict{
		"complete":    {Required: 2, Proven: 2, Complete: true},
		"knownOnly":   {Required: 1, Proven: 1, KnownRefusals: 2, Complete: true},
		"gap":         {Required: 2, Proven: 1, NotProving: 1},
		"onlyRefused": {Required: 1, NotProving: 1},
		"onlyKnown":   {KnownRefusals: 1},
	}
	for op, w := range want {
		if got := s.ByOperation[op]; got != w {
			t.Errorf("%s: %#v want %#v", op, got, w)
		}
	}
}
