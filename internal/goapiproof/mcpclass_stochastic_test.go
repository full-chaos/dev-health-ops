package goapiproof

// CHAOS-7499: a root whose shapes are proven under the stochastic leaf class (CHAOS-5901) is recorded as the CITED MISMATCH `enable` already
// admits (terminal mismatch, nothing outside the citation, a named citation), never as a match, and a shape that is not proven under the
// class by every clause of the predicate still blocks the root.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

const testStochasticCitation = StochasticLeafCitationPrefix + "CHAOS-5901"

func sealedStochastic(operation, variant string) sealedOutcome {
	s := sealedMatch(operation, variant)
	s.terminalState = TerminalStateMismatch
	s.provenUnder = ProvenUnderStochasticLeafClass
	s.baselineDefects = []string{testStochasticCitation}
	return s
}

func TestSealedStochasticCitationNeedsEveryClause(t *testing.T) {
	if got := sealedStochasticCitation(sealedStochastic("capacityForecast", "")); got != testStochasticCitation {
		t.Fatalf("a measurement proven under the class returned %q", got)
	}
	for name, mutate := range map[string]func(*sealedOutcome){
		"terminal state is match, not mismatch":           func(s *sealedOutcome) { s.terminalState = TerminalStateMatch },
		"not admitted":                                    func(s *sealedOutcome) { s.admitted = false },
		"not executed":                                    func(s *sealedOutcome) { s.executed = false },
		"a difference outside the citation":               func(s *sealedOutcome) { s.differencesOutsideBaselineDefect = 1 },
		"serving build not bound":                         func(s *sealedOutcome) { s.edgeBinding = EdgeBuildAbsent },
		"not measured through the proof route":            func(s *sealedOutcome) { s.route = RouteEdge },
		"the comparator did not prove it under the class": func(s *sealedOutcome) { s.provenUnder = "" },
		"proven under go-only, not the stochastic class":  func(s *sealedOutcome) { s.provenUnder = ProvenUnderGoOnly },
		"no citation on the outcome":                      func(s *sealedOutcome) { s.baselineDefects = nil },
		"a citation that is not the class's":              func(s *sealedOutcome) { s.baselineDefects = []string{"CHAOS-5448"} },
		"a blank class citation":                          func(s *sealedOutcome) { s.baselineDefects = []string{StochasticLeafCitationPrefix + "  "} },
	} {
		t.Run(name, func(t *testing.T) {
			s := sealedStochastic("capacityForecast", "")
			mutate(&s)
			if got := sealedStochasticCitation(s); got != "" {
				t.Fatalf("a measurement that fails the clause %q still returned the citation %q", name, got)
			}
		})
	}
}

func TestMCPClassReceiptsForAStochasticRoot(t *testing.T) {
	sources := map[string][]string{mcpclass.Operation("capacityForecast"): {"capacityForecast", "capacityForecastAll"}}
	removed := sealedStochastic("capacityForecast", "")
	removed.provenUnder, removed.baselineDefects = "", nil // the declaration removed: a plain mismatch
	otherLeaf := sealedStochastic("capacityForecast", "")
	otherLeaf.differencesOutsideBaselineDefect = 2 // a NON-stochastic leaf of the same shape also differs
	type want struct {
		state      string
		failed     int
		stochastic int
		matched    int
		defects    []string
	}
	for name, tc := range map[string]struct {
		sealed []sealedOutcome
		want   want
	}{
		"every shape proven under the class: terminal mismatch with the citation, never match": {
			[]sealedOutcome{sealedStochastic("capacityForecast", ""), sealedStochastic("capacityForecastAll", "")},
			want{TerminalStateMismatch, 0, 2, 0, []string{testStochasticCitation}}},
		"a matching shape beside a stochastic one": {
			[]sealedOutcome{sealedMatch("capacityForecast", ""), sealedStochastic("capacityForecastAll", "")},
			want{TerminalStateMismatch, 0, 1, 1, []string{testStochasticCitation}}},
		"plant: the stochastic declaration removed is a plain mismatch with no citation": {
			[]sealedOutcome{removed, sealedMatch("capacityForecastAll", "")},
			want{TerminalStateMismatch, 1, 0, 1, nil}},
		"plant: a non-stochastic leaf that also differs blocks the root and carries no citation": {
			[]sealedOutcome{otherLeaf, sealedStochastic("capacityForecastAll", "")},
			want{TerminalStateMismatch, 1, 1, 0, nil}},
		"a root with no stochastic shape is unchanged: match with no citation": {
			[]sealedOutcome{sealedMatch("capacityForecast", ""), sealedMatch("capacityForecastAll", "")},
			want{TerminalStateMatch, 0, 0, 2, nil}},
	} {
		t.Run(name, func(t *testing.T) {
			outcomes := make([]Outcome, len(tc.sealed))
			for i, s := range tc.sealed {
				outcomes[i] = executedOutcome(s.operation, s.variant)
			}
			receipts, verdicts, err := classRunner(tc.sealed).MCPClassReceipts(outcomes, sources, nil, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if len(receipts) != 1 || len(verdicts) != 1 {
				t.Fatalf("receipts=%d verdicts=%d", len(receipts), len(verdicts))
			}
			v, r := verdicts[0], receipts[0]
			if v.TerminalState != tc.want.state || len(v.Failed) != tc.want.failed || len(v.Stochastic) != tc.want.stochastic || v.Matched != tc.want.matched {
				t.Fatalf("verdict %+v, want %+v", v, tc.want)
			}
			if r.TerminalState != tc.want.state || strings.Join(r.BaselineDefects, "|") != strings.Join(tc.want.defects, "|") {
				t.Fatalf("receipt state %q defects %v, want %q %v", r.TerminalState, r.BaselineDefects, tc.want.state, tc.want.defects)
			}
			if tc.want.state == TerminalStateMismatch && len(tc.want.defects) > 0 && r.DifferencesOutsideBaselineDefect != 0 {
				t.Fatalf("a cited receipt must carry nothing outside the citation: %d", r.DifferencesOutsideBaselineDefect)
			}
		})
	}
}

func TestFormatMCPClassVerdictNamesAStochasticProof(t *testing.T) {
	line := FormatMCPClassVerdict(MCPClassVerdict{Operation: "mcp:capacityForecast", TerminalState: TerminalStateMismatch,
		Executed: 2, Matched: 1, Stochastic: []string{"capacityForecastAll"}, Written: true})
	for _, part := range []string{" state=mismatch proven_under=stochastic_leaf_class ", " executed=2 ", " failed=0 ", " receipt=true", " stochastic=[capacityForecastAll]"} {
		if !strings.Contains(line, part) {
			t.Fatalf("verdict line %q lacks %q", line, part)
		}
	}
	if plain := FormatMCPClassVerdict(MCPClassVerdict{Operation: "mcp:x", TerminalState: TerminalStateMismatch, Executed: 1, Failed: []string{"x=mismatch"}, Written: true}); strings.Contains(plain, "proven_under") {
		t.Fatalf("a failed mismatch must not read as proven under the class: %q", plain)
	}
}

// The real doc-route Runner on a real stochastic operation: two Go pipelines draw different values for the covered leaves.
func stochasticDocRouteRun(t *testing.T, candidate, reference string) ([]Outcome, *Runner) {
	t.Helper()
	runner := newDocRouteRunner(t, goLeg(candidate), goLeg(reference), nil)
	runner.Documents = map[string]string{"capacityForecast": capacityForecastDocument(t)}
	runner.Registry.DocumentDigest = map[string]string{"capacityForecast": "b4fb8f07"}
	runner.Routing = map[string]RoutingRow{"capacityForecast": {Mode: "shadow", CandidateBuild: goEdgeBuild}}
	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return outcomes, runner
}

func classOf(t *testing.T, outcomes []Outcome, runner *Runner) (Receipt, MCPClassVerdict, bool) {
	t.Helper()
	sources := map[string][]string{mcpclass.Operation("capacityForecast"): {"capacityForecast"}}
	receipts, verdicts, err := runner.MCPClassReceipts(outcomes, sources, map[string]bool{"capacityForecast": true}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(verdicts) != 1 {
		t.Fatalf("verdicts %+v", verdicts)
	}
	if len(receipts) == 0 {
		return Receipt{}, verdicts[0], false
	}
	return receipts[0], verdicts[0], true
}

func TestDocRouteClassProofOfAStochasticRootIsTheCitedMismatch(t *testing.T) {
	outcomes, runner := stochasticDocRouteRun(t,
		forecastBody(t, "go", map[string]any{"p85Days": 4, "p85Date": forecastDay(4)}), forecastBody(t, "go", nil))
	if o := outcomes[0]; !o.Executed || o.ProvenUnder != ProvenUnderStochasticLeafClass || o.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("the doc-route run did not prove the shape under the class: %+v", o)
	}
	receipt, verdict, written := classOf(t, outcomes, runner)
	if !written || verdict.TerminalState != TerminalStateMismatch || len(verdict.Failed) != 0 || len(verdict.Stochastic) != 1 || verdict.Matched != 0 {
		t.Fatalf("verdict %+v written=%v", verdict, written)
	}
	if receipt.TerminalState != EnablementCitedMismatchState || receipt.DifferencesOutsideBaselineDefect != 0 ||
		len(receipt.BaselineDefects) != 1 || !strings.HasPrefix(receipt.BaselineDefects[0], StochasticLeafCitationPrefix) || NamesNothing(receipt.BaselineDefects[0]) {
		t.Fatalf("receipt is not the cited mismatch: %+v", receipt)
	}
	if line := FormatMCPClassVerdict(verdict); !strings.Contains(line, "state=mismatch proven_under=stochastic_leaf_class") || strings.Contains(line, "state=match ") {
		t.Fatalf("the verdict line reads wrongly: %q", line)
	}
}

// Plant (ii)/(iii): a NON-stochastic leaf of the same shape also differs under doc-route mode: the kept class must not hide it.
func TestDocRouteClassProofBlocksANonStochasticDifferenceBesideTheClass(t *testing.T) {
	outcomes, runner := stochasticDocRouteRun(t,
		forecastBody(t, "go", map[string]any{"backlogSize": 13, "p85Days": 4, "p85Date": forecastDay(4)}), forecastBody(t, "go", nil))
	receipt, verdict, _ := classOf(t, outcomes, runner)
	if len(verdict.Failed) != 1 || len(verdict.Stochastic) != 0 || len(receipt.BaselineDefects) != 0 {
		t.Fatalf("a real difference beside the class was not blocked: verdict %+v defects %v", verdict, receipt.BaselineDefects)
	}
}

// Plant (i): the same drawn-value difference with the class declaration removed is a plain mismatch.
func TestDocRouteClassProofOfAStochasticRootNeedsTheDeclaration(t *testing.T) {
	spec, err := SpecFor("capacityForecast")
	if err != nil {
		t.Fatal(err)
	}
	parity := spec.Parity
	parity.StochasticLeaves = nil
	withOverriddenParity(t, "capacityForecast", parity)
	outcomes, runner := stochasticDocRouteRun(t,
		forecastBody(t, "go", map[string]any{"p85Days": 4, "p85Date": forecastDay(4)}), forecastBody(t, "go", nil))
	receipt, verdict, _ := classOf(t, outcomes, runner)
	if len(verdict.Failed) != 1 || len(verdict.Stochastic) != 0 || len(receipt.BaselineDefects) != 0 {
		t.Fatalf("a drawn-value difference without the declaration was not blocked: verdict %+v defects %v", verdict, receipt.BaselineDefects)
	}
}

// A stochastic shape of a document operation that is not itself receipt-backed is EXCLUDED and named, exactly like a match would be: the
// class proof never counts a shape whose document operation has no admissible receipt at this build.
func TestStochasticShapeOfAnUnbackedDocumentOperationIsExcluded(t *testing.T) {
	outcomes, runner := stochasticDocRouteRun(t,
		forecastBody(t, "go", map[string]any{"p85Days": 4, "p85Date": forecastDay(4)}), forecastBody(t, "go", nil))
	sources := map[string][]string{mcpclass.Operation("capacityForecast"): {"capacityForecast"}}
	receipts, verdicts, err := runner.MCPClassReceipts(outcomes, sources, map[string]bool{"capacityForecast": false}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	v := verdicts[0]
	if len(receipts) != 0 || v.Executed != 0 || len(v.Stochastic) != 0 || len(v.Excluded) != 1 || !strings.HasSuffix(v.Excluded[0], "=doc_operation_not_receipt_backed") {
		t.Fatalf("an unbacked stochastic shape was counted: receipts=%d verdict %+v", len(receipts), v)
	}
}
