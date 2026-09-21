package goapiproof

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func goOnlyRunner(t *testing.T, ledger *GoServedLedger, bound bool) *Runner {
	t.Helper()
	build := ""
	if bound {
		build = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	}
	return goOnlyRunnerAtBuild(t, ledger, build)
}

// goOnlyRunnerAtBuild stamps the fake edge's go answers with build; empty
// leaves them unbound, as a deployment without the pass-through does.
func goOnlyRunnerAtBuild(t *testing.T, ledger *GoServedLedger, build string) *Runner {
	t.Helper()
	message := defaultLedgerForTest(t).ExpectedMessage("capacityForecast")
	edge := &fakeEdge{
		pythonBody: envelope(nil, deletionErrorJSON("capacityForecast", message, nil)),
		goBody:     forecastBody(t, "go", nil),
	}
	runner := capacityForecastRunner(t, edge)
	edge.goBuild = build
	runner.GoServed = ledger
	runner.Config.RecordedBy = "test"
	runner.Config.ReviewEvidence = "why"
	return runner
}

// A deleted-Python operation is proven from the candidate alone: the run
// records the ledger's citation on the mismatch arm, counts it apart from a
// two-plane proof and prints a verdict word no match can carry.
func TestARunProvesALedgeredOperationWithNoPythonAnswer(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	runner := goOnlyRunner(t, ledger, true)

	outcomes, summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	outcome := outcomes[0]
	if !outcome.Executed || !outcome.Admitted || outcome.ProvenUnder != ProvenUnderGoOnly {
		t.Fatalf("executed=%v admitted=%v provenUnder=%q refusal=%s: %s", outcome.Executed, outcome.Admitted, outcome.ProvenUnder, outcome.RefusalReason, outcome.RefusalDetail)
	}
	citation, _ := NewGoOnlyCitation(ledger, "capacityForecast")
	if outcome.TerminalState != TerminalStateMismatch || len(outcome.BaselineDefects) != 1 || outcome.BaselineDefects[0] != citation {
		t.Fatalf("terminal=%s citations=%v", outcome.TerminalState, outcome.BaselineDefects)
	}
	if outcome.DifferencesOutsideBaselineDefect != 0 || len(outcome.Findings) != 0 {
		t.Fatalf("outside=%d findings=%v", outcome.DifferencesOutsideBaselineDefect, outcome.Findings)
	}
	if summary.ProvenGoOnly != 1 || summary.Executed != 1 || summary.ByTerminalState[TerminalStateMismatch] != 1 {
		t.Fatalf("summary %+v", summary)
	}

	receipts, err := runner.ReceiptsFor(time.Now().UTC())
	if err != nil || len(receipts) != 1 {
		t.Fatalf("ReceiptsFor: %v (%d)", err, len(receipts))
	}
	receipt := receipts[0]
	if receipt.TerminalState != TerminalStateMismatch || receipt.Stage != EnablementProofStage ||
		receipt.DifferencesOutsideBaselineDefect != 0 || receipt.BuildBinding != EdgeBuildPresent ||
		len(receipt.BaselineDefects) != 1 || receipt.BaselineDefects[0] != citation {
		t.Fatalf("receipt %+v", receipt)
	}
	if err := ValidateGoOnlyReceiptCitations(ledger, receipt.SelectedOperation, receipt.TerminalState, receipt.BaselineDefects); err != nil {
		t.Fatalf("the receipt the run built is refused by the writer's own check: %v", err)
	}
}

// A runner with no ledger behaves as a runner always has: the errored
// baseline is refused and nothing is written.
func TestARunWithNoLedgerRefusesTheDeletionError(t *testing.T) {
	runner := goOnlyRunner(t, nil, true)
	outcomes, summary, err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("a run that proved nothing did not fail")
	}
	if outcomes[0].RefusalReason != RefusalErroredResponse || summary.ProvenGoOnly != 0 || summary.Executed != 0 {
		t.Fatalf("reason=%s summary=%+v", outcomes[0].RefusalReason, summary)
	}
	receipts, _ := runner.ReceiptsFor(time.Now().UTC())
	if len(receipts) != 0 {
		t.Fatalf("%d receipts for a refusal", len(receipts))
	}
}

// An edge measurement whose response carries no serving-build header proves
// nothing in any mode, go-only included: the missing binding counts outside
// the citation, so the receipt is inert.
func TestAnUnboundGoOnlyMeasurementIsInert(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	runner := goOnlyRunner(t, ledger, false)
	outcomes, summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	outcome := outcomes[0]
	if outcome.ProvenUnder != "" || summary.ProvenGoOnly != 0 {
		t.Fatalf("an unbound measurement was proven go-only: %+v", outcome)
	}
	if outcome.DifferencesOutsideBaselineDefect == 0 || outcome.EdgeBuildBinding != EdgeBuildAbsent {
		t.Fatalf("outside=%d binding=%s", outcome.DifferencesOutsideBaselineDefect, outcome.EdgeBuildBinding)
	}
}

// The reverse direction: a ledgered operation whose baseline still answers
// data takes the ordinary two-plane path, and no ordinary receipt carries
// the go-only prefix.
func TestALedgeredOperationWhoseBaselineAnswersTakesTheTwoPlanePath(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	edge := &fakeEdge{
		pythonBody: forecastBody(t, "python", nil),
		goBody:     forecastBody(t, "go", map[string]any{"p85Days": 4, "p85Date": forecastDay(4)}),
	}
	runner := capacityForecastRunner(t, edge)
	runner.GoServed = ledger
	outcomes, summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	outcome := outcomes[0]
	if !outcome.Executed || outcome.ProvenUnder == ProvenUnderGoOnly || summary.ProvenGoOnly != 0 {
		t.Fatalf("a baseline that answered was proven go-only: %+v", outcome)
	}
	for _, citation := range outcome.BaselineDefects {
		if HasGoOnlyPrefix(citation) {
			t.Fatalf("an ordinary receipt carries the go-only prefix: %q", citation)
		}
	}
}

// The writer refuses every forged use of the prefix before touching the
// database, and passes a receipt without it.
func TestTheWriterRefusesAForgedGoOnlyCitation(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	own, _ := NewGoOnlyCitation(ledger, "capacityForecast")
	base := Receipt{
		SchemaDigest: "s", DocumentDigest: "d", SelectedOperation: "capacityForecast", CandidateBuild: "b",
		Stage: EnablementProofStage, TerminalState: TerminalStateMismatch,
		MeasurementRoute: RouteEdge, BuildBinding: EdgeBuildPresent,
	}
	for name, mutate := range map[string]func(*Receipt){
		"a typed citation": func(r *Receipt) {
			r.BaselineDefects = []string{"GO-ONLY:op=capacityForecast;two_plane=" + strings.Repeat("0", 40) + ";guards=TestX"}
		},
		"own citation on match": func(r *Receipt) { r.TerminalState = TerminalStateMatch; r.BaselineDefects = []string{own} },
		"own plus another":      func(r *Receipt) { r.BaselineDefects = []string{own, "ABC-123"} },
		"another operation's":   func(r *Receipt) { r.SelectedOperation = "throughputForecast"; r.BaselineDefects = []string{own} },
		"a lowercase claim":     func(r *Receipt) { r.BaselineDefects = []string{strings.ToLower(own)} },
	} {
		receipt := base
		mutate(&receipt)
		if _, err := Write(context.Background(), nil, receipt); err == nil {
			t.Errorf("%s: err=%v", name, err)
		}
	}
	rest := RESTReceipt{
		Method: "GET", Path: "/x", CandidateBuild: "b", Stage: EnablementProofStage, TerminalState: TerminalStateMismatch,
		MeasurementRoute: RouteProof, BuildBinding: EdgeBuildPresent, BaselineDefects: []string{own},
	}
	if _, err := WriteREST(context.Background(), nil, rest); err == nil {
		t.Error("the REST writer accepted the go-only prefix")
	}
}

// An operation whose ledger entry is the unproven form is admitted and its
// candidate is checked, but the run never counts it as proving, counts it under
// no proof word and writes no receipt for it: nothing compared a leaf.
func TestARunNeverProvesAnUnprovenLedgerEntry(t *testing.T) {
	raw, err := json.Marshal(GoServedLedger{
		MessageTemplate: defaultLedgerForTest(t).MessageTemplate,
		Entries: []GoServedEntry{{
			Operation:      "capacityForecast",
			UnprovenReason: "no data in any venue, so no two-plane run compared a leaf; enable record cited",
			Guards:         []GoServedGuard{{File: "a_test.go", Test: "TestA"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := ParseGoServedLedger(raw)
	if err != nil {
		t.Fatal(err)
	}
	runner := goOnlyRunner(t, ledger, true)

	outcomes, summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	outcome := outcomes[0]
	if !outcome.Executed || !outcome.Admitted || outcome.ProvenUnder != ProvenUnderGoOnlyUnproven {
		t.Fatalf("executed=%v admitted=%v provenUnder=%q", outcome.Executed, outcome.Admitted, outcome.ProvenUnder)
	}
	if outcomeProves(outcome) {
		t.Fatal("an unproven entry proves its request")
	}
	if summary.ProvenGoOnly != 0 || summary.NotProving != 1 || summary.ByOperation["capacityForecast"].Proven != 0 || summary.ByOperation["capacityForecast"].NotProving != 1 {
		t.Fatalf("summary counts an unproven entry as proof: %+v", summary)
	}
	receipts, err := runner.ReceiptsFor(time.Now().UTC())
	if err != nil || len(receipts) != 0 {
		t.Fatalf("ReceiptsFor wrote %d receipts for an unproven entry (err %v)", len(receipts), err)
	}
}
