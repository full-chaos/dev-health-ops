package goapiproof

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

func TestMCPClassCountsAMatchedDocRouteShapeOfABornInGoOperation(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	born := mcpclass.Operation("sourceHealth")
	hot := mcpclass.Operation("hotspots")
	for name, tc := range map[string]struct {
		root     string
		ops      []string
		sealed   sealedOutcome
		ledger   *GoServedLedger
		docRoute bool
		state    string
		receipts int
		bornList int
	}{
		"python-reference mode names nothing born-in-go": {born, []string{"sourceHealth"}, sealedMatch("sourceHealth", ""), ledger, false, TerminalStateMatch, 1, 0},
		"born-in-go matched shape is counted":            {born, []string{"sourceHealth"}, sealedMatch("sourceHealth", ""), ledger, true, TerminalStateMatch, 1, 1},
		"no ledger on the runner: excluded":              {born, []string{"sourceHealth"}, sealedMatch("sourceHealth", ""), nil, true, "", 0, 0},
		"unbacked non-born operation: excluded":          {hot, []string{"hotspots"}, sealedMatch("hotspots", ""), ledger, true, "", 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			runner := classRunner([]sealedOutcome{tc.sealed})
			runner.Config.DocRouteReference = tc.docRoute
			runner.GoServed = tc.ledger
			receipts, verdicts, err := runner.MCPClassReceipts([]Outcome{executedOutcome(tc.ops[0], "")}, map[string][]string{tc.root: tc.ops}, map[string]bool{}, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if len(receipts) != tc.receipts || len(verdicts) != 1 || verdicts[0].TerminalState != tc.state || len(verdicts[0].BornInGo) != tc.bornList {
				t.Fatalf("receipts=%d verdicts=%+v, want %d receipts, state %q, %d born-in-go", len(receipts), verdicts, tc.receipts, tc.state, tc.bornList)
			}
			if tc.receipts == 1 && tc.bornList == 0 {
				return
			}
			if tc.receipts == 1 {
				var provenance ReceiptProvenance
				if err := json.Unmarshal([]byte(receipts[0].ReviewEvidence), &provenance); err != nil || provenance.MCPClass == nil ||
					len(provenance.MCPClass.BornInGo) != 1 || len(provenance.MCPClass.Excluded) != 0 {
					t.Fatalf("provenance %q (err %v), want the born-in-go shape named and nothing excluded", receipts[0].ReviewEvidence, err)
				}
			} else if tc.docRoute && len(verdicts[0].Excluded) != 1 || !strings.Contains(verdicts[0].Excluded[0], "doc_operation_not_receipt_backed") {
				t.Fatalf("excluded %v, want the shape named", verdicts[0].Excluded)
			}
		})
	}
}

func TestBornInGoShapeThatDivergesStillBlocks(t *testing.T) {
	mismatch := sealedMatch("sourceHealth", "")
	mismatch.terminalState = TerminalStateMismatch
	mismatch.differencesOutsideBaselineDefect = 1
	runner := classRunner([]sealedOutcome{mismatch})
	runner.Config.DocRouteReference = true
	runner.GoServed = defaultLedgerForTest(t)
	_, verdicts, err := runner.MCPClassReceipts([]Outcome{executedOutcome("sourceHealth", "")}, map[string][]string{mcpclass.Operation("sourceHealth"): {"sourceHealth"}}, map[string]bool{}, time.Now().UTC())
	if err != nil || len(verdicts) != 1 || verdicts[0].TerminalState != TerminalStateMismatch || len(verdicts[0].Failed) != 1 {
		t.Fatalf("verdicts %+v err %v, want a blocking mismatch", verdicts, err)
	}
}

// born_in_go is derived, not listed by hand: it marks exactly the ledger
// operations whose Python field the frozen schema.py facts show never
// existed (pythonFieldDeletedOutright), except `home`, whose Python body was
// deleted after it had existed.
func TestBornInGoMarkIsExactlyTheOperationsPythonNeverHad(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	for _, entry := range ledger.Entries {
		want := pythonFieldDeletedOutright[entry.Operation] && entry.Operation != "home"
		if entry.BornInGo != want {
			t.Errorf("%s: born_in_go=%v, want %v (Python never had it: %v)", entry.Operation, entry.BornInGo, want, want)
		}
	}
}

func TestLedgerRefusesBornInGoWithoutAnUnprovenReasonOrBesideATwoPlaneSHA(t *testing.T) {
	guard := `"guards":[{"file":"f.go","test":"TestA"}]`
	reason := strings.Repeat("r", minUnprovenReasonLength)
	for name, entry := range map[string]string{
		"no unproven_reason":   `{"operation":"a","born_in_go":true,"two_plane_ops_sha":"` + strings.Repeat("a", 40) + `",` + guard + `}`,
		"two-plane sha beside": `{"operation":"a","born_in_go":true,"unproven_reason":"` + reason + `","two_plane_ops_sha":"` + strings.Repeat("a", 40) + `",` + guard + `}`,
	} {
		if _, err := ParseGoServedLedger([]byte(`{"deletion_error_message":"{operation} moved","entries":[` + entry + `]}`)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestBornInGoEntryIsNotANamedLimitForAllowExcluded(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	if ledgerHoldsUnprovenLimit(ledger, "sourceHealth") {
		t.Error("sourceHealth is born_in_go: -allow-excluded must give it nothing")
	}
	if !ledgerHoldsUnprovenLimit(ledger, "featureFlagTimeseries") {
		t.Error("featureFlagTimeseries lost its named limit")
	}
}

// Two empty answers agree on nothing. With no document receipt behind a
// born-in-Go shape, the class receipt is the only evidence, so a match that
// compared no leaf is NOT_MEASURED and writes no receipt.
func TestBornInGoShapeThatComparedNoLeafWritesNoReceipt(t *testing.T) {
	empty := sealedMatch("sourceHealth", "")
	empty.comparedLeaves = 0
	runner := classRunner([]sealedOutcome{empty})
	runner.Config.DocRouteReference = true
	runner.GoServed = defaultLedgerForTest(t)
	receipts, verdicts, err := runner.MCPClassReceipts([]Outcome{executedOutcome("sourceHealth", "")},
		map[string][]string{mcpclass.Operation("sourceHealth"): {"sourceHealth"}}, map[string]bool{}, time.Now().UTC())
	if err != nil || len(receipts) != 0 || len(verdicts) != 1 || verdicts[0].Executed != 0 || verdicts[0].TerminalState != "" ||
		len(verdicts[0].Excluded) != 1 || !strings.Contains(verdicts[0].Excluded[0], "doc_operation_not_receipt_backed") {
		t.Fatalf("receipts=%d verdicts=%+v err=%v, want NOT_MEASURED: no receipt, executed 0, the shape excluded", len(receipts), verdicts, err)
	}
}

// The count is taken from the admitted candidate answer by Run and sealed.
func TestRunSealsTheNonNullLeafCountOfTheCandidateAnswer(t *testing.T) {
	runner := newDocRouteRunner(t, goLeg(docRouteAnswer), goLeg(docRouteAnswer), nil)
	if _, _, err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := runner.sealed[0].comparedLeaves; got != 1 {
		t.Fatalf("sealed comparedLeaves %d, want 1 (one non-null key)", got)
	}
}
