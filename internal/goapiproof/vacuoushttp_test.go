package goapiproof

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// TestRunVacuityRequiresTheSameHTTPAnswer is the runner half of the
// vacuity rule: two legs with identical zero-leaf bodies are "nothing to
// compare" only when the planes also gave the same HTTP answer (status and
// every compared header). A status or header difference is a difference
// between the planes, and it is reported as a mismatch naming it -- never
// swallowed as a vacuous refusal.
func TestRunVacuityRequiresTheSameHTTPAnswer(t *testing.T) {
	withOverriddenParity(t, "featureFlags", Options{BaselineDefects: []BaselineDefect{{
		Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.featureFlags.unused"},
	}}})
	var rows []string
	for _, cell := range []struct {
		name            string
		candidateStatus int
		candidateType   string
		wantVacuous     bool
		wantFinding     string
	}{
		{"same status, same content type", http.StatusOK, "", true, ""},
		{"candidate 202, baseline 200", http.StatusAccepted, "", false, "$.http.status"},
		{"candidate 203, baseline 200", http.StatusNonAuthoritativeInfo, "", false, "$.http.status"},
		{"content type differs", http.StatusOK, "application/problem+json", false, "$.http.header.content-type"},
		{"status and content type differ", http.StatusAccepted, "application/problem+json", false, "$.http.status"},
	} {
		runner := runnerAgainst(t, &planeStampingEdge{
			body:           `{"data":{"featureFlags":[]}}`,
			candidatePlane: "go", baselinePlane: "python",
			candidateStatus: cell.candidateStatus, candidateType: cell.candidateType,
		}, "canary")
		outcomes, _, err := runner.Run(context.Background())
		got := fmt.Sprintf("terminal=%q refusal=%q findings=%v", outcomes[0].TerminalState, outcomes[0].RefusalReason, findingPathsOf(outcomes[0].Findings))
		rows = append(rows, cell.name+" -> "+got)
		if cell.wantVacuous {
			if !errors.Is(err, ErrNothingMeasured) || outcomes[0].RefusalReason != RefusalVacuousEmptyLegs {
				t.Errorf("%s: %s (err %v), want a vacuous refusal", cell.name, got, err)
			}
			continue
		}
		if outcomes[0].RefusalReason == RefusalVacuousEmptyLegs || outcomes[0].TerminalState != TerminalStateMismatch {
			t.Errorf("%s: %s, want a mismatch", cell.name, got)
			continue
		}
		named := false
		for _, f := range outcomes[0].Findings {
			if f.Path == cell.wantFinding {
				named = true
			}
		}
		if !named {
			t.Errorf("%s: %s, want a finding at %s", cell.name, got, cell.wantFinding)
		}
	}
	t.Logf("vacuity x HTTP answer:\n%s", strings.Join(rows, "\n"))
}

func findingPathsOf(findings []Finding) []string {
	paths := make([]string, 0, len(findings))
	for _, f := range findings {
		paths = append(paths, f.Path)
	}
	return paths
}

// TestRunStructuralRefusalWithADifferentHTTPAnswerIsAMismatch pins the
// sibling exit: a pair Compare refuses as legs_do_not_overlap (no shared
// ids) while the candidate answers a different status. The planes did not
// give the same answer, so the outcome is a mismatch carrying both the
// HTTP difference and the body's structural reason -- never a refusal.
func TestRunStructuralRefusalWithADifferentHTTPAnswerIsAMismatch(t *testing.T) {
	runner := runnerAgainst(t, &planeStampingEdge{
		body:           `{"data":{"featureFlags":[{"id":"ABC-1"}]}}`,
		candidatePlane: "go", baselinePlane: "python",
		candidateStatus: http.StatusAccepted,
		candidateBody:   `{"data":{"featureFlags":[{"id":"ABC-2"}]}}`,
	}, "canary")
	outcomes, _, err := runner.Run(context.Background())
	if err != nil && !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("-> terminal=%q refusal=%q findings=%v", outcomes[0].TerminalState, outcomes[0].RefusalReason, findingPathsOf(outcomes[0].Findings))
	paths := strings.Join(findingPathsOf(outcomes[0].Findings), " ")
	if outcomes[0].TerminalState != TerminalStateMismatch || !strings.Contains(paths, "$.http.status") || !strings.Contains(paths, "$.data") {
		t.Fatalf("outcome = %+v, want a mismatch naming $.http.status and $.data", outcomes[0])
	}
}
