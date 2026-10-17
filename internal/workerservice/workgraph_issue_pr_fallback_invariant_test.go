package workerservice

import (
	"testing"
)

// The invariant check must read work_graph_issue_pr after every step that
// writes it; a check placed earlier reads a table a later writer still changes
// and reports 0 for a breach that same build makes.
func TestFallbackInvariantRunsAfterEveryIssuePRLinkWriter(t *testing.T) {
	order := buildPreStepOrder()
	position := map[string]int{}
	for index, name := range order {
		position[name] = index
	}
	check, ok := position["issue_pr_fallback_invariant"]
	if !ok {
		t.Fatalf("build pre-step order %v has no issue_pr_fallback_invariant step: a native-first breach is silent", order)
	}
	for _, writer := range []string{"issue_pr_links", "issue_pr_fallback_retraction", "issue_pr_edges_text_parse", "issue_pr_edges_heuristic"} {
		at, declared := position[writer]
		if !declared {
			t.Fatalf("writer %q is not in the build pre-step order %v", writer, order)
		}
		if at >= check {
			t.Fatalf("issue_pr_fallback_invariant (%d) runs before %q (%d), which writes work_graph_issue_pr", check, writer, at)
		}
	}
}

func TestFallbackInvariantPreStepRefusesANilService(t *testing.T) {
	step, err := newIssuePRFallbackInvariantPreStep(nil)
	if err == nil || step != nil {
		t.Fatalf("newIssuePRFallbackInvariantPreStep(nil) = %v, %v; want a refusal", step, err)
	}
}

func TestFallbackInvariantPreStepNameIsTheDeclaredOne(t *testing.T) {
	if got := (&issuePRFallbackInvariantPreStep{}).Name(); got != "issue_pr_fallback_invariant" {
		t.Fatalf("Name() = %q", got)
	}
}
