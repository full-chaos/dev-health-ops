package issueprlinks

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

func TestCountFallbacksBesideNativeCountsBothSidesOncePerRow(t *testing.T) {
	stored := []Link{
		link(fallbackRepoA, 704, "linear:CHAOS-1", ProvenanceNative),
		link(fallbackRepoA, 704, "jira:API-1", edges.ProvenanceExplicitText),     // PR side
		link(fallbackRepoA, 705, "linear:CHAOS-1", edges.ProvenanceHeuristic),    // issue side
		link(fallbackRepoA, 704, "linear:CHAOS-1", edges.ProvenanceExplicitText), // both sides
		link(fallbackRepoB, 704, "jira:OPS-7", edges.ProvenanceExplicitText),     // other repo, same number: control
		link(fallbackRepoA, 706, "jira:BILL-1", edges.ProvenanceHeuristic),       // control
	}
	got := CountFallbacksBesideNative(stored)
	want := FallbacksBesideNative{ByPullRequest: 2, ByIssue: 2, Total: 3}
	if got != want {
		t.Fatalf("CountFallbacksBesideNative = %+v, want %+v", got, want)
	}
}

func TestCountFallbacksBesideNativeIsZeroWithoutAnOutrankedRow(t *testing.T) {
	controls := map[string][]Link{
		"no native link": {
			link(fallbackRepoA, 704, "jira:API-1", edges.ProvenanceExplicitText),
			link(fallbackRepoA, 705, "jira:API-1", edges.ProvenanceHeuristic),
		},
		"native links only": {
			link(fallbackRepoA, 704, "linear:CHAOS-1", ProvenanceNative),
			link(fallbackRepoA, 704, "jira:OPS-7", ProvenanceNative),
		},
		"fallbacks on other sides": {
			link(fallbackRepoA, 704, "linear:CHAOS-1", ProvenanceNative),
			link(fallbackRepoA, 705, "jira:API-1", edges.ProvenanceExplicitText),
			link(fallbackRepoB, 704, "jira:BILL-1", edges.ProvenanceHeuristic),
		},
		"empty": nil,
	}
	for name, stored := range controls {
		t.Run(name, func(t *testing.T) {
			if got := CountFallbacksBesideNative(stored); got != (FallbacksBesideNative{}) {
				t.Fatalf("CountFallbacksBesideNative = %+v, want all 0", got)
			}
		})
	}
}

// The invariant count and the retraction plan apply one rule: every row the
// count finds is one the retraction removes.
func TestCountFallbacksBesideNativeAgreesWithTheRetractionPlan(t *testing.T) {
	for nativeProvider, nativeIDs := range trackerIDs {
		for textProvider, textIDs := range trackerIDs {
			t.Run(nativeProvider+"_native/"+textProvider+"_text", func(t *testing.T) {
				stored := []Link{
					link(fallbackRepoA, 704, nativeIDs[0], ProvenanceNative),
					link(fallbackRepoA, 704, textIDs[1], edges.ProvenanceExplicitText),
					link(fallbackRepoA, 705, nativeIDs[0], edges.ProvenanceHeuristic),
					link(fallbackRepoB, 706, textIDs[1]+"x", edges.ProvenanceExplicitText),
				}
				if got, want := CountFallbacksBesideNative(stored).Total, len(PlanOutrankedFallbacks(stored)); got != want || got != 2 {
					t.Fatalf("count %d, retraction plan %d, want 2 and 2", got, want)
				}
			})
		}
	}
}

// A lower-tier row whose issue side is a pull request work item breaks the
// same rule the retraction enforces, with or without a native link.
func TestCountFallbacksBesideNativeCountsAPullRequestIssueSide(t *testing.T) {
	stored := []Link{
		link(fallbackRepoA, 704, "ghpr:full-chaos/acr#5", edges.ProvenanceExplicitText),
		link(fallbackRepoA, 705, "gitlab:full-chaos/acr!9", edges.ProvenanceHeuristic),
		link(fallbackRepoA, 706, "jira:API-1", edges.ProvenanceExplicitText), // control
	}
	got := CountFallbacksBesideNative(stored)
	if got.PullRequestIssueSide != 2 || got.Total != 2 || got.Total != len(PlanOutrankedFallbacks(stored)) {
		t.Fatalf("CountFallbacksBesideNative = %+v, retraction plan %d; want 2 pull-request issue sides, total 2", got, len(PlanOutrankedFallbacks(stored)))
	}
}
