package aianalytics

import (
	"context"
	"testing"
)

// CHAOS-14294: clusters equal on count, repository and title prefix are served by anchor, as a number, whatever order
// the read returned them in.
func TestCell14294TiedClustersServedByNumericAnchor(t *testing.T) {
	for _, pair := range [][2]string{{"9", "10"}, {"999999999", "1000000000"}, {"4294967294", "4294967295"}} {
		low, high := pair[0], pair[1]
		got, err := AiOpportunities(context.Background(), &fixtureClientD{c: oracleDCase{Rep: []map[string]any{
			cluster14266("bump deps for", "a@example.com", "pull_request", 6, high),
			cluster14266("bump deps for", "b@example.com", "pull_request", 6, low),
		}}}, "org-1", nil, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Recommendations) != 2 {
			t.Fatalf("%v: want 2 recommendations, got %d", pair, len(got.Recommendations))
		}
		if first := got.Recommendations[0].EvidenceRefs[0]; first != ref14266(low) {
			t.Errorf("%v: first served cluster starts at %s, want anchor %s first", pair, first, low)
		}
	}
}

// One author with two work types keeps its order by work type, whatever the anchors are (the anchor is the last key).
func TestCell14294WorkTypeOrdersBeforeAnchor(t *testing.T) {
	got, err := AiOpportunities(context.Background(), &fixtureClientD{c: oracleDCase{Rep: []map[string]any{
		cluster14266("bump deps for", "a@example.com", "pull_request", 6, "1"),
		cluster14266("bump deps for", "a@example.com", "bug", 6, "3"),
	}}}, "org-1", nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Recommendations) != 2 {
		t.Fatalf("want 2 recommendations, got %d", len(got.Recommendations))
	}
	if first := got.Recommendations[0].EvidenceRefs[0]; first != ref14266("3") {
		t.Errorf("first served cluster starts at %s, want the bug cluster (anchor 3) before pull_request (anchor 1)", first)
	}
}
