package providersync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

func drainUnitFixture() ([]workitemmetrics.BlockingRelation, []workitemmetrics.RelationEnd) {
	day := func(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }
	started := day(2, 1)
	relations := []workitemmetrics.BlockingRelation{
		{SourceID: "linear:OPS-1", TargetID: "gh:a/r#1", RelationshipType: "blocks", Raw: "linear_relation:blocks",
			SemanticsVersion: workitemmetrics.CanonicalBlocksSemantics, LastSynced: day(5, 1), StartedAt: &started},
		{SourceID: "linear:OPS-1", TargetID: "gh:a/r#2", RelationshipType: "blocks", Raw: "linear_relation:blocks",
			SemanticsVersion: workitemmetrics.CanonicalBlocksSemantics, LastSynced: day(9, 20), StartedAt: &started},
	}
	ends := []workitemmetrics.RelationEnd{
		{WorkItemID: "linear:OPS-1", Provider: "linear", Status: "in_progress", CreatedAt: day(1, 1), LastSynced: day(9, 20)},
		{WorkItemID: "gh:a/r#1", Provider: "github", Status: "in_progress", CreatedAt: day(1, 1), LastSynced: day(9, 1)},
		{WorkItemID: "gh:a/r#2", Provider: "github", Status: "in_progress", CreatedAt: day(1, 1), LastSynced: day(9, 20)},
	}
	return relations, ends
}

// The differential must fail when the drain list holds a relation a reader still sees: a plant of the one clause
// the proof stands on.
func TestDrainDifferentialFailsWhenALiveRelationIsInTheDrainList(t *testing.T) {
	relations, ends := drainUnitFixture()
	cutoff := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	ended, current, _ := workitemmetrics.EndedBlockingRelations(relations, ends)
	if len(ended) != 1 || current != 1 || ended[0].TargetID != "gh:a/r#1" {
		t.Fatalf("the rule split: ended %d current %d", len(ended), current)
	}
	if _, mismatches := dependencyDrainDifferential(relations, ends, ended, cutoff); mismatches != 0 {
		t.Fatalf("the removed old relation changes an interval inside retention: %d", mismatches)
	}
	items, mismatches := dependencyDrainDifferential(relations, ends, relations[1:], cutoff)
	if mismatches == 0 || items == 0 {
		t.Fatalf("a live relation in the drain list went unseen: items %d mismatches %d", items, mismatches)
	}
}
