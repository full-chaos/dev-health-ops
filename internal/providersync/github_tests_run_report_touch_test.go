package providersync

import (
	"fmt"
	"testing"
)

// CHAOS-14045: two matrix legs run the same tests, so their totals share every case identity. A mismatch keeps the artifacts it
// touches (a copy link, an equal suite instance), not every artifact that shares an identity: the exact total of the other leg is
// still dropped, in any upload order.

type matrixLegsLayout struct {
	shards        int
	otherLegFirst bool
	totalsFirst   bool
}

// matrixLegs returns the artifacts of two legs of n shards (leg A cases 0.125 s, leg B 0.25 s) and a total per leg; mismatchA
// and mismatchB fail shard 0 in that leg's total only. It also returns the ids of the two totals.
func matrixLegs(layout matrixLegsLayout, mismatchA, mismatchB bool) (artifacts []rowsArtifact, totalA, totalB string) {
	leg := func(dur string, failFirst bool) (shards [][]fxSuite, total []fxSuite) {
		for index := range layout.shards {
			name := fmt.Sprintf("shard-%d", index)
			shard := fxSuite{name: name, cases: []fxCase{{name: name, dur: dur}}}
			shards = append(shards, []fxSuite{shard})
			if index == 0 && failFirst {
				shard = fxSuite{name: name, cases: []fxCase{{name: name, dur: dur, status: "failed"}}}
			}
			total = append(total, shard)
		}
		return shards, total
	}
	shardsA, sumA := leg("0.125", mismatchA)
	shardsB, sumB := leg("0.25", mismatchB)
	add := func(suites []fxSuite) string {
		id := fmt.Sprint(len(artifacts) + 1)
		artifacts = append(artifacts, rowsArtifact{id: id, size: int64(len(artifacts)+1) * 10, body: fxJUnit(suites)})
		return id
	}
	addTotals := func() { totalA, totalB = add(sumA), add(sumB) }
	if layout.totalsFirst {
		addTotals()
	}
	first, second := shardsA, shardsB
	if layout.otherLegFirst {
		first, second = shardsB, shardsA
	}
	for _, shard := range append(first, second...) {
		add(shard)
	}
	if !layout.totalsFirst {
		addTotals()
	}
	return artifacts, totalA, totalB
}

var matrixLegsLayouts = []matrixLegsLayout{
	{shards: 2}, {shards: 2, otherLegFirst: true}, {shards: 2, totalsFirst: true},
	{shards: 4, otherLegFirst: true}, {shards: 5}, {shards: 5, otherLegFirst: true}, {shards: 8, otherLegFirst: true, totalsFirst: true},
}

func (layout matrixLegsLayout) String() string {
	return fmt.Sprintf("shards-%d/other-leg-first-%t/totals-first-%t", layout.shards, layout.otherLegFirst, layout.totalsFirst)
}

// The red cell of CHAOS-14045: leg A's total mismatches its parts, leg B's total is exact; leg B's total is dropped.
func TestGitHubTestsRunReportsDropLegBExactTotalWhenLegAMismatches(t *testing.T) {
	legA := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.125"}}}, {name: "right", cases: []fxCase{{name: "b", dur: "0.125"}}}}
	legB := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.25"}}}, {name: "right", cases: []fxCase{{name: "b", dur: "0.25"}}}}
	totalA := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.125", status: "failed"}}}, legA[1]}
	artifacts := []rowsArtifact{
		{id: "1", size: 10, body: fxJUnit(legA[:1])}, {id: "2", size: 20, body: fxJUnit(legA[1:])},
		{id: "3", size: 30, body: fxJUnit(legB[:1])}, {id: "4", size: 40, body: fxJUnit(legB[1:])},
		{id: "5", size: 50, body: fxJUnit(totalA)}, {id: "6", size: 60, body: fxJUnit(legB)},
	}
	assertGroupDecision(t, artifacts, map[string]string{"5": githubTestsAggregateMismatchCause, "6": githubTestsAggregateOfPartsCause})
}

// The same in every upload order and shard count, and mirrored: the mismatched total is leg B's, its parts sit in leg B, and leg A's
// exact total is the one dropped. Both legs' shards are always committed.
func TestGitHubTestsRunReportsDropTheOtherLegsExactTotalInEveryLayout(t *testing.T) {
	for _, layout := range matrixLegsLayouts {
		for _, mismatchedLeg := range []string{"A", "B"} {
			t.Run(layout.String()+"/mismatch-in-leg-"+mismatchedLeg, func(t *testing.T) {
				artifacts, totalA, totalB := matrixLegs(layout, mismatchedLeg == "A", mismatchedLeg == "B")
				want := map[string]string{totalA: githubTestsAggregateMismatchCause, totalB: githubTestsAggregateOfPartsCause}
				if mismatchedLeg == "B" {
					want = map[string]string{totalA: githubTestsAggregateOfPartsCause, totalB: githubTestsAggregateMismatchCause}
				}
				assertGroupDecision(t, artifacts, want)
			})
		}
	}
}

// Both totals mismatch: both are kept, and no shard of either leg is dropped.
func TestGitHubTestsRunReportsKeepBothMismatchedTotals(t *testing.T) {
	for _, layout := range matrixLegsLayouts {
		t.Run(layout.String(), func(t *testing.T) {
			artifacts, totalA, totalB := matrixLegs(layout, true, true)
			assertGroupDecision(t, artifacts, map[string]string{totalA: githubTestsAggregateMismatchCause, totalB: githubTestsAggregateMismatchCause})
		})
	}
}

// R-IDENT-CONTENT: when the legs hold an equal suite instance (here leg B's "right" shard equals leg A's, so it is a copy), a
// mismatch in leg A's total touches leg B through it: leg B's exact total is kept and reported, as before CHAOS-14045, and no
// shard of either leg is dropped.
func TestGitHubTestsRunReportsKeepTheOtherTotalWhenLegsShareASuiteInstance(t *testing.T) {
	right := fxSuite{name: "right", cases: []fxCase{{name: "b", dur: "0.125"}}}
	legA := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.125"}}}, right}
	legB := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.25"}}}, right}
	totalA := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.125", status: "failed"}}}, right}
	artifacts := []rowsArtifact{
		{id: "1", size: 10, body: fxJUnit(legA[:1])}, {id: "2", size: 20, body: fxJUnit(legA[1:])},
		{id: "3", size: 30, body: fxJUnit(legB[:1])}, {id: "4", size: 40, body: fxJUnit(legB[1:])},
		{id: "5", size: 50, body: fxJUnit(totalA)}, {id: "6", size: 60, body: fxJUnit(legB)},
	}
	mismatch := githubTestsAggregateMismatchCause
	assertGroupDecision(t, artifacts, map[string]string{"4": mismatch, "5": mismatch, "6": mismatch})
}
