package providersync

import (
	"testing"
	"time"
)

// A runs phase that stops at its page budget inside a PEELED range must not hand that range's end to the artifacts
// listing: the artifacts listing starts its own range over the full window (pre-existing defect fixed by CHAOS-14316).
// 1,200 runs: the first runs range saturates at GitHub's cap of 1,000 and is peeled; the 11-page budget stops the runs
// phase on the peeled range's first page. The newest runs, above the peeled end, must still get their artifacts read.
func TestGitHubTestsRunsBudgetStopInAPeeledRangeLeavesTheArtifactsListingWhole(t *testing.T) {
	claim := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	fake := newListingFake(t, makeListingRuns(1, 1200, listingSince.Add(time.Minute), 10*time.Second))
	walk := walkListing(t, GitHubTestsRouteHandler{MaxRuns: 1050}, claim, fake, 0)
	end := claim.BeforeAt.UTC().Format(time.RFC3339)
	peeled, artifactsWhole := false, false
	for _, created := range fake.listRanges {
		if len(created) > 10 && created[10] == 'T' && created[len(created)-len(end):] != end {
			peeled = true
		}
		if len(created) > 10 && created[10] == '.' && created[len(created)-len(end):] == end {
			artifactsWhole = true
		}
	}
	t.Logf("ranges=%v incomplete=%v newest_artifact_reads=%d", fake.listRanges, walk.incompleteComponents(), fake.artifactGets[1200])
	if !peeled {
		t.Fatal("premise: the runs phase never listed a peeled range")
	}
	if !artifactsWhole || fake.artifactGets[1200] != 1 {
		t.Fatalf("artifacts listing over the full window=%v, newest run artifact reads=%d; want true and 1", artifactsWhole, fake.artifactGets[1200])
	}
}
