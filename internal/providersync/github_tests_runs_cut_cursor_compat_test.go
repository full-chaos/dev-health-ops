package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// runsCompatWalk drives CollectChunks to its final emission, yields once at the first after cursor yieldAt picks, and
// resumes a new invocation from that cursor as rewrite returns it.
func runsCompatWalk(t *testing.T, claim Claim, fake *listingFake, yieldAt func(githubTestsChunkCursor) bool, rewrite func(string) string) (listingWalk, bool) {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(fake))
	stop := errors.New("yield")
	walk, resume, yielded := listingWalk{}, "", false
	for pass := 1; pass <= 3; pass++ {
		err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, claim.BeforeAt.Add(time.Hour), resume,
			func(emission ChunkRouteEmission) error {
				resume = emission.CursorAfter
				for _, effect := range emission.Batch.Effects {
					if effect.Destination == "ci_pipeline_runs" {
						rows, decodeErr := decodeEffectRows[githubTestsPipelineRow](effect)
						if decodeErr != nil {
							t.Fatal(decodeErr)
						}
						walk.pipelines = append(walk.pipelines, rows...)
					}
				}
				if emission.Final {
					walk.final = emission.Batch
					return nil
				}
				after, decodeErr := decodeGitHubTestsChunkCursor(resume)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if !yielded && yieldAt(after) {
					yielded = true
					return stop
				}
				return nil
			})
		if err == nil {
			return walk, yielded
		}
		if !errors.Is(err, stop) {
			t.Fatalf("pass %d: %v", pass, err)
		}
		resume = rewrite(resume)
	}
	t.Fatal("the walk never reached a final emission")
	return walk, yielded
}

// A cursor written by the binary before CHAOS-14321 (cut lists of at most 52 bytes, no rq) decodes and resumes: every
// run is read once, mid-range (active list) and in the revisit (carried list). The same cell runs on the old binary.
func TestGitHubTestsRunsCutCursorFromBeforeTheCapResumes(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	since := claim.SinceAt.UTC()
	var runs []listingRun
	for index := range 1001 {
		created := since.Add(time.Minute + time.Duration(index)*time.Second)
		runs = append(runs, listingRun{id: index + 1, created: created, updated: created.Add(time.Minute), status: "completed", conclusion: "success", attempt: 2})
	}
	for index := 1; index < 3; index++ {
		runs[index].created = runs[0].created
	}
	old := func(raw string) string {
		var fields map[string]any
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			t.Fatal(err)
		}
		delete(fields, "rq")
		for _, key := range []string{"ak", "ck"} {
			if list, _ := fields[key].(string); len(list) > 52 {
				t.Fatalf("premise: %s is %d bytes, an old cursor holds at most 52", key, len(list))
			}
		}
		out, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		return string(out)
	}
	for _, tc := range []struct {
		name    string
		yieldAt func(githubTestsChunkCursor) bool
	}{
		{"mid_range_active_list", func(c githubTestsChunkCursor) bool {
			return c.Phase == githubTestsRunsPhase && c.RangeHi == "" && c.RangeItems == 999 && c.ActiveCutKeys != "" && c.NextURL != ""
		}},
		{"revisit_carried_list", func(c githubTestsChunkCursor) bool {
			return c.Phase == githubTestsRunsPhase && c.CarriedCutKeys != ""
		}},
	} {
		fake := newListingFake(t, runs)
		walk, yielded := runsCompatWalk(t, claim, fake, tc.yieldAt, old)
		t.Logf("%s: yielded=%t rows=%d unique=%d extra_jobs=%d watermark=%t", tc.name, yielded, len(walk.pipelines), len(walk.pipelineIDs()),
			runsTwiceExtraJobs(fake), walk.final.Watermark != nil)
		if !yielded || len(walk.pipelines) != len(runs) || len(walk.pipelineIDs()) != len(runs) || runsTwiceExtraJobs(fake) != 0 || walk.final.Watermark == nil {
			t.Errorf("%s: yielded=%t rows=%d unique=%d extra jobs=%d, want a resume and each of %d runs once", tc.name, yielded,
				len(walk.pipelines), len(walk.pipelineIDs()), runsTwiceExtraJobs(fake), len(runs))
		}
	}
}
