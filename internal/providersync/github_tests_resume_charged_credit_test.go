package providersync

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// chargedCreditWalk walks the runs listing of claim to its final emission and returns the final cursor. Each emission of
// the runs range whose after cursor yieldAt accepts ends the pass; mutate changes the fake and the walk resumes from that
// cursor. Executor flush requests carry no cursor and are passed over.
func chargedCreditWalk(
	t *testing.T, handler GitHubTestsRouteHandler, claim Claim, fake *listingFake,
	yieldAt func(githubTestsChunkCursor) bool, mutate func(githubTestsChunkCursor),
) githubTestsChunkCursor {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(fake))
	resume, stop := "", errors.New("yield")
	for pass := 1; pass < 1000; pass++ {
		var final *githubTestsChunkCursor
		var at githubTestsChunkCursor
		err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
			time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume, func(emission ChunkRouteEmission) error {
				if emission.Barrier {
					return nil
				}
				resume = emission.CursorAfter
				after, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if emission.Final {
					final = &after
					return nil
				}
				if yieldAt(after) {
					at = after
					return stop
				}
				return nil
			})
		if final != nil {
			if err != nil {
				t.Fatalf("final emission err=%v", err)
			}
			return *final
		}
		if !errors.Is(err, stop) {
			t.Fatalf("pass %d err=%v, want the yield", pass, err)
		}
		mutate(at)
	}
	t.Fatal("the walk never reached a final emission")
	return githubTestsChunkCursor{}
}

// A run created in the anchor's second appears AHEAD of the anchor between two attempts, four times in one walk: each
// resume finds the anchor one place LATER than its stored index and seeks the listing again. Nothing was deleted, so the
// walk earns no deletion credit: its record credit stays the re-seek re-read credit main earns (the anchor and the new run
// of each boundary second, 4 x 2 = 8).
func TestGitHubTestsResumeInsertedRunMoveEarnsNoDeletionCredit(t *testing.T) {
	window := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	for _, coalesce := range deletionShiftFlags() {
		t.Run(deletionShiftFlag(coalesce), func(t *testing.T) {
			fake := newListingFake(t, makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Second))
			moves := 0
			yieldAt := func(c githubTestsChunkCursor) bool {
				return moves < 4 && c.Phase == githubTestsRunsPhase && !c.LateRuns && c.Index > 0 && c.RangeItems == 30
			}
			insert := func(c githubTestsChunkCursor) {
				var anchor listingRun
				for _, run := range fake.runs {
					if strconv.Itoa(run.id) == c.AnchorID {
						anchor = run
					}
				}
				if anchor.id == 0 {
					t.Fatalf("premise: anchor %q not in the fake", c.AnchorID)
				}
				moves++
				fake.runs = append(fake.runs, listingRun{id: 100_000 + moves, created: anchor.created, updated: anchor.updated,
					status: "completed", conclusion: "success"})
			}
			final := chargedCreditWalk(t, GitHubTestsRouteHandler{CoalesceRuns: coalesce}, window, fake, yieldAt, insert)
			t.Logf("CELL inserted/%s moves=%d rc=%d", deletionShiftFlag(coalesce), moves, final.RecordCredit)
			if moves != 4 {
				t.Fatalf("premise: %d moves, want 4", moves)
			}
			if final.RecordCredit != 8 {
				t.Errorf("record credit %d, want main's re-seek re-read credit 8 (an inserted run earns no deletion credit)", final.RecordCredit)
			}
		})
	}
}

// 250 runs of one record each, record bound 200 (deletion-credit ceiling 200/20 = 10): 20 read runs before the anchor are
// deleted at the 30th run. The deletion credit is capped at the ceiling, so the walk reads at most 10 runs past main's
// 200 (main reads 200: the bound with the re-seek re-read credit of the anchor).
func TestGitHubTestsResumeDeletionCreditStopsAtItsCeiling(t *testing.T) {
	listing := deletionShiftListings()[0]
	runs := listing.runs(1, 250)
	deleted := make([]int, 0, 20)
	for id := 250; id > 230; id-- {
		deleted = append(deleted, id)
	}
	for _, coalesce := range deletionShiftFlags() {
		t.Run(deletionShiftFlag(coalesce), func(t *testing.T) {
			fake := newListingFake(t, slices.Clone(runs))
			handler := GitHubTestsRouteHandler{MaxWalkRecords: 200, CoalesceRuns: coalesce}
			at := func(c githubTestsChunkCursor) bool { return listing.in(c) && c.RangeItems >= 30 }
			deletionShiftWalk(t, handler, listing.claim, fake, at, func(githubTestsChunkCursor) { resumeDeletionDelete(fake, deleted...) }, nil)
			read := len(fake.jobGets)
			t.Logf("CELL ceiling/%s deleted=%d read=%d", deletionShiftFlag(coalesce), len(deleted), read)
			if read > 210 {
				t.Errorf("read %d runs, want at most 210 (the bound 200 plus the deletion-credit ceiling 10)", read)
			}
		})
	}
}
