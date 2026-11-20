package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// resumeDeletionWalk drives CollectChunks to its final emission. The first emission whose after cursor satisfies yieldAt
// ends the pass; mutate then changes the fake (between two chunks, as GitHub can) and the walk resumes from that cursor,
// first passed through rewrite when it is set (a cursor an older binary wrote).
func resumeDeletionWalk(
	t *testing.T, handler GitHubTestsRouteHandler, claim Claim, fake *listingFake,
	yieldAt func(after githubTestsChunkCursor) bool, mutate func(after githubTestsChunkCursor), rewrite func(string) string,
) listingWalk {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(fake))
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	walk := listingWalk{}
	resume := ""
	stop := errors.New("yield")
	yielded := false
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for pass := 1; pass < 1000; pass++ {
		last, finalSeen := resume, false
		var at githubTestsChunkCursor
		err := handler.CollectChunks(ctx, claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
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
					walk.final, finalSeen = emission.Batch, true
					return nil
				}
				walk.chunks++
				after, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if !yielded && yieldAt(after) {
					yielded, at = true, after
					return stop
				}
				return nil
			})
		if finalSeen {
			if err != nil {
				t.Fatalf("final emission returned err=%v", err)
			}
			if !yielded {
				t.Fatal("the walk never reached its yield point")
			}
			cursor, decodeErr := decodeGitHubTestsChunkCursor(last)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			walk.cursor, walk.passes = cursor, pass
			return walk
		}
		if !errors.Is(err, stop) {
			t.Fatalf("pass %d err=%v, want the yield", pass, err)
		}
		mutate(at)
		resume = last
		if rewrite != nil {
			resume = rewrite(resume)
		}
	}
	t.Fatal("the walk never reached a final emission")
	return walk
}

// resumeDeletionConsumed lists the ids read so far (jobs or artifacts requests), newest first.
func resumeDeletionConsumed(reads map[int]int) []int {
	ids := make([]int, 0, len(reads))
	for id, count := range reads {
		if count > 0 {
			ids = append(ids, id)
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(ids)))
	return ids
}

func resumeDeletionDelete(fake *listingFake, ids ...int) {
	gone := map[int]bool{}
	for _, id := range ids {
		gone[id] = true
	}
	kept := fake.runs[:0:0]
	for _, run := range fake.runs {
		if !gone[run.id] {
			kept = append(kept, run)
		}
	}
	fake.runs = kept
}

// A run deleted on GitHub BEFORE the resume point between two chunks must not move the resume point: every run that is
// still listed is read (one jobs request in the runs phase, one artifacts request in the artifacts phase), the same set a
// fresh walk over the remaining runs reads, and none is skipped. Each run is read exactly once, except the runs of the
// anchor's created second after a re-seek (githubTestsReseek): those are read twice, never more (a dup whose rows replace
// the stored ones, never a skip). With nothing deleted nothing is read twice.
func TestGitHubTestsResumeAfterARunDeletedBeforeTheResumePointReadsEachRunOnce(t *testing.T) {
	window := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	windowRuns := func() []listingRun {
		return makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Minute)
	}
	// Five runs share each created second: the anchor's second holds runs listed before it.
	sharedRuns := func() []listingRun {
		runs := makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Minute)
		for index := range runs {
			runs[index].created = listingSince.Add(time.Minute + time.Duration(index/5)*time.Second)
			runs[index].updated = runs[index].created.Add(time.Minute)
		}
		return runs
	}
	deep := lateRerunMidnightClaim("incremental")
	deepRuns := func() []listingRun { return lateRerunRuns(1, 250, deep.SinceAt.UTC()) }
	inRuns := func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && !c.LateRuns }
	inDeep := func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && c.LateRuns }
	inArtifacts := func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsArtifactsPhase && c.Relist == 0 }
	newest := func(n int) func(consumed []int) []int {
		return func(consumed []int) []int { return consumed[len(consumed)-n:] }
	}
	none := func([]int) []int { return nil }
	oldestRun := func([]int) []int { return []int{1} }           // not read yet: after the resume point
	anchor := func(consumed []int) []int { return consumed[:1] } // the oldest read run: the last one listed
	at := func(phase func(githubTestsChunkCursor) bool, items int) func(githubTestsChunkCursor) bool {
		return func(c githubTestsChunkCursor) bool { return phase(c) && c.RangeItems == items }
	}
	for _, tc := range []struct {
		name      string
		claim     Claim
		runs      func() []listingRun
		artifacts bool
		legacy    bool // the resume cursor was written before AnchorID existed
		yieldAt   func(c githubTestsChunkCursor) bool
		victims   func(consumed []int) []int
	}{
		{"control: runs range mid page, nothing deleted", window, windowRuns, false, false, at(inRuns, 50), none},
		{"control: runs range page boundary, nothing deleted", window, windowRuns, false, false, at(inRuns, 100), none},
		{"control: artifacts mid page, nothing deleted", window, windowRuns, true, false, at(inArtifacts, 50), none},
		{"runs range mid page, newest consumed run deleted", window, windowRuns, false, false, at(inRuns, 50), newest(1)},
		{"runs range page boundary, newest consumed run deleted", window, windowRuns, false, false, at(inRuns, 100), newest(1)},
		{"runs range mid page, the resume anchor itself deleted", window, windowRuns, false, false, at(inRuns, 50), anchor},
		{"runs range second page, 120 consumed runs deleted", window, windowRuns, false, false, at(inRuns, 150), newest(120)},
		{"runs range mid page, five runs per second, newest consumed run deleted", window, sharedRuns, false, false, at(inRuns, 52), newest(1)},
		{"deep range mid page, newest consumed run deleted", deep, deepRuns, false, false, at(inDeep, 50), newest(1)},
		{"deep range page boundary, newest consumed run deleted", deep, deepRuns, false, false, at(inDeep, 100), newest(1)},
		{"artifacts mid page, newest consumed run deleted", window, windowRuns, true, false, at(inArtifacts, 50), newest(1)},
		{"artifacts page boundary, newest consumed run deleted", window, windowRuns, true, false, at(inArtifacts, 100), newest(1)},
		// The run after the anchor is the last one: deleting it leaves the anchor at the end of the last page.
		{"runs range last page, the only run after the anchor deleted", window, windowRuns, false, false, at(inRuns, 249), oldestRun},
		{"artifacts last page, the only run after the anchor deleted", window, windowRuns, true, false, at(inArtifacts, 249), oldestRun},
		{"legacy cursor: runs range mid page, newest consumed run deleted", window, windowRuns, false, true, at(inRuns, 50), newest(1)},
		{"legacy cursor: runs range mid page, nothing deleted", window, windowRuns, false, true, at(inRuns, 50), none},
		{"legacy cursor: artifacts mid page, newest consumed run deleted", window, windowRuns, true, true, at(inArtifacts, 50), newest(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newListingFake(t, tc.runs())
			reads := func(f *listingFake) map[int]int {
				if tc.artifacts {
					return f.artifactGets
				}
				return f.jobGets
			}
			var rewrite func(string) string
			if tc.legacy {
				rewrite = func(raw string) string {
					cursor, err := decodeGitHubTestsChunkCursor(raw)
					if err != nil || cursor.AnchorID == "" || cursor.Index == 0 {
						t.Fatalf("the yield cursor is not a mid-page anchored cursor: %v %+v", err, cursor)
					}
					cursor.AnchorID = ""
					legacy, err := encodeGitHubTestsChunkCursor(cursor)
					if err != nil {
						t.Fatal(err)
					}
					return legacy
				}
			}
			var deleted []int
			created := map[int]time.Time{}
			for _, run := range fake.runs {
				created[run.id] = run.created
			}
			anchorSecond := map[int]bool{}
			walk := resumeDeletionWalk(t, GitHubTestsRouteHandler{}, tc.claim, fake, tc.yieldAt, func(at githubTestsChunkCursor) {
				oldest, err := time.Parse(time.RFC3339, at.RangeOldest)
				if err != nil {
					t.Fatalf("yield cursor has no oldest created second: %q", at.RangeOldest)
				}
				for id, createdAt := range created {
					if createdAt.UTC().Truncate(time.Second).Equal(oldest) {
						anchorSecond[id] = true
					}
				}
				consumed := resumeDeletionConsumed(reads(fake))
				sort.Ints(consumed) // oldest first: the newest consumed runs are at the end
				deleted = tc.victims(consumed)
				resumeDeletionDelete(fake, deleted...)
			}, rewrite)
			oracle := newListingFake(t, fake.runs)
			walkListing(t, GitHubTestsRouteHandler{}, tc.claim, oracle, 0)
			var skipped, twice, dups []string
			for id := range reads(oracle) {
				if reads(fake)[id] == 0 {
					skipped = append(skipped, strconv.Itoa(id))
				}
			}
			for id, count := range reads(fake) {
				switch {
				case count == 2 && anchorSecond[id]:
					dups = append(dups, strconv.Itoa(id))
				case count > 1:
					twice = append(twice, strconv.Itoa(id)+"x"+strconv.Itoa(count))
				}
			}
			sort.Strings(skipped)
			sort.Strings(twice)
			sort.Strings(dups)
			moved := len(deleted) > 0 && reads(fake)[deleted[0]] > 0 || tc.legacy
			t.Logf("deleted=%d first=%v remaining_expected=%d skipped=%v read_twice=%v anchor_second_dups=%v complete=%v watermark=%v archives_known=%v",
				len(deleted), deleted[:min(3, len(deleted))], len(reads(oracle)), skipped, twice, dups,
				walk.final.Result[GitHubTestsResultKeyReportsComplete], walk.final.Watermark != nil, walk.cursor.ArchivesSeen != nil)
			if len(reads(oracle)) == 0 {
				t.Fatal("the oracle read no run: the cell proves nothing")
			}
			if !moved && len(dups) != 0 {
				t.Errorf("nothing moved, yet runs were read twice: %v", dups)
			}
			if len(skipped) != 0 || len(twice) != 0 {
				t.Errorf("a deletion before the resume point moved the resume: skipped=%v read_twice=%v", skipped, twice)
			}
			// A re-seek in the artifacts phase reads runs again, so the totality counters become unknown, as a re-anchor did.
			if tc.artifacts && moved == (walk.cursor.ArchivesSeen != nil) {
				t.Errorf("archive counters known=%v after moved=%v, want unknown exactly when the listing was sought again", walk.cursor.ArchivesSeen != nil, moved)
			}
		})
	}
}

// A re-seek inside the cut second of a peeled range keeps the carried list as its skip source: the runs the first range
// read whole at that second are not read again, the runs this range read there are read again once (the named dup), and
// no run is skipped.
func TestGitHubTestsResumeReseekInsideTheCutSecondKeepsTheCarriedRuns(t *testing.T) {
	claim := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	cut := listingSince.Add(time.Hour)
	runs := makeListingRuns(1, 50, listingSince.Add(10*time.Minute), time.Second) // older than the cut second
	for index := range 8 {                                                        // the cut second: ids 101..108
		runs = append(runs, listingRun{id: 101 + index, created: cut, updated: cut.Add(time.Minute), status: "completed", conclusion: "success"})
	}
	runs = append(runs, makeListingRuns(1001, 995, cut.Add(time.Second), 10*time.Second)...) // newer fillers
	fake := newListingFake(t, runs)
	var carried []string
	var deleted int
	walk := resumeDeletionWalk(t, GitHubTestsRouteHandler{}, claim, fake, func(c githubTestsChunkCursor) bool {
		return c.Phase == githubTestsRunsPhase && c.RangeHi == cut.Format(time.RFC3339) && c.RangeItems == 7
	}, func(c githubTestsChunkCursor) {
		carried = strings.Split(c.CarriedCutKeys, ",")
		deleted, _ = strconv.Atoi(carried[0])
		resumeDeletionDelete(fake, deleted)
	}, nil)
	if len(carried) != 5 {
		t.Fatalf("carried list %v, want the 5 runs of the cut second the first range read", carried)
	}
	oracle := newListingFake(t, fake.runs)
	walkListing(t, GitHubTestsRouteHandler{}, claim, oracle, 0)
	for id := range oracle.jobGets {
		if fake.jobGets[id] == 0 {
			t.Errorf("run %d skipped", id)
		}
	}
	for _, key := range carried {
		id, _ := strconv.Atoi(key)
		if fake.jobGets[id] != 1 {
			t.Errorf("carried run %d read %d times, want 1 (the re-seek keeps the carried skip list)", id, fake.jobGets[id])
		}
	}
	for id := 101; id <= 108; id++ {
		if fake.jobGets[id] > 2 {
			t.Errorf("cut-second run %d read %d times, want at most 2", id, fake.jobGets[id])
		}
	}
	for id, count := range fake.jobGets {
		if (id < 101 || id > 108) && count != 1 {
			t.Errorf("run %d read %d times, want 1", id, count)
		}
	}
	t.Logf("carried=%v deleted=%d cut_second_reads=%v complete=%v", carried, deleted,
		[]int{fake.jobGets[101], fake.jobGets[102], fake.jobGets[103], fake.jobGets[104], fake.jobGets[105], fake.jobGets[106], fake.jobGets[107], fake.jobGets[108]},
		walk.final.Result[GitHubTestsResultKeyReportsComplete])
}

// A resume on a page end re-reads the finished page to check its anchor; that read is free in every listing's page budget,
// so a walk that needs exactly its budget still ends whole when it resumes on each page end.
func TestGitHubTestsResumeOnAPageEndCostsNoBudgetPage(t *testing.T) {
	claim := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	for _, phase := range []string{githubTestsRunsPhase, githubTestsArtifactsPhase} {
		t.Run(phase, func(t *testing.T) {
			fake := newListingFake(t, makeListingRuns(1, 300, listingSince.Add(time.Minute), time.Minute))
			walk := resumeDeletionWalk(t, GitHubTestsRouteHandler{MaxRuns: 300}, claim, fake, func(c githubTestsChunkCursor) bool {
				return c.Phase == phase && c.Relist == 0 && !c.LateRuns && c.RangeItems == 200
			}, func(githubTestsChunkCursor) {}, nil)
			if incomplete := walk.incompleteComponents(); len(incomplete) != 0 || walk.final.Watermark == nil {
				t.Fatalf("a walk of exactly its page budget resumed on a page end: incomplete=%v watermark=%v", incomplete, walk.final.Watermark != nil)
			}
			if len(fake.jobGets) != 300 || len(fake.artifactGets) != 300 {
				t.Fatalf("runs read: jobs %d, artifacts %d, want 300 each", len(fake.jobGets), len(fake.artifactGets))
			}
		})
	}
}

// The resume anchor is the last field the cursor fit drops (after both cut lists); a cursor without it is sought again on
// resume, never resumed by position.
func TestGitHubTestsCursorFitDropsTheAnchorLast(t *testing.T) {
	base := githubTestsChunkCursor{Phase: githubTestsRunsPhase, Index: 3, Repo: "acme/api", RangeHi: "2026-07-20T06:30:00Z",
		RangeOldest: "2026-07-20T06:30:00Z", ActiveCutKeys: "11", CarriedCutKeys: "12", AnchorID: githubTestsWidestAnchorID}
	empty, _ := json.Marshal(githubTestsChunkCursor{Phase: base.Phase, Index: base.Index, Repo: base.Repo, RangeHi: base.RangeHi, RangeOldest: base.RangeOldest})
	base.NextURL = "https://api.github.com/x?" + strings.Repeat("a", maxChunkCursorBytes-len(empty)-len(`,"next_url":""`)-len("https://api.github.com/x?"))
	fitted, dropped := githubTestsFitCursor(base)
	if !reflect.DeepEqual(dropped, []string{"active", "carried", "anchor"}) || fitted.AnchorID != "" || fitted.NextURL != base.NextURL {
		t.Fatalf("dropped %v anchor %q, want active, carried, anchor and the URL kept", dropped, fitted.AnchorID)
	}
	if start, reseek := githubTestsResumeStart(fitted, providerfoundation.PageVisit{CursorBefore: fitted.NextURL, Items: make([]json.RawMessage, 5)}, nil, Claim{}, githubTestsRunsPhase); start != 0 || !reseek {
		t.Fatalf("a cursor without its anchor resumed at %d (reseek %v), want a re-seek", start, reseek)
	}
}
