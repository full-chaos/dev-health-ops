package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
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
	return resumeDeletionWalkDoer(t, handler, claim, fake, yieldAt, mutate, rewrite)
}

// resumeDeletionWalkDoer is resumeDeletionWalk over any fake GitHub.
func resumeDeletionWalkDoer(
	t *testing.T, handler GitHubTestsRouteHandler, claim Claim, doer providerfoundation.HTTPDoer,
	yieldAt func(after githubTestsChunkCursor) bool, mutate func(after githubTestsChunkCursor), rewrite func(string) string,
) listingWalk {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(doer))
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
		// An artifacts cursor without an anchor resumes by position (acp r1 ruling P1-3): nothing moved reads nothing twice and
		// keeps the totality counters known. A same-length move there keeps the old skip class (named residual, not pinned).
		{"legacy cursor: artifacts mid page, nothing deleted", window, windowRuns, true, true, at(inArtifacts, 50), none},
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
			moved := len(deleted) > 0 && reads(fake)[deleted[0]] > 0 || tc.legacy && !tc.artifacts
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

// The resume anchor is the last field the cursor fit drops (after both cut lists); a runs cursor without it is sought again
// on resume, never resumed by position; an artifacts cursor without it resumes by position.
func TestGitHubTestsCursorFitDropsTheAnchorLast(t *testing.T) {
	base := githubTestsChunkCursor{Phase: githubTestsRunsPhase, Index: 3, Repo: "acme/api", RangeHi: "2026-07-20T06:30:00Z",
		RangeOldest: "2026-07-20T06:30:00Z", ActiveCutKeys: "11", CarriedCutKeys: "12", AnchorID: githubTestsWidestAnchorID}
	empty, _ := json.Marshal(githubTestsChunkCursor{Phase: base.Phase, Index: base.Index, Repo: base.Repo, RangeHi: base.RangeHi, RangeOldest: base.RangeOldest})
	base.NextURL = "https://api.github.com/x?" + strings.Repeat("a", maxChunkCursorBytes-len(empty)-len(`,"next_url":""`)-len("https://api.github.com/x?"))
	fitted, dropped := githubTestsFitCursor(base)
	if !reflect.DeepEqual(dropped, []string{"active", "carried", "anchor"}) || fitted.AnchorID != "" || fitted.NextURL != base.NextURL {
		t.Fatalf("dropped %v anchor %q, want active, carried, anchor and the URL kept", dropped, fitted.AnchorID)
	}
	page := providerfoundation.PageVisit{CursorBefore: fitted.NextURL, Items: make([]json.RawMessage, 5)}
	if start, action := githubTestsResumeStart(fitted, page, true, githubTestsPageEnd{}, nil, Claim{}, githubTestsRunsPhase); start != 0 || action != githubTestsResumeReseek {
		t.Fatalf("a runs cursor without its anchor resumed at %d (action %d), want a re-seek", start, action)
	}
	// The artifacts phases resume such a cursor by position (acp r1 ruling P1-3): the all-unreadable gate keeps its counters.
	fitted.Phase = githubTestsArtifactsPhase
	if start, action := githubTestsResumeStart(fitted, page, true, githubTestsPageEnd{}, nil, Claim{}, githubTestsArtifactsPhase); start != 3 || action != githubTestsResumeAt {
		t.Fatalf("an artifacts cursor without its anchor resumed at %d (action %d), want 3 by position", start, action)
	}
}

// acp r1 ruling P1-1: a runs cursor written by the 896-byte writer (no anchor, an 890-byte carried list) decodes and
// resumes; the list is still the skip source of its cut second.
func TestGitHubTestsResumeA896ByteWriterCutListOf890BytesResumes(t *testing.T) {
	claim := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	cut := listingSince.Add(3 * time.Hour)
	keys := make([]string, 0, 74)
	for index := range 73 {
		keys = append(keys, strconv.Itoa(90000000000+index))
	}
	keys = append(keys, "12345678901234")
	list := strings.Join(keys, ",")
	if len(list) != 890 || len(list) <= githubTestsMaxRunsCutKeysBytes {
		t.Fatalf("list is %d bytes, want 890 (over the %d-byte write cap)", len(list), githubTestsMaxRunsCutKeysBytes)
	}
	cursor, err := decodeGitHubTestsChunkCursor("")
	if err != nil {
		t.Fatal(err)
	}
	cursor.Repo, cursor.RangeHi, cursor.CarriedCutKeys = "acme/api", cut.Format(time.RFC3339), list
	raw, err := encodeGitHubTestsChunkCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	runs := makeListingRuns(1, 120, listingSince.Add(time.Minute), time.Minute)
	runs = append(runs, listingRun{id: 90000000000, created: cut, updated: cut.Add(time.Minute), status: "completed", conclusion: "success"},
		listingRun{id: 777, created: cut, updated: cut.Add(time.Minute), status: "completed", conclusion: "success"})
	fake := newListingFake(t, runs)
	client := githubTestsClient(t, fakehttp.Client(fake))
	finals := 0
	collectErr := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), raw, func(emission ChunkRouteEmission) error {
			if emission.Final {
				finals++
			}
			return nil
		})
	if collectErr != nil || finals != 1 {
		t.Fatalf("a cursor with an 890-byte carried list: err=%v finals=%d, want it to resume to the final emission", collectErr, finals)
	}
	if fake.jobGets[90000000000] != 0 || fake.jobGets[777] != 1 || len(fake.jobGets) != 121 {
		t.Fatalf("carried run reads=%d, other cut-second run reads=%d, runs read=%d; want 0, 1 and 121", fake.jobGets[90000000000], fake.jobGets[777], len(fake.jobGets))
	}
}

// acp r1 ruling P1-3: an artifacts cursor without an anchor resumes by position, so 150 of 150 unreadable archives still
// fail the unit with the all-unreadable error, as before (a re-seek there would make the counters unknown).
func TestGitHubTestsResumeArtifactsCursorWithoutAnchorKeepsTheAllUnreadableGate(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		doer := &githubTestsAllUnreadableDoer{t: t, runs: 150}
		client := githubTestsClient(t, fakehttp.Client(doer))
		claim := nativeTestClaim("github", "cicd")
		resume, yielded := "", false
		stop := errors.New("yield")
		var collectErr error
		for pass := 0; pass < 10; pass++ {
			collectErr = GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
				time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume, func(emission ChunkRouteEmission) error {
					resume = emission.CursorAfter
					after, err := decodeGitHubTestsChunkCursor(emission.CursorAfter)
					if err != nil {
						t.Fatal(err)
					}
					if !yielded && after.Phase == githubTestsArtifactsPhase && after.Index == 50 {
						yielded = true
						if legacy {
							after.AnchorID = ""
							if resume, err = encodeGitHubTestsChunkCursor(after); err != nil {
								t.Fatal(err)
							}
						}
						return stop
					}
					return nil
				})
			if !errors.Is(collectErr, stop) {
				break
			}
		}
		if !yielded {
			t.Fatal("the walk never yielded inside the artifacts page")
		}
		if !errors.Is(collectErr, ErrGitHubTestsAllArtifactsUnreadable) || !strings.Contains(collectErr.Error(), "seen=150") ||
			!strings.Contains(collectErr.Error(), "unreadable=150") || doer.archiveRequests != 150 {
			t.Fatalf("legacy=%v: err=%v archive requests=%d, want the all-unreadable error at seen=150 unreadable=150 and 150 requests", legacy, collectErr, doer.archiveRequests)
		}
	}
}

// acp r1 ruling P1-2, the invariant: a resume over a moved page costs no budget. A walk that resumes over a deletion before
// its resume point and then stops at a budget edge (page budget or record bound) reads every surviving run an uninterrupted
// walk of the same runs reads under the same budget, and advances the watermark past an unread run only when that
// uninterrupted walk does too (the deep listings' stop advances by design). The re-seek's first pages and its re-read runs
// are free.
func TestGitHubTestsResumeAtABudgetEdgeReadsWhatAFreshWalkReads(t *testing.T) {
	window := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	deep := lateRerunMidnightClaim("incremental")
	windowRuns := func() []listingRun { return makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Minute) }
	deepRuns := func() []listingRun { return lateRerunRuns(1, 250, deep.SinceAt.UTC()) }
	for _, listing := range []struct {
		name      string
		claim     Claim
		runs      func() []listingRun
		artifacts bool
		in        func(githubTestsChunkCursor) bool
		budgets   []GitHubTestsRouteHandler
	}{
		{"runs", window, windowRuns, false, func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && !c.LateRuns },
			[]GitHubTestsRouteHandler{{MaxRuns: 300}, {MaxRuns: 200}, {MaxWalkRecords: 120}, {MaxWalkRecords: 230}}},
		{"deep", deep, deepRuns, false, func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && c.LateRuns },
			[]GitHubTestsRouteHandler{{LateRerunMaxPages: 3}, {LateRerunMaxPages: 2}, {MaxWalkRecords: 120}, {MaxWalkRecords: 230}}},
		{"artifacts", window, windowRuns, true, func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsArtifactsPhase && c.Relist == 0 },
			[]GitHubTestsRouteHandler{{MaxRuns: 300}, {MaxRuns: 200}}},
	} {
		for _, handler := range listing.budgets {
			for _, items := range []int{50, 100, 150} {
				name := fmt.Sprintf("%s/runs=%d,pages=%d,records=%d/yield=%d", listing.name, handler.MaxRuns, handler.LateRerunMaxPages, handler.MaxWalkRecords, items)
				t.Run(name, func(t *testing.T) {
					reads := func(f *listingFake) map[int]int {
						if listing.artifacts {
							return f.artifactGets
						}
						return f.jobGets
					}
					original := newListingFake(t, listing.runs())
					uninterrupted := walkListing(t, handler, listing.claim, original, 0)
					if len(reads(original)) <= items {
						t.Skipf("the uninterrupted walk reads %d runs: it stops before the resume point", len(reads(original)))
					}
					fake := newListingFake(t, listing.runs())
					deleted := 0
					walk := resumeDeletionWalk(t, handler, listing.claim, fake, func(c githubTestsChunkCursor) bool {
						return listing.in(c) && c.RangeItems == items
					}, func(githubTestsChunkCursor) {
						consumed := resumeDeletionConsumed(reads(fake))
						sort.Ints(consumed)
						deleted = consumed[len(consumed)-1]
						resumeDeletionDelete(fake, deleted)
					}, nil)
					survivors := newListingFake(t, fake.runs)
					walkListing(t, GitHubTestsRouteHandler{}, listing.claim, survivors, 0)
					var unread, unreadSurvivors []int
					for id := range reads(original) {
						if id != deleted && reads(fake)[id] == 0 {
							unread = append(unread, id)
						}
					}
					for id := range reads(survivors) {
						if reads(fake)[id] == 0 {
							unreadSurvivors = append(unreadSurvivors, id)
						}
					}
					// The listing after the resumed one starts its own range: a re-seek's range end must not narrow it.
					for id := range original.artifactGets {
						if !listing.artifacts && id != deleted && fake.artifactGets[id] == 0 {
							unread = append(unread, -id)
						}
					}
					sort.Ints(unread)
					advancedPastUnread := walk.final.Watermark != nil && len(unreadSurvivors) > 0 && uninterrupted.final.Watermark == nil
					if len(unread) != 0 || advancedPastUnread {
						t.Errorf("resumed over a deletion: unread=%v watermark=%v unread_survivors=%d; uninterrupted walk %v watermark=%v",
							unread, walk.final.Watermark != nil, len(unreadSurvivors), uninterrupted.incompleteComponents(), uninterrupted.final.Watermark != nil)
					}
				})
			}
		}
	}
}

// acp r1 ruling P2-2: a page-end cursor names the next page by its own (short) URL at index 0, so a long Link of a finished
// page does not grow the page-end cursor: a walk at the byte budget's edge completes, in the runs and the artifacts listing.
func TestGitHubTestsResumePageEndCursorAfterALongLinkFitsAndCompletes(t *testing.T) {
	claim := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	for _, phase := range []string{githubTestsRunsPhase, githubTestsArtifactsPhase} {
		t.Run(phase, func(t *testing.T) {
			// The runs listing's created range starts at a time, the artifacts listing's at a date.
			padded := func(request *http.Request) bool {
				lo, _, _ := strings.Cut(request.URL.Query().Get("created"), "..")
				return strings.Contains(lo, "T") == (phase == githubTestsRunsPhase)
			}
			walk := func(linkBytes int) (int, error, *listingFake) {
				fake := newListingFake(t, makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Minute))
				doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
					response, err := fake.Do(request)
					if err != nil || request.URL.Path != "/repos/acme/api/actions/runs" || request.URL.Query().Get("page") != "" || !padded(request) {
						return response, err
					}
					if link := response.Header.Get("Link"); link != "" {
						target, _, _ := strings.Cut(strings.TrimPrefix(link, "<"), ">")
						if len(target)+len("&pad=") < linkBytes {
							target += "&pad=" + strings.Repeat("a", linkBytes-len(target)-len("&pad="))
						}
						response.Header.Set("Link", "<"+target+">; rel=\"next\"")
					}
					return response, nil
				})
				overhead := 0
				err := GitHubTestsRouteHandler{}.CollectChunks(t.Context(), claim, providerfoundation.Credential{}, githubTestsClient(t, fakehttp.Client(doer)),
					time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "", func(emission ChunkRouteEmission) error {
						cursor, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
						if decodeErr != nil {
							return decodeErr
						}
						// The widest mid-page cursor on the long-URL page, with every droppable field dropped, sets the
						// edge; a page-end cursor must then still fit.
						if cursor.Phase != phase || !strings.Contains(cursor.NextURL, "pad=") || cursor.Index == 0 || cursor.Index >= 100 {
							return nil
						}
						cursor.ActiveCutKeys, cursor.CarriedCutKeys, cursor.RangeOldestItems, cursor.AnchorID = "", "", 0, ""
						plain, encodeErr := encodeGitHubTestsChunkCursor(cursor)
						if encodeErr != nil {
							return encodeErr
						}
						overhead = max(overhead, len(plain)-len(cursor.NextURL))
						return nil
					})
				return overhead, err, fake
			}
			overhead, err, _ := walk(1000)
			if err != nil || overhead == 0 {
				t.Fatalf("premise: no %s cursor carried the long page-2 URL (overhead=%d err=%v)", phase, overhead, err)
			}
			linkBytes := maxChunkCursorBytes - overhead
			_, err, fake := walk(linkBytes)
			if err != nil || len(fake.jobGets) != 250 || len(fake.artifactGets) != 250 {
				t.Fatalf("a %d-byte page-2 link: err=%v, runs read jobs=%d artifacts=%d; want the walk to complete with 250 each", linkBytes, err, len(fake.jobGets), len(fake.artifactGets))
			}
			t.Logf("%s: link_bytes=%d overhead=%d", phase, linkBytes, overhead)
		})
	}
}

// resumeReportsDoer serves every run of a listingFake with one JUnit report artifact.
type resumeReportsDoer struct {
	fake    *listingFake
	archive []byte
}

func (doer resumeReportsDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	header := http.Header{"Content-Type": {"application/json"}}
	switch {
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/artifacts"):
		if _, err := doer.fake.Do(request); err != nil {
			return nil, err
		}
		id := strings.Split(path, "/")[6]
		return doer.fake.respond(request, header, fmt.Sprintf(`{"artifacts":[{"id":%s,"name":"junit","size_in_bytes":%d,"archive_download_url":"https://api.github.com/repos/acme/api/actions/artifacts/%s/zip"}]}`, id, len(doer.archive), id), 200), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(bytes.NewReader(doer.archive)), Request: request}, nil
	}
	return doer.fake.Do(request)
}

// acp r2 close path (a): a page-end resume whose page allowance is already spent checks its anchor on the page before it
// first; on a move it seeks the listing again with its free pages, so the walk reads every survivor an uninterrupted
// walk of the survivors reads under the same budget.
func TestGitHubTestsResumePageEndAtASpentBudgetChecksTheAnchor(t *testing.T) {
	window := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	deep := lateRerunMidnightClaim("incremental")
	for _, listing := range []struct {
		name      string
		claim     Claim
		runs      []listingRun
		artifacts bool
		in        func(githubTestsChunkCursor) bool
		handler   GitHubTestsRouteHandler
	}{
		{"runs", window, makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Minute), false,
			func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && !c.LateRuns }, GitHubTestsRouteHandler{MaxRuns: 200}},
		{"deep", deep, lateRerunRuns(1, 250, deep.SinceAt.UTC()), false,
			func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && c.LateRuns }, GitHubTestsRouteHandler{LateRerunMaxPages: 2}},
		{"artifacts", window, makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Minute), true,
			func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsArtifactsPhase && c.Relist == 0 }, GitHubTestsRouteHandler{MaxRuns: 200}},
	} {
		t.Run(listing.name, func(t *testing.T) {
			reads := func(f *listingFake) map[int]int {
				if listing.artifacts {
					return f.artifactGets
				}
				return f.jobGets
			}
			fake := newListingFake(t, slices.Clone(listing.runs))
			walk := resumeDeletionWalk(t, listing.handler, listing.claim, fake, func(c githubTestsChunkCursor) bool {
				return listing.in(c) && c.RangeItems == 200 && c.Index == 0
			}, func(githubTestsChunkCursor) {
				consumed := resumeDeletionConsumed(reads(fake))
				sort.Ints(consumed)
				resumeDeletionDelete(fake, consumed[len(consumed)-61:]...)
			}, nil)
			survivors := newListingFake(t, fake.runs)
			fresh := walkListing(t, listing.handler, listing.claim, survivors, 0)
			var unread []int
			for id := range reads(survivors) {
				if reads(fake)[id] == 0 {
					unread = append(unread, id)
				}
			}
			sort.Ints(unread)
			t.Logf("unread=%d fresh_reads=%d watermark=%v fresh_watermark=%v", len(unread), len(reads(survivors)), walk.final.Watermark != nil, fresh.final.Watermark != nil)
			if len(unread) != 0 || !listing.artifacts && (walk.final.Watermark != nil) != (fresh.final.Watermark != nil) {
				t.Errorf("a page-end resume at a spent budget: unread survivors=%v watermark=%v; the uninterrupted walk of the survivors read them (watermark=%v)",
					unread, walk.final.Watermark != nil, fresh.final.Watermark != nil)
			}
		})
	}
}

// acp r2 close path (new): the re-seek's record credit belongs to the listing that re-sought. A runs or deep listing that
// re-reads a run of its re-seek second gives the next listing no credit: that listing's report rows of the second are first
// reads and are charged, so it stops at the record bound where the uninterrupted walk stops.
func TestGitHubTestsResumeReseekCreditStaysInItsListing(t *testing.T) {
	archive := githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture})
	for _, cell := range []struct {
		name   string
		deep   bool
		offset time.Duration
		cap    int
	}{
		{"runs_to_artifacts", false, 0, 26},
		{"deep_runs_to_artifacts", true, -12 * time.Hour, 15},
		{"deep_runs_to_relist", true, 0, 15},
	} {
		t.Run(cell.name, func(t *testing.T) {
			claim := listingClaim(listingSince, listingSince.Add(6*time.Hour))
			runs := makeListingRuns(1, 12, listingSince.Add(time.Minute), time.Second)
			if cell.deep {
				claim = lateRerunMidnightClaim("incremental")
				since := claim.SinceAt.UTC()
				runs = lateRerunRuns(1, 12, since)
				for index := range runs {
					runs[index].created = since.Add(-72*time.Hour + time.Duration(index)*time.Second)
					if cell.offset != 0 {
						runs[index].created = since.Add(cell.offset + time.Duration(index)*time.Second)
					}
				}
			}
			in := func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && c.LateRuns == cell.deep }
			handler := GitHubTestsRouteHandler{MaxWalkRecords: cell.cap}
			fake := newListingFake(t, runs)
			resumeDeletionWalkDoer(t, handler, claim, resumeReportsDoer{fake, archive}, func(c githubTestsChunkCursor) bool {
				return in(c) && c.RangeItems == 6
			}, func(githubTestsChunkCursor) { resumeDeletionDelete(fake, 12) }, nil)
			fresh := newListingFake(t, fake.runs)
			client := githubTestsClient(t, fakehttp.Client(resumeReportsDoer{fresh, archive}))
			if err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
				time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "", func(ChunkRouteEmission) error { return nil }); err != nil {
				t.Fatal(err)
			}
			t.Logf("cap=%d anchor_jobs=%d anchor_report_reads=%d run6_reports=%d fresh_run7_reports=%d fresh_run6_reports=%d",
				cell.cap, fake.jobGets[7], fake.artifactGets[7], fake.artifactGets[6], fresh.artifactGets[7], fresh.artifactGets[6])
			if fake.jobGets[7] != 2 || fake.artifactGets[7] != 1 || fresh.artifactGets[7] != 1 || fresh.artifactGets[6] != 0 {
				t.Fatal("premise: the re-read run's first report read must sit at the record bound's edge")
			}
			if fake.artifactGets[6] != 0 {
				t.Errorf("run 6's reports were read past the record bound on credit earned by the %s listing", map[bool]string{false: "runs", true: "deep"}[cell.deep])
			}
		})
	}
}

// A boundary second that spans more than one page: the re-seek gets as many free pages as the moved search entered
// (CHAOS-14385), so a walk at the page budget edge reads every survivor an uninterrupted walk of the survivors reads.
func TestGitHubTestsResumeMultiPageBoundarySecondReadsEverySurvivor(t *testing.T) {
	claim := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	runs := makeListingRuns(1, 250, listingSince.Add(time.Hour), time.Second)
	for index := range 150 { // ids 1..150 share one second: the second spans pages 2 and 3
		runs[index].created, runs[index].updated = listingSince.Add(30*time.Minute), listingSince.Add(31*time.Minute)
	}
	handler := GitHubTestsRouteHandler{MaxRuns: 200}
	fake := newListingFake(t, runs)
	resumeDeletionWalk(t, handler, claim, fake, func(c githubTestsChunkCursor) bool {
		return c.Phase == githubTestsRunsPhase && c.RangeItems == 150
	}, func(githubTestsChunkCursor) { resumeDeletionDelete(fake, 250) }, nil)
	survivors := newListingFake(t, fake.runs)
	walkListing(t, handler, claim, survivors, 0)
	skipped := 0
	for id := range survivors.jobGets {
		if fake.jobGets[id] == 0 {
			skipped++
		}
	}
	if skipped != 0 {
		t.Errorf("multi-page boundary second: %d survivors an uninterrupted walk of the survivors reads were not read, want 0", skipped)
	}
}

// The re-seek's record credit is in the cursor (CHAOS-14385): a worker yield inside the re-seek's second keeps it, so the
// re-read runs after the yield are not charged to the record bound and no run an uninterrupted walk reads is skipped.
func TestGitHubTestsResumeRecordCreditSurvivesYields(t *testing.T) {
	claim := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	runs := makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Minute)
	for index := range runs { // five runs share each second
		runs[index].created = listingSince.Add(time.Minute + time.Duration(index/5)*time.Second)
		runs[index].updated = runs[index].created.Add(time.Minute)
	}
	handler := GitHubTestsRouteHandler{MaxWalkRecords: 230}
	original := newListingFake(t, slices.Clone(runs))
	walkListing(t, handler, claim, original, 0)
	fake := newListingFake(t, runs)
	client := githubTestsClient(t, fakehttp.Client(fake))
	resume, phaseYields := "", 0
	stop := errors.New("yield")
	for pass := 0; pass < 100; pass++ {
		final := false
		err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
			time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume, func(emission ChunkRouteEmission) error {
				resume = emission.CursorAfter
				if emission.Final {
					final = true
					return nil
				}
				after, _ := decodeGitHubTestsChunkCursor(emission.CursorAfter)
				switch {
				case phaseYields == 0 && after.Phase == githubTestsRunsPhase && after.RangeItems == 52:
					phaseYields = 1
					resumeDeletionDelete(fake, 250)
					return stop
				case phaseYields == 1 && after.Phase == githubTestsRunsPhase:
					phaseYields = 2 // one re-read run of the re-seek second, then a worker yield
					return stop
				}
				return nil
			})
		if final {
			break
		}
		if !errors.Is(err, stop) {
			t.Fatal(err)
		}
	}
	skipped := 0
	for id := range original.jobGets {
		if id != 250 && fake.jobGets[id] == 0 {
			skipped++
		}
	}
	if skipped != 0 {
		t.Errorf("record credit across yields: %d runs the uninterrupted walk reads were not read, want 0", skipped)
	}
}
