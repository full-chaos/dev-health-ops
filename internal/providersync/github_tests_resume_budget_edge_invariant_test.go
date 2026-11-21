package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/google/uuid"
)

// budgetEdgeWalk drives CollectChunks to its final emission. yield sees every non-final after cursor; true ends the pass
// there (a worker yield), and the next pass resumes from that cursor, first passed through rewrite when it is set. yield
// may change the fake (GitHub between chunks). It returns the walk and every cursor the walk emitted.
func budgetEdgeWalk(
	t *testing.T, handler GitHubTestsRouteHandler, claim Claim, fake *listingFake,
	yield func(after githubTestsChunkCursor) bool, rewrite func(string) string,
) (listingWalk, []string) {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(fake))
	walk, resume, stop := listingWalk{}, "", errors.New("yield")
	var emitted []string
	for pass := 1; pass < 1000; pass++ {
		final := false
		err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
			time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume, func(emission ChunkRouteEmission) error {
				if emission.Barrier { // the executor's flush request (CoalesceRuns); it carries no cursor
					return nil
				}
				resume = emission.CursorAfter
				emitted = append(emitted, emission.CursorAfter)
				if emission.Final {
					walk.final, final = emission.Batch, true
					return nil
				}
				after, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if yield(after) {
					return stop
				}
				return nil
			})
		if final {
			cursor, decodeErr := decodeGitHubTestsChunkCursor(resume)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			walk.cursor, walk.passes = cursor, pass
			return walk, emitted
		}
		if !errors.Is(err, stop) {
			t.Fatalf("pass %d err=%v, want a yield", pass, err)
		}
		if rewrite != nil {
			resume = rewrite(resume)
		}
	}
	t.Fatal("the walk never reached a final emission")
	return walk, nil
}

// coalesceFlags are both values of GitHubTestsRouteHandler.CoalesceRuns (CHAOS-14408): off is the shipped default, so it
// comes first; every budget-edge cell runs with each. Under -race only off runs (the race legs' time budget, as
// cutDifferentialRace); the plain test legs run both.
var coalesceFlags = func() []bool {
	if cutDifferentialRace {
		return []bool{false}
	}
	return []bool{false, true}
}()

func coalesceName(coalesce bool) string {
	if coalesce {
		return "coalesce_on"
	}
	return "coalesce_off"
}

// budgetEdgeStrip is the cursor an older binary writes: no re-seek pages, no record credit.
func budgetEdgeStrip(t *testing.T) func(string) string {
	return func(raw string) string {
		fields := map[string]json.RawMessage{}
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			t.Fatal(err)
		}
		delete(fields, "rp")
		delete(fields, "rc")
		out, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		return string(out)
	}
}

// budgetEdgeListing is one listing of the budget-edge cells: the claim, which cursor is in it, which reads it makes, its page
// budget, and its fixtures (ids 1..150 in one created second; or five runs per second).
type budgetEdgeListing struct {
	name       string
	claim      Claim
	in         func(githubTestsChunkCursor) bool
	artifacts  bool
	pageBudget GitHubTestsRouteHandler
	multiPage  func() []listingRun
	fivePerSec func() []listingRun
}

func (listing budgetEdgeListing) reads(fake *listingFake) map[int]int {
	if listing.artifacts {
		return fake.artifactGets
	}
	return fake.jobGets
}

func budgetEdgeListings() []budgetEdgeListing {
	window := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	deep := lateRerunMidnightClaim("incremental")
	deepSince := deep.SinceAt.UTC()
	windowMultiPage := func() []listingRun {
		runs := makeListingRuns(1, 250, listingSince.Add(time.Hour), time.Second)
		for index := range 150 { // ids 1..150 share one second: the second spans two pages
			runs[index].created, runs[index].updated = listingSince.Add(30*time.Minute), listingSince.Add(31*time.Minute)
		}
		return runs
	}
	windowFive := func() []listingRun {
		runs := makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Minute)
		for index := range runs {
			runs[index].created = listingSince.Add(time.Minute + time.Duration(index/5)*time.Second)
			runs[index].updated = runs[index].created.Add(time.Minute)
		}
		return runs
	}
	return []budgetEdgeListing{
		{"runs", window, func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && !c.LateRuns }, false,
			GitHubTestsRouteHandler{MaxRuns: 200}, windowMultiPage, windowFive},
		{"deep", deep, func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && c.LateRuns }, false,
			GitHubTestsRouteHandler{LateRerunMaxPages: 2},
			func() []listingRun {
				runs := lateRerunRuns(1, 250, deepSince)
				for index := range 150 {
					runs[index].created = deepSince.Add(-80 * time.Hour)
				}
				return runs
			},
			func() []listingRun {
				runs := lateRerunRuns(1, 250, deepSince)
				for index := range runs {
					runs[index].created = deepSince.Add(-72*time.Hour + time.Duration(index/5)*time.Second)
				}
				return runs
			}},
		{"artifacts", window, func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsArtifactsPhase && c.Relist == 0 }, true,
			GitHubTestsRouteHandler{MaxRuns: 200}, windowMultiPage, nil},
	}
}

// budgetEdgeShape is one way a walk meets its budget edge after a re-seek. The first yield deletes run 250 (before the
// resume point); then is the second yield (or nil), and then, when set, the third (moved_twice deletes another run there).
type budgetEdgeShape struct {
	name    string
	five    bool // the five-runs-per-second fixture and the record bound
	yieldAt int
	then    func(after githubTestsChunkCursor) bool
	twice   bool
}

func budgetEdgeShapes() []budgetEdgeShape {
	return []budgetEdgeShape{
		{"multi_page", false, 150, nil, false},
		// a yield at the end of the re-seek's first page: the next page is the re-seek's second free page
		{"multi_page_yield_at_page_end", false, 150, func(c githubTestsChunkCursor) bool { return c.RangeItems == 100 && c.Index == 0 }, false},
		// a second deletion before the anchor inside the re-seek's first page: a re-seek of a re-seek keeps its free pages
		{"multi_page_moved_twice", false, 150, func(c githubTestsChunkCursor) bool { return c.RangeItems == 30 }, true},
		{"two_yields_inside", true, 52, func(githubTestsChunkCursor) bool { return true }, false},
		{"two_yields_after", true, 52, func(c githubTestsChunkCursor) bool { return c.RangeOldest != "" && c.RangeOldest < c.RangeHi }, false},
	}
}

// budgetEdgeRun walks one listing and shape with run 250 deleted at the first yield, then walks the survivors uninterrupted
// under the same budget. It returns the survivors the resumed walk did not read, the runs it read that the survivors walk
// did not (250 aside), and both walks.
func budgetEdgeRun(
	t *testing.T, listing budgetEdgeListing, shape budgetEdgeShape, coalesce bool, rewrite func(string) string,
) (unread, extra []int, walk, fresh listingWalk) {
	t.Helper()
	handler := listing.pageBudget
	runs := listing.multiPage()
	if shape.five {
		handler, runs = GitHubTestsRouteHandler{MaxWalkRecords: 230}, listing.fivePerSec()
	}
	handler.CoalesceRuns = coalesce
	fake := newListingFake(t, runs)
	first, second := false, false
	walk, _ = budgetEdgeWalk(t, handler, listing.claim, fake, func(after githubTestsChunkCursor) bool {
		switch {
		case !first && listing.in(after) && after.RangeItems == shape.yieldAt:
			first = true
			resumeDeletionDelete(fake, 250)
			return true
		case first && !second && shape.then != nil && listing.in(after) && shape.then(after):
			second = true
			if shape.twice {
				resumeDeletionDelete(fake, 140)
			}
			return true
		}
		return false
	}, rewrite)
	if !first || shape.then != nil && !second {
		t.Fatalf("premise: yields first=%v second=%v", first, second)
	}
	survivors := newListingFake(t, slices.Clone(fake.runs))
	fresh = walkListing(t, handler, listing.claim, survivors, 0)
	for id := range listing.reads(survivors) {
		if listing.reads(fake)[id] == 0 {
			unread = append(unread, id)
		}
	}
	for id := range listing.reads(fake) {
		if id != 250 && listing.reads(survivors)[id] == 0 {
			extra = append(extra, id)
		}
	}
	sort.Ints(unread)
	sort.Ints(extra)
	return unread, extra, walk, fresh
}

// The invariant (CHAOS-14385): a walk that re-sought its listing after a run was deleted before its resume point, and then
// stops at a budget edge (page budget or record bound), reads every run an uninterrupted walk of the surviving runs reads
// under the same budget. So its watermark never advances past an unread run. Shapes: the re-sought boundary second spans
// two pages (with a yield at the re-seek's page end, and with a second move inside the re-seek); a second worker yield inside,
// or right after, the boundary second at the record bound. The over-read of the record shapes stays inside the boundary
// second's five runs.
func TestGitHubTestsResumeBudgetEdgeNeverAdvancesPastAnUnreadRun(t *testing.T) {
	for _, coalesce := range coalesceFlags {
		for _, listing := range budgetEdgeListings() {
			for _, shape := range budgetEdgeShapes() {
				if shape.five && listing.fivePerSec == nil {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%s", coalesceName(coalesce), listing.name, shape.name), func(t *testing.T) {
					unread, extra, walk, fresh := budgetEdgeRun(t, listing, shape, coalesce, nil)
					t.Logf("unread_survivors=%d first_unread=%v extra=%d resumed_watermark=%v fresh_watermark=%v passes=%d",
						len(unread), unread[:min(3, len(unread))], len(extra), walk.final.Watermark != nil, fresh.final.Watermark != nil, walk.passes)
					if len(unread) != 0 {
						t.Errorf("the resumed walk skipped %d runs the uninterrupted walk of the survivors reads (watermark advanced=%v)",
							len(unread), walk.final.Watermark != nil)
					}
					// The re-seek's free pages end with its range: the artifacts listing after it pays its pages as fresh.
					if !listing.artifacts && walk.cursor.ArtifactPages != fresh.cursor.ArtifactPages {
						t.Errorf("the artifacts listing after the re-sought one charged %d pages, the uninterrupted walk %d", walk.cursor.ArtifactPages, fresh.cursor.ArtifactPages)
					}
					if shape.five && len(extra) > 5 {
						t.Errorf("the record credit read %d runs past the uninterrupted walk, want at most the boundary second's 5: %v", len(extra), extra)
					}
				})
			}
		}
	}
}

// The re-seek's free pages are the pages the moved search entered, no more: 350 runs, ids 101..250 in one second, a page
// budget of 2. The move at 150 runs listed gives 2 free pages, so the re-seek reads down to run 51 and not run 50.
func TestGitHubTestsResumeReseekFreePagesAreBounded(t *testing.T) {
	for _, coalesce := range coalesceFlags {
		for _, listing := range budgetEdgeListings() {
			t.Run(coalesceName(coalesce)+"/"+listing.name, func(t *testing.T) {
				var runs []listingRun
				if listing.name == "deep" {
					since := listing.claim.SinceAt.UTC()
					runs = lateRerunRuns(1, 350, since)
					for index := 100; index < 250; index++ {
						runs[index].created = since.Add(-80 * time.Hour)
					}
					for index := range 100 {
						runs[index].created = since.Add(-80*time.Hour - time.Duration(100-index)*time.Second)
					}
				} else {
					runs = makeListingRuns(1, 350, listingSince.Add(time.Hour), time.Second)
					for index := 100; index < 250; index++ {
						runs[index].created, runs[index].updated = listingSince.Add(30*time.Minute), listingSince.Add(31*time.Minute)
					}
					for index := range 100 {
						runs[index].created = listingSince.Add(10*time.Minute + time.Duration(index)*time.Second)
						runs[index].updated = runs[index].created.Add(time.Minute)
					}
				}
				fake := newListingFake(t, runs)
				moved := false
				handler := listing.pageBudget
				handler.CoalesceRuns = coalesce
				budgetEdgeWalk(t, handler, listing.claim, fake, func(after githubTestsChunkCursor) bool {
					if !moved && listing.in(after) && after.RangeItems == 150 {
						moved = true
						resumeDeletionDelete(fake, 350)
						return true
					}
					return false
				}, nil)
				reads := listing.reads(fake)
				t.Logf("run51=%d run50=%d", reads[51], reads[50])
				if !moved || reads[51] == 0 || reads[50] != 0 {
					t.Errorf("moved=%v run 51 read %d times, run 50 read %d times; want run 51 read (2 free pages) and run 50 not (no third)", moved, reads[51], reads[50])
				}
			})
		}
	}
}

// Without a move a walk with the same yields reads and charges exactly what an uninterrupted walk does, and its cursors
// carry no re-seek pages and no record credit (the page count pe is the one field every mid-range cursor gains).
func TestGitHubTestsResumeWithoutAMoveChargesWhatAFreshWalkCharges(t *testing.T) {
	for _, coalesce := range coalesceFlags {
		for _, listing := range budgetEdgeListings() {
			for _, shape := range budgetEdgeShapes() {
				if shape.five && listing.fivePerSec == nil || shape.twice {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%s", coalesceName(coalesce), listing.name, shape.name), func(t *testing.T) {
					handler, runs := listing.pageBudget, listing.multiPage()
					if shape.five {
						handler, runs = GitHubTestsRouteHandler{MaxWalkRecords: 230}, listing.fivePerSec()
					}
					handler.CoalesceRuns = coalesce
					fake := newListingFake(t, slices.Clone(runs))
					yields := 0
					walk, emitted := budgetEdgeWalk(t, handler, listing.claim, fake, func(after githubTestsChunkCursor) bool {
						if listing.in(after) && after.RangeItems == shape.yieldAt && yields == 0 {
							yields++
							return true
						}
						return false
					}, nil)
					original := newListingFake(t, runs)
					fresh := walkListing(t, handler, listing.claim, original, 0)
					if yields != 1 || !maps.Equal(listing.reads(fake), listing.reads(original)) ||
						walk.cursor.RunPages != fresh.cursor.RunPages || walk.cursor.LateRunPages != fresh.cursor.LateRunPages ||
						walk.cursor.ArtifactPages != fresh.cursor.ArtifactPages {
						t.Errorf("yields=%d reads %d vs %d, pages run %d/%d late %d/%d artifacts %d/%d", yields,
							len(listing.reads(fake)), len(listing.reads(original)), walk.cursor.RunPages, fresh.cursor.RunPages,
							walk.cursor.LateRunPages, fresh.cursor.LateRunPages, walk.cursor.ArtifactPages, fresh.cursor.ArtifactPages)
					}
					for _, raw := range emitted {
						if strings.Contains(raw, `"rp":`) || strings.Contains(raw, `"rc":`) {
							t.Fatalf("a walk without a move wrote re-seek state: %s", raw)
						}
					}
				})
			}
		}
	}
}

// A cursor of an older binary carries no re-seek pages and no record credit (both decode as 0). A move found on its resume
// takes its free pages from the cursor's RangeItems (at least one), so it skips nothing. A cursor that an older binary wrote
// INSIDE its own re-seek resumes as a plain range: the page budget or the record bound can only stop it earlier than the
// uninterrupted walk. In the runs and artifacts listings that stop is recorded (truncation, no watermark): never a silent
// skip. The deep listing's stop advances by design: TestGitHubTestsResumeOlderBinaryCursorTransitionResidualSameAsMain.
func TestGitHubTestsResumeOlderBinaryCursorWithoutReseekState(t *testing.T) {
	for _, coalesce := range coalesceFlags {
		for _, listing := range budgetEdgeListings() {
			for _, shape := range budgetEdgeShapes() {
				if shape.five && listing.fivePerSec == nil || shape.twice {
					continue
				}
				t.Run(fmt.Sprintf("%s/move_on_resume/%s/%s", coalesceName(coalesce), listing.name, shape.name), func(t *testing.T) {
					move := shape
					move.then = nil
					unread, _, walk, _ := budgetEdgeRun(t, listing, move, coalesce, budgetEdgeStrip(t))
					t.Logf("unread_survivors=%d watermark=%v", len(unread), walk.final.Watermark != nil)
					if len(unread) != 0 {
						t.Errorf("an older binary's cursor at the move skipped %d survivors", len(unread))
					}
				})
				if shape.then == nil || listing.name == "deep" {
					continue
				}
				t.Run(fmt.Sprintf("%s/inside_reseek/%s/%s", coalesceName(coalesce), listing.name, shape.name), func(t *testing.T) {
					unread, _, walk, _ := budgetEdgeRun(t, listing, shape, coalesce, budgetEdgeStrip(t))
					t.Logf("unread_survivors=%d watermark=%v incomplete=%v", len(unread), walk.final.Watermark != nil, walk.incompleteComponents())
					if len(unread) != 0 && (walk.final.Watermark != nil || len(walk.cursor.Incomplete) == 0) {
						t.Errorf("an older binary's cursor inside its re-seek skipped %d survivors without a recorded stop (watermark=%v)",
							len(unread), walk.final.Watermark != nil)
					}
				})
			}
		}
	}
}

// The named transition residual (CHAOS-14385, acp ruling (a)): a deep walk that an OLDER binary re-sought and yielded
// inside, resumed by this binary, reads what main reads: its cursor carries no re-seek pages and no credit, so the deep
// stop advances the watermark past the same unread runs as main (1, 3 and 3 here). This cell fails if this binary does
// worse than main; a later fix of the residual changes it on purpose.
func TestGitHubTestsResumeOlderBinaryCursorTransitionResidualSameAsMain(t *testing.T) {
	mainUnread := map[string]int{"multi_page_yield_at_page_end": 1, "two_yields_inside": 3, "two_yields_after": 3}
	deep := budgetEdgeListings()[1]
	for _, coalesce := range coalesceFlags {
		for _, shape := range budgetEdgeShapes() {
			want, ok := mainUnread[shape.name]
			if !ok {
				continue
			}
			t.Run(coalesceName(coalesce)+"/"+shape.name, func(t *testing.T) {
				unread, _, walk, _ := budgetEdgeRun(t, deep, shape, coalesce, budgetEdgeStrip(t))
				t.Logf("unread_survivors=%d watermark=%v", len(unread), walk.final.Watermark != nil)
				if len(unread) != want || walk.final.Watermark == nil {
					t.Errorf("deep residual: unread=%d watermark=%v, want main's %d unread with the watermark advanced", len(unread), walk.final.Watermark != nil, want)
				}
			})
		}
	}
}

// A negative re-seek page count, page count or record credit is a corrupt cursor.
func TestGitHubTestsCursorRejectsNegativeReseekState(t *testing.T) {
	for _, raw := range []string{
		`{"phase":"runs","repo":"x","archives_seen":0,"archives_unreadable":0,"rp":-1}`,
		`{"phase":"runs","repo":"x","archives_seen":0,"archives_unreadable":0,"rc":-1}`,
		`{"phase":"runs","repo":"x","archives_seen":0,"archives_unreadable":0,"pe":-1}`,
	} {
		if _, err := decodeGitHubTestsChunkCursor(raw); !errors.Is(err, ErrChunkCheckpointConflict) {
			t.Errorf("%s: err=%v, want ErrChunkCheckpointConflict", raw, err)
		}
	}
}

// The invariant through the chunked executor, with the CHAOS-14408 run groups off (the shipped default: one run per group)
// and on (the attempts commit groups of several runs and resume from the group's last cursor, so the re-seek state the
// route wrote on the group's last run is the state the next attempt reads). Run 250 is deleted at the first attempt end past the yield point; a walk at the budget edge still reads
// every run the uninterrupted walk of the survivors reads.
func TestGitHubTestsResumeBudgetEdgeThroughTheChunkedExecutor(t *testing.T) {
	listings := budgetEdgeListings()
	if cutDifferentialRace {
		listings = listings[:1]
	}
	for _, coalesce := range coalesceFlags {
		for _, listing := range listings {
			for _, five := range []bool{false, true} {
				if five && listing.fivePerSec == nil {
					continue
				}
				name := fmt.Sprintf("%s/%s/%s", coalesceName(coalesce), listing.name, map[bool]string{false: "multi_page", true: "record_bound"}[five])
				t.Run(name, func(t *testing.T) {
					handler, runs, yieldAt := listing.pageBudget, listing.multiPage(), 150
					if five {
						handler, runs, yieldAt = GitHubTestsRouteHandler{MaxWalkRecords: 230}, listing.fivePerSec(), 52
					}
					handler.CoalesceRuns = coalesce
					now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
					// The cicd unit (the same route and listing as tests) over the listing's window.
					unit := nativeTestClaim("github", "cicd").Unit
					unit.SinceAt, unit.BeforeAt, unit.Mode = listing.claim.SinceAt, listing.claim.BeforeAt, listing.claim.Mode
					leases := newMemoryLeaseRepository(unit, "dispatching")
					claim, err := leases.Claim(context.Background(), ClaimRequest{UnitID: listing.claim.ID, Owner: uuid.NewString(), Now: now, LeaseDuration: 15 * time.Minute})
					if err != nil {
						t.Fatal(err)
					}
					session := &LeaseSession{Repository: leases, Claim: claim, LeaseDuration: 15 * time.Minute,
						Deadline: now.Add(48 * time.Hour), Now: func() time.Time { return now }}
					descriptor, ok := Descriptor("github", unit.Dataset)
					if !ok || !descriptor.Chunked {
						t.Fatalf("descriptor=%+v", descriptor)
					}
					descriptor.ChunkPolicy.MaxEffectRows, descriptor.ChunkPolicy.MaxChunksPerAttempt = 8, 2
					fake, sink, store := newListingFake(t, runs), newRunsGroupSink(), newChunkMemoryStore()
					executor := completeRouteExecutor(now, handler, store, sink)
					executor.Committer.Readback = sink
					executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
					executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
					executor.Doer = fakehttp.Client(fake)
					executor.BudgetLimits = map[CostClass]int{CostMedium: 100_000, CostHeavy: 100_000, CostLight: 100_000}
					moved, attempts := false, 0
					for ; attempts < 500; attempts++ {
						_, execErr := executor.Execute(context.Background(), session, descriptor)
						if execErr == nil {
							break
						}
						if !errors.As(execErr, new(ChunkContinuationError)) {
							t.Fatalf("attempt %d: %v", attempts+1, execErr)
						}
						checkpoint, loadErr := store.LoadChunkCheckpoint(context.Background(), claim, now)
						if loadErr != nil {
							t.Fatal(loadErr)
						}
						cursor, decodeErr := decodeGitHubTestsChunkCursor(checkpoint.NextCursor)
						if decodeErr != nil {
							t.Fatal(decodeErr)
						}
						if !moved && listing.in(cursor) && cursor.RangeItems >= yieldAt {
							moved = true
							resumeDeletionDelete(fake, 250)
						}
					}
					grouped := 0
					store.mu.Lock()
					for _, chunk := range store.chunks {
						for _, effect := range chunk.Effects {
							if effect.Destination == "ci_pipeline_runs" && len(effect.Rows) > 1 {
								grouped++
							}
						}
					}
					store.mu.Unlock()
					if !moved || coalesce != (grouped > 0) {
						t.Fatalf("premise: moved=%v coalesced groups=%d attempts=%d", moved, grouped, attempts)
					}
					survivors := newListingFake(t, slices.Clone(fake.runs))
					walkListing(t, handler, listing.claim, survivors, 0)
					var unread []int
					for id := range listing.reads(survivors) {
						if listing.reads(fake)[id] == 0 {
							unread = append(unread, id)
						}
					}
					sort.Ints(unread)
					t.Logf("attempts=%d coalesced_groups=%d unread_survivors=%d first=%v", attempts, grouped, len(unread), unread[:min(3, len(unread))])
					if len(unread) != 0 {
						t.Errorf("with coalesced groups the resumed walk skipped %d runs the uninterrupted walk of the survivors reads", len(unread))
					}
				})
			}
		}
	}
}

// GitHub may serve fewer runs than per_page on a page that is not the last. The re-seek's free pages count the pages the
// moved search entered, not its runs divided by 100: two 50-run pages entered before a move at run 51 give two free pages,
// so a walk at the page budget edge still reads every survivor.
func TestGitHubTestsResumeReseekCountsShortPagesItEntered(t *testing.T) {
	for _, coalesce := range coalesceFlags {
		for _, listing := range deletionShiftListings() {
			t.Run(coalesceName(coalesce)+"/"+listing.name, func(t *testing.T) {
				handler := GitHubTestsRouteHandler{MaxRuns: 200}
				if listing.deep {
					handler = GitHubTestsRouteHandler{LateRerunMaxPages: 2}
				}
				handler.CoalesceRuns = coalesce
				fake := newListingFake(t, listing.runs(1, 250))
				fake.perPage = 50
				deletionShiftWalk(t, handler, listing.claim, deletionShiftDoer(t, fake, listing.reports), func(c githubTestsChunkCursor) bool {
					return listing.in(c) && c.RangeItems == 51
				}, func(githubTestsChunkCursor) { resumeDeletionDelete(fake, 250) }, nil)
				survivors := newListingFake(t, slices.Clone(fake.runs))
				survivors.perPage = 50
				deletionShiftPlainWalk(t, handler, listing.claim, deletionShiftDoer(t, survivors, listing.reports))
				var unread []int
				for id := range listing.reads(survivors) {
					if listing.reads(fake)[id] == 0 {
						unread = append(unread, id)
					}
				}
				sort.Ints(unread)
				t.Logf("CELL short_pages/%s/%s unread=%d first=%v", coalesceName(coalesce), listing.name, len(unread), unread[:min(3, len(unread))])
				if len(unread) != 0 {
					t.Errorf("short pages: %d survivors unread, want 0", len(unread))
				}
			})
		}
	}
}

// A claim with no window edges (a full resync) lists without a created range, so GitHub's 1,000-result cap does not bound a
// search: a move after run 1,001 gives a re-seek 11 free pages. The free pages only let the re-seek list again what the
// moved search listed (an over-read, idempotent rows); the walk reads every survivor and completes.
func TestGitHubTestsResumeUnfilteredClaimReseekPagesOverReadOnly(t *testing.T) {
	for _, coalesce := range coalesceFlags {
		t.Run(coalesceName(coalesce), func(t *testing.T) { budgetEdgeUnfiltered(t, coalesce) })
	}
}

func budgetEdgeUnfiltered(t *testing.T, coalesce bool) {
	claim := nativeTestClaim("github", "tests")
	claim.SinceAt, claim.BeforeAt = nil, nil
	fake := newListingFake(t, makeListingRuns(1, 1100, listingSince.Add(time.Minute), time.Second))
	maxPages := 0
	_, emitted := budgetEdgeWalk(t, GitHubTestsRouteHandler{CoalesceRuns: coalesce}, claim, fake, func(c githubTestsChunkCursor) bool {
		if c.Phase == githubTestsRunsPhase && c.RangeItems == 1001 && len(fake.runs) == 1100 {
			resumeDeletionDelete(fake, 1100)
			return true
		}
		return false
	}, nil)
	for _, raw := range emitted {
		if cursor, err := decodeGitHubTestsChunkCursor(raw); err == nil {
			maxPages = max(maxPages, cursor.ReseekPages)
		}
	}
	survivors := newListingFake(t, slices.Clone(fake.runs))
	walkListing(t, GitHubTestsRouteHandler{CoalesceRuns: coalesce}, claim, survivors, 0)
	unread, twice := 0, 0
	for id := range survivors.jobGets {
		if fake.jobGets[id] == 0 {
			unread++
		}
		if fake.jobGets[id] > 1 {
			twice++
		}
	}
	t.Logf("CELL unfiltered rp=%d unread=%d read_again=%d read=%d", maxPages, unread, twice, len(fake.jobGets))
	if maxPages != 11 || unread != 0 {
		t.Errorf("unfiltered claim: rp=%d unread=%d, want rp 11 and no survivor unread", maxPages, unread)
	}
}
