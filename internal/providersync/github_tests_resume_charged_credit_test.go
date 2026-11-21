package providersync

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// chargedCreditWalk walks the runs listing of the window claim, yielding at each listed count in yieldAt; mutate runs at
// each yield (before the resume). It returns the last cursor and the walk's re-seeks.
func chargedCreditWalk(
	t *testing.T, handler GitHubTestsRouteHandler, fake *listingFake, yieldAt map[int]bool, mutate func(at githubTestsChunkCursor),
) githubTestsChunkCursor {
	t.Helper()
	claim := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	client := githubTestsClient(t, fakehttp.Client(fake))
	resume, stop := "", errors.New("yield")
	done := map[int]bool{}
	var last githubTestsChunkCursor
	for pass := 1; pass < 500; pass++ {
		final := false
		err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
			time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume, func(emission ChunkRouteEmission) error {
				if emission.Barrier {
					return nil
				}
				resume = emission.CursorAfter
				cursor, err := decodeGitHubTestsChunkCursor(emission.CursorAfter)
				if err != nil {
					t.Fatal(err)
				}
				last = cursor
				if emission.Final {
					final = true
					return nil
				}
				listed := len(fake.jobGets)
				if cursor.Phase == githubTestsRunsPhase && yieldAt[listed] && !done[listed] {
					done[listed] = true
					mutate(cursor)
					return stop
				}
				return nil
			})
		if final {
			return last
		}
		if !errors.Is(err, stop) {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}
	t.Fatal("the walk never reached a final emission")
	return last
}

// A move that is not a deletion earns no deletion credit (CHAOS-14434): a run that appears in the anchor's created second
// ahead of it (a higher id lists first there) moves the anchor to a HIGHER index, four times in one walk. Each move
// re-seeks, and the re-seek credit (CHAOS-14385: the runs of that second read again) is all the walk earns: the record
// credit equals main's (insertedMainCredit, the same cell on main).
const insertedMainCredit = 8

func TestGitHubTestsResumeInsertedRunMoveEarnsNoDeletionCredit(t *testing.T) {
	for _, coalesce := range coalesceFlags {
		t.Run(coalesceName(coalesce), func(t *testing.T) {
			fake := newListingFake(t, makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Minute))
			inserted := 0
			last := chargedCreditWalk(t, GitHubTestsRouteHandler{CoalesceRuns: coalesce}, fake,
				map[int]bool{30: true, 60: true, 90: true, 120: true}, func(at githubTestsChunkCursor) {
					second, err := time.Parse(time.RFC3339, at.RangeOldest)
					if err != nil {
						t.Fatal(err)
					}
					inserted++
					fake.runs = append(fake.runs, listingRun{id: 1000 + inserted, created: second, updated: second.Add(time.Minute),
						status: "completed", conclusion: "success"})
				})
			t.Logf("CELL inserted/%s moves=%d rc=%d", coalesceName(coalesce), inserted, last.RecordCredit)
			if inserted != 4 || last.RecordCredit != insertedMainCredit {
				t.Errorf("%d moves by an inserted run: record credit %d, want main's %d (no deletion credit)", inserted, last.RecordCredit, insertedMainCredit)
			}
		})
	}
}

// The deletion credit has a ceiling (CHAOS-14434): many deletions in one walk at the record bound credit at most
// maxWalkRecords/20 records in all, so the walk writes at most the bound plus that ceiling plus the re-seeks' re-reads.
// 250 runs, bound 200 (ceiling 10), at 20 yields the run listed just before the anchor (read, charged, inside the active
// range) is deleted, so each resume proves a deletion: the credit reaches the ceiling and more than main's 200 runs are
// read, at most 210.
func TestGitHubTestsResumeDeletionCreditStopsAtItsCeiling(t *testing.T) {
	for _, coalesce := range coalesceFlags {
		t.Run(coalesceName(coalesce), func(t *testing.T) {
			fake := newListingFake(t, makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Minute))
			yields := map[int]bool{}
			for listed := 10; listed <= 200; listed += 10 {
				yields[listed] = true
			}
			deleted := 0
			last := chargedCreditWalk(t, GitHubTestsRouteHandler{MaxWalkRecords: 200, CoalesceRuns: coalesce}, fake, yields, func(at githubTestsChunkCursor) {
				anchor, err := strconv.Atoi(at.AnchorID)
				if err != nil {
					t.Fatal(err)
				}
				deleted++
				resumeDeletionDelete(fake, anchor+1) // listed just before the anchor: read, charged, in the active range
			})
			read := len(fake.jobGets)
			t.Logf("CELL ceiling/%s deleted=%d read=%d rc=%d", coalesceName(coalesce), deleted, read, last.RecordCredit)
			if last.RecordCredit < 200/20 || read <= 202 || read > 200+200/20 {
				t.Errorf("%d deletions: %d runs read, record credit %d; want the credit at the ceiling 10 and 203..210 runs read (the bound 200 plus at most the ceiling)",
					deleted, read, last.RecordCredit)
			}
		})
	}
}

// chargedResumeWalk walks claim to its final emission and returns the final cursor. Each emission whose after cursor
// yieldAt accepts ends the pass; mutate changes the fake and the walk resumes from that cursor. Executor flush requests
// carry no cursor and are passed over.
func chargedResumeWalk(
	t *testing.T, handler GitHubTestsRouteHandler, claim Claim, doer providerfoundation.HTTPDoer,
	yieldAt func(githubTestsChunkCursor) bool, mutate func(githubTestsChunkCursor),
) githubTestsChunkCursor {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(doer))
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

// chargedJobsDoer serves a listingFake whose run jobs listing answers jobs[id] jobs (base for any other run): a run then
// writes 1 + its jobs records.
type chargedJobsDoer struct {
	fake *listingFake
	jobs map[int]int
	base int
}

func (doer chargedJobsDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	if !strings.HasPrefix(path, "/repos/acme/api/actions/runs/") || !strings.HasSuffix(path, "/jobs") {
		return doer.fake.Do(request)
	}
	if _, err := doer.fake.Do(request); err != nil {
		return nil, err
	}
	id, _ := strconv.Atoi(strings.Split(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/")[0])
	count, ok := doer.jobs[id]
	if !ok {
		count = doer.base
	}
	doer.fake.mu.Lock()
	defer doer.fake.mu.Unlock()
	return doer.fake.respond(request, http.Header{"Content-Type": {"application/json"}}, githubTestsJobsFixture(count), 200), nil
}

// chargedGap is the survivors the resumed walk leaves unread and the survivors it reads past the uninterrupted walk of the
// survivors (under the same handler).
type chargedGap struct {
	unread, overRead []int
	credit           int
}

func chargedGapMeasure(
	t *testing.T, handler GitHubTestsRouteHandler, claim Claim, runs []listingRun, jobs map[int]int,
	at func(githubTestsChunkCursor) bool, deleted ...int,
) chargedGap {
	t.Helper()
	fake := newListingFake(t, slices.Clone(runs))
	yielded := false
	final := chargedResumeWalk(t, handler, claim, chargedJobsDoer{fake, jobs, 1},
		func(c githubTestsChunkCursor) bool { return !yielded && at(c) },
		func(githubTestsChunkCursor) { yielded = true; resumeDeletionDelete(fake, deleted...) })
	if !yielded {
		t.Fatal("premise: the walk never reached the deletion point")
	}
	survivors := newListingFake(t, slices.Clone(fake.runs))
	chargedResumeWalk(t, handler, claim, chargedJobsDoer{survivors, jobs, 1},
		func(githubTestsChunkCursor) bool { return false }, func(githubTestsChunkCursor) {})
	gone := map[int]bool{}
	for _, id := range deleted {
		gone[id] = true
	}
	gap := chargedGap{credit: final.RecordCredit}
	for id := range survivors.jobGets {
		if fake.jobGets[id] == 0 {
			gap.unread = append(gap.unread, id)
		}
	}
	for id := range fake.jobGets {
		if !gone[id] && survivors.jobGets[id] == 0 {
			gap.overRead = append(gap.overRead, id)
		}
	}
	sort.Ints(gap.unread)
	sort.Ints(gap.overRead)
	return gap
}

// The per-run average is a residual, not exact parity: runs of 3 records (pipeline, one job, its acceptance row), a heavy
// run of 19 (9 jobs) and a light run of 1 (no job), record bound 300 (ceiling 15). Both are read before the deletion at the
// 50th run, where the average is ceil(164 / 50) = 4 records. A heavy deleted run is under-credited (4 of 19: 5 survivors
// left unread), a light one over-credited (4 of 1: 1 survivor read past the uninterrupted walk), both together
// under-credited (8 of 20: 4 unread). The record credit adds the re-seek re-read of the anchor (3). The counts are pinned so
// a change of the estimate shows here.
func TestGitHubTestsResumeDeletionCreditAverageGap(t *testing.T) {
	if cutDifferentialRace {
		t.Skip("run in the plain test legs; -race keeps the guard cells")
	}
	window := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	runs := makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Second)
	jobs := map[int]int{240: 9, 230: 0}
	for _, cell := range []struct {
		name             string
		deleted          []int
		unread, overRead int
		credit           int
	}{
		{"heavy", []int{240}, 5, 0, 4 + 3},
		{"light", []int{230}, 0, 1, 4 + 3},
		{"heavy_and_light", []int{240, 230}, 4, 0, 8 + 3},
	} {
		for _, coalesce := range deletionShiftFlags() {
			t.Run(cell.name+"/"+deletionShiftFlag(coalesce), func(t *testing.T) {
				handler := GitHubTestsRouteHandler{MaxWalkRecords: 300, CoalesceRuns: coalesce}
				gap := chargedGapMeasure(t, handler, window, runs, jobs,
					func(c githubTestsChunkCursor) bool { return c.Phase == githubTestsRunsPhase && c.RangeItems >= 50 }, cell.deleted...)
				t.Logf("CELL gap/%s/%s unread=%v over_read=%v rc=%d", cell.name, deletionShiftFlag(coalesce), gap.unread, gap.overRead, gap.credit)
				if len(gap.unread) != cell.unread || len(gap.overRead) != cell.overRead || gap.credit != cell.credit {
					t.Errorf("unread=%d over_read=%d rc=%d, want unread=%d over_read=%d rc=%d",
						len(gap.unread), len(gap.overRead), gap.credit, cell.unread, cell.overRead, cell.credit)
				}
			})
		}
	}
}

// N1: the ceiling is checked against all the record credit, the uncapped re-seek re-read credit included. Record bound 200
// (ceiling 10); the anchor's second holds 11 runs, and a run inserted ahead of the anchor makes the re-seek read 12 runs of
// it again (re-seek credit 12 >= 10). A read run deleted before the anchor later in the walk then earns no deletion
// credit: the credit grows by the second re-seek's re-read of its anchor (1) only, and the charged residual stays for
// this walk.
func TestGitHubTestsResumeDeletionCreditAfterReseekCreditAtTheCeilingIsZero(t *testing.T) {
	if cutDifferentialRace {
		t.Skip("run in the plain test legs; -race keeps the guard cells")
	}
	window := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	runs := makeListingRuns(1, 250, listingSince.Add(time.Minute), time.Second)
	for index := 214; index <= 224; index++ { // ids 215..225: the 26th..36th listed
		runs[index].created, runs[index].updated = runs[224].created, runs[224].updated
	}
	for _, coalesce := range deletionShiftFlags() {
		t.Run(deletionShiftFlag(coalesce), func(t *testing.T) {
			fake := newListingFake(t, slices.Clone(runs))
			moves := 0
			yieldAt := func(c githubTestsChunkCursor) bool {
				return moves < 2 && c.Phase == githubTestsRunsPhase && c.Index > 0 && c.RangeItems == 30 && (c.RangeHi != "") == (moves == 1)
			}
			var reseekCredit int
			mutate := func(c githubTestsChunkCursor) {
				moves++
				if moves == 1 {
					fake.runs = append(fake.runs, listingRun{id: 100_001, created: runs[224].created, updated: runs[224].updated,
						status: "completed", conclusion: "success"})
					return
				}
				reseekCredit = c.RecordCredit
				resumeDeletionDelete(fake, 214)
			}
			final := chargedResumeWalk(t, GitHubTestsRouteHandler{MaxWalkRecords: 200, CoalesceRuns: coalesce}, window, fake, yieldAt, mutate)
			t.Logf("CELL n1/%s moves=%d reseek_rc=%d rc=%d", deletionShiftFlag(coalesce), moves, reseekCredit, final.RecordCredit)
			if moves != 2 || reseekCredit < 10 {
				t.Fatalf("premise: moves=%d re-seek credit %d, want 2 moves and a re-seek credit at the ceiling 10", moves, reseekCredit)
			}
			if final.RecordCredit != reseekCredit+1 {
				t.Errorf("record credit %d after the deletion, want %d + the anchor re-read 1 (no deletion credit past the ceiling)", final.RecordCredit, reseekCredit)
			}
		})
	}
}

// Control: a resume with nothing moved earns no credit, and reads exactly the runs and records of an uninterrupted walk,
// in all four listings at their record bound.
func TestGitHubTestsResumeWithoutADeletionReadsWhatMainReads(t *testing.T) {
	if cutDifferentialRace {
		t.Skip("run in the plain test legs; -race keeps the guard cells")
	}
	for _, listing := range deletionShiftRaceListings() {
		runs := listing.runs(1, 250)
		bound := 230
		if listing.reports {
			bound = deletionShiftReportRecords(t, listing) * 130
			if !listing.deep {
				bound += 250
			}
		}
		for _, coalesce := range deletionShiftFlags() {
			t.Run(listing.name+"/"+deletionShiftFlag(coalesce), func(t *testing.T) {
				handler := GitHubTestsRouteHandler{MaxWalkRecords: bound, CoalesceRuns: coalesce}
				walk := func(yield bool) (githubTestsChunkCursor, map[int]int) {
					fake := newListingFake(t, slices.Clone(runs))
					yielded := false
					final := chargedResumeWalk(t, handler, listing.claim, deletionShiftDoer(t, fake, listing.reports),
						func(c githubTestsChunkCursor) bool { return yield && !yielded && listing.in(c) && c.RangeItems >= 50 },
						func(githubTestsChunkCursor) { yielded = true })
					if yield && !yielded {
						t.Fatal("premise: the walk never reached the yield")
					}
					return final, listing.reads(fake)
				}
				resumed, resumedReads := walk(true)
				plain, plainReads := walk(false)
				t.Logf("CELL control/%s/%s records=%d runs=%d rc=%d", listing.name, deletionShiftFlag(coalesce),
					githubTestsWalkRecords(resumed), len(resumedReads), resumed.RecordCredit)
				if resumed.RecordCredit != 0 || githubTestsWalkRecords(resumed) != githubTestsWalkRecords(plain) ||
					len(resumedReads) != len(plainReads) {
					t.Errorf("resumed records=%d runs=%d rc=%d, uninterrupted records=%d runs=%d", githubTestsWalkRecords(resumed),
						len(resumedReads), resumed.RecordCredit, githubTestsWalkRecords(plain), len(plainReads))
				}
				for id := range plainReads {
					if resumedReads[id] == 0 {
						t.Errorf("run %d read by the uninterrupted walk only", id)
					}
				}
			})
		}
	}
}
