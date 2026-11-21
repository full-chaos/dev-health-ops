package providersync

import (
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"
)

// Red cells of the listing-replay design (CHAOS-14435 + CHAOS-14461): a run deleted before the resume point changes where
// an uninterrupted walk of the surviving runs peels (its cut seconds and how many of each it lists again), so at the page
// budget edge it reads deeper. WANT: a walk that stops at its page budget leaves 0 survivors unread that the uninterrupted
// walk of the survivors reads (and so is never worse than main). Each cell logs its count; run on main they record main's.

// peelReplayRuns builds count runs one a second (deep: in the deep range), ids 1..count created ascending with the id, and
// gives one created second to the runs at newest-first positions from..to.
func peelReplayRuns(listing deletionShiftListing, count, from, to int) []listingRun {
	runs := listing.runs(1, count)
	if from == 0 {
		return runs
	}
	newest, oldest := count+1-from, count+1-to // ids at those positions
	for index := oldest - 1; index < newest; index++ {
		runs[index].created = runs[newest-1].created
		runs[index].updated = runs[newest-1].updated
	}
	return runs
}

func peelReplayHandler(listing deletionShiftListing, pages int, coalesce bool) GitHubTestsRouteHandler {
	if listing.deep {
		return GitHubTestsRouteHandler{LateRerunMaxPages: pages, CoalesceRuns: coalesce}
	}
	return GitHubTestsRouteHandler{MaxRuns: pages * 100, CoalesceRuns: coalesce}
}

func peelReplaySecondRange(c githubTestsChunkCursor) bool { return c.RangeHi != "" }

func peelReplayCell(t *testing.T, name string, outcome deletionShiftOutcome) {
	t.Helper()
	t.Logf("CELL %s %s", name, outcome)
	if len(outcome.unread) != 0 {
		t.Errorf("%s: %d survivors unread, want 0 (watermark advanced=%v)", name, len(outcome.unread), outcome.watermark)
	}
}

// peelReplayMeasure is deletionShiftMeasure with the premise that each deleted run exists and was listed before the yield:
// GitHub lists newest first and ids grow with created, so a run with an id at or above the yield cursor's anchor, or
// created in the anchor's second (that second's order is the fake's), was listed.
func peelReplayMeasure(
	t *testing.T, listing deletionShiftListing, handler GitHubTestsRouteHandler, coalesce bool, runs []listingRun, mode string,
	at func(githubTestsChunkCursor) bool, deleted ...int,
) deletionShiftOutcome {
	t.Helper()
	anchor := ""
	capture := func(c githubTestsChunkCursor) bool {
		if !at(c) {
			return false
		}
		if anchor == "" {
			anchor = c.AnchorID
		}
		return true
	}
	outcome := deletionShiftMeasure(t, listing, handler, coalesce, runs, mode, capture, deleted...)
	listed, err := strconv.Atoi(anchor)
	if err != nil {
		t.Fatalf("premise: the yield cursor has no anchor (%q)", anchor)
	}
	created := map[int]time.Time{}
	for _, run := range runs {
		created[run.id] = run.created
	}
	for _, id := range deleted {
		at, present := created[id]
		if !present || id < listed && at.Before(created[listed]) {
			t.Fatalf("premise: deleted run %d was not listed before the yield (anchor %d, present %v)", id, listed, present)
		}
	}
	return outcome
}

func peelReplaySkipRace(t *testing.T) {
	if cutDifferentialRace {
		t.Skip("run in the plain test legs (minutes of walks under -race)")
	}
}

// r2 P1 of PR 6790: the first range lists 99, 100 or 101 runs of its cut second (5 more lie past the cap); the newest run is
// deleted after the peel, so the uninterrupted walk of the survivors lists one run more of that second at the same cut.
func TestGitHubTestsPeelReplayRoundingEdge(t *testing.T) {
	peelReplaySkipRace(t)
	for _, listing := range deletionShiftListings()[:2] {
		for _, pages := range []int{11, 100} {
			for _, listed := range []int{99, 100, 101} {
				if pages == 100 && (listing.name != "runs" || listed != 100) {
					continue // plain-leg time: the shipped budget on the r2 shape of the runs listing
				}
				count := pages*100 + 400
				runs := peelReplayRuns(listing, count, 1001-listed, 1005)
				for _, coalesce := range []bool{false, true} {
					if coalesce && pages == 100 {
						continue // plain-leg time: the 100-page cell with run groups off
					}
					name := fmt.Sprintf("rounding/%s/pages=%d/listed=%d/%s", listing.name, pages, listed, deletionShiftFlag(coalesce))
					t.Run(name, func(t *testing.T) {
						outcome := peelReplayMeasure(t, listing, peelReplayHandler(listing, pages, coalesce), coalesce, runs, "route",
							func(c githubTestsChunkCursor) bool {
								return peelReplaySecondRange(c) && c.RangeItems == 2 && c.Index > 0
							}, count)
						peelReplayCell(t, name, outcome)
					})
				}
			}
		}
	}
}

// r2 "missed in round 1" of PR 6790: one run of a three-run overlap (newest-first positions 998..1,000) is deleted after the
// peel, so the uninterrupted walk's first cut moves to the next older second; enough older runs that both walks exhaust
// the page budget (11 pages, and the shipped 100 on the runs listing).
func TestGitHubTestsPeelReplayOverlapDeletionAtThePageBudget(t *testing.T) {
	peelReplaySkipRace(t)
	for _, listing := range deletionShiftListings() {
		for _, pages := range []int{11, 100} {
			if pages == 100 && listing.name != "runs" {
				continue
			}
			count := pages*100 + 400
			runs := peelReplayRuns(listing, count, 998, 1000)
			// page_end only at the shipped budget on runs: at budget 11 (4 listings) and through the executor the cell stays at 0
			// under a mutant that gives a resumed walk one page less than main, so it cannot fail; the 100-page executor
			// cells took 193 s.
			for _, mode := range []string{"midpage", "page_end"} {
				if mode == "page_end" && (listing.name != "runs" || pages != 100) {
					continue
				}
				at := func(c githubTestsChunkCursor) bool {
					return peelReplaySecondRange(c) && c.RangeItems == 2 && c.Index > 0
				}
				if mode == "page_end" {
					at = func(c githubTestsChunkCursor) bool {
						return peelReplaySecondRange(c) && c.RangeItems == 100 && c.Index == 0
					}
				}
				for _, coalesce := range []bool{false, true} {
					if coalesce && (pages == 100 || listing.reports) {
						continue // plain-leg time: the 100-page cells and the reports listings with run groups off
					}
					name := fmt.Sprintf("overlap/%s/pages=%d/%s/%s", listing.name, pages, mode, deletionShiftFlag(coalesce))
					t.Run(name, func(t *testing.T) {
						outcome := peelReplayMeasure(t, listing, peelReplayHandler(listing, pages, coalesce), coalesce, runs, "route", at, count+1-998)
						peelReplayCell(t, name, outcome)
					})
				}
			}
		}
	}
}

// r1 P1 of PR 6790: a run newer than the cut deleted after the peel (the active range's anchor does not move).
func TestGitHubTestsPeelReplayNewerDeletion(t *testing.T) {
	peelReplaySkipRace(t)
	type shape struct {
		name     string
		count    int
		from, to int // newest-first positions of one created second (0: none)
		pages    int
		deleted  []int
	}
	shapes := []shape{
		{"sparse", 1120, 0, 0, 11, []int{1100}},
		{"dense_same_cut", 1120, 951, 1050, 11, []int{1100}},
		{"dense_cut_moves", 1120, 951, 1000, 11, []int{1100}},
		{"sparse_listing_end", 1201, 0, 0, 11, []int{1181}},
		{"sparse_listing_end_two", 1201, 0, 0, 11, []int{1181, 1180}},
		{"sparse_second_peel", 2150, 0, 0, 12, []int{2130}},
		{"sparse_second_peel_two", 2150, 0, 0, 12, []int{2130, 2129}},
	}
	for _, listing := range deletionShiftListings() {
		for _, sh := range shapes {
			runs := peelReplayRuns(listing, sh.count, sh.from, sh.to)
			for _, coalesce := range []bool{false, true} {
				if coalesce && listing.reports {
					continue // plain-leg time: CoalesceRuns groups runs-phase emissions; the reports listings run it off
				}
				name := fmt.Sprintf("newer/%s/%s/%s", listing.name, sh.name, deletionShiftFlag(coalesce))
				t.Run(name, func(t *testing.T) {
					outcome := peelReplayMeasure(t, listing, peelReplayHandler(listing, sh.pages, coalesce), coalesce, runs, "route",
						func(c githubTestsChunkCursor) bool {
							return peelReplaySecondRange(c) && c.RangeItems == 2 && c.Index > 0
						}, sh.deleted...)
					peelReplayCell(t, name, outcome)
				})
			}
		}
	}
}

// The property: a deletion at any position before the resume point, against any fill of the first cut second (1, 50, 99,
// 100, 101 runs listed by the first range, 5 more past the cap), at a budget of 11 pages: the walk leaves 0 survivors
// unread that the uninterrupted walk of the survivors reads. Positions: the newest run, the middle of the first range, the
// newest and the oldest run of the cut second the first range listed.
func TestGitHubTestsPeelReplayDeletionPositionByPageFill(t *testing.T) {
	peelReplaySkipRace(t)
	listing := deletionShiftListings()[0]
	const count, pages = 1500, 11
	for _, fill := range []int{1, 50, 99, 100, 101} {
		runs := peelReplayRuns(listing, count, 1001-fill, 1005)
		positions := []int{1, 500, 1001 - fill}
		if fill != 1 {
			positions = append(positions, 1000) // at fill 1 the cut second's newest run is its oldest
		}
		for _, position := range positions {
			for _, coalesce := range []bool{false, true} {
				if coalesce && fill < 100 {
					continue // plain-leg time: run groups on at the fills main leaves a survivor unread
				}
				name := fmt.Sprintf("property/fill=%d/position=%d/%s", fill, position, deletionShiftFlag(coalesce))
				t.Run(name, func(t *testing.T) {
					outcome := peelReplayMeasure(t, listing, peelReplayHandler(listing, pages, coalesce), coalesce, slices.Clone(runs), "route",
						func(c githubTestsChunkCursor) bool {
							return peelReplaySecondRange(c) && c.RangeItems == 2 && c.Index > 0
						}, count+1-position)
					peelReplayCell(t, name, outcome)
				})
			}
		}
	}
}
