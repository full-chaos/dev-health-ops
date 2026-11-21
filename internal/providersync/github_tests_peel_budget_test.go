package providersync

import (
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"
)

// peelBudgetMain is each cell's number on main before CHAOS-14435 (the same cells run there; logs in the PR): distinct
// runs read for "first_reads/...", survivors unread for "newer/...". This binary must not do worse.
var peelBudgetMain = map[string]int{
	"first_reads/runs/coalesce_off":                                   1010,
	"first_reads/runs/coalesce_on":                                    1010,
	"first_reads/deep_runs/coalesce_off":                              1010,
	"first_reads/deep_runs/coalesce_on":                               1010,
	"first_reads/artifacts/coalesce_off":                              1000,
	"first_reads/artifacts/coalesce_on":                               1000,
	"first_reads/deep_reports/coalesce_off":                           1410,
	"first_reads/deep_reports/coalesce_on":                            1410,
	"newer/runs/sparse/midpage/coalesce_off":                          1,
	"newer/runs/sparse/midpage/coalesce_on":                           1,
	"newer/runs/sparse/page_end/coalesce_off":                         1,
	"newer/runs/sparse/page_end/coalesce_on":                          1,
	"newer/runs/sparse/executor/coalesce_off":                         1,
	"newer/runs/sparse/executor/coalesce_on":                          1,
	"newer/runs/dense_same_cut/midpage/coalesce_off":                  0,
	"newer/runs/dense_same_cut/midpage/coalesce_on":                   0,
	"newer/runs/dense_same_cut/page_end/coalesce_off":                 0,
	"newer/runs/dense_same_cut/page_end/coalesce_on":                  0,
	"newer/runs/dense_same_cut/executor/coalesce_off":                 0,
	"newer/runs/dense_same_cut/executor/coalesce_on":                  0,
	"newer/runs/dense_cut_moves/midpage/coalesce_off":                 50,
	"newer/runs/dense_cut_moves/midpage/coalesce_on":                  50,
	"newer/runs/dense_cut_moves/page_end/coalesce_off":                50,
	"newer/runs/dense_cut_moves/page_end/coalesce_on":                 50,
	"newer/runs/dense_cut_moves/executor/coalesce_off":                50,
	"newer/runs/dense_cut_moves/executor/coalesce_on":                 50,
	"newer/runs/sparse_listing_end/midpage/coalesce_off":              1,
	"newer/runs/sparse_listing_end/midpage/coalesce_on":               1,
	"newer/runs/sparse_listing_end/page_end/coalesce_off":             1,
	"newer/runs/sparse_listing_end/page_end/coalesce_on":              1,
	"newer/runs/sparse_listing_end/executor/coalesce_off":             1,
	"newer/runs/sparse_listing_end/executor/coalesce_on":              1,
	"newer/runs/sparse_listing_end_two/midpage/coalesce_off":          2,
	"newer/runs/sparse_listing_end_two/midpage/coalesce_on":           2,
	"newer/runs/sparse_listing_end_two/page_end/coalesce_off":         2,
	"newer/runs/sparse_listing_end_two/page_end/coalesce_on":          2,
	"newer/runs/sparse_listing_end_two/executor/coalesce_off":         2,
	"newer/runs/sparse_listing_end_two/executor/coalesce_on":          2,
	"newer/runs/sparse_second_peel/midpage/coalesce_off":              1,
	"newer/runs/sparse_second_peel/midpage/coalesce_on":               1,
	"newer/runs/sparse_second_peel/page_end/coalesce_off":             1,
	"newer/runs/sparse_second_peel/page_end/coalesce_on":              1,
	"newer/runs/sparse_second_peel/executor/coalesce_off":             1,
	"newer/runs/sparse_second_peel/executor/coalesce_on":              1,
	"newer/runs/sparse_second_peel_two/midpage/coalesce_off":          2,
	"newer/runs/sparse_second_peel_two/midpage/coalesce_on":           2,
	"newer/runs/sparse_second_peel_two/page_end/coalesce_off":         2,
	"newer/runs/sparse_second_peel_two/page_end/coalesce_on":          2,
	"newer/runs/sparse_second_peel_two/executor/coalesce_off":         2,
	"newer/runs/sparse_second_peel_two/executor/coalesce_on":          2,
	"newer/deep_runs/sparse/midpage/coalesce_off":                     1,
	"newer/deep_runs/sparse/midpage/coalesce_on":                      1,
	"newer/deep_runs/sparse/page_end/coalesce_off":                    1,
	"newer/deep_runs/sparse/page_end/coalesce_on":                     1,
	"newer/deep_runs/sparse/executor/coalesce_off":                    1,
	"newer/deep_runs/sparse/executor/coalesce_on":                     1,
	"newer/deep_runs/dense_same_cut/midpage/coalesce_off":             0,
	"newer/deep_runs/dense_same_cut/midpage/coalesce_on":              0,
	"newer/deep_runs/dense_same_cut/page_end/coalesce_off":            0,
	"newer/deep_runs/dense_same_cut/page_end/coalesce_on":             0,
	"newer/deep_runs/dense_same_cut/executor/coalesce_off":            0,
	"newer/deep_runs/dense_same_cut/executor/coalesce_on":             0,
	"newer/deep_runs/dense_cut_moves/midpage/coalesce_off":            50,
	"newer/deep_runs/dense_cut_moves/midpage/coalesce_on":             50,
	"newer/deep_runs/dense_cut_moves/page_end/coalesce_off":           50,
	"newer/deep_runs/dense_cut_moves/page_end/coalesce_on":            50,
	"newer/deep_runs/dense_cut_moves/executor/coalesce_off":           50,
	"newer/deep_runs/dense_cut_moves/executor/coalesce_on":            50,
	"newer/deep_runs/sparse_listing_end/midpage/coalesce_off":         1,
	"newer/deep_runs/sparse_listing_end/midpage/coalesce_on":          1,
	"newer/deep_runs/sparse_listing_end/page_end/coalesce_off":        1,
	"newer/deep_runs/sparse_listing_end/page_end/coalesce_on":         1,
	"newer/deep_runs/sparse_listing_end/executor/coalesce_off":        1,
	"newer/deep_runs/sparse_listing_end/executor/coalesce_on":         1,
	"newer/deep_runs/sparse_listing_end_two/midpage/coalesce_off":     2,
	"newer/deep_runs/sparse_listing_end_two/midpage/coalesce_on":      2,
	"newer/deep_runs/sparse_listing_end_two/page_end/coalesce_off":    2,
	"newer/deep_runs/sparse_listing_end_two/page_end/coalesce_on":     2,
	"newer/deep_runs/sparse_listing_end_two/executor/coalesce_off":    2,
	"newer/deep_runs/sparse_listing_end_two/executor/coalesce_on":     2,
	"newer/deep_runs/sparse_second_peel/midpage/coalesce_off":         1,
	"newer/deep_runs/sparse_second_peel/midpage/coalesce_on":          1,
	"newer/deep_runs/sparse_second_peel/page_end/coalesce_off":        1,
	"newer/deep_runs/sparse_second_peel/page_end/coalesce_on":         1,
	"newer/deep_runs/sparse_second_peel/executor/coalesce_off":        1,
	"newer/deep_runs/sparse_second_peel/executor/coalesce_on":         1,
	"newer/deep_runs/sparse_second_peel_two/midpage/coalesce_off":     2,
	"newer/deep_runs/sparse_second_peel_two/midpage/coalesce_on":      2,
	"newer/deep_runs/sparse_second_peel_two/page_end/coalesce_off":    2,
	"newer/deep_runs/sparse_second_peel_two/page_end/coalesce_on":     2,
	"newer/deep_runs/sparse_second_peel_two/executor/coalesce_off":    2,
	"newer/deep_runs/sparse_second_peel_two/executor/coalesce_on":     2,
	"newer/artifacts/sparse/midpage/coalesce_off":                     1,
	"newer/artifacts/sparse/midpage/coalesce_on":                      1,
	"newer/artifacts/sparse/page_end/coalesce_off":                    1,
	"newer/artifacts/sparse/page_end/coalesce_on":                     1,
	"newer/artifacts/sparse/executor/coalesce_off":                    1,
	"newer/artifacts/sparse/executor/coalesce_on":                     1,
	"newer/artifacts/dense_same_cut/midpage/coalesce_off":             0,
	"newer/artifacts/dense_same_cut/midpage/coalesce_on":              0,
	"newer/artifacts/dense_same_cut/page_end/coalesce_off":            0,
	"newer/artifacts/dense_same_cut/page_end/coalesce_on":             0,
	"newer/artifacts/dense_same_cut/executor/coalesce_off":            0,
	"newer/artifacts/dense_same_cut/executor/coalesce_on":             0,
	"newer/artifacts/dense_cut_moves/midpage/coalesce_off":            50,
	"newer/artifacts/dense_cut_moves/midpage/coalesce_on":             50,
	"newer/artifacts/dense_cut_moves/page_end/coalesce_off":           50,
	"newer/artifacts/dense_cut_moves/page_end/coalesce_on":            50,
	"newer/artifacts/dense_cut_moves/executor/coalesce_off":           50,
	"newer/artifacts/dense_cut_moves/executor/coalesce_on":            50,
	"newer/artifacts/sparse_listing_end/midpage/coalesce_off":         1,
	"newer/artifacts/sparse_listing_end/midpage/coalesce_on":          1,
	"newer/artifacts/sparse_listing_end/page_end/coalesce_off":        1,
	"newer/artifacts/sparse_listing_end/page_end/coalesce_on":         1,
	"newer/artifacts/sparse_listing_end/executor/coalesce_off":        1,
	"newer/artifacts/sparse_listing_end/executor/coalesce_on":         1,
	"newer/artifacts/sparse_listing_end_two/midpage/coalesce_off":     2,
	"newer/artifacts/sparse_listing_end_two/midpage/coalesce_on":      2,
	"newer/artifacts/sparse_listing_end_two/page_end/coalesce_off":    2,
	"newer/artifacts/sparse_listing_end_two/page_end/coalesce_on":     2,
	"newer/artifacts/sparse_listing_end_two/executor/coalesce_off":    2,
	"newer/artifacts/sparse_listing_end_two/executor/coalesce_on":     2,
	"newer/artifacts/sparse_second_peel/midpage/coalesce_off":         1,
	"newer/artifacts/sparse_second_peel/midpage/coalesce_on":          1,
	"newer/artifacts/sparse_second_peel/page_end/coalesce_off":        1,
	"newer/artifacts/sparse_second_peel/page_end/coalesce_on":         1,
	"newer/artifacts/sparse_second_peel/executor/coalesce_off":        1,
	"newer/artifacts/sparse_second_peel/executor/coalesce_on":         1,
	"newer/artifacts/sparse_second_peel_two/midpage/coalesce_off":     2,
	"newer/artifacts/sparse_second_peel_two/midpage/coalesce_on":      2,
	"newer/artifacts/sparse_second_peel_two/page_end/coalesce_off":    2,
	"newer/artifacts/sparse_second_peel_two/page_end/coalesce_on":     2,
	"newer/artifacts/sparse_second_peel_two/executor/coalesce_off":    2,
	"newer/artifacts/sparse_second_peel_two/executor/coalesce_on":     2,
	"newer/deep_reports/sparse/midpage/coalesce_off":                  1,
	"newer/deep_reports/sparse/midpage/coalesce_on":                   1,
	"newer/deep_reports/sparse/page_end/coalesce_off":                 1,
	"newer/deep_reports/sparse/page_end/coalesce_on":                  1,
	"newer/deep_reports/sparse/executor/coalesce_off":                 1,
	"newer/deep_reports/sparse/executor/coalesce_on":                  1,
	"newer/deep_reports/dense_same_cut/midpage/coalesce_off":          0,
	"newer/deep_reports/dense_same_cut/midpage/coalesce_on":           0,
	"newer/deep_reports/dense_same_cut/page_end/coalesce_off":         0,
	"newer/deep_reports/dense_same_cut/page_end/coalesce_on":          0,
	"newer/deep_reports/dense_same_cut/executor/coalesce_off":         0,
	"newer/deep_reports/dense_same_cut/executor/coalesce_on":          0,
	"newer/deep_reports/dense_cut_moves/midpage/coalesce_off":         50,
	"newer/deep_reports/dense_cut_moves/midpage/coalesce_on":          50,
	"newer/deep_reports/dense_cut_moves/page_end/coalesce_off":        50,
	"newer/deep_reports/dense_cut_moves/page_end/coalesce_on":         50,
	"newer/deep_reports/dense_cut_moves/executor/coalesce_off":        50,
	"newer/deep_reports/dense_cut_moves/executor/coalesce_on":         50,
	"newer/deep_reports/sparse_listing_end/midpage/coalesce_off":      1,
	"newer/deep_reports/sparse_listing_end/midpage/coalesce_on":       1,
	"newer/deep_reports/sparse_listing_end/page_end/coalesce_off":     1,
	"newer/deep_reports/sparse_listing_end/page_end/coalesce_on":      1,
	"newer/deep_reports/sparse_listing_end/executor/coalesce_off":     1,
	"newer/deep_reports/sparse_listing_end/executor/coalesce_on":      1,
	"newer/deep_reports/sparse_listing_end_two/midpage/coalesce_off":  2,
	"newer/deep_reports/sparse_listing_end_two/midpage/coalesce_on":   2,
	"newer/deep_reports/sparse_listing_end_two/page_end/coalesce_off": 2,
	"newer/deep_reports/sparse_listing_end_two/page_end/coalesce_on":  2,
	"newer/deep_reports/sparse_listing_end_two/executor/coalesce_off": 2,
	"newer/deep_reports/sparse_listing_end_two/executor/coalesce_on":  2,
	"newer/deep_reports/sparse_second_peel/midpage/coalesce_off":      1,
	"newer/deep_reports/sparse_second_peel/midpage/coalesce_on":       1,
	"newer/deep_reports/sparse_second_peel/page_end/coalesce_off":     1,
	"newer/deep_reports/sparse_second_peel/page_end/coalesce_on":      1,
	"newer/deep_reports/sparse_second_peel/executor/coalesce_off":     1,
	"newer/deep_reports/sparse_second_peel/executor/coalesce_on":      1,
	"newer/deep_reports/sparse_second_peel_two/midpage/coalesce_off":  2,
	"newer/deep_reports/sparse_second_peel_two/midpage/coalesce_on":   2,
	"newer/deep_reports/sparse_second_peel_two/page_end/coalesce_off": 2,
	"newer/deep_reports/sparse_second_peel_two/page_end/coalesce_on":  2,
	"newer/deep_reports/sparse_second_peel_two/executor/coalesce_off": 2,
	"newer/deep_reports/sparse_second_peel_two/executor/coalesce_on":  2,
	"newer/shipped_budget/coalesce_off":                               1,
	"newer/shipped_budget/coalesce_on":                                1,
}

func peelBudgetCheck(t *testing.T, name string, got int, higherIsWorse bool) {
	t.Helper()
	want, ok := peelBudgetMain[name]
	t.Logf("CELL %s got=%d main=%d", name, got, want)
	if !ok {
		t.Errorf("no main number for %s", name)
		return
	}
	if higherIsWorse && got > want || !higherIsWorse && got < want {
		t.Errorf("%s: %d, main %d: this binary does worse than main", name, got, want)
	}
}

// peelBudgetRuns builds count runs one a second (deep: in the deep range), ids 1..count created ascending with the id, and
// gives ids from..to one created second.
func peelBudgetRuns(listing deletionShiftListing, count, from, to int) []listingRun {
	runs := listing.runs(1, count)
	for index := from - 1; index < to; index++ {
		runs[index].created = runs[to-1].created
		runs[index].updated = runs[to-1].updated
	}
	return runs
}

// A peel's pages given back are page accounting only (CHAOS-14435, Codex r1 P2): the runs of the cut second the next range
// lists for the first time are first reads and are charged to the record bound. 1,500 runs, ids 401..600 one second: the
// first range lists 100 of them (positions 901..1,000) and peels there; the next range reads ids 500..401 for the first
// time. The walk reads no more distinct runs than main under the same record bound.
func TestGitHubTestsPeelGivesNoRecordCreditToFirstReads(t *testing.T) {
	if cutDifferentialRace {
		t.Skip("run in the plain test legs (the race legs' weight budget)")
	}
	for _, listing := range deletionShiftRaceListings() {
		runs := peelBudgetRuns(listing, 1500, 401, 600)
		bound := 1010
		if listing.reports {
			bound = 1500 + deletionShiftReportRecords(t, listing)*1010
		}
		for _, coalesce := range deletionShiftFlags() {
			name := "first_reads/" + listing.name + "/" + deletionShiftFlag(coalesce)
			t.Run(listing.name+"/"+deletionShiftFlag(coalesce), func(t *testing.T) {
				handler := GitHubTestsRouteHandler{MaxWalkRecords: bound, CoalesceRuns: coalesce}
				fake := newListingFake(t, slices.Clone(runs))
				deletionShiftPlainWalk(t, handler, listing.claim, deletionShiftDoer(t, fake, listing.reports))
				peelBudgetCheck(t, name, len(listing.reads(fake)), true)
			})
		}
	}
}

// NOT closed (Codex r1 P1 of CHAOS-14435, follow-up ticket): a run NEWER than the cut, deleted after the peel, leaves the
// active range's anchor in place, so the walk keeps its cut, while an uninterrupted walk of the surviving runs peels one
// run lower (sparse) or lists one run more of a dense cut second. At the page budget edge the uninterrupted walk reads
// deeper. These cells pin the gap: the survivors unread here are never more than on main.
func TestGitHubTestsPeelKeepsItsCutAfterANewerDeletionNoWorseThanMain(t *testing.T) {
	if cutDifferentialRace {
		t.Skip("run in the plain test legs (minutes of walks under -race)")
	}
	type shape struct {
		name     string
		count    int
		from, to int // one created second (0: none)
		pages    int
		deleted  []int
	}
	shapes := []shape{
		{"sparse", 1120, 0, 0, 11, []int{1100}},
		{"dense_same_cut", 1120, 71, 170, 11, []int{1100}},   // positions 951..1,050: the cut second straddles the cap
		{"dense_cut_moves", 1120, 121, 170, 11, []int{1100}}, // positions 951..1,000: the cap ends exactly at the second's end
		// The listing ends inside the next range's budget, or the budget ends two ranges on: this binary's gaps.
		{"sparse_listing_end", 1201, 0, 0, 11, []int{1181}},
		{"sparse_listing_end_two", 1201, 0, 0, 11, []int{1181, 1180}},
		{"sparse_second_peel", 2150, 0, 0, 12, []int{2130}},
		{"sparse_second_peel_two", 2150, 0, 0, 12, []int{2130, 2129}},
	}
	for _, listing := range deletionShiftRaceListings() {
		secondRange := func(c githubTestsChunkCursor) bool { return c.RangeHi != "" }
		for _, sh := range shapes {
			handler := GitHubTestsRouteHandler{MaxRuns: sh.pages * 100}
			if listing.deep {
				handler = GitHubTestsRouteHandler{LateRerunMaxPages: sh.pages}
			}
			runs := listing.runs(1, sh.count)
			if sh.from > 0 {
				runs = peelBudgetRuns(listing, sh.count, sh.from, sh.to)
			}
			for _, cell := range []struct {
				mode string
				at   func(githubTestsChunkCursor) bool
			}{
				{"midpage", func(c githubTestsChunkCursor) bool { return secondRange(c) && c.RangeItems == 2 && c.Index > 0 }},
				{"page_end", func(c githubTestsChunkCursor) bool { return secondRange(c) && c.RangeItems == 100 && c.Index == 0 }},
				{"executor", func(c githubTestsChunkCursor) bool { return secondRange(c) && c.RangeItems >= 2 }},
			} {
				if cell.mode == "executor" && listing.name != "runs" {
					continue // the executor path is the same for every listing; the runs listing pins it
				}
				for _, coalesce := range deletionShiftFlags() {
					name := fmt.Sprintf("newer/%s/%s/%s/%s", listing.name, sh.name, cell.mode, deletionShiftFlag(coalesce))
					t.Run(fmt.Sprintf("%s/%s/%s/%s", listing.name, sh.name, cell.mode, deletionShiftFlag(coalesce)), func(t *testing.T) {
						deletionShiftRaceSkip(t, cell.mode)
						mode := "route"
						if cell.mode == "executor" {
							mode = "executor"
						}
						outcome := deletionShiftMeasure(t, listing, handler, coalesce, runs, mode, cell.at, sh.deleted...)
						peelBudgetCheck(t, name, len(outcome.unread), true)
					})
				}
			}
		}
	}
}

// The same gap at the shipped page budget (100 pages): 10,300 runs one a second in the runs listing, ten peels; the
// newest run is deleted at the last range's first page end.
func TestGitHubTestsPeelKeepsItsCutAtTheShippedBudgetNoWorseThanMain(t *testing.T) {
	if cutDifferentialRace {
		t.Skip("run in the plain test legs")
	}
	listing := deletionShiftListings()[0]
	runs := makeListingRuns(1, 10300, listingSince.Add(time.Minute), time.Second)
	claim := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	listing.claim = claim
	for _, coalesce := range deletionShiftFlags() {
		name := "newer/shipped_budget/" + deletionShiftFlag(coalesce)
		t.Run(deletionShiftFlag(coalesce), func(t *testing.T) {
			outcome := deletionShiftMeasure(t, listing, GitHubTestsRouteHandler{}, coalesce, runs, "route",
				func(c githubTestsChunkCursor) bool {
					return c.RangeHi != "" && c.RangeItems == 100 && c.Index == 0 && c.RunPages >= 90
				}, 10300)
			unread := slices.Clone(outcome.unread)
			sort.Ints(unread)
			peelBudgetCheck(t, name, len(unread), true)
		})
	}
}
