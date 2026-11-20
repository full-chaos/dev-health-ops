package providersync

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The cut-second skip must never cost a report main reads: a report that becomes listable between the first listing and
// the peeled re-listing is read when the run had no complete verdict, and a run is never skipped inside the range that
// recorded it, after a resume, or by a list the active range changed.

// cutLateArtifactDoer serves run runID's artifacts from an admission doer and publishes late on its publishLate-th list.
type cutLateArtifactDoer struct {
	base *listingFake
	*githubTestsAdmissionDoer
	late        githubTestsAdmissionArtifact
	lists       int
	publishLate func(int) bool
}

func (doer *cutLateArtifactDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api/actions/runs/"+doer.runID+"/artifacts":
		doer.lists++
		if doer.publishLate(doer.lists) {
			doer.artifacts = append(doer.artifacts, doer.late)
		}
		return doer.githubTestsAdmissionDoer.Do(request)
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/"):
		return doer.githubTestsAdmissionDoer.Do(request)
	default:
		return doer.base.Do(request)
	}
}

// A cut-second run with no verdict (no report source, no loss) is not read whole: the revisit reads it again and finds the
// late report, as main does. A run with a complete verdict (a valid zero-test report) stays skip-eligible.
func TestGitHubTestsCutSecondRunWithoutAVerdictIsReadAgain(t *testing.T) {
	type tally struct{ lists, verdicts, suites, cases, coverage int }
	for _, cell := range []string{"empty_inventory", "name_suffix", "name_prefix", "full_archive_no_report", "directory_no_report", "zero_test_report_control"} {
		for _, layout := range cutDedupLayouts {
			archiveWithoutReport := githubTestsZip(t, map[string]string{"output.log": "ci build log"})
			artifact := githubTestsAdmissionArtifact{id: 9901, name: "build-log", archive: archiveWithoutReport}
			var initial []githubTestsAdmissionArtifact
			switch cell {
			case "name_suffix":
				artifact.name = "image.dockerbuild"
				initial = append(initial, artifact)
			case "name_prefix":
				artifact.name = "go-test-timings-shard-1"
				initial = append(initial, artifact)
			case "full_archive_no_report":
				initial = append(initial, artifact)
			case "directory_no_report":
				for index := 0; index <= githubTestsMaxArtifacts; index++ {
					initial = append(initial, githubTestsAdmissionArtifact{id: 9901 + int64(index), name: fmt.Sprintf("build-log-%d", index), archive: archiveWithoutReport})
				}
			case "zero_test_report_control":
				artifact.name = "filtered-tests"
				artifact.archive = githubTestsZip(t, map[string]string{"junit.xml": `<testsuites tests="0"></testsuites>`})
				initial = append(initial, artifact)
			}
			admission := newGitHubTestsAdmissionDoer(t, "5004", initial)
			admission.direct = true
			doer := &cutLateArtifactDoer{
				base: newListingFake(t, cutDedupRuns(layout.updated)), githubTestsAdmissionDoer: admission,
				late:        githubTestsAdmissionArtifact{id: 9999, name: "test-results-late", archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture, "lcov.info": githubTestsLCOVFixture})},
				publishLate: func(list int) bool { return list == 2 },
			}
			walk := walkCutDedup(t, cutDedupClaim(layout.backfill), doer, nil, nil)
			counter := func(key string) int {
				value, _ := strconv.Atoi(fmt.Sprint(walk.final.Result[key]))
				return value
			}
			got := tally{doer.lists, walk.rows[testOpsRunReportCompletenessDestination]["5004"], counter("test_suites_synced"), counter("test_cases_synced"), counter("coverage_snapshots_synced")}
			want := tally{2, 1, 1, 2, 1}
			if cell == "zero_test_report_control" {
				want = tally{1, 1, 0, 0, 0}
			}
			if got != want {
				t.Errorf("%s, %s: lists/verdicts/suites/cases/coverage = %+v, want %+v", cell, layout.name, got, want)
			}
		}
	}
}

// A run is never skipped inside the range that read it: a page re-anchor on resume, a page shifted by a new run, and a
// duplicate on one page each read the run again and find the late report, in the first range and in a peeled range.
func TestGitHubTestsCutSecondSameRangeNeverSkips(t *testing.T) {
	for _, cell := range []string{"page_reanchor", "pagination_offset_shift", "same_page_duplicate"} {
		for _, peeled := range []bool{false, true} {
			name := fmt.Sprintf("%s/peeled=%t", cell, peeled)
			const id = 6001
			at, preceding := cutDedupSecond, 0
			var runs []listingRun
			if peeled {
				at = at.Add(-5 * time.Minute)
				preceding = len(cutDedupIDs)
				runs = cutDedupRuns(cutDedupBefore.Add(-time.Hour))
			}
			run := func(id int, created time.Time) listingRun {
				return listingRun{id: id, created: created, updated: cutDedupBefore.Add(-time.Hour), status: "completed", conclusion: "success"}
			}
			target := run(id, at)
			runs = append(runs, target)
			switch cell {
			case "page_reanchor":
				runs = append(runs, run(id-1, at.Add(-time.Second)))
			case "pagination_offset_shift":
				runs = append(runs, makeListingRuns(7000, 99-preceding, at.Add(time.Second), time.Second)...)
				runs = append(runs, run(id-1, at.Add(-time.Second)))
			case "same_page_duplicate":
				runs = append(runs, target)
			}
			admission := newGitHubTestsAdmissionDoer(t, strconv.Itoa(id), []githubTestsAdmissionArtifact{{id: 9901, name: "existing-tests", archive: githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit("first-", 1)})}})
			admission.direct = true
			doer := &cutLateArtifactDoer{
				base: newListingFake(t, runs), githubTestsAdmissionDoer: admission,
				late:        githubTestsAdmissionArtifact{id: 9999, name: "test-results-late", archive: githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit("late-", 1)})},
				publishLate: func(list int) bool { return list == 2 },
			}
			yielded := false
			walkCutDedup(t, cutDedupClaim(false), doer, func(before, after githubTestsChunkCursor) bool {
				if yielded || doer.lists != 1 || before.Phase != githubTestsArtifactsPhase || after.Phase != githubTestsArtifactsPhase {
					return false
				}
				if (before.RangeHi != "") != peeled {
					t.Fatalf("%s: the target was read in the wrong range %q", name, before.RangeHi)
				}
				switch cell {
				case "page_reanchor":
					if after.Index != preceding+1 {
						t.Fatalf("%s: index %d, want %d", name, after.Index, preceding+1)
					}
					doer.base.runs = []listingRun{target}
				case "pagination_offset_shift":
					if after.RangeItems != 100 || after.Index != 0 {
						t.Fatalf("%s: want a page boundary: %+v", name, after)
					}
					doer.base.runs = append(doer.base.runs, run(id+1, at.Add(2*time.Second)))
				case "same_page_duplicate":
					return false
				}
				yielded = true
				return true
			}, nil)
			if cell != "same_page_duplicate" && !yielded {
				t.Fatalf("%s: the resume never happened", name)
			}
			if doer.lists != 2 || admission.api[9999] != 1 {
				t.Errorf("%s: artifact lists = %d, late report reads = %d; want 2 and 1 (as main)", name, doer.lists, admission.api[9999])
			}
		}
	}
}

// The carried list is frozen at the peel: an older run read in the revisit before a re-anchor does not replace it, so the
// carried run is skipped, and the run of the revisit itself is read again on the re-anchor (as main).
func TestGitHubTestsCutSecondCarriedListIsFrozen(t *testing.T) {
	updated := cutDedupBefore.Add(-time.Hour)
	runs := cutDedupRuns(updated)
	firstOlder := listingRun{id: 6001, created: cutDedupSecond.Add(-time.Second), updated: updated, status: "completed", conclusion: "success"}
	lastOlder := listingRun{id: 6000, created: cutDedupSecond.Add(-2 * time.Second), updated: updated, status: "completed", conclusion: "success"}
	runs = append(runs, firstOlder, lastOlder)
	doer := newCutDedupDoer(t, runs)
	doer.reports[5004], doer.reports[6001] = true, true
	yielded := false
	walkCutDedup(t, cutDedupClaim(false), doer, func(_, after githubTestsChunkCursor) bool {
		if yielded || after.Phase != githubTestsArtifactsPhase || after.RangeHi == "" || after.RangeItems != 6 {
			return false
		}
		yielded = true
		doer.fake.runs = []listingRun{doer.byID[5004], firstOlder}
		return true
	}, nil)
	if !yielded {
		t.Fatal("never reached the first older run of the revisit")
	}
	if doer.lists[5004] != 1 || doer.lists[6001] != 2 {
		t.Fatalf("carried run lists = %d (want 1: skipped in the revisit), revisit run lists = %d (want 2: read again on the re-anchor)", doer.lists[5004], doer.lists[6001])
	}
}

// cutAttemptReads walks newer runs plus 8 or 12 report runs at the cut second, run index newAttempt (or none) listed with
// attempt 2 by the revisit, and returns the artifact lists per cut run and the suites counter.
func cutAttemptReads(t *testing.T, newer, cut, newAttempt int) ([]int, string) {
	t.Helper()
	runs := makeListingRuns(1, newer, cutDedupSince.Add(time.Hour), 30*time.Second)
	var ids []int
	for index := 0; index < cut; index++ {
		id := 33301167231 + index
		ids = append(ids, id)
		runs = append(runs, listingRun{id: id, created: cutDedupSecond, updated: cutDedupBefore.Add(-time.Hour), status: "completed", conclusion: "success"})
	}
	doer := newCutDedupDoer(t, runs)
	if newAttempt >= 0 {
		doer.attemptAt[ids[newAttempt]] = cutDedupSecond.Format(time.RFC3339)
	}
	for _, id := range ids {
		doer.reports[id] = true
	}
	walk := walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
	return doer.reads(ids), fmt.Sprint(walk.final.Result["test_suites_synced"])
}

// A new attempt in the revisit never changes which other carried runs are skipped: the count does not depend on where the
// new attempt is listed. The reviewer's layout (8 cut-second runs in the first range, past the list's cap) reads as main at
// every position; the resized layout (4 runs, within the cap) skips every unchanged carried run at every position.
func TestGitHubTestsCutSecondNewAttemptDoesNotMixSkips(t *testing.T) {
	for _, tc := range []struct {
		name       string
		newer, cut int
		attempts   []int
		want       func(newAttempt int) ([]int, string)
	}{
		{"8 runs past the cap", 992, 12, []int{11, 8, 4, -1}, func(int) ([]int, string) {
			return []int{1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2}, "20"
		}},
		{"4 runs within the cap", 996, 8, []int{7, 5, 4, -1}, func(newAttempt int) ([]int, string) {
			reads := []int{1, 1, 1, 1, 1, 1, 1, 1}
			if newAttempt < 0 {
				return reads, "8"
			}
			reads[newAttempt] = 2
			return reads, "9"
		}},
		{"already overflowed", 995, 5, []int{-1}, func(int) ([]int, string) {
			return []int{2, 2, 2, 2, 2}, "10"
		}},
	} {
		for _, newAttempt := range tc.attempts {
			reads, suites := cutAttemptReads(t, tc.newer, tc.cut, newAttempt)
			wantReads, wantSuites := tc.want(newAttempt)
			if !reflect.DeepEqual(reads, wantReads) || suites != wantSuites {
				t.Errorf("%s, new attempt at %d: reads %v suites %s, want %v suites %s", tc.name, newAttempt, reads, suites, wantReads, wantSuites)
			}
		}
	}
}

// The active list holds only the runs of the RangeOldest second: runs read whole at a newer second earlier in the range
// are dropped when the oldest second moves, so they never fill the list and push the cut-second runs past its cap.
func TestGitHubTestsCutSecondActiveListHoldsOnlyTheOldestSecond(t *testing.T) {
	runs := makeListingRuns(1, 996, cutDedupSince.Add(time.Hour), 30*time.Second)
	var ids []int
	for index := 0; index < 4; index++ {
		id := 33301167231 + index
		ids = append(ids, id)
		runs = append(runs, listingRun{id: id, created: cutDedupSecond, updated: cutDedupBefore.Add(-time.Hour), status: "completed", conclusion: "success"})
	}
	doer := newCutDedupDoer(t, runs).withReports(append([]int{1, 2, 3, 4}, ids...)...)
	walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
	if got, want := doer.reads(append([]int{1, 2, 3, 4}, ids...)), []int{1, 1, 1, 1, 1, 1, 1, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("artifact lists of the 4 newer report runs and the 4 cut-second runs = %v, want %v", got, want)
	}
}

// cutRepeatedReadDoer serves run target's artifacts per list: the first list holds first; the second (the repeated read in
// the same range) holds second, or nothing for no_source; artifact 9902 fails on the second list (404, or a redirect with
// no location) and reads on the revisit.
type cutRepeatedReadDoer struct {
	*githubTestsAdmissionDoer
	listing       *listingFake
	target        int
	first, second []githubTestsAdmissionArtifact
	secondRead    string
	lists         int
}

func (doer *cutRepeatedReadDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api/actions/runs/"+strconv.Itoa(doer.target)+"/artifacts":
		doer.lists++
		doer.artifacts = doer.second
		if doer.lists == 1 || doer.lists == 2 && doer.secondRead == "complete_control" || doer.lists >= 3 && doer.secondRead == "no_source" {
			doer.artifacts = doer.first
		} else if doer.lists == 2 && doer.secondRead == "no_source" {
			doer.artifacts = nil
		}
		return doer.githubTestsAdmissionDoer.Do(request)
	case path == "/repos/acme/api/actions/artifacts/9902/zip" && doer.lists == 2:
		status := http.StatusFound
		if doer.secondRead == "download_404" {
			status = http.StatusNotFound
		}
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: http.NoBody, Request: request}, nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/"):
		return doer.githubTestsAdmissionDoer.Do(request)
	default:
		return doer.listing.Do(request)
	}
}

// A run read whole and then read again in the same range (same-page duplicate, shifted page, resume re-anchor) with no
// verdict or an incomplete verdict (empty listing, an additional report's download 404, a redirect with no location) is
// dropped from the active list in that emission: the revisit reads it as main does. A complete repeated read keeps it.
func TestGitHubTestsCutSecondRepeatedReadWithALossDropsTheKey(t *testing.T) {
	type tally struct {
		lists, verdicts, suites, additional int
		carried                             string
	}
	for _, shape := range []string{"duplicate", "offset_shift", "reanchor"} {
		for _, layout := range cutDedupLayouts {
			for _, secondRead := range []string{"no_source", "download_404", "unavailable_download", "complete_control"} {
				name := fmt.Sprintf("%s/%s/%s", shape, layout.name, secondRead)
				runs := cutDedupRuns(layout.updated)
				target := runs[len(runs)-1]
				switch shape {
				case "duplicate":
					runs = append(runs, target)
				case "offset_shift":
					for index := 0; index < 99; index++ {
						runs[index].created = cutDedupSecond
					}
				}
				first := []githubTestsAdmissionArtifact{{id: 9901, name: "existing-tests", archive: githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit("existing-", 1)})}}
				second := first
				if secondRead == "download_404" || secondRead == "unavailable_download" {
					second = append(append([]githubTestsAdmissionArtifact(nil), first...),
						githubTestsAdmissionArtifact{id: 9902, name: "additional-tests", archive: githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit("additional-", 1)})})
				}
				admission := newGitHubTestsAdmissionDoer(t, strconv.Itoa(target.id), first)
				admission.direct = true
				doer := &cutRepeatedReadDoer{githubTestsAdmissionDoer: admission, listing: newListingFake(t, runs), target: target.id, first: first, second: second, secondRead: secondRead}
				var carried string
				var peeled, yielded bool
				walk := walkCutDedup(t, cutDedupClaim(layout.backfill), doer, func(before, after githubTestsChunkCursor) bool {
					if before.Phase != githubTestsArtifactsPhase || before.RangeHi != "" {
						return false
					}
					if after.RangeHi == cutDedupSecond.Format(time.RFC3339) {
						peeled, carried = true, after.CarriedCutKeys
						if shape == "duplicate" {
							doer.listing.runs = doer.listing.runs[:len(doer.listing.runs)-1]
						}
						return false
					}
					if doer.lists != 1 || yielded {
						return false
					}
					switch shape {
					case "offset_shift":
						if after.RangeItems != 900 || after.Index != 0 {
							t.Fatalf("%s: the page did not end at the target: items=%d index=%d", name, after.RangeItems, after.Index)
						}
						doer.listing.runs = append(doer.listing.runs, listingRun{id: 7000, created: cutDedupBefore.Add(-time.Minute), updated: layout.updated, status: "completed", conclusion: "success"})
					case "reanchor":
						if after.RangeItems != 999 || after.Index != 99 {
							t.Fatalf("%s: the walk did not stop inside the page: items=%d index=%d", name, after.RangeItems, after.Index)
						}
						doer.listing.runs = doer.listing.runs[98:]
					default:
						return false
					}
					yielded = true
					return true
				}, nil)
				if !peeled || (shape != "duplicate") != yielded {
					t.Fatalf("%s: want a peel and the resume branch: peeled=%t resumed=%t", name, peeled, yielded)
				}
				id := strconv.Itoa(target.id)
				got := tally{doer.lists, walk.rows[testOpsRunReportCompletenessDestination][id], walk.rows["test_suite_results"][id], admission.api[9902], carried}
				want := map[string]tally{
					"no_source":            {3, 2, 2, 0, ""},
					"download_404":         {3, 3, 4, 1, ""},
					"unavailable_download": {3, 3, 4, 1, ""},
					"complete_control":     {2, 2, 2, 0, id},
				}[secondRead]
				if got != want {
					t.Errorf("%s: lists/verdicts/suites/additional reads/carried = %+v, want %+v", name, got, want)
				}
			}
		}
	}
}

// cutReAddDoer serves run 5004's three reads in the first range: complete (9901), a loss (9902 listed, its download 404),
// then complete (9901 and 9902 read).
type cutReAddDoer struct {
	*githubTestsAdmissionDoer
	listing *listingFake
	both    []githubTestsAdmissionArtifact
	lists   int
}

func (doer *cutReAddDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api/actions/runs/5004/artifacts":
		doer.lists++
		if doer.lists >= 2 {
			doer.artifacts = doer.both
		}
		return doer.githubTestsAdmissionDoer.Do(request)
	case path == "/repos/acme/api/actions/artifacts/9902/zip" && doer.lists == 2:
		return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: http.NoBody, Request: request}, nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/"):
		return doer.githubTestsAdmissionDoer.Do(request)
	default:
		return doer.listing.Do(request)
	}
}

// The active list holds a run's key only while its latest read in the active range was complete: a complete read adds it,
// a later read with a loss removes it, and a later complete read adds it again, so the peel carries it and the revisit
// skips all three listed entries.
func TestGitHubTestsCutSecondLaterCompleteReadReAddsTheKey(t *testing.T) {
	runs := makeListingRuns(1, 997, cutDedupSince.Add(time.Hour), 30*time.Second)
	for _, id := range cutDedupIDs {
		runs = append(runs, listingRun{id: id, created: cutDedupSecond, updated: cutDedupBefore.Add(-time.Hour), status: "completed", conclusion: "success"})
	}
	target := runs[len(runs)-1]
	runs = append(runs, target, target)
	first := []githubTestsAdmissionArtifact{{id: 9901, name: "existing-tests", archive: githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit("existing-", 1)})}}
	both := append(append([]githubTestsAdmissionArtifact(nil), first...), githubTestsAdmissionArtifact{id: 9902, name: "additional-tests", archive: githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit("additional-", 1)})})
	admission := newGitHubTestsAdmissionDoer(t, "5004", first)
	admission.direct = true
	doer := &cutReAddDoer{githubTestsAdmissionDoer: admission, listing: newListingFake(t, runs), both: both}
	var active []string
	carried, seen := "", 0
	walkCutDedup(t, cutDedupClaim(false), doer, func(before, after githubTestsChunkCursor) bool {
		if before.Phase != githubTestsArtifactsPhase || before.RangeHi != "" {
			return false
		}
		if doer.lists > seen {
			seen = doer.lists
			// The read that ends the range is emitted with the peel: its list is already frozen into the carried one.
			if after.RangeHi != "" {
				active = append(active, after.CarriedCutKeys)
			} else {
				active = append(active, after.ActiveCutKeys)
			}
		}
		if after.RangeHi != "" {
			carried = after.CarriedCutKeys
		}
		return false
	}, nil)
	if want := []string{"5004", "", "5004"}; !reflect.DeepEqual(active, want) || carried != "5004" || doer.lists != 3 {
		t.Fatalf("active list after each read (the last as carried at the peel) = %q, carried = %q, lists = %d; want %q, \"5004\", 3 (the revisit skips all three)", active, carried, doer.lists, want)
	}
}
