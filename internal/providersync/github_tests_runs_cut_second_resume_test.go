package providersync

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// runsResumeQuery is the created range a runs-phase listing of the layout's range sends with the end hi ("" = unpeeled).
func runsResumeQuery(claim Claim, handler GitHubTestsRouteHandler, rangeName, hi string) string {
	if rangeName == "deep" {
		return githubTestsLateRerunRunsQuery(claim, handler.refreshLookback(), handler.lateRerunLookback(), hi).Get("created")
	}
	return githubTestsRunsQuery(claim, handler.refreshLookback(), hi).Get("created")
}

func runsResumeVisits(fake *listingFake, query string) int {
	visits := 0
	for _, listed := range fake.listRanges {
		if listed == query {
			visits++
		}
	}
	return visits
}

// A page that shrinks under a resume (runs deleted between chunks) is walked again from its first item: those items are
// not a second sighting of the oldest second. 1,000 runs share one second, a resume lands after 950 of them, and 50 are
// gone before it: the replayed page brings the count to exactly 1,000, the range is not saturated any more, and the unit
// completes as on main.
func TestGitHubTestsRunsShrunkPageOnResumeIsNotASaturatedSecond(t *testing.T) {
	for _, rangeName := range []string{"regular", "deep"} {
		for _, tc := range []struct{ resumeAt, deleted int }{{950, 50}} {
			t.Run(fmt.Sprintf("%s/delete_%d", rangeName, tc.deleted), func(t *testing.T) {
				claim, handler := runsTwiceClaim(rangeName), runsTwiceHandler(rangeName)
				runs, _ := runsTwiceRuns(runsTwiceCell{count: 1000, ties: 1000, saturated: true}, rangeName, claim.SinceAt.UTC())
				fake := newListingFake(t, runs)
				peeled := runsResumeQuery(claim, handler, rangeName, runs[0].created.UTC().Format(time.RFC3339))
				walk, yielded := runsCompatWalk(t, handler, claim, fake, func(c githubTestsChunkCursor) bool {
					return c.Phase == githubTestsRunsPhase && c.LateRuns == (rangeName == "deep") && c.RangeItems == tc.resumeAt
				}, func(raw string) string {
					fake.mu.Lock()
					fake.runs = fake.runs[tc.deleted:]
					fake.mu.Unlock()
					return raw
				})
				complete := walk.final.Result[GitHubTestsResultKeyReportsComplete]
				incomplete := lateRerunIncomplete(t, walk)
				t.Logf("%s delete %d: peeled_listing_pages=%d watermark=%t reports_complete=%v incomplete=%v",
					rangeName, tc.deleted, runsResumeVisits(fake, peeled), walk.final.Watermark != nil, complete, incomplete)
				if !yielded || walk.final.Watermark == nil || complete != true || len(incomplete) != 0 {
					t.Errorf("%s delete %d: yielded=%t watermark=%t reports_complete=%v incomplete=%v, want a complete unit as on main",
						rangeName, tc.deleted, yielded, walk.final.Watermark != nil, complete, incomplete)
				}
			})
		}
	}
}

// A second that holds GitHub's cap of 1,000 runs is listed once on both routes: Collect stops at it as CollectChunks does,
// with no second listing that ends at that second.
func TestGitHubTestsRunsSaturatedSecondCostsTheSameListingOnBothRoutes(t *testing.T) {
	for _, rangeName := range []string{"regular", "deep"} {
		t.Run(rangeName, func(t *testing.T) {
			claim, handler := runsTwiceClaim(rangeName), runsTwiceHandler(rangeName)
			runs, _ := runsTwiceRuns(runsTwiceCell{count: 1001, ties: 1001, saturated: true}, rangeName, claim.SinceAt.UTC())
			initial := runsResumeQuery(claim, handler, rangeName, "")
			peeled := runsResumeQuery(claim, handler, rangeName, runs[0].created.UTC().Format(time.RFC3339))
			chunkFake, oracleFake := newListingFake(t, runs), newListingFake(t, runs)
			walkListing(t, handler, claim, chunkFake, 0)
			_, oracleErr := handler.Collect(t.Context(), claim, providerfoundation.Credential{}, githubTestsClient(t, fakehttp.Client(oracleFake)), claim.BeforeAt.Add(time.Hour))
			chunk := [2]int{runsResumeVisits(chunkFake, initial), runsResumeVisits(chunkFake, peeled)}
			oracle := [2]int{runsResumeVisits(oracleFake, initial), runsResumeVisits(oracleFake, peeled)}
			t.Logf("%s: CollectChunks pages initial/peeled=%v Collect=%v oracle_err=%v", rangeName, chunk, oracle, oracleErr)
			if chunk != oracle || chunk[1] != 0 {
				t.Errorf("%s: listing pages (initial, peeled) CollectChunks=%v Collect=%v, want the same and no peeled listing", rangeName, chunk, oracle)
			}
			if rangeName == "regular" && !errors.Is(oracleErr, ErrPaginationCapExceeded) || rangeName == "deep" && oracleErr != nil {
				t.Errorf("%s: Collect err=%v, want main's disposition of a saturated range", rangeName, oracleErr)
			}
		})
	}
}

// A continuation URL that fit the cursor budget on main still fits: when the cut lists are dropped and the cursor is still
// over the budget, the count of runs at the oldest second is dropped too (the range then takes the confirmation listing),
// never a checkpoint conflict. The Link of the runs listing is padded so that the cursor without rq, ak and ck is exactly
// maxChunkCursorBytes long, with no cut list (rq is then the only field over the budget) or with both lists.
func TestGitHubTestsRunsCursorAtTheBudgetDropsTheOldestCount(t *testing.T) {
	runsBudgetDropsTheOldestCount(t, "none")
}

// The same with both cut lists in the cursor: they are dropped first, then the count.
func TestGitHubTestsRunsCursorAtTheBudgetDropsTheOldestCountAfterBothCutLists(t *testing.T) {
	runsBudgetDropsTheOldestCount(t, "both")
}

func runsBudgetDropsTheOldestCount(t *testing.T, lists string) {
	t.Helper()
	for _, rangeName := range []string{"regular", "deep"} {
		t.Run(rangeName+"/"+lists, func(t *testing.T) {
			claim, handler := runsTwiceClaim(rangeName), runsTwiceHandler(rangeName)
			// both: 149 of 1,050 runs share the oldest second; the first range holds 99 of them (run 149 is read and
			// carried) and the revisit lists all 149 in two pages, run 50 last on its first page (read into the active list).
			// none: 102 runs at one second; the second page holds two, so a cursor inside it carries the padded URL (a
			// finished page keeps its own URL, CHAOS-14316).
			count, ties := 102, 102
			peeled := lists == "both"
			if peeled {
				count, ties = 1050, 149
			}
			runs, _ := runsTwiceRuns(runsTwiceCell{count: count, ties: ties}, rangeName, claim.SinceAt.UTC())
			for index := range runs {
				runs[index].updated = runs[index].created.Add(time.Minute)
			}
			changed := claim.SinceAt.UTC().Add(time.Minute)
			if rangeName == "deep" {
				changed = claim.SinceAt.UTC().Add(-time.Hour)
			}
			if peeled {
				runs[ties-1].updated, runs[ties-100].updated = changed, changed
			}
			hi := ""
			if peeled {
				hi = runs[0].created.UTC().Format(time.RFC3339)
			}
			padQuery := runsResumeQuery(claim, handler, rangeName, hi)
			walk := func(urlBytes int) (int, bool, error) {
				fake := newListingFake(t, runs)
				doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
					response, err := fake.Do(request)
					if err != nil || request.URL.Query().Get("created") != padQuery {
						return response, err
					}
					if link := response.Header.Get("Link"); link != "" {
						target, _, _ := strings.Cut(strings.TrimPrefix(link, "<"), ">")
						if len(target)+len("&pad=") < urlBytes {
							target += "&pad=" + strings.Repeat("a", urlBytes-len(target)-len("&pad="))
						}
						response.Header.Set("Link", "<"+target+">; rel=\"next\"")
					}
					return response, nil
				})
				overhead, shape := 0, false
				err := handler.CollectChunks(t.Context(), claim, providerfoundation.Credential{}, githubTestsClient(t, fakehttp.Client(doer)), claim.BeforeAt.Add(time.Hour), "",
					func(emission ChunkRouteEmission) error {
						if !strings.Contains(emission.CursorAfter, "pad=") {
							return nil
						}
						cursor, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
						if decodeErr != nil {
							return decodeErr
						}
						shape = shape || (cursor.ActiveCutKeys != "") == peeled && (cursor.CarriedCutKeys != "") == peeled
						cursor.ActiveCutKeys, cursor.CarriedCutKeys, cursor.RangeOldestItems = "", "", 0
						plain, encodeErr := encodeGitHubTestsChunkCursor(cursor)
						if encodeErr != nil {
							return encodeErr
						}
						overhead = max(overhead, len(plain)-len(cursor.NextURL))
						return nil
					})
				return overhead, shape, err
			}
			overhead, shape, err := walk(1000)
			if err != nil || !shape || overhead == 0 {
				t.Fatalf("premise: the fixture did not write the %s cut lists (overhead=%d err=%v)", lists, overhead, err)
			}
			linkBytes := maxChunkCursorBytes - overhead
			_, _, err = walk(linkBytes)
			t.Logf("%s/%s: link_bytes=%d err=%v", rangeName, lists, linkBytes, err)
			if err != nil {
				t.Errorf("%s/%s: a %d-byte continuation that fits without rq, ak and ck failed: %v", rangeName, lists, linkBytes, err)
			}
		})
	}
}
