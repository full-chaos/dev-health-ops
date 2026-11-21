package providersync

import (
	"context"
	"net/url"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTestsListingWalk is one visit of a workflow runs listing (the runs, deep runs, artifacts and deep relist listings
// of the tests route): the only caller of the paginator for that route, so the walk and its replay list the same pages.
type githubTestsListingWalk struct {
	ctx        context.Context
	client     *providerfoundation.HTTPClient
	path       string
	query      url.Values
	initialURL string
	maxPages   int
}

func (walk githubTestsListingWalk) run(visit func(providerfoundation.PageVisit) error) (providerfoundation.PageCollection, error) {
	return providerfoundation.VisitGitHubLinkPages(walk.ctx, walk.client, providerfoundation.GitHubPageOptions{
		Path: walk.path, Route: githubRunsRoute, Query: walk.query, DataKey: "workflow_runs",
		MaxPages: walk.maxPages, InitialURL: walk.initialURL,
	}, visit)
}

// githubTestsListRun counts one listed run in the active range: RangeItems, RangeOldest (a new oldest second drops the
// active cut keys) and, in the runs phase, RangeOldestItems (CHAOS-14321).
func githubTestsListRun(cursor githubTestsChunkCursor, run gitHubWorkflowRunPayload, runsPhase bool) githubTestsChunkCursor {
	cursor.RangeItems++
	created := parseGitHubWorkflowTime(run.CreatedAt)
	if oldest := githubTestsOlderCreated(cursor.RangeOldest, created); oldest != cursor.RangeOldest {
		cursor.RangeOldest, cursor.ActiveCutKeys = oldest, ""
		if runsPhase {
			cursor.RangeOldestItems = 0
		}
	}
	if runsPhase && created != nil && created.UTC().Format(time.RFC3339) == cursor.RangeOldest {
		cursor.RangeOldestItems++
	}
	return cursor
}

// githubTestsRangeEnd is how one listing range ends.
type githubTestsRangeEnd int

const (
	// githubTestsRangeNext: the range consumed GitHub's cap of results; the next range ends at its oldest second.
	githubTestsRangeNext githubTestsRangeEnd = iota
	// githubTestsRangeListingEnd: the listing is complete.
	githubTestsRangeListingEnd
	// githubTestsRangeStuck: the next range would make no progress (a saturated second, or the interval's start).
	githubTestsRangeStuck
)

// githubTestsEndRange is the end of the active range of a listing, without its records: it clears the range state and,
// for githubTestsRangeNext, sets RangeHi to the next range's end. hi and items are the ended range's, for the records
// (hi is the stuck second when the runs phase's oldest second alone fills the cap).
func githubTestsEndRange(
	cursor githubTestsChunkCursor, interval githubTestsCreatedInterval, runsPhase bool,
) (next githubTestsChunkCursor, end githubTestsRangeEnd, hi string, items int) {
	nextHi, saturated, stuck := interval.peel(cursor.RangeItems, cursor.RangeOldest, cursor.RangeHi)
	items, hi = cursor.RangeItems, cursor.RangeHi
	if runsPhase && saturated && !stuck && cursor.RangeOldestItems >= githubTestsSearchResultCap {
		stuck, hi = true, nextHi
	}
	cursor.RangeItems, cursor.RangeOldest, cursor.AnchorID, cursor.ReseekPages, cursor.PagesEntered = 0, "", "", 0, 0
	if runsPhase {
		cursor.RangeOldestItems = 0
	}
	switch {
	case !saturated:
		return cursor, githubTestsRangeListingEnd, hi, items
	case stuck:
		return cursor, githubTestsRangeStuck, hi, items
	}
	cursor.RangeHi = nextHi
	return cursor, githubTestsRangeNext, hi, items
}

// githubTestsFrontier is where a fresh walk of a listing stops under its page budget (CHAOS-14435): the pages it entered,
// the runs it listed (a re-listed cut second counts again), and each distinct dated run's second and order in it.
type githubTestsFrontier struct {
	pages, listed int
	seconds       map[int64]int
	runs          map[string]githubTestsFrontierRun
}

type githubTestsFrontierRun struct {
	second  int64
	ordinal int
}

// grant is the pages a walk stopped at its page budget may list on top of it to reach the frontier. The frontier walk listed
// behind runs in seconds older than the walk's (its active range's oldest second; at a range start the range end; neither:
// the listing start): ceil(behind / per page) + 1 (the +1 covers the walk's own re-listed cut second). GitHub does not
// order runs inside a second, so the walk's own second is compared by run id: the runs of it the frontier walk listed and
// known (the ids the walk listed of that second) does not hold earn ceil(missing / per page) pages; none missing, none.
func (frontier githubTestsFrontier) grant(cursor githubTestsChunkCursor, known func(second int64) map[string]bool) (behind, pages int) {
	walked, dated := githubTestsCutSecond(cursor.RangeOldest)
	if cursor.RangeItems == 0 {
		walked, dated = githubTestsCutSecond(cursor.RangeHi)
	}
	for second, runs := range frontier.seconds {
		if !dated || second < walked {
			behind += runs
		}
	}
	if behind > 0 {
		return behind, (behind+nativePerPage-1)/nativePerPage + 1
	}
	if !dated {
		return 0, 0
	}
	listed := known(walked)
	missing := 0
	for id, run := range frontier.runs {
		if run.second == walked && !listed[id] {
			missing++
		}
	}
	return missing, (missing + nativePerPage - 1) / nativePerPage
}

// githubTestsReplayListing walks a listing from its start under its page budget with a count-only visitor: the same
// paginator call, page entries and range ends (peel, stuck, saturated) as the walk; it reads and emits nothing. healthy is
// asked after every page and once more at the end: an error ends the replay unfinished (no frontier).
func githubTestsReplayListing(
	walk githubTestsListingWalk, query func(rangeHi string) url.Values, interval githubTestsCreatedInterval, runsPhase bool, budget int,
	healthy func() error,
) (githubTestsFrontier, error) {
	frontier := githubTestsFrontier{seconds: map[int64]int{}, runs: map[string]githubTestsFrontierRun{}}
	var cursor githubTestsChunkCursor
	for frontier.pages < budget {
		ended := false
		walk.query, walk.initialURL, walk.maxPages = query(cursor.RangeHi), "", budget-frontier.pages
		collection, err := walk.run(func(page providerfoundation.PageVisit) error {
			cursor = githubTestsEnterPage(cursor, func(*githubTestsChunkCursor) { frontier.pages++ })
			for _, item := range page.Items {
				run, decodeErr := decodeGitHubWorkflowRun(item)
				if decodeErr != nil {
					return providerfoundation.RouteFailure(githubRunsRoute, providerfoundation.ReasonBodyDecode, decodeErr)
				}
				cursor = githubTestsListRun(cursor, run, runsPhase)
				frontier.listed++
				id, created := githubTestsAnchorID(run), parseGitHubWorkflowTime(run.CreatedAt)
				if _, seen := frontier.runs[id]; id != "" && created != nil && !seen {
					second := created.UTC().Unix()
					frontier.runs[id] = githubTestsFrontierRun{second: second, ordinal: frontier.seconds[second]}
					frontier.seconds[second]++
				}
			}
			if page.CursorAfter == "" {
				var end githubTestsRangeEnd
				cursor, end, _, _ = githubTestsEndRange(cursor, interval, runsPhase)
				ended = end != githubTestsRangeNext
			}
			return healthy()
		})
		if err != nil {
			return frontier, err
		}
		if ended || collection.PageBudgetExhausted {
			break
		}
	}
	return frontier, healthy()
}

// githubTestsRunsListingName and githubTestsArtifactsListingName name the active listing of the runs and the artifacts phase.
func githubTestsRunsListingName(cursor githubTestsChunkCursor) string {
	if cursor.LateRuns {
		return "deep_runs"
	}
	return "runs"
}

func githubTestsArtifactsListingName(cursor githubTestsChunkCursor) string {
	if cursor.Relist == githubTestsRelistWalking {
		return "deep_reports"
	}
	return "artifacts"
}
