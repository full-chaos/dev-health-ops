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
