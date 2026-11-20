package providersync

import (
	"log/slog"
	"net/url"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The late re-run re-list. GitHub lists workflow runs by created time only, and a re-run keeps the run's created time. An
// incremental claim lists runs created from its floor, date(since - max(1 d, width)) (githubTestsArtifactRunsCreatedFrom),
// so a run re-run more than about that long after its creation is listed by no claim that holds the re-run: the claims that
// list it end before the re-run starts. Once a day the claim that holds a UTC midnight therefore lists a second, deeper
// created range: from date(since - max(1 d, width) - L) to one second before the floor. The two ranges are disjoint, so no
// run is listed twice in one claim.
//
// A run of the deep range is read when it is completed, started at or before before, and was last updated at or after
// since - max(1 d, width). Consecutive owning claims cover the updated times end to end: with a width under one day the next
// owning claim starts less than one day after this claim's midnight, which is at or before this claim's before; with a width of
// one day or more every claim owns a midnight and the next one starts at this before. A re-run with a created-to-start lag up
// to about L is read, at most one day plus a width after it changed. A longer lag stays unread (GitHub allows a re-run up to 30
// days after creation).
//
// The deep range has its own page budget, 1,000 pages a claim (githubTestsLateRerunMaxPagesDefault); a chunked walk resumes it
// across chunks like any listing. Its exhaustion or a saturated second records one late_rerun_reports observation
// that keeps reports_complete false and advances the watermark: holding it would hold a busy repository for ever, and the
// regular range of the claim was walked whole.
const (
	githubTestsLateRerunReportsComponent = "late_rerun_reports"
	githubTestsLateRerunLookbackDefault  = 7 * 24 * time.Hour
	githubTestsLateRerunMaxPagesDefault  = 1000
)

// The values of githubTestsChunkCursor.Relist; zero is the regular range (or no deep range).
const (
	githubTestsRelistWalking = iota + 1
	githubTestsRelistStoppedBudget
	githubTestsRelistStoppedSaturated
)

// lateRerunLookback is L, the depth of the deep range below the regular floor.
func (handler GitHubTestsRouteHandler) lateRerunLookback() time.Duration {
	if handler.LateRerunLookback > 0 {
		return handler.LateRerunLookback
	}
	return githubTestsLateRerunLookbackDefault
}

// lateRerunMaxPages is the deep range's page budget; zero keeps githubTestsLateRerunMaxPagesDefault, and a value outside
// 1..githubTestsLateRerunMaxPagesDefault is refused as a configuration error by both routes.
func (handler GitHubTestsRouteHandler) lateRerunMaxPages() int {
	if handler.LateRerunMaxPages == 0 {
		return githubTestsLateRerunMaxPagesDefault
	}
	return handler.LateRerunMaxPages
}

func (handler GitHubTestsRouteHandler) lateRerunConfigValid() bool {
	pages := handler.lateRerunMaxPages()
	return handler.LateRerunLookback >= 0 && pages >= 1 && pages <= githubTestsLateRerunMaxPagesDefault
}

// githubTestsArtifactFloorWidth is max(1 d, width), the distance from since to the regular artifacts floor.
func githubTestsArtifactFloorWidth(since, before time.Time) time.Duration {
	return max(24*time.Hour, before.Sub(since))
}

// githubTestsRelistOwned reports whether the claim lists the deep range: an incremental claim with both edges that holds a
// UTC midnight M with since < M <= before. Contiguous claims hold each midnight exactly once; a retry of the claim, or a claim
// held by its regular budget, keeps the same window and owns the same midnight again.
func githubTestsRelistOwned(claim Claim) bool {
	if claim.Mode != "incremental" || claim.SinceAt == nil || claim.BeforeAt == nil {
		return false
	}
	since, before := claim.SinceAt.UTC(), claim.BeforeAt.UTC()
	midnight := time.Date(since.Year(), since.Month(), since.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	return !midnight.After(before)
}

// githubTestsRelistQueryAt is the created range of the deep listing, its end moved to peeledHi when a saturated range was
// peeled.
func githubTestsRelistQueryAt(claim Claim, lookback time.Duration, peeledHi string) url.Values {
	since, before := claim.SinceAt.UTC(), claim.BeforeAt.UTC()
	hi := peeledHi
	if hi == "" {
		hi = githubTestsRelistFloor(since, before).Add(-time.Second).Format(time.RFC3339)
	}
	lo := since.Add(-githubTestsArtifactFloorWidth(since, before) - lookback).Format(time.DateOnly)
	return url.Values{"per_page": {"100"}, "created": {lo + ".." + hi}}
}

// githubTestsRelistFloor is the regular artifacts floor as a time: the start of the date githubTestsArtifactRunsCreatedFrom
// names.
func githubTestsRelistFloor(since, before time.Time) time.Time {
	floor, _ := time.Parse(time.DateOnly, since.Add(-githubTestsArtifactFloorWidth(since, before)).Format(time.DateOnly))
	return floor
}

// githubTestsRelistRunOutsideWindow is the read rule of the deep range: the incremental rule with the width floored at one day.
// A run created at or after the regular floor belongs to the regular range, whatever the listing answered, so the two ranges
// never read one run twice in a claim. A run with no last-updated time is never excluded.
func githubTestsRelistRunOutsideWindow(pipeline githubTestsPipelineRow, claim Claim) bool {
	since, before := claim.SinceAt.UTC(), claim.BeforeAt.UTC()
	if pipeline.StartedAt.After(before) {
		return true
	}
	if pipeline.QueuedAt != nil && !pipeline.QueuedAt.UTC().Before(githubTestsRelistFloor(since, before)) {
		return true
	}
	if pipeline.FinishedAt == nil {
		return false
	}
	return pipeline.FinishedAt.UTC().Before(since.Add(-githubTestsArtifactFloorWidth(since, before)))
}

// githubTestsArtifactRunOutside is the read rule of the range a run was listed in.
func githubTestsArtifactRunOutside(pipeline githubTestsPipelineRow, claim Claim, relist bool) bool {
	if relist {
		return githubTestsRelistRunOutsideWindow(pipeline, claim)
	}
	return githubTestsReportRunOutsideWindow(pipeline, claim)
}

// githubTestsEndArtifactsListing ends a regular or deep range that was walked whole: a regular range of an owning claim
// starts the deep range; anything else leaves the artifacts phase.
func githubTestsEndArtifactsListing(cursor githubTestsChunkCursor, claim Claim, handler GitHubTestsRouteHandler) githubTestsChunkCursor {
	if cursor.Relist == 0 && githubTestsRelistOwned(claim) {
		cursor.Relist, cursor.ArtifactPages = githubTestsRelistWalking, 0
		return cursor
	}
	if cursor.Relist == githubTestsRelistWalking {
		cursor.Relist = 0
	}
	cursor.Phase = githubTestsPhaseAfterArtifacts(handler)
	return cursor
}

// recordGitHubTestsArtifactsWalkBound ends the artifacts phase at the walk record bound. In the regular range it holds the
// watermark as before; in the deep range the regular range was walked whole, so it is a deep-range stop.
func recordGitHubTestsArtifactsWalkBound(
	cursor githubTestsChunkCursor, client *providerfoundation.HTTPClient, claim Claim, bound int,
) githubTestsChunkCursor {
	if cursor.Relist != githubTestsRelistWalking {
		return recordGitHubTestsWalkBound(cursor, client, claim, githubTestsArtifactInventoryComponent, bound)
	}
	logGitHubTestsWalkBound(cursor, claim, githubTestsLateRerunReportsComponent, bound)
	return recordGitHubTestsRelistStop(cursor, client, claim, githubTestsPageBudgetCause, cursor.RangeHi, cursor.RangeItems)
}

// recordGitHubTestsRelistStop ends the deep range before its oldest runs, at its page budget or at a saturated second. The
// cursor keeps one flag per cause; githubTestsSaturatedObservations turns them into the late_rerun_reports observations.
func recordGitHubTestsRelistStop(
	cursor githubTestsChunkCursor, client *providerfoundation.HTTPClient, claim Claim, cause, rangeHi string, items int,
) githubTestsChunkCursor {
	cursor.Relist = githubTestsRelistStoppedBudget
	if cause == githubTestsListingSaturatedCause {
		cursor.Relist = githubTestsRelistStoppedSaturated
	}
	if client != nil {
		client.Metrics.RecordInventoryPageCap(claim.Provider, claim.Dataset)
	}
	slog.Warn(
		"late re-run re-list stopped before its oldest runs; the watermark advances",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", logging.ProviderText(cursor.Repo), "component", githubTestsLateRerunReportsComponent, "cause", cause,
		"range_hi", rangeHi, "items", items, "pages", cursor.ArtifactPages,
	)
	return cursor
}
