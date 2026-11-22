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
// A run of the deep range is read when it is completed, started at or before before, has a created and a last-updated time,
// and was last updated at or after since - max(1 d, width) (githubTestsLateRerunFloor, shared with the runs phase's deep range). Consecutive owning claims cover the updated times end to end: with a width under one day the next
// owning claim starts less than one day after this claim's midnight, which is at or before this claim's before; with a width of
// one day or more every claim owns a midnight and the next one starts at this before. A re-run with a created-to-start lag up
// to about L is read, at most one day plus a width after it changed. A longer lag stays unread (GitHub allows a re-run up to 30
// days after creation).
//
// The deep range has its own page budget, 1,000 pages a claim (githubTestsLateRerunMaxPages, shared with the runs phase's deep
// range), under the shared listing allowance (githubTestsListingAllowance): a page is charged when the walk enters it, and a
// resume inside a page re-fetches it for free. Its exhaustion or a saturated second records one late_rerun_reports observation
// that keeps reports_complete false and advances the watermark: holding it would hold a busy repository for ever, and the
// regular range of the claim was walked whole.
const githubTestsLateRerunReportsComponent = "late_rerun_reports"

// The values of githubTestsChunkCursor.Relist; zero is the regular range (or no deep range).
const (
	githubTestsRelistWalking = iota + 1
	githubTestsRelistStoppedBudget
	githubTestsRelistStoppedSaturated
)

// githubTestsRelistQueryAt is the created range of the deep listing, its end moved to peeledHi when a saturated range was
// peeled.
func githubTestsRelistQueryAt(claim Claim, lookback time.Duration, peeledHi string) url.Values {
	hi := peeledHi
	if hi == "" {
		hi = githubTestsRelistFloor(claim).Add(-time.Second).Format(time.RFC3339)
	}
	return url.Values{"per_page": {"100"}, "created": {githubTestsLateRerunFloor(claim).Add(-lookback).Format(time.DateOnly) + ".." + hi}}
}

// githubTestsRelistFloor is the regular artifacts floor as a time: the start of the date githubTestsArtifactRunsCreatedFrom
// names, the date of githubTestsLateRerunFloor.
func githubTestsRelistFloor(claim Claim) time.Time {
	return githubTestsLateRerunFloor(claim).Truncate(24 * time.Hour)
}

// githubTestsRelistRunOutsideWindow is the read rule of the deep range: the incremental rule with the width floored at one day
// (githubTestsLateRerunFloor). A run with no last-updated time is refused: no late change can be shown, and the regular
// range's rule for it is unchanged (it never excludes a run it cannot date).
func githubTestsRelistRunOutsideWindow(pipeline githubTestsPipelineRow, claim Claim) bool {
	if pipeline.StartedAt.After(claim.BeforeAt.UTC()) || pipeline.FinishedAt == nil {
		return true
	}
	return pipeline.FinishedAt.UTC().Before(githubTestsLateRerunFloor(claim))
}

// githubTestsCreatedInterval is the created interval [lo, hi) of one listing range (the runs phase's,
// githubTestsRunsInterval). In the artifacts phase: the regular range
// [date(since - max(1 d, width)), before + 1 s) of a claim that lists a deep range, or that deep range [date(floor - L),
// regular floor). A zero bound is open. The two intervals of such a claim are disjoint. A claim that lists no deep range
// has an unbounded regular interval: its admission and peel are main's (githubTestsPeel), byte for byte. One interval decides, for its range, both which listed runs are admitted
// and how a full page peels, so a listing looser than its created filter never reads a run outside the interval and
// never moves the next range outside it.
type githubTestsCreatedInterval struct {
	lo, hi time.Time
	deep   bool
}

// githubTestsArtifactInterval is the created interval of the artifacts phase's regular range (deep false) or deep range.
func githubTestsArtifactInterval(claim Claim, deep bool, lookback time.Duration) githubTestsCreatedInterval {
	if deep {
		return githubTestsCreatedInterval{lo: githubTestsLateRerunFloor(claim).Add(-lookback).Truncate(24 * time.Hour), hi: githubTestsRelistFloor(claim), deep: true}
	}
	var interval githubTestsCreatedInterval
	if !githubTestsHoldsMidnight(claim) {
		return interval
	}
	interval.lo, _ = time.Parse(time.DateOnly, githubTestsArtifactRunsCreatedFrom(claim))
	interval.hi = claim.BeforeAt.UTC().Truncate(time.Second).Add(time.Second)
	return interval
}

// admitsUnder is admits with the active query's ceiling: a run created after rangeHi (the peeled end, inclusive to its
// second) belongs to an earlier, already walked range of the same listing, so a loose narrowed page cannot read it again.
func (interval githubTestsCreatedInterval) admitsUnder(created *time.Time, rangeHi string) bool {
	if !interval.admits(created) {
		return false
	}
	if created == nil || rangeHi == "" {
		return true
	}
	ceiling, err := time.Parse(time.RFC3339, rangeHi)
	return err != nil || !created.UTC().Truncate(time.Second).After(ceiling)
}

// admits reports whether a listed run with this created time belongs to the range. A run with no created time is refused by
// the deep range (GitHub cannot list it by created) and kept by the regular range (its rule never excludes a run it cannot
// date).
func (interval githubTestsCreatedInterval) admits(created *time.Time) bool {
	if created == nil {
		return !interval.deep
	}
	at := created.UTC()
	return (interval.lo.IsZero() || !at.Before(interval.lo)) && (interval.hi.IsZero() || at.Before(interval.hi))
}

// peel is githubTestsPeel inside the interval. A full page whose oldest created time is below the interval reached past
// its start, so the range is complete. A next range that would not end strictly before the current one (its end is the
// interval's when rangeHi is empty) makes no progress inside the interval: stuck, so the range stops with its observation
// and nothing outside the interval is listed.
func (interval githubTestsCreatedInterval) peel(items int, oldest, rangeHi string) (nextHi string, saturated, stuck bool) {
	if interval.lo.IsZero() && interval.hi.IsZero() {
		return githubTestsPeel(items, oldest, rangeHi)
	}
	if items < githubTestsSearchResultCap {
		return "", false, false
	}
	oldestTime, err := time.Parse(time.RFC3339, oldest)
	if err != nil {
		return "", true, true
	}
	if !interval.lo.IsZero() && oldestTime.Before(interval.lo) {
		return "", false, false
	}
	end := interval.hi
	if rangeHi != "" {
		hiTime, hiErr := time.Parse(time.RFC3339, rangeHi)
		if hiErr != nil {
			return "", true, true
		}
		end = hiTime
	} else if !end.IsZero() {
		end = end.Add(-time.Second)
	}
	if !end.IsZero() && !oldestTime.Before(end) {
		return "", true, true
	}
	return oldest, true, false
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
	if cursor.Relist == 0 && githubTestsHoldsMidnight(claim) {
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
	cursor = githubTestsHoldDeepStopAfterDeletion(cursor, client, claim, githubTestsArtifactInventoryComponent)
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
	message := "late re-run re-list stopped before its oldest runs; the watermark advances"
	for _, observation := range cursor.Incomplete {
		if _, advancing := githubTestsWatermarkAdvancingPairs[observation.Component]; !advancing {
			message = "late re-run re-list stopped before its oldest runs; the watermark holds"
		}
	}
	slog.Warn(
		message,
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", logging.ProviderText(cursor.Repo), "component", githubTestsLateRerunReportsComponent, "cause", cause,
		"range_hi", rangeHi, "items", items, "pages", cursor.ArtifactPages,
	)
	return cursor
}
