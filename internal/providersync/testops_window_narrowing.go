package providersync

import "time"

// Narrowing of the GitHub tests/cicd window after a unit stopped at the run
// inventory page budget (CHAOS-13827) or at the artifact inventory page budget
// or walk bound (CHAOS-13801).
//
// GitHub lists workflow runs newest first. A unit whose run listing
// ran out of its page budget did not reach the OLD end of its window, so it
// withholds the watermark (recordGitHubTestsInventoryTruncation). The next unit
// plans the same window again, and a source with more runs in that window than
// the budget never drains. The planner halves the next window once per
// budget-blocked unit among the source's newest units, so each unit fits the
// budget and the watermark moves.
const (
	// TestOpsNarrowingLookback is how many of a source's newest terminal units
	// count. It is long on purpose: a count that ended at the first success would
	// widen the window again after each narrowed unit and block once more, so a
	// source far behind would lose more time to blocked units than it gains.
	TestOpsNarrowingLookback = 24
	// TestOpsWindowHalvingFloor is the smallest window a halving can reach: one
	// hourly unit's natural window.
	TestOpsWindowHalvingFloor = time.Hour
)

// TestOpsBudgetBlockedUnitsSQL lists the newest terminal units of one dataset
// per source of an integration, newest first, at most $4 per source, with
// whether each one stopped at an inventory page budget. Arguments: org id,
// integration id, dataset key, per-source limit. Same per-source LATERAL top-N
// and the same columns as RecentTerminalUnitsSQL, so the same index serves it.
// The RUN and the ARTIFACT inventory budgets count (CHAOS-13801). The runs
// phase lists the window plus a fixed lookback, so a smaller window lists fewer
// runs. The artifact listing starts at the UTC date of (window start -
// max(24 h, window)) (githubTestsArtifactRunsCreatedFrom): while the window is
// over 24 h, a smaller window moves that start later and the end earlier, so
// the listing gets shorter. The walk record bound in the artifacts phase
// records the same artifact_inventory page_budget_exhausted observation
// (recordGitHubTestsArtifactsWalkBound), and a smaller window also has fewer
// records to walk. The late re-run re-list has its own budget and advances the
// watermark when it stops, so it never blocks a unit either. A per-run cap
// does not hold the watermark, and a listing that stopped inside one second
// (listing_saturated) is not cured by a smaller window.
const TestOpsBudgetBlockedUnitsSQL = `
SELECT s.id::text, recent.budget_blocked, recent.updated_at
FROM public.integration_sources AS s
CROSS JOIN LATERAL (
  SELECT u.id, u.updated_at,
         u.status = 'success' AND COALESCE(jsonb_path_exists(u.result::jsonb,
           '$.incomplete[*] ? (@.cause == "` + githubTestsPageBudgetCause + `" && (@.component == "` +
	githubTestsRunInventoryComponent + `" || @.component == "` + githubTestsArtifactInventoryComponent + `"))'), false)
           AS budget_blocked
  FROM public.sync_run_units AS u
  WHERE u.org_id = $1 AND u.integration_id = $2::uuid AND u.source_id = s.id
    AND u.dataset_key = $3 AND u.status IN ('success', 'failed')
  ORDER BY u.updated_at DESC, u.id DESC
  LIMIT $4
) AS recent
WHERE s.org_id = $1 AND s.integration_id = $2::uuid
ORDER BY s.id, recent.updated_at DESC, recent.id DESC`

// TestOpsBudgetBlockedCount counts the budget-blocked units among a source's
// newest units, given newest first, looking at most TestOpsNarrowingLookback
// units back.
func TestOpsBudgetBlockedCount(blockedNewestFirst []bool) int {
	count := 0
	for index, blocked := range blockedNewestFirst {
		if index >= TestOpsNarrowingLookback {
			break
		}
		if blocked {
			count++
		}
	}
	return count
}

// NarrowTestOpsWindow halves the span from start to end once per budget-blocked
// unit, never below TestOpsWindowHalvingFloor and never past end. The new end is
// truncated to the microsecond: PostgreSQL stores timestamptz in microseconds,
// and the materializer compares the stored end with the planned one exactly, so
// a halving that leaves a nanosecond remainder would make the persisted unit
// differ from its plan. ok is false when nothing changes: an open start, no
// blocked unit, or a span already at or below the floor.
func NarrowTestOpsWindow(start *time.Time, end time.Time, blocked int) (time.Time, bool) {
	if start == nil || blocked <= 0 {
		return end, false
	}
	span := end.Sub(*start)
	narrowed := span
	for i := 0; i < blocked && narrowed > TestOpsWindowHalvingFloor; i++ {
		narrowed /= 2
	}
	if narrowed < TestOpsWindowHalvingFloor {
		narrowed = TestOpsWindowHalvingFloor
	}
	narrowedEnd := start.Add(narrowed).Truncate(time.Microsecond)
	if !narrowedEnd.Before(end) {
		return end, false
	}
	return narrowedEnd, true
}

// TestOpsNarrowingCandidates is every end NarrowTestOpsWindow can return for the
// span from start to end with 1 to TestOpsNarrowingLookback budget-blocked
// units. A replay of an occurrence keeps the end its committed plan stored only
// when that end is one of these.
func TestOpsNarrowingCandidates(start *time.Time, end time.Time) []time.Time {
	candidates := make([]time.Time, 0, TestOpsNarrowingLookback)
	for blocked := 1; blocked <= TestOpsNarrowingLookback; blocked++ {
		candidate, ok := NarrowTestOpsWindow(start, end, blocked)
		if !ok {
			break
		}
		if len(candidates) == 0 || !candidates[len(candidates)-1].Equal(candidate) {
			candidates = append(candidates, candidate)
		}
	}
	return candidates
}
