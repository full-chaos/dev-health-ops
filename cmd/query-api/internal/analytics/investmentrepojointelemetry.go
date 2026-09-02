package analytics

// CHAOS-4773 telemetry: the repo dedup fix in investment.go/sankeycoverage.go
// (adding FINAL + org_id to the `repos` join) removes the ONLY join in the
// investment query path capable of fanning out on an unmerged ReplacingMergeTree
// row (see investmentContextFor's doc comment on that join for the root cause
// and the executed repro). Root AGENTS.md standing order: new logic ships with
// its telemetry in the same PR -- this is that telemetry, for the class of
// input this fix defends against rather than for the fix's own code path
// (which, once FINAL is in place, has nothing left to observe: the dedup
// happens server-side and produces no distinguishable Go-side signal).
//
// WHAT THIS RECORDS: the number of repos-table rows an org currently has
// with more than one live (pre-merge) physical version -- i.e. exactly the
// condition that, before this fix, silently doubled (or worse) every
// investment aggregate referencing that repo. Post-fix this should almost
// always be 0 (any org, most of the time -- `repos` merges collapse a
// duplicate within tens of minutes per the CHAOS-4773 investigation's
// system.part_log evidence), so a non-zero reading is a genuine, actionable
// operator signal: it names a *count* of currently-unmerged repo duplicates,
// not a request-scoped inference, and is safe to over-fire (a false-negative
// window between the check and the real query executing does not matter --
// the real query is now dedup-safe regardless of what this check reports).
//
// This does NOT special-case the it-was-actually-triggering-an-over-count
// case: detecting a genuinely inflated PER-UNIT value would require
// re-running the whole per-unit decomposition (guard 2's technique) on every
// request, which is exactly the expensive, request-blocking measurement this
// telemetry is designed to avoid needing. The `repos` duplicate count is the
// precise, cheap proxy for it: zero duplicates makes the fan-out this ticket
// found structurally impossible regardless of any other query shape, and a
// non-zero count is the earliest possible warning that the mechanism this
// class of bug depends on is present again (a NEW unguarded join elsewhere,
// a regression removing FINAL, or unusually slow merges under load).

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// repoJoinDedupCollisionCooldown bounds this telemetry to one extra
// ClickHouse round-trip per org per window rather than one per request
// (team-lead review note, CHAOS-4773: a per-request scan on top of the real
// query -- however cheap `repos` itself is for a given org -- is a second
// scan this fix must not add unconditionally). `repos` is a per-org
// dimension table (tens to low hundreds of rows even for a large org, never
// a per-work-unit fact table), and the condition this check watches for
// (an unmerged duplicate) only changes on the scale of a sync cycle, so a
// short cooldown loses no real signal.
const repoJoinDedupCollisionCooldown = 5 * time.Minute

// repoJoinDedupCollisionErrorCooldown replaces a just-claimed FULL cooldown
// when the check itself fails (query error, mid-stream failure, or a scan
// error) -- codex round 4 (P3, CONFIRMED), mirroring the sibling CHAOS-4759
// guard's identical fix (argMaxNullTransitionErrorCooldown): claiming the
// full 5-minute cooldown BEFORE running the query means a transient
// ClickHouse error (a blip, a deploy-time restart) would otherwise silence
// this check for the full window with nothing but an invisible debug log to
// show for it. A short error cooldown lets the NEXT request retry soon
// while still bounding retry storms during a sustained outage.
const repoJoinDedupCollisionErrorCooldown = 1 * time.Minute

// repoJoinDedupCollisionLastChecked tracks, per org, the next time the
// check query is allowed to run -- a plain mutex-guarded map rather than
// sync.Map: the value is a fixed-size comparable (time.Time), there is no
// high-cardinality churn (bounded by live org count), and a mutex keeps the
// claim-then-maybe-shorten sequence race-free (see
// repoJoinDedupCollisionShorten's doc comment).
var (
	repoJoinDedupCollisionMu          sync.Mutex
	repoJoinDedupCollisionLastChecked = map[string]time.Time{}
	// repoJoinDedupCollisionNow is overridable in tests so the cooldown
	// itself is exercisable without a real 5-minute sleep.
	repoJoinDedupCollisionNow = time.Now
)

// repoJoinDedupCollisionShouldCheck reports whether orgID is due for
// another check, and if so claims the FULL cooldown NOW (single map access
// under the lock, so two concurrent callers cannot both pass the gate for
// the same org at the same instant). A failed check shortens this claim via
// repoJoinDedupCollisionShorten below.
func repoJoinDedupCollisionShouldCheck(orgID string) bool {
	repoJoinDedupCollisionMu.Lock()
	defer repoJoinDedupCollisionMu.Unlock()
	now := repoJoinDedupCollisionNow()
	if next, ok := repoJoinDedupCollisionLastChecked[orgID]; ok && now.Before(next) {
		return false
	}
	repoJoinDedupCollisionLastChecked[orgID] = now.Add(repoJoinDedupCollisionCooldown)
	return true
}

// repoJoinDedupCollisionShorten replaces a just-claimed FULL cooldown with
// the SHORT error cooldown. Unconditional: this is only ever called
// immediately after this same goroutine's own repoJoinDedupCollisionShouldCheck
// succeeded for orgID, so there is no other claimant's window to
// accidentally shorten.
func repoJoinDedupCollisionShorten(orgID string) {
	repoJoinDedupCollisionMu.Lock()
	defer repoJoinDedupCollisionMu.Unlock()
	repoJoinDedupCollisionLastChecked[orgID] = repoJoinDedupCollisionNow().Add(repoJoinDedupCollisionErrorCooldown)
}

// repoJoinDedupCollisionsCounter counts CHECK INVOCATIONS that found at
// least one repos id with more than one live physical version for the
// queried org -- not the raw excess-row count itself (that varies with
// merge timing far more than with anything actionable), so the counter
// answers "how often does this org's investment traffic run into an
// unmerged repos duplicate," a rate an alert can threshold on.
var repoJoinDedupCollisionsCounter = mustAnalyticsCounter(
	"devhealth_query_api_investment_repo_join_dedup_collisions_total",
	"investment-path queries where the org's repos table currently holds an unmerged (pre-FINAL) duplicate physical row for at least one repo id -- CHAOS-4773; the join itself is deduped so this is an early-warning signal, not a defect indicator",
)

// codex round 2 (P2, CONFIRMED): count() is UInt64; the native clickhouse-go
// driver rejects a UInt64->*int64 Scan destination outright (the same
// type-exactness trap this repo's own lane-common-brief documents --
// precedent: investmentmembershipscope.go's lag_seconds). Wrapping in
// toInt64(...) here, matching that precedent, so the *int64 destination
// below is valid against the REAL driver, not just the fake test scanner
// (which does not enforce wire-type exactness and is why round 1's own
// green test did not catch this). This SQL never returned to any caller of
// a real cluster before this fix -- the fail-open swallow on the Scan
// error meant the check silently never fired, at all, in production.
const investmentRepoDedupCollisionCheckSQL = `
SELECT toInt64(count()) AS excess_repo_versions
FROM (
    SELECT id, count() AS versions
    FROM repos
    WHERE org_id = {org_id:String}
    GROUP BY id
    HAVING versions > 1
)
SETTINGS max_execution_time = 5
`

// recordInvestmentRepoJoinDedupCollisions is a package var, same pattern as
// telemetry.go's recordDegradation and investmentmembershiptelemetry.go's
// recordStaleInvestmentMembershipScope, and for the same reason: whether the
// check fired is the only thing a test can observe (the real query's
// correctness no longer depends on this signal at all now that the join
// carries FINAL), so the report must be injectable.
var recordInvestmentRepoJoinDedupCollisions = defaultRecordInvestmentRepoJoinDedupCollisions

func defaultRecordInvestmentRepoJoinDedupCollisions(ctx context.Context, orgID string, excessRepoIDs int64) {
	repoJoinDedupCollisionsCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("org_id", orgID)))
	slog.WarnContext(ctx, "investment repo join: org currently has an unmerged repos duplicate; the join is FINAL-deduped so results are still correct, but this names the window CHAOS-4773's fan-out class depended on",
		"org_id", orgID, "excess_repo_ids", excessRepoIDs)
}

// repoJoinDedupCollisionFetchFailedCounter counts check failures -- codex
// round 4 (P3, CONFIRMED), mirroring the sibling CHAOS-4759 guard's
// identical fix (argMaxNullTransitionFetchFailedCounter): a debug-only log
// line is invisible at this platform's default INFO level
// (DEV_HEALTH_LOG_LEVEL), so a persistently broken check -- a permissions
// change, a network partition -- could silently stop observing this org's
// repos-duplicate state forever, with zero operator-visible signal AND (see
// repoJoinDedupCollisionErrorCooldown) only a short, bounded retry window
// rather than the full 5-minute cooldown silently swallowing the outage.
var repoJoinDedupCollisionFetchFailedCounter = mustAnalyticsCounter(
	"devhealth_query_api_investment_repo_join_dedup_collision_check_failed_total",
	"repo-join dedup-collision checks that errored before a collision could even be checked -- a sustained non-zero rate means this org's repos-duplicate state is not being observed at all",
)

// recordRepoJoinDedupCollisionFetchFailure is a package var for the same
// injectability reason as recordInvestmentRepoJoinDedupCollisions.
var recordRepoJoinDedupCollisionFetchFailure = defaultRecordRepoJoinDedupCollisionFetchFailure

func defaultRecordRepoJoinDedupCollisionFetchFailure(ctx context.Context, orgID string, err error) {
	repoJoinDedupCollisionFetchFailedCounter.Add(ctx, 1)
	slog.WarnContext(ctx, "investment repo join dedup-collision check failed; this org's repos-duplicate state is NOT being observed",
		"org_id", orgID, "error", err)
}

// RecordInvestmentRepoJoinDedupCollisions runs the cheap check above and
// reports only when it finds at least one colliding repo id. Every failure
// path shortens the org's cooldown (repoJoinDedupCollisionShorten) and
// reports through recordRepoJoinDedupCollisionFetchFailure (counter + warn
// log) -- it is never propagated, since an observability query must never
// be able to break or slow the real resolver path it decorates.
//
// CALLED FROM resolveSankey, resolveOneTimeseries, resolveOneBreakdown and
// resolveSankeyCoverage (resolve.go / sankeycoverage.go), each guarded on
// useInvestment && the REPO dimension being present in the dimensions list
// that specific compile call passes to investmentContextFor -- the same
// condition investmentContextFor uses to decide whether the repos join is
// even in the compiled query. Deliberately NOT called from
// resolveFlowMatrix: see that function's doc comment (CHAOS-4773, codex
// round 2) for why flow matrix never compiles this join for any dimension.
func RecordInvestmentRepoJoinDedupCollisions(ctx context.Context, client QueryClient, orgID string) {
	if orgID == "" || client == nil {
		return
	}
	if !repoJoinDedupCollisionShouldCheck(orgID) {
		return
	}
	rows, err := client.Query(ctx, investmentRepoDedupCollisionCheckSQL, bindingsForOrg(orgID))
	if err != nil {
		repoJoinDedupCollisionShorten(orgID)
		recordRepoJoinDedupCollisionFetchFailure(ctx, orgID, err)
		return
	}
	defer rows.Close()

	if !rows.Next() {
		// A mid-stream failure (connection dropped while the aggregate
		// row was in flight) surfaces as Next()==false with a non-nil
		// Err(), same shape FetchInvestmentMembershipScopeState guards
		// against -- without this check it is indistinguishable from
		// the check legitimately returning zero rows (it never does:
		// count() always yields exactly one row, even over an empty
		// input set) and would silently drop the failure this
		// telemetry exists to never let happen silently.
		if err := rows.Err(); err != nil {
			repoJoinDedupCollisionShorten(orgID)
			recordRepoJoinDedupCollisionFetchFailure(ctx, orgID, err)
		}
		return
	}
	var excess int64
	if scanErr := rows.Scan(&excess); scanErr != nil {
		repoJoinDedupCollisionShorten(orgID)
		recordRepoJoinDedupCollisionFetchFailure(ctx, orgID, scanErr)
		return
	}
	if err := rows.Err(); err != nil {
		repoJoinDedupCollisionShorten(orgID)
		recordRepoJoinDedupCollisionFetchFailure(ctx, orgID, err)
		return
	}
	if excess <= 0 {
		return
	}
	recordInvestmentRepoJoinDedupCollisions(ctx, orgID, excess)
}
