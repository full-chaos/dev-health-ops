package remaining

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// recommendationsDetachedWriteTimeout bounds the post-cancellation write.
const recommendationsDetachedWriteTimeout = 15 * time.Second

// discoverTeamIDsSQL mirrors _discover_team_ids
// (workers/recommendations_tasks.py:319-324).
//
// Sourced from work_item_metrics_daily -- the same table that feeds the
// snapshot signals -- so only teams that actually have data are evaluated. FINAL
// is the reference's, and is kept: without it the DISTINCT sees every
// unmerged ReplacingMergeTree version and can return a team whose only rows
// were superseded.
//
// The 30-day recency bound is deliberately WIDER than the evaluation window. A
// team that fired last week but has been quiet since must still be evaluated,
// because that is precisely the team owed a fired=false tombstone to clear its
// stale guidance. Narrowing this to the window would silently strand exactly
// the teams the tombstones exist for.
const discoverTeamIDsSQL = `
    SELECT DISTINCT team_id
    FROM work_item_metrics_daily FINAL
    WHERE day >= today() - 30
      AND team_id != ''`

// DiscoverTeamIDs returns the teams with recent activity for an org.
//
// The org predicate is appended only for a real org, matching the reference:
// "default" is the single-tenant sentinel and is not a value present in
// org_id, so filtering on it would return no teams and silently evaluate
// nothing at all.
func (executor *RecommendationsExecutor) DiscoverTeamIDs(
	ctx context.Context, orgID string,
) ([]string, error) {
	if executor == nil || executor.conn == nil {
		return nil, errRecommendationsUnavailable
	}

	query := discoverTeamIDsSQL
	arguments := []any{}
	if orgID != "" && orgID != "default" {
		query += "\n      AND org_id = ?"
		arguments = append(arguments, orgID)
	}

	rows, err := executor.conn.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("discover recommendation team ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var teamIDs []string
	for rows.Next() {
		var teamID string
		if err := rows.Scan(&teamID); err != nil {
			return nil, fmt.Errorf("scan recommendation team id: %w", err)
		}
		if teamID != "" {
			teamIDs = append(teamIDs, teamID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recommendation team ids: %w", err)
	}

	// SELECT DISTINCT has no inherent order. Sorting makes a run's team
	// sequence reproducible, which is what lets a failed-team list be compared
	// between runs and keeps the batch's row order stable for diffing.
	sort.Strings(teamIDs)
	return teamIDs, nil
}

// TeamEvaluationFailure names the teams whose evaluation raised.
//
// A per-team error is NOT swallowed. A team that fails to evaluate writes no
// fired=false tombstone, so its stale fired guidance lingers while the job
// reports success and neither monitoring nor retries see anything wrong
// (CHAOS-2373 round 2). The reference raises for exactly this reason and so
// does this port.
// The failed team IDs are carried because they are what makes the error
// ACTIONABLE -- an operator needs to know which teams hold stale guidance. The
// COUNTS deliberately do not live here: they belong on OrgOutcome, which the
// caller receives alongside this error, so there is one place a ledger reads
// them from rather than two that can disagree.
type TeamEvaluationFailure struct {
	OrgID       string
	FailedTeams []string
}

func (failure *TeamEvaluationFailure) Error() string {
	return fmt.Sprintf(
		"recommendations: %d team(s) failed to evaluate for org %s: %s",
		len(failure.FailedTeams), failure.OrgID,
		strings.Join(failure.FailedTeams, ", "))
}

// OrgOutcome reports what one org's run did, whether or not it then failed.
type OrgOutcome struct {
	Teams       int
	FailedTeams int
	FiredRows   int
	RowsWritten int
}

// ComputeOrg evaluates every team in an org and writes the batch.
//
// # THE ORDER OF THE LAST TWO STEPS IS THE CONTRACT
//
// Records are PERSISTED BEFORE any per-team failure is surfaced, matching the
// reference. The teams that evaluated cleanly get their fresh tombstones this
// run even though the run as a whole then fails; returning early on the first
// bad team would withhold good state from every team, turning one team's
// loader fault into an org-wide stale-guidance incident.
func (executor *RecommendationsExecutor) ComputeOrg(
	ctx context.Context,
	orgID string,
	now time.Time,
	windowDays int,
	ruleVersion string,
	teamID string,
) (OrgOutcome, error) {
	if executor == nil || executor.conn == nil {
		return OrgOutcome{}, errRecommendationsUnavailable
	}

	// `teamID == ""`, NOT TrimSpace. Python branches on ordinary truthiness
	// (`[team_id] if team_id else discover(...)`), so a whitespace-only id is
	// EXPLICIT there and scopes the run to that one team. Trimming would send a
	// malformed team-scoped payload down the discovery path instead and persist
	// recommendations for every team in the org -- a scope widening, from a
	// defensive-looking normalisation.
	teamIDs := []string{teamID}
	if teamID == "" {
		discovered, err := executor.DiscoverTeamIDs(ctx, orgID)
		if err != nil {
			return OrgOutcome{}, err
		}
		teamIDs = discovered
	}
	if len(teamIDs) == 0 {
		// Not an error: an org with no recent activity has nothing to say.
		return OrgOutcome{}, nil
	}

	outcome := OrgOutcome{Teams: len(teamIDs)}
	var records []RecommendationRecord
	var failedTeams []string
	var cancelled error

	for _, currentTeam := range teamIDs {
		teamRecords, err := executor.computeTeam(
			ctx, currentTeam, orgID, now, windowDays, ruleVersion)
		if err != nil {
			// A context cancellation is the run being torn down, not this
			// team failing; continuing would loop through every remaining
			// team producing the same error and report them all as faulty.
			//
			// But it must not DISCARD what earlier clean teams already
			// produced. The reference has no cancellation concept and always
			// reaches its write; returning here would lose those rows and
			// report cancellation instead of the per-team failure, so the
			// teams that evaluated cleanly would keep their stale fired
			// guidance -- the CHAOS-2373 outcome, reached by a Go-only path.
			// REGRESSION GUARD (round 3 P1). An earlier version of this
			// block set `cancelled = ctx.Err()` and broke unconditionally.
			// With a live context that value is NIL, so an ordinary per-team
			// loader or rule failure recorded NO failed team, skipped every
			// remaining team, and returned SUCCESS -- a silent failure
			// introduced while fixing a different one. The two cases must be
			// distinguished explicitly, not inferred from one call.
			if contextErr := ctx.Err(); contextErr != nil {
				cancelled = contextErr
				break
			}
			failedTeams = append(failedTeams, currentTeam)
			continue
		}
		records = append(records, teamRecords...)
		for _, record := range teamRecords {
			if record.Fired {
				outcome.FiredRows++
			}
		}
		// Nil in production. See the field's comment.
		if executor.afterTeamHook != nil {
			executor.afterTeamHook()
		}
	}
	outcome.FailedTeams = len(failedTeams)

	// CANCELLATION IS RE-CHECKED HERE, NOT ONLY ON A FAILING TEAM.
	//
	// The loop above learns of cancellation only through a team that ERRORS.
	// If the last team SUCCEEDS and the context is cancelled immediately after,
	// the loop exits normally, `cancelled` stays nil, and the write below runs
	// on the cancelled context -- failing, and losing exactly the rows this
	// path exists to keep. The final-team boundary has no next iteration to
	// notice anything.
	//
	// Found in review (round 4 P1). The container test cancels after the first
	// of two teams, so the second observes the cancellation through its own
	// error and the boundary is never reached: the fixture was one team short
	// of the case, which is why the earlier proof passed.
	if cancelled == nil {
		cancelled = ctx.Err()
	}

	// The write context is DETACHED from cancellation, and bounded. A cancelled
	// context cannot execute the insert at all, so without this the rows are
	// lost exactly when the run is being torn down -- which is when a partial
	// result is most worth keeping. The bound is what stops a detached context
	// from outliving the shutdown it was detached from.
	writeCtx := ctx
	var stopWrite context.CancelFunc = func() {}
	if cancelled != nil {
		writeCtx, stopWrite = context.WithTimeout(
			context.WithoutCancel(ctx), recommendationsDetachedWriteTimeout)
	}
	written, err := executor.writeRecommendations(writeCtx, records, executor.wallClock()())
	stopWrite()
	if err != nil {
		// A WRITE FAILURE OUTRANKS THE CANCELLATION, deliberately. On a run that
		// was both interrupted and failed to persist, the caller sees the write
		// error rather than context.Canceled.
		//
		// That is the more actionable of the two: a cancellation is an orderly
		// teardown needing no intervention, while a failed insert means the
		// clean teams' tombstones did NOT land and their stale fired guidance
		// survives -- the outcome this whole path exists to prevent. Reporting
		// the cancellation would describe why the run stopped and hide what it
		// lost.
		//
		// Nothing is dropped silently: outcome carries Teams, FailedTeams and
		// RowsWritten, and the caller receives it alongside this error.
		return outcome, err
	}
	outcome.RowsWritten = written

	// Cancellation is reported AFTER the write, and takes precedence over a
	// per-team failure: the run did not finish, so its team list is incomplete
	// and a TeamEvaluationFailure would understate what went unevaluated.
	if cancelled != nil {
		return outcome, cancelled
	}

	if len(failedTeams) > 0 {
		return outcome, &TeamEvaluationFailure{
			OrgID:       orgID,
			FailedTeams: failedTeams,
		}
	}
	return outcome, nil
}

// computeTeam dispatches to the test double when one is installed.
//
// Nil in production, so this is ComputeTeam by another name there.
func (executor *RecommendationsExecutor) computeTeam(
	ctx context.Context, teamID, orgID string, now time.Time,
	windowDays int, ruleVersion string,
) ([]RecommendationRecord, error) {
	if executor.computeTeamForTest != nil {
		return executor.computeTeamForTest(teamID)
	}
	return executor.ComputeTeam(ctx, teamID, orgID, now, windowDays, ruleVersion)
}

// writeRecommendations stamps and inserts the batch.
//
// # WHY computed_at IS OVERWRITTEN HERE (CHAOS-2398)
//
// The engine derives BOTH window_end and computed_at from `now`, and on the
// as_of path `now` is as_of_day + 1 -- a CONSTANT across re-runs of the same
// finalized day. Two runs would then write rows with an identical computed_at,
// and neither the read side's argMax(fired, computed_at) nor
// ReplacingMergeTree(computed_at) could deterministically pick the later one,
// so a recovered signal might never clear.
//
// A single monotonic write timestamp per run makes the most recent write always
// win, while window_end stays a pure function of as_of. True retries rewrite
// identical content under a newer stamp: idempotent in effect, deterministic in
// winner.
//
// The stamp is taken ONCE for the whole batch, not per row. Per-row stamps
// would split one internally-consistent replacement across several argMax
// generations, so a reader could observe one rule's new state beside another's
// old one -- the torn read the single-batch design exists to prevent.
func (executor *RecommendationsExecutor) writeRecommendations(
	ctx context.Context, records []RecommendationRecord, writeTime time.Time,
) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	// Column order and NAMES follow the Python sink's list exactly
	// (metrics/sinks/clickhouse/recommendations.py:45-58). ClickHouse binds by
	// POSITION, so two same-typed columns swapped here would not fail -- they
	// would write silently crossed values, and title/rationale/severity are all
	// strings sitting next to each other.
	batch, err := executor.conn.PrepareBatch(ctx, `
        INSERT INTO recommendations_daily (
            team_id, org_id, rule_id, rule_version,
            window_start, window_end, fired, severity,
            title, rationale, success_criterion, evidence_json, computed_at
        )`)
	if err != nil {
		return 0, fmt.Errorf("prepare recommendations batch: %w", err)
	}
	for _, record := range records {
		if err := batch.Append(
			record.TeamID, record.OrgID, record.RuleID, record.RuleVersion,
			record.WindowStart, record.WindowEnd,
			// A NATIVE bool, not boolToUInt8. recommendations_daily.fired is
			// declared Bool, unlike capacity_forecasts' UInt8 flags whose
			// append this otherwise mirrors -- and the driver refuses the
			// narrowing outright ("converting uint8 to Bool is unsupported").
			// Copying the sibling's helper along with its shape is what put
			// the wrong type here.
			record.Fired, record.Severity,
			record.Title, record.Rationale, record.SuccessCriterion,
			record.EvidenceJSON,
			// The write stamp, NOT record.ComputedAt -- see above.
			writeTime,
		); err != nil {
			return 0, fmt.Errorf("append recommendation row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send recommendations batch: %w", err)
	}
	return len(records), nil
}
