package remaining

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ErrRecommendationsSchemaIncompatible refuses a database this executor cannot
// compute against.
var ErrRecommendationsSchemaIncompatible = errors.New(
	"recommendations: clickhouse schema incompatible")

// errRecommendationsUnavailable is the nil-connection refusal.
var errRecommendationsUnavailable = errors.New(
	"recommendations: clickhouse connection unavailable")

// recommendationsTableRequirements lists what each table must provide.
//
// The READ tables are the six the loader queries plus the write target. They
// are checked at CONSTRUCTION so a database this code cannot compute against
// refuses the kind once and loudly, rather than letting the handler claim
// partitions and fail every one of them -- capacity's shape
// (capacity_native.go:93-98), for the same reason.
var recommendationsTableRequirements = map[string][]string{
	"work_item_metrics_daily": {
		"day", "provider", "work_scope_id", "team_id", "org_id",
		"wip_count_end_of_day", "items_completed", "cycle_time_p50_hours", "computed_at",
	},
	"repo_metrics_daily": {
		"repo_id", "day", "org_id", "pr_cycle_p75_hours", "pr_rework_ratio", "computed_at",
	},
	"user_metrics_daily": {
		"repo_id", "day", "author_email", "team_id", "org_id", "reviews_given", "computed_at",
	},
	"team_metrics_daily": {
		"day", "team_id", "repo_id", "org_id",
		"commits_count", "after_hours_commits_count", "computed_at",
	},
	"repo_complexity_daily": {
		"repo_id", "day", "org_id", "cyclomatic_per_kloc", "computed_at",
	},
	"file_hotspot_daily": {
		"repo_id", "day", "org_id", "file_path", "risk_score", "computed_at",
	},
	"compounding_risk_daily": {
		"org_id", "day", "scope", "scope_id", "compounding_risk", "severity", "computed_at",
	},
	// The write target. Its column list is the sink's
	// (metrics/sinks/clickhouse/recommendations.py:45-58) -- if a column is
	// missing here the insert fails per row rather than at construction, which
	// is exactly the late, repeated failure the gate exists to convert into one
	// early refusal.
	"recommendations_daily": {
		"team_id", "org_id", "rule_id", "rule_version", "window_start", "window_end",
		"fired", "severity", "title", "rationale", "success_criterion",
		"evidence_json", "computed_at",
	},
}

// RecommendationsExecutor computes recommendations natively.
type RecommendationsExecutor struct {
	conn   driver.Conn
	loader *RecommendationsLoader
	nowUTC func() time.Time

	// afterTeamHook runs after each team is evaluated. It is nil in production
	// and exists solely so an integration test can cancel the run at a
	// DETERMINISTIC point -- after one team has produced records and before the
	// next is evaluated.
	//
	// The alternative is cancelling from a goroutine on a timer, which makes
	// the test's own correctness depend on how long a ClickHouse query takes.
	// That would be flaky in exactly the direction that matters: on a slow run
	// it cancels before any team finishes, the fixture stops carrying records,
	// and the test silently degenerates into the vacuous unit-level version it
	// was written to replace. A hook is honest scaffolding; a sleep is a
	// fixture whose shape changes with the weather.
	//
	// # A DOCUMENTED EXCEPTION TO "PRODUCTION CODE IS NEVER BENT FOR A MOCK"
	//
	// Ruled by the orchestrator after peer review, on this condition: prefer a
	// wrapper around an injected per-TEAM boundary, and keep this hook only if
	// no such boundary exists. It does not.
	//
	//   - `loader` is a CONCRETE *RecommendationsLoader, not an interface, so
	//     there is nothing injected here to wrap. Making it an interface to
	//     serve one test would be a larger production change than this field.
	//   - The one boundary that IS injectable, `conn driver.Conn`, is called
	//     SIX times per team by LoadTeamMetricsWindow. Cancelling on "the
	//     second call" would couple the test to the loader's internal query
	//     count -- a number with no contract, which any added metric silently
	//     changes. That is a worse seam than this one: invisible, and it breaks
	//     without anyone touching the test.
	//
	// So the seam sits at the only per-team boundary that exists, is
	// unexported, is nil in production, and is guarded at its single call site.
	afterTeamHook func()

	// beforeWriteHook runs once, after the loop's post-loop cancellation
	// sample and before the write context is chosen, and is nil in production.
	//
	// It exists for one race (round 5/CHAOS-4935 review): the sample can read
	// ctx.Err() == nil, and a real cancellation can still land in the gap
	// between that read and the write. `afterTeamHook` cannot reach this
	// window -- it only fires INSIDE the loop, before the post-loop sample
	// even runs -- so it is a different seam for a different interval, not a
	// duplicate of it. A timer-based goroutine has the same flakiness problem
	// documented on afterTeamHook above, for the same reason: how long the
	// loop takes to finish is not something a test should have to race.
	beforeWriteHook func()
}

// NewRecommendationsExecutor refuses at construction rather than per partition.
func NewRecommendationsExecutor(
	ctx context.Context, conn driver.Conn, orgID string,
) (*RecommendationsExecutor, error) {
	if conn == nil {
		return nil, errRecommendationsUnavailable
	}
	if err := verifyRecommendationsSchema(ctx, conn); err != nil {
		return nil, err
	}
	loader, err := NewRecommendationsLoader(conn, orgID)
	if err != nil {
		return nil, err
	}
	return &RecommendationsExecutor{
		conn:   conn,
		loader: loader,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// verifyRecommendationsSchema checks every table the executor reads or writes.
//
// Iterated in SORTED order rather than over the map: a map range reports a
// different table first on different runs, so one broken deployment would
// produce a different refusal message each restart and look like several
// distinct faults. Capacity orders its list explicitly for the same reason.
func verifyRecommendationsSchema(ctx context.Context, conn driver.Conn) error {
	tables := make([]string, 0, len(recommendationsTableRequirements))
	for table := range recommendationsTableRequirements {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	for _, table := range tables {
		// Reuses capacity's column reader rather than restating the
		// system.columns query. A second copy would be free to drift, which is
		// the defect the chschema interpreter duplication produced in #2142.
		present, err := capacityTableColumns(ctx, conn, table)
		if err != nil {
			return err
		}
		if len(present) == 0 {
			return fmt.Errorf("%w: table %s does not exist",
				ErrRecommendationsSchemaIncompatible, table)
		}
		var missing []string
		for _, column := range recommendationsTableRequirements[table] {
			if !present[column] {
				missing = append(missing, column)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("%w: %s is missing %s",
				ErrRecommendationsSchemaIncompatible, table, strings.Join(missing, ", "))
		}
	}
	return nil
}

// EvaluationInstant resolves (now, asOfDay) exactly as the live bridge caller
// does at api/internal/worker_metrics.py:1859-1866.
//
//	as_of given:  now = MIDNIGHT UTC on (as_of_day + 1); as_of_day = as_of
//	as_of absent: now = wall clock;                      as_of_day = now's date
//
// # WHY THE +1 IS LOAD-BEARING
//
// The engine derives window_end from now's date, and the loader treats
// window_end as EXCLUSIVE (`day < {end:Date}`). Anchoring now to as_of_day
// itself would therefore make window_end == as_of_day and exclude the very
// partition that just finalized -- every run reading one day short, silently,
// with no error anywhere (CHAOS-2373 round 2). Anchoring to as_of_day + 1 is
// what makes the finalized day actually READ.
//
// # AND WHY MIDNIGHT, NOT WALL CLOCK
//
// `time.min` in the reference is 00:00:00.
//
// CORRECTION (CHAOS-2398, found while porting the sink): an earlier version of
// this comment said `now` becomes the record's persisted computed_at. It does
// not. The engine stamps ComputedAt from `now`, and writeRecommendations then
// OVERWRITES every row with one wall-clock instant taken at write time --
// precisely BECAUSE `now` is constant across re-runs of a finalized day, which
// would leave argMax(fired, computed_at) and ReplacingMergeTree(computed_at)
// unable to pick a winner between two runs.
//
// So midnight matters for the WINDOW, not for the persisted ordering key. Using
// a wall-clock instant here would still produce the right window on the as_of
// path, but it would stop being a pure function of as_of -- and the ordering
// key it once determined is no longer this value's job at all.
func EvaluationInstant(asOf *time.Time, wallClock func() time.Time) (now, asOfDay time.Time) {
	if asOf != nil {
		day := time.Date(asOf.Year(), asOf.Month(), asOf.Day(), 0, 0, 0, 0, time.UTC)
		return day.AddDate(0, 0, 1), day
	}
	current := wallClock()
	return current, time.Date(current.Year(), current.Month(), current.Day(), 0, 0, 0, 0, time.UTC)
}

// wallClock never returns nil.
//
// EvaluationInstant and the write stamp both CALL the function they are given,
// so a zero-valued executor -- constructible in-package, and what a test writes
// when it does not care about time -- panicked with a nil dereference. A panic
// is not a refusal: the partition handler cannot classify it, and the stack
// points at the instant resolver rather than at the missing construction.
func (executor *RecommendationsExecutor) wallClock() func() time.Time {
	if executor.nowUTC != nil {
		return executor.nowUTC
	}
	return func() time.Time { return time.Now().UTC() }
}

// ComputeTeam loads one team's window, evaluates every rule, and returns the
// full state -- fired rows and tombstones alike.
//
// `now` is supplied by the caller rather than read from the clock here,
// because it is NOT wall-clock on the scheduled path: see EvaluationInstant.
// An executor that called time.Now() itself would compute a window one day
// short and there would be no error to notice.
//
// It does NOT write. Persisting is the caller's step so the whole batch lands
// in one insert, matching the reference's "one scheduled run replaces the rule
// state for the team in a single, internally-consistent batch".
func (executor *RecommendationsExecutor) ComputeTeam(
	ctx context.Context, teamID, orgID string, now time.Time, windowDays int, ruleVersion string,
) ([]RecommendationRecord, error) {
	if executor == nil || executor.conn == nil {
		return nil, errRecommendationsUnavailable
	}
	// `== ""`, NOT TrimSpace -- the same correction as ComputeOrg's branch, and
	// round 2's fix missed this second site. Python treats a whitespace team id
	// as truthy and EVALUATES it; rejecting it here made the explicit path fail
	// before the loader, so the outer fix routed correctly and then errored
	// anyway. Half a fix reads as a whole one when only the first site is tested.
	if teamID == "" {
		return nil, fmt.Errorf("%w: empty team id", ErrInvalidState)
	}
	// Both bounds derived ONCE from the caller's instant. Deriving them per
	// rule would let a run straddle UTC midnight and emit two window_ends,
	// which the argMax reader treats as two separate states.
	windowEnd := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowStart := windowEnd.AddDate(0, 0, -windowDays)

	snapshot, err := executor.loader.LoadTeamMetricsWindow(ctx, teamID, orgID, windowStart, windowEnd)
	if err != nil {
		return nil, err
	}
	return EvaluateState(snapshot, now, ruleVersion)
}
