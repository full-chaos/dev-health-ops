package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
)

// recommendationsRuleVersion mirrors the reference's default
// (recommendations/engine.py:156, loader.py:422). It is a CONSTANT rather than
// scope-supplied because the scope has no field for it: a version arriving from
// a payload would let two partitions of one run write different rule_versions
// for the same window.
const recommendationsRuleVersion = "1.0.0"

// ErrRecommendationsSchemaIncompatible refuses a database this executor cannot
// compute against.
var ErrRecommendationsSchemaIncompatible = errors.New(
	"recommendations: clickhouse schema incompatible")

// ErrRecommendationsUnavailable is the nil-connection refusal.
var ErrRecommendationsUnavailable = errors.New(
	"recommendations: clickhouse connection unavailable")

// ErrRecommendationsPostgresUnavailable is the nil-pool refusal. Distinct from
// the ClickHouse one because the remedies differ and the refusal counter is
// labelled by reason: one is a metrics-store outage, the other means the worker
// was wired without the store the readiness gate reads.
var ErrRecommendationsPostgresUnavailable = errors.New(
	"recommendations: postgres pool unavailable for the readiness gate")

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
//
// It holds BOTH stores. ClickHouse carries the metrics it reads and the rows it
// writes; PostgreSQL carries the daily-metrics run state the readiness gate
// consults. The gate is not optional and not the caller's to remember, so its
// dependency is a constructor parameter rather than something discovered at
// call time -- see NewRecommendationsExecutor.
type RecommendationsExecutor struct {
	conn   driver.Conn
	pool   *pgxpool.Pool
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

	// observer and logger are optional; the gate tolerates nil for both.
	observer ReadinessObserver
	logger   ReadinessLogger
}

// NewRecommendationsExecutor refuses at construction rather than per partition.
//
// # WHY A NIL POOL IS A REFUSAL AND NOT A SKIPPED GATE
//
// The readiness gate exists to stop recommendations evaluating against partial
// daily metrics (CHAOS-2373). An executor built without Postgres could still
// compute and write -- it would simply never check -- and the result would be
// well-formed rows that are quietly wrong, which is the exact failure the gate
// was added to prevent. Treating the missing pool as "gate unavailable, proceed"
// would therefore reproduce the incident while looking like graceful
// degradation, so it is a construction refusal: the kind goes unserved, loudly
// and countably, and its siblings stay registered.
func NewRecommendationsExecutor(
	ctx context.Context, conn driver.Conn, pool *pgxpool.Pool, orgID string,
) (*RecommendationsExecutor, error) {
	if conn == nil {
		return nil, ErrRecommendationsUnavailable
	}
	if pool == nil {
		return nil, ErrRecommendationsPostgresUnavailable
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
		pool:   pool,
		loader: loader,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// SetReadinessObserver wires the gate's telemetry. Optional, like the gate
// itself tolerates (DailyMetricsReady accepts a nil observer) -- a caller that
// never sets one still computes correctly, it just cannot see a fail-open or a
// skip happen.
func (executor *RecommendationsExecutor) SetReadinessObserver(observer ReadinessObserver) {
	executor.observer = observer
}

// SetReadinessLogger wires the gate's logging. Optional for the same reason as
// SetReadinessObserver.
func (executor *RecommendationsExecutor) SetReadinessLogger(logger ReadinessLogger) {
	executor.logger = logger
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

// nowOrRefuse is defined in executor_clock.go, alongside DORA's and
// Capacity's -- the write stamp calls it directly (recommendations_native_clickhouse.go)
// rather than through a nil-safe wallClock() accessor. See executor_clock.go's
// doc comment for why a refusal, not a time.Now() fallback, is correct here:
// a zero-valued executor is constructible in-package (what a test writes when
// it does not care about time), and NewRecommendationsExecutor sets nowUTC
// unconditionally on its only non-nil-returning path, so the field cannot be
// nil in production.

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
		return nil, ErrRecommendationsUnavailable
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

// newClickHouseOnlyExecutor builds an executor with NO Postgres pool, for tests
// that exercise the ClickHouse compute/write path and never reach the readiness
// gate.
//
// It is safe ONLY because ComputePartition refuses a nil pool outright: an
// executor built this way cannot reach ComputeOrg through the handler seam, so
// it cannot silently skip the gate. It bypasses NewRecommendationsExecutor's
// validation deliberately and is unexported for that reason -- the constructor
// is what production uses, and it refuses.
func newClickHouseOnlyExecutor(
	ctx context.Context, conn driver.Conn, orgID string,
) (*RecommendationsExecutor, error) {
	if conn == nil {
		return nil, ErrRecommendationsUnavailable
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

// ComputePartition satisfies PartitionExecutor: the seam the partition
// handler drives, and the ONLY place the readiness gate is consulted.
//
// # THE GATE LIVES HERE, NOT IN THE CALLER
//
// Putting it in the caller would make it something a future kind could forget,
// and forgetting it is silent: recommendations evaluate against partial daily
// metrics and persist well-formed rows that are simply wrong (CHAOS-2373).
// Inside ComputePartition it is unskippable by construction -- there is no route
// from the handler to ComputeOrg that does not pass through it.
//
// # A WITHHELD DAY IS A SUCCESS, NOT A FAILURE
//
// Returning an error would make the handler fail the partition and retry it,
// and the retry would read the same unfinished run and fail again -- a loop that
// looks like flapping while the correct behaviour is simply to wait for the
// fan-out to finalize. So the partition completes with zero rows, and the
// SKIPPED COUNTER is what makes that visible: without it a withheld org is
// indistinguishable from a healthy quiet one.
func (executor *RecommendationsExecutor) ComputePartition(
	ctx context.Context, run Run, partition Partition,
) (CompatibilityOutcome, error) {
	if executor == nil || executor.conn == nil {
		return CompatibilityOutcome{}, ErrRecommendationsUnavailable
	}
	if strings.TrimSpace(run.OrganizationID) == "" {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s has no organization", ErrInvalidState, partition.ID))
	}

	var scope recommendationsScope
	if err := json.Unmarshal(partition.Scope, &scope); err != nil {
		// Static format, partition ID plus the decoder's own message; carries no
		// upstream content, so it is safe to surface at WARN.
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s scope: %v", ErrInvalidState, partition.ID, err))
	}
	if scope.Window < 1 {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s window %d", ErrInvalidState, partition.ID, scope.Window))
	}

	var asOf *time.Time
	if scope.AsOf != nil {
		parsed, err := time.Parse("2006-01-02", *scope.AsOf)
		if err != nil {
			return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
				"%w: partition %s as_of %q", ErrInvalidState, partition.ID, *scope.AsOf))
		}
		asOf = &parsed
	}
	// The pool is checked HERE, adjacent to its only use, and AFTER the payload
	// has been validated. Checking it at the top made a malformed scope report
	// as "postgres unavailable" -- sending an operator to the database for a
	// fault that is in the partition's own payload and will never fix itself.
	// An error should name the thing that is actually wrong. Checked BEFORE
	// the clock resolution below too: a nil pool is a reachable production
	// wiring bug, while an unset clock is reachable only through a bare
	// zero-valued struct literal in a test -- NewRecommendationsExecutor sets
	// nowUTC unconditionally on its only non-nil-returning path, same as
	// DORA/Capacity (executor_clock.go) -- so the pool refusal is the one
	// that must fire first when both are unset, naming the fault that can
	// actually happen in production (TestAZeroValuedExecutorRefusesRatherThanPanicking).
	//
	// Unreachable through the constructor, which refuses a nil pool. Kept
	// because a zero-valued struct is constructible in-package, and the
	// alternative failure is a nil dereference inside the gate.
	if executor.pool == nil {
		return CompatibilityOutcome{}, ErrRecommendationsPostgresUnavailable
	}

	// as_of_day is what the gate keys on -- the day whose daily_metrics_runs row
	// must be finalized -- while `now` is as_of_day + 1 so the window actually
	// READS that day. They are deliberately different values; see
	// EvaluationInstant. wallClockNow is resolved via nowOrRefuse
	// (executor_clock.go) rather than a nil-safe wallClock() accessor --
	// CHAOS-4954/CHAOS-4935 merge conflict with #2188, same refuse-loud shape
	// as DORA/Capacity and this executor's own write path (ComputeOrg's
	// writeInstant).
	wallClockNow, err := executor.nowOrRefuse()
	if err != nil {
		return CompatibilityOutcome{}, err
	}
	now, asOfDay := EvaluationInstant(asOf, func() time.Time { return wallClockNow })

	if !DailyMetricsReady(
		ctx, executor.pool, run.OrganizationID, asOfDay,
		executor.observer, executor.logger,
	) {
		// Withheld, not failed. Zero rows, reported as a successful partition.
		zero := 0
		return CompatibilityOutcome{RowsWritten: &zero}, nil
	}

	teamID := ""
	if scope.TeamID != nil {
		teamID = *scope.TeamID
	}
	outcome, err := executor.ComputeOrg(
		ctx, run.OrganizationID, now, scope.Window, recommendationsRuleVersion, teamID)
	// The outcome is read even on error: ComputeOrg persists what it computed
	// BEFORE surfacing a per-team failure, so rows written on a failing run are
	// real and must be reported rather than discarded with the error.
	written := outcome.RowsWritten
	if err != nil {
		return CompatibilityOutcome{RowsWritten: &written}, err
	}
	return CompatibilityOutcome{RowsWritten: &written}, nil
}

// A compile-time pin that this executor IS the seam the handler drives. Without
// it, a signature drift would surface only where daily.go registers the kind --
// far from the code that changed.
var _ PartitionExecutor = (*RecommendationsExecutor)(nil)
