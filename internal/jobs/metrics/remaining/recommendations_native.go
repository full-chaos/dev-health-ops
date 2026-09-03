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

// ComputeTeam loads one team's window, evaluates every rule, and returns the
// full state -- fired rows and tombstones alike.
//
// It does NOT write. Persisting is the caller's step so the whole batch lands
// in one insert, matching the reference's "one scheduled run replaces the rule
// state for the team in a single, internally-consistent batch".
func (executor *RecommendationsExecutor) ComputeTeam(
	ctx context.Context, teamID, orgID string, windowDays int, ruleVersion string,
) ([]RecommendationRecord, error) {
	if executor == nil || executor.conn == nil {
		return nil, errRecommendationsUnavailable
	}
	if strings.TrimSpace(teamID) == "" {
		return nil, fmt.Errorf("%w: empty team id", ErrInvalidState)
	}
	now := executor.nowUTC()
	// window_end is now's UTC DATE and window_start is that minus the window,
	// exactly as engine.py derives them. Both are computed ONCE here: deriving
	// them per rule would let a run straddle UTC midnight and emit two
	// window_ends, which the argMax reader treats as two separate states.
	windowEnd := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowStart := windowEnd.AddDate(0, 0, -windowDays)

	snapshot, err := executor.loader.LoadTeamMetricsWindow(ctx, teamID, orgID, windowStart, windowEnd)
	if err != nil {
		return nil, err
	}
	return EvaluateState(snapshot, now, ruleVersion)
}
