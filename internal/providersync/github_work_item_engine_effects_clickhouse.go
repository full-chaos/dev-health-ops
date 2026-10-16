package providersync

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// These three tables are plain MergeTree tables, so FINAL does not deduplicate
// them. Every inspect() below reads back the row this process would have
// written: dedup to the newest computed_at generation for the logical
// identity (the argMax(tuple(...), computed_at) discipline this package uses
// everywhere else), never a pre-dedup or arbitrarily-chosen historical row.
// A tie at that newest generation -- more than one distinct value combination
// sharing the newest computed_at -- stays a Conflict, the same fail-closed
// rule githubWorkItemDerivedVersionOrder's callers apply throughout this
// file.

type GitHubIssueTypeMetricsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func githubWorkItemEngineNullableUUID(value *uuid.UUID) string {
	if value == nil {
		return ""
	}
	return value.String()
}

func githubIssueTypeMetricsSortingKey(row githubIssueTypeMetricsDailyRow) string {
	return strings.Join([]string{
		row.OrgID, githubWorkItemEngineNullableUUID(row.RepoID), string(row.Day),
		row.Provider, row.TeamID, row.IssueTypeNorm,
	}, "\x00")
}

func githubWorkItemEngineUInt32(value int) bool {
	return value >= 0 && uint64(value) <= math.MaxUint32
}

func githubWorkItemEngineUInt64(value int) bool { return value >= 0 }

func validGitHubIssueTypeMetricsRow(
	identity GitHubWorkItemEffectIdentity,
	row githubIssueTypeMetricsDailyRow,
) bool {
	_, dayErr := row.Day.time()
	return row.OrgID == identity.OrgID && row.Provider == identity.Provider &&
		dayErr == nil && row.TeamID != "" && row.IssueTypeNorm != "" &&
		!row.ComputedAt.IsZero() &&
		githubWorkItemEngineUInt32(row.CreatedCount) &&
		githubWorkItemEngineUInt32(row.CompletedCount) &&
		githubWorkItemEngineUInt32(row.ActiveCount)
}

func (sink GitHubIssueTypeMetricsClickHouseEffects) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := validateGitHubWorkItemDerivedEffect[githubIssueTypeMetricsDailyRow](
		identity, effect, githubIssueTypeMetricsDestination,
	)
	if err != nil || ctx == nil || sink.Conn == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	for _, row := range rows {
		if !validGitHubIssueTypeMetricsRow(identity, row) {
			return ErrInvalidConfiguration
		}
	}
	if !githubWorkItemDerivedUniqueSortingKeys(rows, githubIssueTypeMetricsSortingKey) {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, gitHubIssueTypeMetricsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		day, _ := row.Day.time()
		if err := batch.Append(
			row.RepoID, day, row.Provider, row.TeamID, row.IssueTypeNorm,
			uint32(row.CreatedCount), uint32(row.CompletedCount), uint32(row.ActiveCount),
			row.CycleP50Hours, row.CycleP90Hours, row.LeadP50Hours,
			githubWorkItemDerivedSeconds(row.ComputedAt), identity.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubIssueTypeMetricsClickHouseEffects) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := validateGitHubWorkItemDerivedEffect[githubIssueTypeMetricsDailyRow](
		identity, effect, githubIssueTypeMetricsDestination,
	)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	for _, row := range rows {
		if !validGitHubIssueTypeMetricsRow(identity, row) {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	if !githubWorkItemDerivedUniqueSortingKeys(rows, githubIssueTypeMetricsSortingKey) {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return inspectGitHubWorkItemDerivedRows(rows, func(row githubIssueTypeMetricsDailyRow) (EffectInspection, error) {
		return sink.inspect(ctx, identity, row)
	})
}

// githubIssueTypeMetricsInspectQuery builds the readback SELECT for one
// logical identity: the org filter sits in the SAME top-level statement as
// the GROUP BY that dedups replay duplicates, never a subquery an outer
// WHERE only narrows afterward.
func githubIssueTypeMetricsInspectQuery(
	orgID string, day time.Time, expected githubIssueTypeMetricsDailyRow,
) (string, []any) {
	query := `SELECT repo_id, created_count, completed_count, active_count,
cycle_p50_hours, cycle_p90_hours, lead_p50_hours, computed_at
FROM issue_type_metrics_daily
WHERE org_id = ? AND day = ? AND provider = ? AND team_id = ?
	AND issue_type_norm = ?`
	arguments := []any{
		orgID, day, expected.Provider, expected.TeamID, expected.IssueTypeNorm,
	}
	if expected.RepoID == nil {
		query += ` AND repo_id IS NULL`
	} else {
		query += ` AND repo_id = ?`
		arguments = append(arguments, *expected.RepoID)
	}
	// GROUP BY the full value tuple plus computed_at: this collapses
	// byte-identical replay duplicates into one row per generation, so the
	// caller only has to pick the newest generation and count ties on it,
	// never fold value columns from two different physical rows.
	query += ` GROUP BY repo_id, created_count, completed_count, active_count,
cycle_p50_hours, cycle_p90_hours, lead_p50_hours, computed_at`
	return query, arguments
}

func (sink GitHubIssueTypeMetricsClickHouseEffects) inspect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	expected githubIssueTypeMetricsDailyRow,
) (EffectInspection, error) {
	day, _ := expected.Day.time()
	query, arguments := githubIssueTypeMetricsInspectQuery(identity.OrgID, day, expected)
	scan, err := sink.Conn.Query(ctx, query, arguments...)
	if err != nil {
		return EffectConflict, err
	}
	defer scan.Close()
	var actual githubIssueTypeMetricsDailyRow
	found := 0
	for scan.Next() {
		candidate := githubIssueTypeMetricsDailyRow{
			Day: expected.Day, Provider: expected.Provider, TeamID: expected.TeamID,
			IssueTypeNorm: expected.IssueTypeNorm, OrgID: identity.OrgID,
		}
		var created, completed, active uint32
		if err := scan.Scan(
			&candidate.RepoID, &created, &completed, &active,
			&candidate.CycleP50Hours, &candidate.CycleP90Hours, &candidate.LeadP50Hours,
			&candidate.ComputedAt,
		); err != nil {
			return EffectConflict, err
		}
		candidate.CreatedCount = int(created)
		candidate.CompletedCount = int(completed)
		candidate.ActiveCount = int(active)
		// Dedup by version: keep the newest computed_at seen so far. A
		// candidate tied with the current newest is a second distinct value
		// combination at that same generation, so it counts toward found
		// without replacing actual.
		switch {
		case found == 0 || candidate.ComputedAt.After(actual.ComputedAt):
			actual = candidate
			found = 1
		case candidate.ComputedAt.Equal(actual.ComputedAt):
			found++
		}
	}
	if err := scan.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitHubIssueTypeMetricsVersion(
		expected, actual, found, identity.OrgID,
	), nil
}

func compareGitHubIssueTypeMetricsVersion(
	expected, actual githubIssueTypeMetricsDailyRow,
	found int,
	orgID string,
) EffectInspection {
	if found != 1 {
		if found == 0 {
			return EffectAbsent
		}
		// More than one distinct value combination survived at the newest
		// computed_at: the newest generation itself is ambiguous.
		return EffectConflict
	}
	if actual.OrgID != orgID {
		return EffectConflict
	}
	if verdict, decided := githubWorkItemDerivedVersionOrder(
		actual.ComputedAt, githubWorkItemDerivedSeconds(expected.ComputedAt),
	); decided {
		return verdict
	}
	if actual.Day != expected.Day || actual.Provider != expected.Provider ||
		actual.TeamID != expected.TeamID || actual.IssueTypeNorm != expected.IssueTypeNorm ||
		!githubWorkItemEngineUUIDPointerEqual(actual.RepoID, expected.RepoID) ||
		actual.CreatedCount != expected.CreatedCount ||
		actual.CompletedCount != expected.CompletedCount ||
		actual.ActiveCount != expected.ActiveCount ||
		actual.CycleP50Hours != expected.CycleP50Hours ||
		actual.CycleP90Hours != expected.CycleP90Hours ||
		!githubWorkItemEngineFloatPointerEqual(actual.LeadP50Hours, expected.LeadP50Hours) {
		return EffectConflict
	}
	return EffectExact
}

func githubWorkItemEngineFloatPointerEqual(left, right *float64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func githubWorkItemEngineUUIDPointerEqual(left, right *uuid.UUID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

var (
	_ GitHubWorkItemEffectAdapter = GitHubIssueTypeMetricsClickHouseEffects{}
)

// The INSERT statement has one definition, shared with the daily families:
// internal/jobs/metrics/workitemengine.
const gitHubIssueTypeMetricsInsert = workitemengine.IssueTypeMetricsInsert
