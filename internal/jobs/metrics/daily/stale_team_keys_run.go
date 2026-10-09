package daily

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aigovernance"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// StaleKeyRetractor supersedes, once for a run, the team keys of the day that
// the run's partitions no longer produce, in the tables whose keys more than
// one partition of a run can write. The finalize handler calls it after every
// partition of the run is done and before the finalize families.
type StaleKeyRetractor interface {
	RetractStaleKeys(ctx context.Context, run Run) (rowsWritten int, err error)
}

// RunStaleKeyRetractor is the StaleKeyRetractor of the daily families.
//
// Four tables are shared between the partitions of a run:
//
//   - work_item_metrics_daily, work_item_state_durations_daily and
//     estimate_coverage_metrics_daily: a partition computes every work scope
//     that its repositories have an item in, from the items of every
//     repository (loadWorkItemPartitionScopes), so two partitions whose
//     repositories share a work scope both write the rows of that scope;
//   - ai_governance_coverage_daily: every partition computes the
//     organization's whole day.
//
// See retractStaleTeamKeysOfRun for why a partition cannot decide such a key
// and for the order of the reads. The tables whose key scope is the
// partition's own repository (team_metrics_daily, ai_impact_metrics_daily)
// keep the rule in their family: a repository is in one partition of a run.
type RunStaleKeyRetractor struct {
	conn   driver.Conn
	nowUTC func() time.Time
}

var errRunStaleKeyRetractorUnavailable = errors.New("daily stale team key retractor unavailable")

// RunStaleKeyTables are the tables RunStaleKeyRetractor decides. The census
// (stale_team_keys_census_test.go) holds that no partition family calls the
// rule for one of them.
func RunStaleKeyTables() []StaleKeyTable {
	return []StaleKeyTable{
		teamkeytables.WorkItemMetricsDaily,
		teamkeytables.EstimateCoverageMetricsDaily,
		teamkeytables.WorkItemStateDurationsDaily,
		teamkeytables.AIGovernanceCoverageDaily,
	}
}

// NewRunStaleKeyRetractor fails closed on a nil connection.
func NewRunStaleKeyRetractor(conn driver.Conn) (*RunStaleKeyRetractor, error) {
	if conn == nil {
		return nil, errRunStaleKeyRetractorUnavailable
	}
	return &RunStaleKeyRetractor{conn: conn, nowUTC: func() time.Time { return time.Now().UTC() }}, nil
}

// StaleKeysRetractedLogMessage is the one line a run writes when its
// retraction is done.
const StaleKeysRetractedLogMessage = "daily stale team keys retracted"

// RetractStaleKeys implements StaleKeyRetractor. run.DiscoveredRepoIDs is the
// union of the run's partitions: the work scopes of those repositories are
// the scope of the three work-item tables. A run with no repository computed
// no work scope and supersedes no work-item key.
//
// It stops at the first failure and returns the rows written so far. A
// repeated call is safe: a key whose newest row is a row of zeros is not live.
func (retractor *RunStaleKeyRetractor) RetractStaleKeys(ctx context.Context, run Run) (int, error) {
	if retractor == nil || retractor.conn == nil {
		return 0, errRunStaleKeyRetractorUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: run has no organization or target day", ErrInvalidState)
	}
	partition := Partition{ID: run.ID, RunID: run.ID, RepoIDs: run.DiscoveredRepoIDs}
	scope, err := newWorkItemPartitionScope(run, partition, "stale_team_keys")
	if err != nil {
		return 0, err
	}
	day := scope.day
	conn := retractor.conn

	workItemKeys := func(keyCtx context.Context) (staleKeyScope, []staleKey, error) {
		read, triplet, err := computeWorkItemTriplet(keyCtx, conn, run, partition, scope)
		if err != nil {
			return nil, nil, err
		}
		keys := make([]staleKey, 0, len(triplet.MetricsDaily))
		for _, row := range triplet.MetricsDaily {
			keys = append(keys, staleKey{row.Provider, row.WorkScopeID, row.TeamID})
		}
		return read.staleKeyScope(), keys, nil
	}
	estimateKeys := func(keyCtx context.Context) (staleKeyScope, []staleKey, error) {
		read, rows, err := computeWorkItemEstimateRows(keyCtx, conn, run, partition, scope)
		if err != nil {
			return nil, nil, err
		}
		keys := make([]staleKey, 0, len(rows))
		for _, row := range rows {
			keys = append(keys, staleKey{row.Provider, row.WorkScopeID, row.TeamID})
		}
		return read.staleKeyScope(), keys, nil
	}
	stateKeys := func(keyCtx context.Context) (staleKeyScope, []staleKey, error) {
		computed, err := computeWorkItemStateRows(keyCtx, conn, run, partition, scope, retractor.nowUTC())
		if err != nil {
			return nil, nil, err
		}
		keys := make([]staleKey, 0, len(computed.rows))
		for _, row := range computed.rows {
			keys = append(keys, staleKey{row.Provider, row.WorkScopeID, row.TeamID, row.Status})
		}
		return computed.read.staleKeyScope(), keys, nil
	}
	governanceKeys := func(keyCtx context.Context) (staleKeyScope, []staleKey, error) {
		// The window of the family: the day, inclusive of its last
		// microsecond (see AIGovernanceExecutor).
		artifacts, err := LoadGovernanceArtifacts(keyCtx, conn, run.OrganizationID, day, day.Add(24*time.Hour-time.Microsecond))
		if err != nil {
			return nil, nil, err
		}
		coverage := aigovernance.RollupCoverageDaily(artifacts, day)
		keys := make([]staleKey, 0, len(coverage))
		for _, row := range coverage {
			teamID, repoID := "", uuid.Nil
			if row.TeamID != nil {
				teamID = *row.TeamID
			}
			if row.RepoID != nil {
				repoID = *row.RepoID
			}
			keys = append(keys, staleKey{teamID, repoID.String()})
		}
		return nil, keys, nil
	}

	// Each step names its table in its call, so the census can read which
	// table the run-level rule is applied to.
	//
	// The version of the rows of zeros is read when the step starts, on this
	// host's clock. It only orders the row of zeros after the row it
	// supersedes; which keys are superseded does not depend on it.
	steps := []func() (string, int, error){
		func() (string, int, error) {
			rows, err := retractStaleTeamKeysOfRun(ctx, conn, teamkeytables.WorkItemMetricsDaily,
				run.OrganizationID, day, workItemKeys, retractor.nowUTC())
			return teamkeytables.WorkItemMetricsDaily.Table, rows, err
		},
		func() (string, int, error) {
			rows, err := retractStaleTeamKeysOfRun(ctx, conn, teamkeytables.EstimateCoverageMetricsDaily,
				run.OrganizationID, day, estimateKeys, retractor.nowUTC())
			return teamkeytables.EstimateCoverageMetricsDaily.Table, rows, err
		},
		func() (string, int, error) {
			rows, err := retractStaleTeamKeysOfRun(ctx, conn, teamkeytables.WorkItemStateDurationsDaily,
				run.OrganizationID, day, stateKeys, retractor.nowUTC())
			return teamkeytables.WorkItemStateDurationsDaily.Table, rows, err
		},
		func() (string, int, error) {
			rows, err := retractStaleTeamKeysOfRun(ctx, conn, teamkeytables.AIGovernanceCoverageDaily,
				run.OrganizationID, day, governanceKeys, retractor.nowUTC())
			return teamkeytables.AIGovernanceCoverageDaily.Table, rows, err
		},
	}
	total := 0
	written := make(map[string]int, len(steps))
	for _, step := range steps {
		table, rows, err := step()
		total += rows
		written[table] = rows
		if err != nil {
			slog.Default().Error("daily stale team keys retraction failed",
				"run_id", run.ID, "organization_id", run.OrganizationID,
				"target_day", day.Format("2006-01-02"), "table", table,
				"rows_written", total, "error", err,
			)
			if total == 0 {
				return 0, fmt.Errorf("retract stale team keys of %s: %w", table, err)
			}
			return total, fmt.Errorf("%w: stale team keys of %s failed after %d row(s) already landed: %w",
				ErrPartialWrite, table, total, err)
		}
	}
	slog.Default().Info(StaleKeysRetractedLogMessage,
		"run_id", run.ID, "organization_id", run.OrganizationID,
		"target_day", day.Format("2006-01-02"),
		"repositories", len(run.DiscoveredRepoIDs),
		"work_item_metrics_daily", written[teamkeytables.WorkItemMetricsDaily.Table],
		"estimate_coverage_metrics_daily", written[teamkeytables.EstimateCoverageMetricsDaily.Table],
		"work_item_state_durations_daily", written[teamkeytables.WorkItemStateDurationsDaily.Table],
		"ai_governance_coverage_daily", written[teamkeytables.AIGovernanceCoverageDaily.Table],
	)
	return total, nil
}

var _ StaleKeyRetractor = (*RunStaleKeyRetractor)(nil)
