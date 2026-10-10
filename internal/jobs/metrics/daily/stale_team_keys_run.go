package daily

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aigovernance"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// StaleKeyRetractor supersedes, once for a run, the team keys of the day that
// the run's partitions no longer produce, in the tables whose keys more than
// one partition of a run can write, and settles the rows of the work-item
// tables among them. The finalize handler calls it after every
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
//
// A run of the WHOLE organization (Run.FullOrg) owns the whole day. It reads
// the work scopes from every work item of the organization, so it computes
// and settles every scope, and it supersedes every live key of the day that
// it did not compute, of any provider and any work scope ("unassigned" too):
// a work scope that has no item left for the day is computed by nothing, and
// its keys of an earlier compute would stay counted. It also supersedes the
// keys of the repository-scoped tables for a repository that is in no
// partition of the run. A run of some repositories does none of this: it
// stays inside the scopes its repositories reach.
type RunStaleKeyRetractor struct {
	conn   driver.Conn
	nowUTC func() time.Time
	// presentRepositories reads the repositories the organization holds at
	// the time of the call: the same read that makes the repository list of a
	// run at its dispatch.
	presentRepositories func(ctx context.Context, organizationID string) ([]RepositoryID, error)
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

// OrganizationRunStaleKeyTables are the tables whose keys a partition decides
// inside its own repositories. At the end of a run of the whole organization
// RunStaleKeyRetractor supersedes their live keys of a repository that is in no
// partition of the run AND that the organization does not hold any more
// (retractStaleTeamKeysOutsideRun); a run of some repositories leaves them
// alone.
func OrganizationRunStaleKeyTables() []StaleKeyTable {
	return []StaleKeyTable{
		teamkeytables.TeamMetricsDaily,
		teamkeytables.AIImpactMetricsDaily,
	}
}

// NewRunStaleKeyRetractor fails closed on a nil connection.
func NewRunStaleKeyRetractor(conn driver.Conn) (*RunStaleKeyRetractor, error) {
	if conn == nil {
		return nil, errRunStaleKeyRetractorUnavailable
	}
	discoverer, err := NewClickHouseRepositoryDiscoverer(conn)
	if err != nil {
		return nil, errRunStaleKeyRetractorUnavailable
	}
	return &RunStaleKeyRetractor{
		conn: conn, nowUTC: func() time.Time { return time.Now().UTC() },
		presentRepositories: discoverer.RepositoryIDs,
	}, nil
}

// ErrOrganizationRepositoriesNotRead is returned when the end of a run of the
// whole organization cannot read the repositories the organization holds now.
// Without that read no key of a repository outside the run is proven to be of
// a repository that is gone, so nothing is superseded in the
// repository-scoped tables.
var ErrOrganizationRepositoriesNotRead = errors.New("daily stale team keys: the repositories of the organization are not read")

// StaleKeysRepositoryNotInRunLogMessage is the line the end of a run of the
// whole organization writes when the organization holds a repository that is
// in no partition of the run (the repository list of a run is the one of its
// dispatch). The stored keys of such a repository are left as they are.
const StaleKeysRepositoryNotInRunLogMessage = "daily stale team keys: the organization holds a repository that is in no partition of the run; its keys are left as they are"

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
	// A run of the whole organization owns the whole day: it computes every
	// work scope of the organization and supersedes every live key it did not
	// compute. A run of some repositories stays inside the work scopes those
	// repositories reach.
	wholeOrganization := run.FullOrg
	scope.everyRepository = wholeOrganization
	day := scope.day
	// The second read of an organization-wide run whose scope read gave no
	// work item (see retractStaleTeamKeysOfRun).
	proveNoItem := func(proofCtx context.Context) error {
		return proveOrganizationHasNoWorkItemForDay(proofCtx, retractor.conn, run.OrganizationID, scope.start, scope.end)
	}
	conn := retractor.conn

	// For the three work-item tables the step also stores the rows it
	// computed: it runs after every partition wrote its attributions, so its
	// rows are the rows of the day, and it writes them newer than the rows of
	// the partitions (see retractStaleTeamKeysOfRun, step 4).
	workItemKeys := func(keyCtx context.Context) (runStaleKeys, error) {
		read, triplet, err := computeWorkItemTriplet(keyCtx, conn, run, partition, scope)
		if err != nil {
			return runStaleKeys{}, err
		}
		keys := make([]staleKey, 0, len(triplet.MetricsDaily))
		for _, row := range triplet.MetricsDaily {
			keys = append(keys, staleKey{row.Provider, row.WorkScopeID, row.TeamID})
		}
		return runStaleKeys{scope: read.staleKeyScope(), everyScope: wholeOrganization, proveNoItem: proveNoItem, keys: keys,
			writeRows: func(writeCtx context.Context, version time.Time) (int, error) {
				return WriteWorkItemMetricsDaily(writeCtx, conn, run.OrganizationID, day, triplet.MetricsDaily, version)
			}}, nil
	}
	estimateKeys := func(keyCtx context.Context) (runStaleKeys, error) {
		read, rows, err := computeWorkItemEstimateRows(keyCtx, conn, run, partition, scope)
		if err != nil {
			return runStaleKeys{}, err
		}
		keys := make([]staleKey, 0, len(rows))
		for _, row := range rows {
			keys = append(keys, staleKey{row.Provider, row.WorkScopeID, row.TeamID})
		}
		return runStaleKeys{scope: read.staleKeyScope(), everyScope: wholeOrganization, proveNoItem: proveNoItem, keys: keys,
			writeRows: func(writeCtx context.Context, version time.Time) (int, error) {
				return WriteEstimateCoverageMetricsDaily(writeCtx, conn, run.OrganizationID, day, rows, version)
			}}, nil
	}
	stateKeys := func(keyCtx context.Context) (runStaleKeys, error) {
		computed, err := computeWorkItemStateRows(keyCtx, conn, run, partition, scope, retractor.nowUTC())
		if err != nil {
			return runStaleKeys{}, err
		}
		keys := make([]staleKey, 0, len(computed.rows))
		for _, row := range computed.rows {
			keys = append(keys, staleKey{row.Provider, row.WorkScopeID, row.TeamID, row.Status})
		}
		return runStaleKeys{scope: computed.read.staleKeyScope(), everyScope: wholeOrganization, proveNoItem: proveNoItem, keys: keys,
			writeRows: func(writeCtx context.Context, version time.Time) (int, error) {
				return WriteWorkItemStateDurationsDaily(writeCtx, conn, run.OrganizationID, day, computed.rows, version)
			}}, nil
	}
	// ai_governance_coverage_daily: every partition computes the same rows
	// from the same artifacts, so the step supersedes the stale keys only.
	governanceKeys := func(keyCtx context.Context) (runStaleKeys, error) {
		// The window of the family: the day, inclusive of its last
		// microsecond (see AIGovernanceExecutor).
		artifacts, err := LoadGovernanceArtifacts(keyCtx, conn, run.OrganizationID, day, day.Add(24*time.Hour-time.Microsecond))
		if err != nil {
			return runStaleKeys{}, err
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
		return runStaleKeys{keys: keys}, nil
	}

	// Each step names its table in its call, so the census can read which
	// table the run-level rule is applied to.
	//
	// The clock of this host is read when a step starts. It is only the
	// version of the rows the step writes; which keys are superseded does not
	// depend on it.
	// A step returns its table, the rows it wrote, and how many of them are
	// rows of zeros (the retraction).
	steps := []func() (string, int, int, error){
		func() (string, int, int, error) {
			rows, zeros, err := retractStaleTeamKeysOfRun(ctx, conn, teamkeytables.WorkItemMetricsDaily,
				run.OrganizationID, day, workItemKeys, retractor.nowUTC())
			return teamkeytables.WorkItemMetricsDaily.Table, rows, zeros, err
		},
		func() (string, int, int, error) {
			rows, zeros, err := retractStaleTeamKeysOfRun(ctx, conn, teamkeytables.EstimateCoverageMetricsDaily,
				run.OrganizationID, day, estimateKeys, retractor.nowUTC())
			return teamkeytables.EstimateCoverageMetricsDaily.Table, rows, zeros, err
		},
		func() (string, int, int, error) {
			rows, zeros, err := retractStaleTeamKeysOfRun(ctx, conn, teamkeytables.WorkItemStateDurationsDaily,
				run.OrganizationID, day, stateKeys, retractor.nowUTC())
			return teamkeytables.WorkItemStateDurationsDaily.Table, rows, zeros, err
		},
		func() (string, int, int, error) {
			rows, zeros, err := retractStaleTeamKeysOfRun(ctx, conn, teamkeytables.AIGovernanceCoverageDaily,
				run.OrganizationID, day, governanceKeys, retractor.nowUTC())
			return teamkeytables.AIGovernanceCoverageDaily.Table, rows, zeros, err
		},
	}
	// left is, per repository-scoped table, the live keys of a repository
	// that is in no partition of the run and that the organization holds now.
	left := make(map[string]int, len(OrganizationRunStaleKeyTables()))
	// notInRun is those repositories, and superseded the repositories whose
	// keys got a row of zeros, over both tables.
	notInRun, superseded := map[string]struct{}{}, map[string]struct{}{}
	if wholeOrganization {
		// The tables whose keys a partition decides inside its own
		// repositories. The repository list of the run is the one of its
		// dispatch, so "in no partition of this run" does not say that a
		// repository is gone: a repository the organization got later, and
		// that a run of its own computed for the day, is in no partition
		// too. The organization's repositories are read again, for each
		// table after its live keys are read, and a key is superseded only
		// when its repository is in neither set. A run never hides a key
		// that can be true.
		// scope.repoIDs is the run's repositories, parsed: the text form of
		// each id is the form the keys hold.
		owned := make([][]string, 0, len(scope.repoIDs))
		for _, repoID := range scope.repoIDs {
			owned = append(owned, []string{repoID.String()})
		}
		ownedScope := newStaleKeyScope(owned...)
		readPresent := func(readCtx context.Context) (staleKeyScope, error) {
			if retractor.presentRepositories == nil {
				return nil, fmt.Errorf("%w: no read is wired", ErrOrganizationRepositoriesNotRead)
			}
			present, err := retractor.presentRepositories(readCtx, run.OrganizationID)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrOrganizationRepositoriesNotRead, err)
			}
			tuples := make([][]string, 0, len(present))
			for _, repositoryID := range present {
				tuples = append(tuples, []string{string(repositoryID)})
			}
			return newStaleKeyScope(tuples...), nil
		}
		// A run that cannot read the organization's repositories fails here,
		// before it writes a row of any table. The set this read gives is not
		// used: each table reads its own, after its live keys.
		if _, err := readPresent(ctx); err != nil {
			return 0, err
		}
		for _, table := range OrganizationRunStaleKeyTables() {
			steps = append(steps, func() (string, int, int, error) {
				result, err := retractStaleTeamKeysOutsideRun(ctx, conn, table, run.OrganizationID, day,
					ownedScope, readPresent, retractor.nowUTC())
				left[table.Table] = result.kept
				for _, repository := range result.notInRun {
					notInRun[repository] = struct{}{}
				}
				for _, repository := range result.superseded {
					superseded[repository] = struct{}{}
				}
				return table.Table, result.written, result.written, err
			})
		}
	}
	total := 0
	written := make(map[string]int, len(steps))
	retracted := make(map[string]int, len(steps))
	for _, step := range steps {
		table, rows, zeros, err := step()
		total += rows
		written[table] = rows
		retracted[table] = zeros
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
	if len(notInRun) > 0 {
		ids, more := loggedRepositoryIDs(notInRun)
		slog.Default().Warn(StaleKeysRepositoryNotInRunLogMessage,
			"run_id", run.ID, "organization_id", run.OrganizationID,
			"target_day", day.Format("2006-01-02"),
			"repositories", len(run.DiscoveredRepoIDs),
			"repositories_not_in_run", len(notInRun),
			"repository_ids_not_in_run", ids, "repository_ids_not_in_run_more", more,
			"team_metrics_daily_keys_left", left[teamkeytables.TeamMetricsDaily.Table],
			"ai_impact_metrics_daily_keys_left", left[teamkeytables.AIImpactMetricsDaily.Table],
		)
	}
	supersededIDs, supersededMore := loggedRepositoryIDs(superseded)
	slog.Default().Info(StaleKeysRetractedLogMessage,
		"run_id", run.ID, "organization_id", run.OrganizationID,
		"target_day", day.Format("2006-01-02"),
		"repositories", len(run.DiscoveredRepoIDs),
		"work_item_metrics_daily", written[teamkeytables.WorkItemMetricsDaily.Table],
		"estimate_coverage_metrics_daily", written[teamkeytables.EstimateCoverageMetricsDaily.Table],
		"work_item_state_durations_daily", written[teamkeytables.WorkItemStateDurationsDaily.Table],
		"ai_governance_coverage_daily", written[teamkeytables.AIGovernanceCoverageDaily.Table],
		// The class of the retraction and, per table, the rows of zeros among
		// the rows above (the retraction itself).
		"retraction_scope", staleKeyRetractionScope(wholeOrganization),
		"work_item_metrics_daily_zero_rows", retracted[teamkeytables.WorkItemMetricsDaily.Table],
		"estimate_coverage_metrics_daily_zero_rows", retracted[teamkeytables.EstimateCoverageMetricsDaily.Table],
		"work_item_state_durations_daily_zero_rows", retracted[teamkeytables.WorkItemStateDurationsDaily.Table],
		"ai_governance_coverage_daily_zero_rows", retracted[teamkeytables.AIGovernanceCoverageDaily.Table],
		"team_metrics_daily_zero_rows", retracted[teamkeytables.TeamMetricsDaily.Table],
		"ai_impact_metrics_daily_zero_rows", retracted[teamkeytables.AIImpactMetricsDaily.Table],
		// The repositories that a row of zeros of the two repository-scoped
		// tables hides (an empty id is the rows with no repository).
		"repositories_superseded", len(superseded),
		"repository_ids_superseded", supersededIDs, "repository_ids_superseded_more", supersededMore,
	)
	return total, nil
}

// StaleKeyLoggedRepositoryLimit is the most repository ids one log line of the
// step names. The count beside the list is always whole.
const StaleKeyLoggedRepositoryLimit = 20

// loggedRepositoryIDs gives the ids to name in a log line, sorted, at most
// StaleKeyLoggedRepositoryLimit, and how many more there are.
func loggedRepositoryIDs(repositories map[string]struct{}) (ids []string, more int) {
	ids = make([]string, 0, len(repositories))
	for repository := range repositories {
		ids = append(ids, repository)
	}
	sort.Strings(ids)
	if len(ids) > StaleKeyLoggedRepositoryLimit {
		return ids[:StaleKeyLoggedRepositoryLimit], len(ids) - StaleKeyLoggedRepositoryLimit
	}
	return ids, 0
}

// The two classes of a retraction, as the log line names them.
const (
	StaleKeyRetractionOrganizationDay = "organization_day"
	StaleKeyRetractionRunScopes       = "run_scopes"
)

func staleKeyRetractionScope(wholeOrganization bool) string {
	if wholeOrganization {
		return StaleKeyRetractionOrganizationDay
	}
	return StaleKeyRetractionRunScopes
}

// proveOrganizationHasNoWorkItemForDay is the second read of a run of the
// whole organization that read no work item for a day: a count of the items
// the scope read selects (created before the day ends and open, or completed
// on the day or later). It returns nil only when the count is read and is 0.
// A count above 0 means the first read did not see items that are there, and
// no answer means the read did not end: both are ErrOrganizationDayNotProvenEmpty.
func proveOrganizationHasNoWorkItemForDay(
	ctx context.Context, conn repositoryRows, organizationID string, start, end time.Time,
) error {
	rows, err := conn.Query(ctx, `
SELECT count()
FROM work_items FINAL
WHERE org_id = ?
  AND created_at < ?
  AND (status != 'done' OR completed_at >= ?)`,
		organizationID, end.UTC(), start.UTC(),
	)
	if err != nil {
		return fmt.Errorf("%w: count the work items of the day: %w", ErrOrganizationDayNotProvenEmpty, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("%w: count the work items of the day: %w", ErrOrganizationDayNotProvenEmpty, err)
		}
		return fmt.Errorf("%w: the count of the work items of the day gave no answer", ErrOrganizationDayNotProvenEmpty)
	}
	var items uint64
	if err := rows.Scan(&items); err != nil {
		return fmt.Errorf("%w: scan the count of the work items of the day: %w", ErrOrganizationDayNotProvenEmpty, err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: count the work items of the day: %w", ErrOrganizationDayNotProvenEmpty, err)
	}
	if items != 0 {
		return fmt.Errorf("%w: the scope read gave no work item and a count of the same items gives %d",
			ErrOrganizationDayNotProvenEmpty, items)
	}
	return nil
}

var _ StaleKeyRetractor = (*RunStaleKeyRetractor)(nil)
