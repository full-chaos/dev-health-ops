package daily

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/checkedcast"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/filehotspots"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
)

// FileHotspotsExecutor is the NATIVE implementation of the file_hotspots
// metrics.daily family (CHAOS-4277) -- the third family to leave the Python
// HTTP compatibility bridge, alongside FileRiskHotspotsExecutor for
// file_risk_hotspots (same source data, different table, registered as a
// SEPARATE native family per families.json's per-family granularity).
//
// # Fidelity
//
// The Python job is the authority (src/dev_health_ops/metrics/hotspots.py:39
// compute_file_hotspots, invoked from job_daily.py:1449 inside a `for r_id in
// active_repos:` loop). Three things about the port are easy to get wrong:
//
//  1. ACTIVITY GATING. job_daily.py's `active_repos` restricts file_hotspots
//     to repos with a commit, CI pipeline run, or deployment on the TARGET
//     DAY specifically -- not merely a commit somewhere in the 30-day window
//     h_commit_rows spans. A repo with only historical window churn and no
//     same-day activity must NOT get a file_hotspots row for that day (it
//     would otherwise re-surface the same stale hotspot score every quiet
//     day). loadActiveRepoIDs reproduces this exactly: same-day commit
//     presence comes from the already-loaded window (filtered to
//     [dayStart,dayEnd)), same-day pipeline/deployment presence is two
//     narrow EXISTS-shaped queries against ci_pipeline_runs/deployments.
//     file_risk_hotspots has NO such gate -- see FileRiskHotspotsExecutor's
//     doc comment for why the two families differ here.
//  2. WINDOW SCOPE. window_stats (h_commit_rows) is a 30-calendar-day window
//     ENDING ON the target day (job_daily.py:1359 h_start_date = d -
//     timedelta(days=29)) -- the SAME window
//     repouser.RepoUserCommitExecutor already loads for repo_user_commit
//     (repouser.LoadWindowCommitStats), reused here via the same package
//     rather than re-implementing an identical query.
//  3. THE AGGREGATE-BACKFILL SENTINEL. window_stats rows with
//     file_path=="__AGGREGATE__" (GitLab/GitHub backfill's aggregate-only
//     commit-stat marker) must never be ranked or persisted -- see
//     filehotspots.ComputeFileHotspots's doc comment; this executor never
//     filters that itself, trusting the kernel.
type FileHotspotsExecutor struct {
	conn   driver.Conn
	loader *repouser.ClickHouseLoader
	nowUTC func() time.Time
}

var errFileHotspotsUnavailable = errors.New("file_hotspots native executor unavailable")

// NewFileHotspotsExecutor fails closed on a nil connection, matching
// NewRepoUserCommitExecutor/NewTeamWellbeingExecutor's construction-time
// policy: a refused executor simply never enters PartitionHandler's native
// family map, and file_hotspots stays on the Python compatibility bridge for
// every partition until the worker restarts with a healthy connection.
func NewFileHotspotsExecutor(conn driver.Conn) (*FileHotspotsExecutor, error) {
	if conn == nil {
		return nil, errFileHotspotsUnavailable
	}
	loader, err := repouser.NewClickHouseLoader(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errFileHotspotsUnavailable, err)
	}
	return &FileHotspotsExecutor{
		conn: conn, loader: loader,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *FileHotspotsExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil || executor.loader == nil {
		return 0, errFileHotspotsUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	if len(repoIDs) == 0 {
		return 0, nil
	}

	dayStart, dayEnd, windowStart := filehotspots.DayBoundaries(run.TargetDay, filehotspots.WindowDays)

	windowStats, err := executor.loader.LoadWindowCommitStats(ctx, run.OrganizationID, repoIDs, windowStart, dayEnd)
	if err != nil {
		return 0, err
	}

	activeRepoIDs, err := loadActiveRepoIDs(ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd, windowStats)
	if err != nil {
		return 0, err
	}
	if len(activeRepoIDs) == 0 {
		return 0, nil
	}

	computedAt := executor.nowUTC()
	day := dayStart
	var allRows []fileMetricsDailyRow
	for _, repoID := range activeRepoIDs {
		metrics := filehotspots.ComputeFileHotspots(repoID, windowStats)
		for _, metric := range metrics {
			allRows = append(allRows, fileMetricsDailyRow{
				RepoID: repoID, Day: day, Metric: metric, ComputedAt: computedAt,
			})
		}
	}
	if len(allRows) == 0 {
		return 0, nil
	}
	return writeFileMetricsDaily(ctx, executor.conn, run.OrganizationID, allRows)
}

var _ NativeFamilyExecutor = (*FileHotspotsExecutor)(nil)

// FileRiskHotspotsExecutor is the NATIVE implementation of the
// file_risk_hotspots metrics.daily family (CHAOS-4277), writing
// file_hotspot_daily. It shares FileHotspotsExecutor's window-stats loader
// but is registered as an INDEPENDENT NativeFamilyExecutor: families.json
// and PartitionHandler's fail-open policy operate per family, so one
// family's runtime failure (e.g. a ClickHouse blip mid-partition) must never
// take the other family down with it (CHAOS-4276's ruling, unchanged here).
//
// # Fidelity -- NO activity gate, unlike FileHotspotsExecutor
//
// job_daily.py's risk pass iterates `_hotspot_repo_ids(active_repos,
// repo_names_by_id)` -- the UNION of same-day-active repos and every
// DISCOVERED repo for the call. Because job_daily.py's real invocation is
// scoped to exactly ONE repo per call (the ledger's "each run_daily_metrics_
// job call loads and releases only that repo's source rows"), that union is
// ALWAYS the single repo being processed, active or not: the risk pass
// unconditionally covers it (compute_file_risk_hotspots unions churned files
// with complexity-only files and returns [] only when a repo has neither --
// CHAOS-2376 round-4, "idle-but-risky" repos must still surface static
// complexity risk). This executor mirrors that by running for EVERY repo in
// the partition, with no same-day activity filter at all -- narrowing to
// only "active" repos here would silently drop idle-but-complex repos'
// file_hotspot_daily rows, a real behavior regression from Python.
type FileRiskHotspotsExecutor struct {
	conn   driver.Conn
	loader *repouser.ClickHouseLoader
	nowUTC func() time.Time
}

var errFileRiskHotspotsUnavailable = errors.New("file_risk_hotspots native executor unavailable")

// NewFileRiskHotspotsExecutor fails closed, matching NewFileHotspotsExecutor.
func NewFileRiskHotspotsExecutor(conn driver.Conn) (*FileRiskHotspotsExecutor, error) {
	if conn == nil {
		return nil, errFileRiskHotspotsUnavailable
	}
	loader, err := repouser.NewClickHouseLoader(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errFileRiskHotspotsUnavailable, err)
	}
	return &FileRiskHotspotsExecutor{
		conn: conn, loader: loader,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *FileRiskHotspotsExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil || executor.loader == nil {
		return 0, errFileRiskHotspotsUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	if len(repoIDs) == 0 {
		return 0, nil
	}

	dayStart, dayEnd, windowStart := filehotspots.DayBoundaries(run.TargetDay, filehotspots.WindowDays)

	windowStats, err := executor.loader.LoadWindowCommitStats(ctx, run.OrganizationID, repoIDs, windowStart, dayEnd)
	if err != nil {
		return 0, err
	}

	computedAt := executor.nowUTC()
	day := dayStart
	var allRows []fileHotspotDailyRow
	for _, repoID := range repoIDs {
		complexityMap, err := loadComplexityMap(ctx, executor.conn, run.OrganizationID, repoID, day)
		if err != nil {
			return 0, err
		}
		blameMap, err := loadBlameMap(ctx, executor.conn, run.OrganizationID, repoID)
		if err != nil {
			return 0, err
		}
		metrics := filehotspots.ComputeFileRiskHotspots(repoID, windowStats, complexityMap, blameMap)
		for _, metric := range metrics {
			allRows = append(allRows, fileHotspotDailyRow{
				RepoID: repoID, Day: day, Metric: metric, ComputedAt: computedAt,
			})
		}
	}
	if len(allRows) == 0 {
		return 0, nil
	}
	return writeFileHotspotDaily(ctx, executor.conn, run.OrganizationID, allRows)
}

var _ NativeFamilyExecutor = (*FileRiskHotspotsExecutor)(nil)

// loadActiveRepoIDs reproduces job_daily.py's active_repos set (job_daily.py
// :1424-1429) for the batch of repos in one partition: a repo is active for
// day [dayStart,dayEnd) if it has a commit, a finished CI pipeline run, or a
// deployment in that window. Commit activity is read from the ALREADY-LOADED
// windowStats (filtered to the single day) rather than a second query --
// h_commit_rows is a superset of commit_rows for the target day, so this is
// the identical predicate with no extra round trip. Returned in repoIDs'
// own order for determinism.
func loadActiveRepoIDs(
	ctx context.Context, conn repositoryRows, organizationID string, repoIDs []uuid.UUID,
	dayStart, dayEnd time.Time, windowStats []repouser.CommitStatRow,
) ([]uuid.UUID, error) {
	active := make(map[uuid.UUID]struct{}, len(repoIDs))
	for _, row := range windowStats {
		if !row.CommitterWhen.Before(dayStart) && row.CommitterWhen.Before(dayEnd) {
			active[row.RepoID] = struct{}{}
		}
	}

	pipelineActive, err := loadSameDayActivity(ctx, conn, organizationID, repoIDs, dayStart, dayEnd, "ci_pipeline_runs", "finished_at")
	if err != nil {
		return nil, err
	}
	for _, id := range pipelineActive {
		active[id] = struct{}{}
	}
	deploymentActive, err := loadSameDayActivity(ctx, conn, organizationID, repoIDs, dayStart, dayEnd, "deployments", "deployed_at")
	if err != nil {
		return nil, err
	}
	for _, id := range deploymentActive {
		active[id] = struct{}{}
	}

	result := make([]uuid.UUID, 0, len(active))
	for _, repoID := range repoIDs {
		if _, ok := active[repoID]; ok {
			result = append(result, repoID)
		}
	}
	return result, nil
}

// loadSameDayActivity mirrors load_cicd_data's (loaders/clickhouse.py:1196)
// finished_at/deployed_at windowing for ci_pipeline_runs/deployments,
// narrowed to DISTINCT repo_id (this call only needs presence, never the
// rows themselves).
func loadSameDayActivity(
	ctx context.Context, conn repositoryRows, organizationID string, repoIDs []uuid.UUID,
	dayStart, dayEnd time.Time, table, timeColumn string,
) ([]uuid.UUID, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || len(repoIDs) == 0 {
		return nil, nil
	}
	query := fmt.Sprintf(
		"SELECT DISTINCT repo_id FROM %s WHERE org_id = ? AND repo_id IN ? AND %s >= ? AND %s < ?",
		table, timeColumn, timeColumn,
	)
	rows, err := conn.Query(ctx, query, organizationID, repositoryUUIDStrings(repoIDs), dayStart.UTC(), dayEnd.UTC())
	if err != nil {
		return nil, fmt.Errorf("load %s same-day activity: %w", table, err)
	}
	defer rows.Close()

	var result []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan %s repo_id: %w", table, err)
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s same-day activity: %w", table, err)
	}
	return result, nil
}

// loadComplexityMap ports job_daily.py's _load_complexity_map_for_repo
// (job_daily.py:943): the latest file_complexity_snapshots row per file, on
// or before day, keyed by (as_of_day, computed_at) via argMax -- as_of_day
// MUST lead the tie-break key (see that function's doc comment: keying on
// computed_at alone would let an older as_of_day backfilled/recomputed later
// clobber a newer snapshot). Only the two columns filehotspots.ComputeFile
// RiskHotspots reads are selected -- see filehotspots.ComplexitySnapshot's
// doc comment for why the other file_complexity_snapshots columns are
// intentionally not read here.
func loadComplexityMap(
	ctx context.Context, conn repositoryRows, organizationID string, repoID uuid.UUID, day time.Time,
) (map[string]filehotspotsComplexitySnapshotAlias, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	rows, err := conn.Query(ctx, `
SELECT
	file_path,
	argMax(cyclomatic_total, (as_of_day, computed_at)) AS cyclomatic_total,
	argMax(cyclomatic_avg,   (as_of_day, computed_at)) AS cyclomatic_avg
FROM file_complexity_snapshots
WHERE repo_id = ? AND org_id = ? AND as_of_day <= ?
GROUP BY file_path`, repoID, organizationID, day.UTC())
	if err != nil {
		return nil, fmt.Errorf("load complexity map: %w", err)
	}
	defer rows.Close()

	result := make(map[string]filehotspotsComplexitySnapshotAlias)
	for rows.Next() {
		var path string
		// file_complexity_snapshots.cyclomatic_total is UInt32
		// (migration 007_complexity_investment_issues.sql), and argMax
		// preserves its argument's type -- clickhouse-go refuses to scan a
		// UInt32 column into an *int64 destination ("converting UInt32 to
		// *int64 is unsupported"), caught by the seeded integration test
		// against a real ClickHouse instance, not by any mocked test.
		var cyclomaticTotal uint32
		var cyclomaticAvg float64
		if err := rows.Scan(&path, &cyclomaticTotal, &cyclomaticAvg); err != nil {
			return nil, fmt.Errorf("scan complexity snapshot: %w", err)
		}
		// Mirrors _load_complexity_map_for_repo's `if not path: continue`
		// (job_daily.py:1000): a blank file_path must never enter the map --
		// it is not a real file, and compute_file_risk_hotspots would union
		// it in as a phantom row, corrupting both its own output AND the
		// z-score of every genuine file (codex round 1, finding 4).
		if path == "" {
			continue
		}
		result[path] = filehotspotsComplexitySnapshotAlias{CyclomaticTotal: int(cyclomaticTotal), CyclomaticAvg: cyclomaticAvg}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate complexity map: %w", err)
	}
	return result, nil
}

// blameOwnershipConn is the capability loadBlameMap needs: it settles
// git_blame_file_ownership (a write) before it reads it.
type blameOwnershipConn interface {
	Exec(context.Context, string, ...any) error
	Query(context.Context, string, ...any) (driver.Rows, error)
}

// settleBlameOwnershipQuery recomputes git_blame_file_ownership for the files
// of one repository that git_blame_dirty_paths marks as written since their
// last settle, reading git_blame for those files only. The per-file
// aggregation is the full-scan reader's own expression -- argMax author per
// line by last_synced, email falling back to name, blank authors excluded --
// with one difference: a file whose lines all have a blank author still
// settles, with attributed_lines = 0, so it does not stay dirty forever. The
// read query skips those rows exactly as the full scan's `author != ”`
// filter dropped such a file.
//
// A file stays dirty until 300 seconds after its latest marker. The
// materialized view stamps the marker while the git_blame insert runs, before
// that insert's parts commit, so a settle starting in that gap reads a
// snapshot without the new lines although its settled_at is later than the
// marker. Keeping the file dirty for the margin makes the next call settle it
// again, which is exact as long as no single git_blame insert takes longer
// than the margin to commit. A file with no ownership row yet compares
// against the epoch and is always dirty.
const settleBlameOwnershipQuery = `
INSERT INTO git_blame_file_ownership
    (org_id, repo_id, path, owner_lines, attributed_lines, settled_at)
SELECT
    org_id,
    repo_id,
    path,
    maxIf(author_lines, author != '') AS owner_lines,
    sumIf(author_lines, author != '') AS attributed_lines,
    now64(3, 'UTC') AS settled_at
FROM
(
    SELECT
        org_id,
        repo_id,
        path,
        author,
        count() AS author_lines
    FROM
    (
        SELECT
            org_id,
            repo_id,
            path,
            line_no,
            argMax(
                coalesce(author_email, author_name, ''),
                last_synced
            ) AS author
        FROM git_blame
        WHERE org_id = ? AND repo_id = ?
          AND path IN
          (
              SELECT path
              FROM
              (
                  SELECT
                      path,
                      max(marked_at) AS last_marked,
                      toDateTime64(0, 3, 'UTC') AS last_settled
                  FROM git_blame_dirty_paths
                  WHERE org_id = ? AND repo_id = ?
                  GROUP BY path
                  UNION ALL
                  SELECT
                      path,
                      toDateTime64(0, 3, 'UTC') AS last_marked,
                      max(settled_at) AS last_settled
                  FROM git_blame_file_ownership
                  WHERE org_id = ? AND repo_id = ?
                  GROUP BY path
              )
              GROUP BY path
              HAVING max(last_marked) + INTERVAL 300 SECOND > max(last_settled)
          )
        GROUP BY org_id, repo_id, path, line_no
    )
    GROUP BY org_id, repo_id, path, author
)
GROUP BY org_id, repo_id, path`

// readBlameOwnershipQuery reads the latest settled ownership per file. The
// owner/attributed pair is taken as one tuple so a settled_at tie cannot pair
// one settle's numerator with another's denominator. UInt64 / UInt64 is the
// same Float64 division the full scan performed on the same two counts.
const readBlameOwnershipQuery = `
SELECT
    path,
    tupleElement(latest, 1) / tupleElement(latest, 2) AS concentration
FROM
(
    SELECT
        path,
        argMax((owner_lines, attributed_lines), settled_at) AS latest
    FROM git_blame_file_ownership
    WHERE org_id = ? AND repo_id = ?
    GROUP BY path
)
WHERE tupleElement(latest, 2) > 0`

// loadBlameMap ports job_daily.py's _load_blame_map_for_repo
// (job_daily.py:1022): per-file dominant-owner concentration, scoped by BOTH
// org_id and repo_id so a stale or default-org row for a reused repo_id never
// contaminates another tenant.
//
// It never aggregates the repository's whole git_blame. It first settles the
// files written since their last settle, then reads the per-file ownership
// rows. A file's concentration depends on that file's own lines alone, so
// recomputing only the changed files yields the same map as a full scan.
func loadBlameMap(
	ctx context.Context, conn blameOwnershipConn, organizationID string, repoID uuid.UUID,
) (map[string]float64, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	if err := conn.Exec(ctx, settleBlameOwnershipQuery,
		organizationID, repoID, organizationID, repoID, organizationID, repoID,
	); err != nil {
		return nil, fmt.Errorf("settle blame ownership: %w", err)
	}
	rows, err := conn.Query(ctx, readBlameOwnershipQuery, organizationID, repoID)
	if err != nil {
		return nil, fmt.Errorf("load blame map: %w", err)
	}
	defer rows.Close()

	result := make(map[string]float64)
	for rows.Next() {
		var path string
		var concentration float64
		if err := rows.Scan(&path, &concentration); err != nil {
			return nil, fmt.Errorf("scan blame concentration: %w", err)
		}
		result[path] = concentration
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate blame map: %w", err)
	}
	return result, nil
}

// filehotspotsComplexitySnapshotAlias is a thin local alias kept distinct
// from filehotspots.ComplexitySnapshot at the loader boundary only so this
// file's signatures stay readable without a package-qualified type name in
// every loader signature; it is structurally identical and converted 1:1 in
// ComputeFamily below.
type filehotspotsComplexitySnapshotAlias = filehotspots.ComplexitySnapshot

// fileMetricsDailyRow and fileHotspotDailyRow bundle a computed kernel
// metric with the (repo_id, day, computed_at) envelope the writer needs --
// kept here rather than widening filehotspots.FileMetric/RiskMetric, since
// the kernel package is deliberately connection- and envelope-agnostic (see
// its package doc comment).
type fileMetricsDailyRow struct {
	RepoID     uuid.UUID
	Day        time.Time
	Metric     filehotspots.FileMetric
	ComputedAt time.Time
}

type fileHotspotDailyRow struct {
	RepoID     uuid.UUID
	Day        time.Time
	Metric     filehotspots.RiskMetric
	ComputedAt time.Time
}

// fileMetricsBatchConn is the narrow write capability the two writers below
// need.
type fileMetricsBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// uint32ColumnValue converts a Go int to a uint32 for a UInt32 ClickHouse
// destination column, REFUSING (rather than silently wrapping) any value
// outside [0, MaxUint32]. This is the single choke point for the bug class
// codex rounds 3 and 6 both found on this port -- a Go-computed int
// (churn_loc_30d, then churn) silently wrapping into a fixed-width column
// via a bare uint32(...)/uint64(...) conversion -- on two different columns.
// Team-lead's ruling (2026-09-01): a fix closes the CLASS, not one column at
// a time. Every UInt32 destination this port writes goes through this
// function (or is documented as provably safe without it -- see
// writeFileHotspotDaily's cyclomaticTotal comment). Matches Python's own
// fail-loud behavior: clickhouse_connect's encoder raises a DataError
// narrowing an out-of-range Python int into a UInt32 column, so an error
// here (Refused, falls back to the Python bridge, which fails identically)
// is fidelity-correct, not merely defensive.
func uint32ColumnValue(value int, table, column string, repoID uuid.UUID, key string) (uint32, error) {
	v, err := checkedcast.Uint32(value, table, column)
	if err != nil {
		return 0, fmt.Errorf("%w: %w for repo %s %q", ErrInvalidState, err, repoID, key)
	}
	return v, nil
}

// writeFileMetricsDaily ports the write side of write_file_metrics
// (sinks/clickhouse/work_graph.py:139) -- same table, same column order.
// file_metrics_daily is APPEND-ONLY MergeTree (migration
// 083_dedup_file_hotspots_windowed_view.sql), so this INSERT never
// overwrites a prior day's row for the same (org_id, repo_id, day, path);
// every reader dedups via `ORDER BY computed_at DESC LIMIT 1 BY org_id,
// repo_id, day, path`, matching that migration's v_file_hotspots_windowed
// view and clickhouse_dedup.dedup_from()'s other Python readers.
func writeFileMetricsDaily(
	ctx context.Context, conn fileMetricsBatchConn, organizationID string, rows []fileMetricsDailyRow,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO file_metrics_daily (
		repo_id, day, path, churn, contributors, commits_count, hotspot_score, computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare file_metrics_daily batch: %w", err)
	}
	for _, row := range rows {
		// Every UInt32 destination column is bounds-checked via
		// uint32ColumnValue rather than cast with a bare uint32(...) --
		// codex rounds 3 and 6 found the SAME class of bug (a Go-computed
		// int silently wrapping into a fixed-width ClickHouse column) on two
		// different columns; team-lead's ruling (2026-09-01) is that the fix
		// is only done when the CLASS stops reproducing, not the two
		// instances. See uint32ColumnValue's doc comment and this PR's
		// RISK-NOTES cast-safety table for the full sweep.
		churn, err := uint32ColumnValue(row.Metric.Churn, "file_metrics_daily", "churn", row.RepoID, row.Metric.Path)
		if err != nil {
			return 0, err
		}
		contributors, err := uint32ColumnValue(row.Metric.Contributors, "file_metrics_daily", "contributors", row.RepoID, row.Metric.Path)
		if err != nil {
			return 0, err
		}
		commitsCount, err := uint32ColumnValue(row.Metric.CommitsCount, "file_metrics_daily", "commits_count", row.RepoID, row.Metric.Path)
		if err != nil {
			return 0, err
		}
		if err := batch.Append(
			row.RepoID, row.Day, row.Metric.Path, churn, contributors, commitsCount,
			row.Metric.HotspotScore, row.ComputedAt.UTC(), organizationID,
		); err != nil {
			return 0, fmt.Errorf("append file_metrics_daily row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send file_metrics_daily batch: %w", err)
	}
	return len(rows), nil
}

// writeFileHotspotDaily ports the write side of write_file_hotspot_daily
// (sinks/clickhouse/work_graph.py:499) -- same table, same column order.
// Like file_metrics_daily, file_hotspot_daily is APPEND-ONLY MergeTree;
// readers dedup via argMax(<col>, computed_at) grouped by (repo_id,
// file_path) (v_file_hotspots_windowed's `lookup` subquery) or the full
// (org_id, repo_id, day, file_path) key (clickhouse_dedup.dedup_from()'s
// other Python readers).
func writeFileHotspotDaily(
	ctx context.Context, conn fileMetricsBatchConn, organizationID string, rows []fileHotspotDailyRow,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO file_hotspot_daily (
		repo_id, day, file_path, churn_loc_30d, churn_commits_30d, cyclomatic_total,
		cyclomatic_avg, blame_concentration, risk_score, computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare file_hotspot_daily batch: %w", err)
	}
	for _, row := range rows {
		// churn_loc_30d is UInt64 in production (migration
		// 007_complexity_investment_issues.sql:43) -- NOT UInt32 like its
		// sibling columns here (codex round 3, P2). Safe by construction: it
		// is a non-negative accumulation of nonNegative()-clamped additions/
		// deletions (see filehotspots.ComputeFileRiskHotspots), so it is
		// always >= 0 and Go's `int` is 64-bit, meaning uint64(int) can only
		// wrap if the value were negative -- which it structurally cannot be.
		if row.Metric.ChurnLOC30d < 0 {
			return 0, fmt.Errorf("%w: file_hotspot_daily.churn_loc_30d %d for repo %s path %q is negative",
				ErrInvalidState, row.Metric.ChurnLOC30d, row.RepoID, row.Metric.FilePath)
		}
		churnCommits30d, err := uint32ColumnValue(row.Metric.ChurnCommits30d, "file_hotspot_daily", "churn_commits_30d", row.RepoID, row.Metric.FilePath)
		if err != nil {
			return 0, err
		}
		// cyclomatic_total is also UInt32, but is provably safe without a
		// runtime check: loadComplexityMap (this file) reads it via a Go
		// `uint32` scan straight off the UInt32 file_complexity_snapshots
		// column, converts to `int` (always safe, UInt32 always fits in a
		// 64-bit Go int), and that value flows through the kernel unchanged
		// (filehotspots.ComputeFileRiskHotspots never arithmetically combines
		// it with anything). The round trip UInt32 -> int -> UInt32 cannot
		// exceed UInt32 range because the value never left it.
		cyclomaticTotal := uint32(row.Metric.CyclomaticTotal)
		if err := batch.Append(
			row.RepoID, row.Day, row.Metric.FilePath, uint64(row.Metric.ChurnLOC30d),
			churnCommits30d, cyclomaticTotal,
			row.Metric.CyclomaticAvg, row.Metric.BlameConcentration,
			row.Metric.RiskScore, row.ComputedAt.UTC(), organizationID,
		); err != nil {
			return 0, fmt.Errorf("append file_hotspot_daily row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send file_hotspot_daily batch: %w", err)
	}
	return len(rows), nil
}
