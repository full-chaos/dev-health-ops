package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const maximumGitHubBlameCoveragePaths = 100_000

// GitHubBlameClickHouseCoverage reads both durable blame rows and the
// path-progress journal. The full tenant/repository key is mandatory because
// repository UUIDs are stable across tenants and an organization may own
// multiple repositories with the same paths.
type GitHubBlameClickHouseCoverage struct {
	Conn     driver.Conn
	Lease    providerfoundation.LeaseGuard
	MaxPaths int
}

func (coverage GitHubBlameClickHouseCoverage) HasGenerationProgress(
	ctx context.Context,
	claim Claim,
	generation string,
) (bool, error) {
	if ctx == nil || coverage.Conn == nil || coverage.Lease == nil ||
		claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "blame" || generation == "" ||
		generation != claim.GenerationKey() {
		return false, ErrInvalidConfiguration
	}
	if err := coverage.Lease.Assert(ctx); err != nil {
		return false, err
	}
	var count uint64
	if err := coverage.Conn.QueryRow(ctx, `
SELECT count()
FROM github_blame_path_progress FINAL
WHERE org_id = ? AND generation = ?`, claim.OrgID, generation).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (coverage GitHubBlameClickHouseCoverage) Progress(
	ctx context.Context,
	claim Claim,
	repoID, treeRef, recoveryGeneration string,
) (GitHubBlameProgressState, error) {
	if ctx == nil || coverage.Conn == nil || coverage.Lease == nil ||
		claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "blame" || repoID == "" || treeRef == "" {
		return GitHubBlameProgressState{}, ErrInvalidConfiguration
	}
	if recoveryGeneration != "" && recoveryGeneration != claim.GenerationKey() {
		return GitHubBlameProgressState{}, ErrEffectLedgerConflict
	}
	if err := coverage.Lease.Assert(ctx); err != nil {
		return GitHubBlameProgressState{}, err
	}
	maxPaths := coverage.MaxPaths
	if maxPaths == 0 {
		maxPaths = maximumGitHubBlameCoveragePaths
	}
	if maxPaths < 1 || maxPaths > maximumGitHubBlameCoveragePaths {
		return GitHubBlameProgressState{}, ErrInvalidConfiguration
	}
	state := GitHubBlameProgressState{
		FailedAttempts:   make(map[string]uint64),
		InFlightOutcomes: make(map[string]string),
	}
	blameRows, err := coverage.Conn.Query(ctx, `
SELECT DISTINCT path
FROM git_blame FINAL
WHERE org_id = ? AND repo_id = ?
LIMIT ?`, claim.OrgID, repoID, maxPaths+1)
	if err != nil {
		return GitHubBlameProgressState{}, err
	}
	for blameRows.Next() {
		var path string
		if err := blameRows.Scan(&path); err != nil {
			_ = blameRows.Close()
			return GitHubBlameProgressState{}, err
		}
		if path == "" {
			_ = blameRows.Close()
			return GitHubBlameProgressState{}, providerfoundation.ErrNormalizationInvalid
		}
		state.BlamedPaths = append(state.BlamedPaths, path)
		if gitHubBlameProgressPathCount(state) > maxPaths {
			_ = blameRows.Close()
			return GitHubBlameProgressState{}, ErrGitHubBlameProgressUnavailable
		}
	}
	if err := blameRows.Err(); err != nil {
		_ = blameRows.Close()
		return GitHubBlameProgressState{}, err
	}
	if err := blameRows.Close(); err != nil {
		return GitHubBlameProgressState{}, err
	}

	progressRows, err := coverage.Conn.Query(ctx, `
SELECT path,
       countIf(outcome = 'retryable_error') AS failed_attempts,
       countIf(outcome = 'empty') > 0 AS completed_empty,
       argMaxIf(outcome, attempted_at, generation = ?) AS in_flight_outcome
FROM github_blame_path_progress FINAL
WHERE org_id = ? AND repo_id = ? AND tree_ref = ?
GROUP BY path
LIMIT ?`, recoveryGeneration, claim.OrgID, repoID, treeRef, maxPaths+1)
	if err != nil {
		return GitHubBlameProgressState{}, err
	}
	defer progressRows.Close()
	for progressRows.Next() {
		var path, inFlightOutcome string
		var failedAttempts uint64
		var completedEmpty bool
		if err := progressRows.Scan(
			&path, &failedAttempts, &completedEmpty, &inFlightOutcome,
		); err != nil {
			return GitHubBlameProgressState{}, err
		}
		if path == "" {
			return GitHubBlameProgressState{}, providerfoundation.ErrNormalizationInvalid
		}
		if completedEmpty {
			state.EmptyPaths = append(state.EmptyPaths, path)
		}
		if failedAttempts > 0 {
			state.FailedAttempts[path] = failedAttempts
		}
		if inFlightOutcome != "" {
			switch inFlightOutcome {
			case gitHubBlameOutcomeRows, gitHubBlameOutcomeEmpty, gitHubBlameOutcomeRetryableError:
				state.InFlightOutcomes[path] = inFlightOutcome
			default:
				return GitHubBlameProgressState{}, providerfoundation.ErrNormalizationInvalid
			}
		}
		if gitHubBlameProgressPathCount(state) > maxPaths {
			return GitHubBlameProgressState{}, ErrGitHubBlameProgressUnavailable
		}
	}
	if err := progressRows.Err(); err != nil {
		return GitHubBlameProgressState{}, err
	}
	return state, nil
}

func gitHubBlameProgressPathCount(state GitHubBlameProgressState) int {
	paths := make(map[string]struct{}, len(state.BlamedPaths)+len(state.EmptyPaths)+len(state.FailedAttempts)+len(state.InFlightOutcomes))
	for _, path := range state.BlamedPaths {
		paths[path] = struct{}{}
	}
	for _, path := range state.EmptyPaths {
		paths[path] = struct{}{}
	}
	for path := range state.FailedAttempts {
		paths[path] = struct{}{}
	}
	for path := range state.InFlightOutcomes {
		paths[path] = struct{}{}
	}
	return len(paths)
}

var _ GitHubBlameCoverage = GitHubBlameClickHouseCoverage{}
var _ GitHubBlameGenerationProgressProbe = GitHubBlameClickHouseCoverage{}

// FilesInventory reads the blob paths of one repository banked by the files
// dataset: the rows written within window of the newest write. git_files is
// never pruned, so a path deleted from the repository keeps its last row; the
// window drops it once a later sync rewrote the rest. It reads one repository
// of one tenant and returns at most maximumGitHubBlameCoveragePaths paths.
func (coverage GitHubBlameClickHouseCoverage) FilesInventory(
	ctx context.Context,
	claim Claim,
	repoID string,
	window time.Duration,
) (GitHubBlameFilesInventoryResult, error) {
	if ctx == nil || coverage.Conn == nil || coverage.Lease == nil ||
		claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "blame" || repoID == "" || window <= 0 {
		return GitHubBlameFilesInventoryResult{}, ErrInvalidConfiguration
	}
	if err := coverage.Lease.Assert(ctx); err != nil {
		return GitHubBlameFilesInventoryResult{}, err
	}
	maxPaths := coverage.MaxPaths
	if maxPaths == 0 {
		maxPaths = maximumGitHubBlameCoveragePaths
	}
	if maxPaths < 1 || maxPaths > maximumGitHubBlameCoveragePaths {
		return GitHubBlameFilesInventoryResult{}, ErrInvalidConfiguration
	}
	rows, err := coverage.Conn.Query(ctx, `
SELECT path, last_synced
FROM git_files FINAL
WHERE org_id = ? AND repo_id = ?
  AND last_synced >= (
      SELECT max(last_synced) FROM git_files WHERE org_id = ? AND repo_id = ?
  ) - toIntervalSecond(?)
LIMIT ?`, claim.OrgID, repoID, claim.OrgID, repoID, int64(window/time.Second), maxPaths+1)
	if err != nil {
		return GitHubBlameFilesInventoryResult{}, err
	}
	defer rows.Close()
	var result GitHubBlameFilesInventoryResult
	for rows.Next() {
		var path string
		var syncedAt time.Time
		if err := rows.Scan(&path, &syncedAt); err != nil {
			return GitHubBlameFilesInventoryResult{}, err
		}
		if path == "" {
			return GitHubBlameFilesInventoryResult{}, providerfoundation.ErrNormalizationInvalid
		}
		if len(result.Paths) >= maxPaths {
			return GitHubBlameFilesInventoryResult{}, ErrGitHubBlameProgressUnavailable
		}
		result.Paths = append(result.Paths, path)
		if syncedAt.After(result.NewestSyncedAt) {
			result.NewestSyncedAt = syncedAt
		}
	}
	if err := rows.Err(); err != nil {
		return GitHubBlameFilesInventoryResult{}, err
	}
	return result, nil
}
