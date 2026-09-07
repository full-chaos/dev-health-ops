package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/complexity"
)

// ComplexityExecutor is the NATIVE implementation of the complexity
// remaining-metric kind (CHAOS-4291) -- it was the last kind still served
// through the HTTP compatibility bridge (see cmd/dev-health-worker/daily.go's
// KindRemainingComplexity case: every sibling kind already held a native
// executor there); with this executor, no bridge remains in this package.
//
// The Python job is the authority (src/dev_health_ops/metrics/
// job_complexity_db.py: run_complexity_db_job). Its file-selection control
// flow (git_files vs git_blame, the max_files budget across both phases,
// when the blame fallback is even attempted) is ported in
// internal/jobs/metrics/complexity/fileselect.go; the per-file/per-repo
// aggregation (_build_result/_build_snapshots) in that package's compute.go;
// this file is the ClickHouse-connected glue between them and the one
// caller-visible behavior Python has that a native port could easily lose:
//
//  1. BACKFILL_DAYS IS ALWAYS EFFECTIVELY 1. run_complexity_db_job calls
//     `_date_range(date, 1)` -- a HARDCODED 1, ignoring its own
//     backfill_days parameter -- because complexity has no historical
//     snapshot storage; reusing one current-contents scan across a window
//     would fabricate a flat trend (CHAOS-2850). The scope contract enforces
//     this at the type level (ComplexityScope.backfill_days: Literal[1]), so
//     this executor never even has a multi-day case to get wrong.
//
//  2. A PARTITION WHERE NO REPO PRODUCES DATA IS A FAILURE, NOT A QUIET DAY.
//     Unlike DORA/membership/recommendations (zero rows is legitimate and
//     never fails the partition), run_complexity_db_job returns exit code 1
//     when repos were found but NONE of them yielded scannable file
//     contents, and worker_metrics.py's _run_complexity turns that into a
//     raised RuntimeError. This executor reproduces that asymmetry exactly:
//     zero REPOS FOUND (search_pattern matched nothing, or the org has none)
//     is success with zero rows; repos found but zero produced data is an
//     error. Softening the second case into a quiet success would hide a
//     real problem (e.g. every repo missing file contents) behind a green
//     partition.
//
//  3. AN UNPORTED LANGUAGE FAILS THE PARTITION, NEVER SILENTLY SKIPS.
//     AnalyzeFile returns ErrLanguageNotPorted for any extension Python's
//     LANGUAGE_BY_EXTENSION analyses that this package's DefaultAnalyzers
//     does not yet cover (see that package's own doc for the current
//     coverage and what remains). Catching that error here and skipping the
//     file would silently under-count every repo containing an unported
//     language -- the exact silent-undercount shape CHAOS-4243 exists to
//     prevent.
type ComplexityExecutor struct {
	conn       driver.Conn
	baseConfig complexity.ScanConfig
	observer   ComplexityObserver
	nowUTC     func() time.Time
}

// ComplexityObserver reports what one completed partition actually did --
// the bridge could only ever report a status code.
type ComplexityObserver interface {
	// ObserveComplexityPartition reports: how many repos in this partition
	// were scanned, how many of them produced at least one analysable file,
	// and how many files were skipped because the sole available source
	// (git_blame) carries no usable line text for them.
	ObserveComplexityPartition(reposScanned, reposWithData, blameUnusableRepos int) error
}

// ErrComplexitySchemaIncompatible refuses a database this executor cannot
// compute against.
var ErrComplexitySchemaIncompatible = errors.New(
	"complexity: clickhouse schema incompatible")

// ErrComplexityUnavailable is the nil-connection refusal.
var ErrComplexityUnavailable = errors.New(
	"complexity: clickhouse connection unavailable")

// ErrComplexityNoDataProduced mirrors run_complexity_db_job's exit code 1
// ("Complexity DB job wrote no data") -- repos were found but none of them
// had any scannable file content. worker_metrics.py's _run_complexity turns
// this same condition into a raised RuntimeError, so a bridge-served
// partition already fails identically; this is parity, not a new failure
// mode this port introduces.
var ErrComplexityNoDataProduced = errors.New(
	"complexity: none of the org's repos in this partition produced scannable data")

// NewComplexityExecutor refuses at construction rather than per partition,
// matching every other native remaining-metric executor's shape.
//
// configPath is the complexity.yaml this executor reads its default
// include/exclude globs and thresholds from -- the same file
// ComplexityScanner loads on the Python side
// (src/dev_health_ops/config/complexity.yaml). Wiring the deployed path for
// this parameter (container mount vs repo-relative dev path) is the
// caller's concern, mirroring providersync.LoadStatusMapping's identical
// caller-supplied-path shape.
func NewComplexityExecutor(
	ctx context.Context, conn driver.Conn, configPath string, observer ComplexityObserver,
) (*ComplexityExecutor, error) {
	if conn == nil {
		return nil, ErrComplexityUnavailable
	}
	if err := verifyComplexitySchema(ctx, conn); err != nil {
		return nil, err
	}
	baseConfig, err := complexity.LoadScanConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrComplexityUnavailable, err)
	}
	return &ComplexityExecutor{
		conn:       conn,
		baseConfig: baseConfig,
		observer:   observer,
		nowUTC:     func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputePartition satisfies PartitionExecutor: the seam the partition
// handler drives.
func (executor *ComplexityExecutor) ComputePartition(
	ctx context.Context, run Run, partition Partition,
) (CompatibilityOutcome, error) {
	if executor == nil || executor.conn == nil {
		return CompatibilityOutcome{}, ErrComplexityUnavailable
	}
	if strings.TrimSpace(run.OrganizationID) == "" {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s has no organization", ErrInvalidState, partition.ID))
	}

	var scope complexityScope
	if err := json.Unmarshal(partition.Scope, &scope); err != nil {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s scope: %v", ErrInvalidState, partition.ID, err))
	}
	day, err := time.Parse("2006-01-02", scope.Day)
	if err != nil {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s day %q", ErrInvalidState, partition.ID, scope.Day))
	}
	// backfill_days is validated to the literal 1 before this partition is
	// ever published (scopes.go); no day loop needed -- see the type doc.

	var repoIDFilter *uuid.UUID
	if scope.RepoID != nil {
		parsed, parseErr := uuid.Parse(*scope.RepoID)
		if parseErr != nil {
			return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
				"%w: partition %s repo_id %q", ErrInvalidState, partition.ID, *scope.RepoID))
		}
		repoIDFilter = &parsed
	}

	repos, err := loadComplexityRepos(ctx, executor.conn, repoIDFilter, scope.SearchPattern, run.OrganizationID)
	if err != nil {
		return CompatibilityOutcome{}, err
	}
	if len(repos) == 0 {
		// Python: `if not repos: logger.warning(...); return 0` -- a
		// search_pattern matching nothing, or an org with no repos, is a
		// legitimate empty result, not a failure.
		zero := 0
		if executor.observer != nil {
			_ = executor.observer.ObserveComplexityPartition(0, 0, 0)
		}
		return CompatibilityOutcome{RowsWritten: &zero}, nil
	}

	scanConfig := executor.baseConfig
	if len(scope.LanguageGlobs) > 0 {
		scanConfig.IncludeGlobs = scope.LanguageGlobs
	}
	if len(scope.ExcludeGlobs) > 0 {
		scanConfig.ExcludeGlobs = scope.ExcludeGlobs
	}

	computedAt, err := executor.nowOrRefuse()
	if err != nil {
		return CompatibilityOutcome{}, err
	}
	reposWithData := 0
	blameUnusableRepos := 0
	rowsWritten := 0

	for _, repo := range repos {
		files, blameUnusable, err := executor.loadRepoFileContents(
			ctx, repo.ID, run.OrganizationID, scanConfig, scope.MaxFiles,
		)
		if err != nil {
			return CompatibilityOutcome{}, err
		}
		if blameUnusable {
			blameUnusableRepos++
		}

		filtered := filterComplexityFiles(scanConfig, files, scope.MaxFiles)
		if len(filtered) == 0 {
			continue
		}

		ref, err := buildComplexityRef(ctx, executor.conn, repo.ID, run.OrganizationID)
		if err != nil {
			return CompatibilityOutcome{}, err
		}

		fileResults := make([]complexity.FileComplexity, 0, len(filtered))
		for _, file := range filtered {
			result, analyzeErr := complexity.AnalyzeFile(file.Path, file.Contents, scanConfig.Thresholds)
			if analyzeErr != nil {
				// ErrLanguageNotPorted included: fail loud, no fallback --
				// see this type's own doc, point 3.
				return CompatibilityOutcome{}, fmt.Errorf(
					"complexity: repo %s file %s: %w", repo.ID, file.Path, analyzeErr)
			}
			if result == nil {
				// Not analysed at all, or the analyzer skipped an
				// unparseable source -- both are Python's `None`, never a
				// zero row.
				continue
			}
			fileResults = append(fileResults, *result)
		}
		if len(fileResults) == 0 {
			continue
		}

		reposWithData++
		built, buildErr := complexity.BuildSnapshots(
			repo.ID.String(), day, ref, fileResults, computedAt, run.OrganizationID,
		)
		if buildErr != nil {
			return CompatibilityOutcome{}, buildErr
		}
		if len(built.Snapshots) == 0 {
			continue
		}

		if err := writeFileComplexitySnapshots(ctx, executor.conn, built.Snapshots); err != nil {
			return CompatibilityOutcome{}, err
		}
		if err := writeRepoComplexityDaily(ctx, executor.conn, built.Repo); err != nil {
			return CompatibilityOutcome{}, err
		}
		rowsWritten += len(built.Snapshots) + 1
	}

	if reposWithData == 0 {
		// Python: repos_with_data == 0 after a non-empty repos list is a
		// hard failure (exit 1 -> RuntimeError through the bridge), not a
		// quiet zero-row success -- see this type's own doc, point 2.
		return CompatibilityOutcome{}, fmt.Errorf(
			"%w: %d repos, org_id=%s", ErrComplexityNoDataProduced, len(repos), run.OrganizationID)
	}

	if executor.observer != nil {
		_ = executor.observer.ObserveComplexityPartition(len(repos), reposWithData, blameUnusableRepos)
	}
	return CompatibilityOutcome{RowsWritten: &rowsWritten}, nil
}

// loadRepoFileContents ports run_complexity_db_job's per-repo file-source
// selection (job_complexity_db.py:303-374): git_files first, then a
// git_blame fallback for whatever git_files is missing, budgeted by
// max_files across both phases via fileselect.go's plan functions.
//
// The returned blameUnusable mirrors Python's own flag: true only when there
// were files this repo could not recover from EITHER source (see
// complexity.BlameUnusable / complexity.EmptyGitFilesBlameUnusable) --
// purely a telemetry signal, it does not change what gets returned.
func (executor *ComplexityExecutor) loadRepoFileContents(
	ctx context.Context, repoID uuid.UUID, orgID string, scanConfig complexity.ScanConfig, maxFiles *int,
) ([]complexityFileContent, bool, error) {
	totalFiles, nonEmpty, err := gitFileCounts(ctx, executor.conn, repoID, orgID)
	if err != nil {
		return nil, false, err
	}

	var files []complexityFileContent
	blameUnusable := false

	if nonEmpty > 0 {
		gitFiles, loadErr := loadComplexityGitFiles(ctx, executor.conn, repoID, orgID, maxFiles)
		if loadErr != nil {
			return nil, false, loadErr
		}
		files = append(files, gitFiles...)

		plan := complexity.PlanNonEmptyBranch(maxFiles, totalFiles, nonEmpty, len(gitFiles))
		if plan.AttemptBlameFallback {
			missingPaths, missingErr := loadComplexityMissingPaths(ctx, executor.conn, repoID, orgID, plan.BlameLimit)
			if missingErr != nil {
				return nil, false, missingErr
			}
			filteredMissing := make([]string, 0, len(missingPaths))
			for _, path := range missingPaths {
				if scanConfig.ShouldProcess(path) {
					filteredMissing = append(filteredMissing, path)
				}
			}
			if len(filteredMissing) > 0 {
				hasBlameText, blameTextErr := hasComplexityBlameLineText(ctx, executor.conn, repoID, orgID)
				if blameTextErr != nil {
					return nil, false, blameTextErr
				}
				if hasBlameText {
					blameFiles, blameErr := loadComplexityBlameContents(
						ctx, executor.conn, repoID, orgID, filteredMissing, plan.BlameLimit,
					)
					if blameErr != nil {
						return nil, false, blameErr
					}
					files = append(files, blameFiles...)
				} else {
					blameUnusable = complexity.BlameUnusable(len(filteredMissing), hasBlameText)
				}
			}
		}
	} else {
		hasBlameText, blameTextErr := hasComplexityBlameLineText(ctx, executor.conn, repoID, orgID)
		if blameTextErr != nil {
			return nil, false, blameTextErr
		}
		blameUnusable = complexity.EmptyGitFilesBlameUnusable(totalFiles, hasBlameText)
		plan := complexity.PlanEmptyBranch(maxFiles, hasBlameText)
		if plan.AttemptBlameFallback {
			blameFiles, blameErr := loadComplexityBlameContents(ctx, executor.conn, repoID, orgID, nil, plan.BlameLimit)
			if blameErr != nil {
				return nil, false, blameErr
			}
			files = append(files, blameFiles...)
		}
	}

	return files, blameUnusable, nil
}

// filterComplexityFiles ports _filter_files (job_complexity_db.py:227-236):
// the FINAL should_process gate and max_files cap, applied to the merged
// git_files+git_blame result -- the one place both apply together, even
// though max_files also bounded each DB read individually.
func filterComplexityFiles(
	scanConfig complexity.ScanConfig, files []complexityFileContent, maxFiles *int,
) []complexityFileContent {
	filtered := make([]complexityFileContent, 0, len(files))
	for _, file := range files {
		if !scanConfig.ShouldProcess(file.Path) {
			continue
		}
		filtered = append(filtered, file)
		if maxFiles != nil && len(filtered) >= *maxFiles {
			break
		}
	}
	return filtered
}
