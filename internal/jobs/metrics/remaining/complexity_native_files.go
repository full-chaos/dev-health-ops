package remaining

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/complexity"
)

// complexityFetchBudgetBytes caps the file text one ClickHouse fetch brings
// into memory. go-heavy runs several queues in one 768Mi container, and the
// scope has no max_files, so a repo's whole git_files text used to be held at
// once (CHAOS-10586: OOMKilled 4 s into the job). A single file larger than
// the budget is still fetched alone, and so are all rows of one path: git_files
// can hold several unmerged versions of a path, the old flow analysed each
// row, and splitting them across batches would double-fetch them. The budget
// is therefore soft by the listed bytes of one path's duplicate rows.
const complexityFetchBudgetBytes = 16 << 20

// complexityFileSize is one candidate file with the bytes its text will cost.
type complexityFileSize struct {
	Path  string
	Bytes int64
}

// complexityFileSource is the read seam of the per-repo file selection: a
// cheap size listing first, then the text in budgeted batches.
type complexityFileSource interface {
	fileCounts(ctx context.Context, repoID uuid.UUID, orgID string) (total, nonEmpty int, err error)
	listGitFiles(ctx context.Context, repoID uuid.UUID, orgID string, limit *int) ([]complexityFileSize, error)
	fetchGitFiles(ctx context.Context, repoID uuid.UUID, orgID string, paths []string) ([]complexityFileContent, error)
	missingPaths(ctx context.Context, repoID uuid.UUID, orgID string, limit *int) ([]string, error)
	hasBlameText(ctx context.Context, repoID uuid.UUID, orgID string) (bool, error)
	listBlameFiles(ctx context.Context, repoID uuid.UUID, orgID string, paths []string, limit *int) ([]complexityFileSize, error)
	fetchBlameFiles(ctx context.Context, repoID uuid.UUID, orgID string, paths []string) ([]complexityFileContent, error)
}

// complexityCollector is _filter_files + the analysis loop folded into one
// streaming step: a file is judged, analysed and its text dropped before the
// next one arrives. Order and the max_files cap match the old
// load-everything-then-filter flow.
type complexityCollector struct {
	scanConfig complexity.ScanConfig
	maxFiles   *int
	accepted   int
	repoID     uuid.UUID
	results    []complexity.FileComplexity
}

func (collector *complexityCollector) remaining() (int, bool) {
	if collector.maxFiles == nil {
		return 0, false
	}
	left := *collector.maxFiles - collector.accepted
	if left < 0 {
		left = 0
	}
	return left, true
}

func (collector *complexityCollector) full() bool {
	left, capped := collector.remaining()
	return capped && left == 0
}

func (collector *complexityCollector) add(file complexityFileContent) error {
	if collector.full() || !collector.scanConfig.ShouldProcess(file.Path) {
		return nil
	}
	collector.accepted++
	result, err := complexity.AnalyzeFile(file.Path, file.Contents, collector.scanConfig.Thresholds)
	if err != nil {
		// ErrLanguageNotPorted included: fail loud, no fallback.
		return fmt.Errorf("complexity: repo %s file %s: %w", collector.repoID, file.Path, err)
	}
	if result != nil {
		collector.results = append(collector.results, *result)
	}
	return nil
}

// feed analyses the listed files in budgeted fetches. Files the scan config
// would drop are never fetched, and the list is cut to the cap that is left.
func (collector *complexityCollector) feed(
	sizes []complexityFileSize,
	budget int64,
	fetch func(paths []string) ([]complexityFileContent, error),
) error {
	wanted := make([]complexityFileSize, 0, len(sizes))
	for _, size := range sizes {
		if collector.scanConfig.ShouldProcess(size.Path) {
			wanted = append(wanted, size)
		}
	}
	if left, capped := collector.remaining(); capped && len(wanted) > left {
		wanted = wanted[:left]
	}
	for start := 0; start < len(wanted); {
		end, used := start, int64(0)
		for end < len(wanted) {
			next := wanted[end]
			sameAsPrev := end > start && next.Path == wanted[end-1].Path
			if end > start && !sameAsPrev && used+next.Bytes > budget {
				break
			}
			used += next.Bytes
			end++
		}
		paths := make([]string, 0, end-start)
		for index := start; index < end; index++ {
			if index == start || wanted[index].Path != wanted[index-1].Path {
				paths = append(paths, wanted[index].Path)
			}
		}
		files, err := fetch(paths)
		if err != nil {
			return err
		}
		for _, file := range files {
			if err := collector.add(file); err != nil {
				return err
			}
		}
		start = end
	}
	return nil
}

// analyzeRepoFiles ports run_complexity_db_job's per-repo file-source
// selection (job_complexity_db.py:303-374): git_files first, then a git_blame
// fallback for whatever git_files is missing, budgeted by max_files across
// both phases via fileselect.go's plan functions.
//
// blameUnusable mirrors Python's own flag (telemetry only).
func analyzeRepoFiles(
	ctx context.Context, source complexityFileSource, repoID uuid.UUID, orgID string,
	scanConfig complexity.ScanConfig, maxFiles *int, budget int64,
) (results []complexity.FileComplexity, blameUnusable bool, err error) {
	collector := &complexityCollector{scanConfig: scanConfig, maxFiles: maxFiles, repoID: repoID}

	totalFiles, nonEmpty, err := source.fileCounts(ctx, repoID, orgID)
	if err != nil {
		return nil, false, err
	}

	fetchGit := func(paths []string) ([]complexityFileContent, error) {
		return source.fetchGitFiles(ctx, repoID, orgID, paths)
	}
	fetchBlame := func(paths []string) ([]complexityFileContent, error) {
		return source.fetchBlameFiles(ctx, repoID, orgID, paths)
	}

	if nonEmpty > 0 {
		gitSizes, listErr := source.listGitFiles(ctx, repoID, orgID, maxFiles)
		if listErr != nil {
			return nil, false, listErr
		}
		if err := collector.feed(gitSizes, budget, fetchGit); err != nil {
			return nil, false, err
		}

		plan := complexity.PlanNonEmptyBranch(maxFiles, totalFiles, nonEmpty, len(gitSizes))
		if plan.AttemptBlameFallback {
			missing, missingErr := source.missingPaths(ctx, repoID, orgID, plan.BlameLimit)
			if missingErr != nil {
				return nil, false, missingErr
			}
			filteredMissing := make([]string, 0, len(missing))
			for _, path := range missing {
				if scanConfig.ShouldProcess(path) {
					filteredMissing = append(filteredMissing, path)
				}
			}
			if len(filteredMissing) > 0 {
				hasBlameText, textErr := source.hasBlameText(ctx, repoID, orgID)
				if textErr != nil {
					return nil, false, textErr
				}
				if hasBlameText {
					blameSizes, listErr := source.listBlameFiles(ctx, repoID, orgID, filteredMissing, plan.BlameLimit)
					if listErr != nil {
						return nil, false, listErr
					}
					if err := collector.feed(blameSizes, budget, fetchBlame); err != nil {
						return nil, false, err
					}
				} else {
					blameUnusable = complexity.BlameUnusable(len(filteredMissing), hasBlameText)
				}
			}
		}
	} else {
		hasBlameText, textErr := source.hasBlameText(ctx, repoID, orgID)
		if textErr != nil {
			return nil, false, textErr
		}
		blameUnusable = complexity.EmptyGitFilesBlameUnusable(totalFiles, hasBlameText)
		plan := complexity.PlanEmptyBranch(maxFiles, hasBlameText)
		if plan.AttemptBlameFallback {
			blameSizes, listErr := source.listBlameFiles(ctx, repoID, orgID, nil, plan.BlameLimit)
			if listErr != nil {
				return nil, false, listErr
			}
			if err := collector.feed(blameSizes, budget, fetchBlame); err != nil {
				return nil, false, err
			}
		}
	}
	return collector.results, blameUnusable, nil
}

type clickhouseComplexitySource struct {
	conn driver.Conn
}

func (source clickhouseComplexitySource) fileCounts(
	ctx context.Context, repoID uuid.UUID, orgID string,
) (int, int, error) {
	return gitFileCounts(ctx, source.conn, repoID, orgID)
}

func (source clickhouseComplexitySource) missingPaths(
	ctx context.Context, repoID uuid.UUID, orgID string, limit *int,
) ([]string, error) {
	return loadComplexityMissingPaths(ctx, source.conn, repoID, orgID, limit)
}

func (source clickhouseComplexitySource) hasBlameText(
	ctx context.Context, repoID uuid.UUID, orgID string,
) (bool, error) {
	return hasComplexityBlameLineText(ctx, source.conn, repoID, orgID)
}

func (source clickhouseComplexitySource) listGitFiles(
	ctx context.Context, repoID uuid.UUID, orgID string, limit *int,
) ([]complexityFileSize, error) {
	query := `
		SELECT path, ifNull(length(contents), 0)
		FROM git_files
		WHERE repo_id = {repo_id:UUID}
		  AND org_id = {org_id:String}
		  AND contents IS NOT NULL
		  AND contents != ''
		ORDER BY path
	`
	arguments := map[string]any{"repo_id": repoID.String(), "org_id": orgID}
	if limit != nil {
		query += " LIMIT {limit:UInt64}"
		arguments["limit"] = uint64(*limit)
	}
	return queryFileSizes(ctx, source.conn, query, arguments, "list git files")
}

func (source clickhouseComplexitySource) fetchGitFiles(
	ctx context.Context, repoID uuid.UUID, orgID string, paths []string,
) ([]complexityFileContent, error) {
	return queryFileContents(ctx, source.conn, `
		SELECT path, contents
		FROM git_files
		WHERE repo_id = {repo_id:UUID}
		  AND org_id = {org_id:String}
		  AND contents IS NOT NULL
		  AND contents != ''
		  AND path IN {paths:Array(String)}
		ORDER BY path
	`, map[string]any{"repo_id": repoID.String(), "org_id": orgID, "paths": paths}, "load git files")
}

// blameFilter is the shared WHERE of the blame listing and fetch.
const blameFilter = `
		WHERE repo_id = {repo_id:UUID}
		  AND org_id = {org_id:String}
`

func (source clickhouseComplexitySource) listBlameFiles(
	ctx context.Context, repoID uuid.UUID, orgID string, paths []string, limit *int,
) ([]complexityFileSize, error) {
	query := `SELECT path, sum(length(ifNull(line, ''))) + count() AS bytes FROM git_blame` + blameFilter
	arguments := map[string]any{"repo_id": repoID.String(), "org_id": orgID}
	if len(paths) > 0 {
		query += " AND path IN {paths:Array(String)}"
		arguments["paths"] = paths
	}
	query += " GROUP BY path ORDER BY path"
	if limit != nil {
		query += " LIMIT {limit:UInt64}"
		arguments["limit"] = uint64(*limit)
	}
	return queryFileSizes(ctx, source.conn, query, arguments, "list blame files")
}

func (source clickhouseComplexitySource) fetchBlameFiles(
	ctx context.Context, repoID uuid.UUID, orgID string, paths []string,
) ([]complexityFileContent, error) {
	return queryFileContents(ctx, source.conn, `
		SELECT
		  path,
		  arrayStringConcat(
		    arrayMap(
		      x -> x.2,
		      arraySort(groupArray((line_no, ifNull(line, ''))))
		    ),
		    '\n'
		  ) AS contents
		FROM git_blame`+blameFilter+`
		  AND path IN {paths:Array(String)}
		GROUP BY path ORDER BY path
	`, map[string]any{"repo_id": repoID.String(), "org_id": orgID, "paths": paths}, "load blame contents")
}

func queryFileSizes(
	ctx context.Context, conn driver.Conn, query string, arguments map[string]any, errContext string,
) ([]complexityFileSize, error) {
	rows, err := conn.Query(ctx, query, namedArguments(arguments)...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errContext, err)
	}
	defer rows.Close()

	var sizes []complexityFileSize
	for rows.Next() {
		var (
			path  string
			bytes uint64
		)
		if err := rows.Scan(&path, &bytes); err != nil {
			return nil, fmt.Errorf("scan %s: %w", errContext, err)
		}
		sizes = append(sizes, complexityFileSize{Path: path, Bytes: int64(bytes)})
	}
	return sizes, rows.Err()
}
