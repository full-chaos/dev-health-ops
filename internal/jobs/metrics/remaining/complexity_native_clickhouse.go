package remaining

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/complexity"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// complexityTableRequirements mirrors membership's/capacity's construction-
// time schema guard: a database this executor cannot compute against refuses
// the kind once, loudly, at NewComplexityExecutor -- rather than letting the
// handler claim partitions and fail every one of them.
var complexityTableRequirements = map[string][]string{
	"repos": {
		"id", "repo", "org_id",
	},
	"git_files": {
		"repo_id", "org_id", "path", "contents",
	},
	"git_blame": {
		"repo_id", "org_id", "path", "line_no", "line",
	},
	"file_complexity_snapshots": {
		"repo_id", "as_of_day", "ref", "file_path", "language", "loc",
		"functions_count", "cyclomatic_total", "cyclomatic_avg",
		"high_complexity_functions", "very_high_complexity_functions",
		"computed_at", "org_id",
	},
	"repo_complexity_daily": {
		"repo_id", "day", "loc_total", "cyclomatic_total", "cyclomatic_per_kloc",
		"high_complexity_functions", "very_high_complexity_functions",
		"computed_at", "org_id",
	},
}

func verifyComplexitySchema(ctx context.Context, conn driver.Conn) error {
	tables := make([]string, 0, len(complexityTableRequirements))
	for table := range complexityTableRequirements {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	for _, table := range tables {
		// Reuses capacity's column reader (system.columns query), same as
		// membership's own guard -- a second copy would be free to drift.
		present, err := capacityTableColumns(ctx, conn, table)
		if err != nil {
			return err
		}
		if len(present) == 0 {
			return fmt.Errorf("%w: table %s does not exist",
				ErrComplexitySchemaIncompatible, table)
		}
		var missing []string
		for _, column := range complexityTableRequirements[table] {
			if !present[column] {
				missing = append(missing, column)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("%w: %s is missing %s",
				ErrComplexitySchemaIncompatible, table, strings.Join(missing, ", "))
		}
	}
	return nil
}

// complexityRepoRef mirrors one row of `_load_repos`' result: an id, and the
// name Python fnmatch-filters client-side against search_pattern (there is no
// SQL LIKE here on purpose -- fnmatch semantics, ported via pythonparity, are
// not the same as SQL LIKE or Go's path.Match).
type complexityRepoRef struct {
	ID   uuid.UUID
	Name *string
}

// loadComplexityRepos ports _load_repos (job_complexity_db.py:47-64).
//
// A caller-supplied repoID short-circuits the whole thing to a single-repo
// slice with no DB read at all, exactly as Python's `if repo_id is not None:
// return [(repo_id, None)]` does -- including that the returned "name" is
// always None/nil in that path (never fetched), which matters because a
// scope with BOTH repo_id and search_pattern set never applies the pattern:
// Python's early return happens before fnmatch is ever reached.
func loadComplexityRepos(
	ctx context.Context, conn driver.Conn, repoID *uuid.UUID, searchPattern *string, orgID string,
) ([]complexityRepoRef, error) {
	if repoID != nil {
		return []complexityRepoRef{{ID: *repoID}}, nil
	}

	query := "SELECT id, repo FROM repos"
	arguments := map[string]any{}
	if orgID != "" {
		query += " WHERE org_id = {org_id:String}"
		arguments["org_id"] = orgID
	}
	rows, err := conn.Query(ctx, query, namedArguments(arguments)...)
	if err != nil {
		return nil, fmt.Errorf("load repos: %w", err)
	}
	defer rows.Close()

	var repos []complexityRepoRef
	for rows.Next() {
		var (
			id   uuid.UUID
			name *string
		)
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("scan repo: %w", err)
		}
		if searchPattern != nil {
			nameValue := ""
			if name != nil {
				nameValue = *name
			}
			if !pythonparity.FnMatch(nameValue, *searchPattern) {
				continue
			}
		}
		repos = append(repos, complexityRepoRef{ID: id, Name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load repos: %w", err)
	}
	return repos, nil
}

// gitFileCounts ports _git_file_counts (job_complexity_db.py:67-83).
func gitFileCounts(
	ctx context.Context, conn driver.Conn, repoID uuid.UUID, orgID string,
) (total int, nonEmpty int, err error) {
	rows, queryErr := conn.Query(ctx, `
		SELECT
		  count() AS total,
		  countIf(contents IS NOT NULL AND contents != '') AS non_empty
		FROM git_files
		WHERE repo_id = {repo_id:UUID}
		  AND org_id = {org_id:String}
	`, namedArguments(map[string]any{"repo_id": repoID, "org_id": orgID})...)
	if queryErr != nil {
		return 0, 0, fmt.Errorf("git file counts: %w", queryErr)
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, 0, rows.Err()
	}
	if err := rows.Scan(&total, &nonEmpty); err != nil {
		return 0, 0, fmt.Errorf("scan git file counts: %w", err)
	}
	return total, nonEmpty, rows.Err()
}

// complexityFileContent mirrors a (path, contents) pair -- both
// _load_git_files and _load_blame_contents return this same shape.
type complexityFileContent struct {
	Path     string
	Contents string
}

// loadComplexityGitFiles ports _load_git_files (job_complexity_db.py:86-102).
func loadComplexityGitFiles(
	ctx context.Context, conn driver.Conn, repoID uuid.UUID, orgID string, limit *int,
) ([]complexityFileContent, error) {
	query := `
		SELECT path, contents
		FROM git_files
		WHERE repo_id = {repo_id:UUID}
		  AND org_id = {org_id:String}
		  AND contents IS NOT NULL
		  AND contents != ''
		ORDER BY path
	`
	arguments := map[string]any{"repo_id": repoID, "org_id": orgID}
	if limit != nil {
		query += " LIMIT {limit:UInt64}"
		arguments["limit"] = uint64(*limit)
	}
	return queryFileContents(ctx, conn, query, arguments, "load git files")
}

// loadComplexityMissingPaths ports _load_missing_paths (job_complexity_db.py:105-121).
func loadComplexityMissingPaths(
	ctx context.Context, conn driver.Conn, repoID uuid.UUID, orgID string, limit *int,
) ([]string, error) {
	query := `
		SELECT path
		FROM git_files
		WHERE repo_id = {repo_id:UUID}
		  AND org_id = {org_id:String}
		  AND (contents IS NULL OR contents = '')
		ORDER BY path
	`
	arguments := map[string]any{"repo_id": repoID, "org_id": orgID}
	if limit != nil {
		query += " LIMIT {limit:UInt64}"
		arguments["limit"] = uint64(*limit)
	}
	rows, err := conn.Query(ctx, query, namedArguments(arguments)...)
	if err != nil {
		return nil, fmt.Errorf("load missing paths: %w", err)
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("scan missing path: %w", err)
		}
		paths = append(paths, path)
	}
	return paths, rows.Err()
}

// loadComplexityBlameContents ports _load_blame_contents
// (job_complexity_db.py:124-149): reconstructs file contents from git_blame
// by concatenating each line in line_no order.
func loadComplexityBlameContents(
	ctx context.Context, conn driver.Conn, repoID uuid.UUID, orgID string, paths []string, limit *int,
) ([]complexityFileContent, error) {
	query := `
		SELECT
		  path,
		  arrayStringConcat(
		    arrayMap(
		      x -> x.2,
		      arraySort(groupArray((line_no, ifNull(line, ''))))
		    ),
		    '\n'
		  ) AS contents
		FROM git_blame
		WHERE repo_id = {repo_id:UUID}
		  AND org_id = {org_id:String}
	`
	arguments := map[string]any{"repo_id": repoID, "org_id": orgID}
	if len(paths) > 0 {
		query += " AND path IN {paths:Array(String)}"
		arguments["paths"] = paths
	}
	query += " GROUP BY path ORDER BY path"
	if limit != nil {
		query += " LIMIT {limit:UInt64}"
		arguments["limit"] = uint64(*limit)
	}
	return queryFileContents(ctx, conn, query, arguments, "load blame contents")
}

func queryFileContents(
	ctx context.Context, conn driver.Conn, query string, arguments map[string]any, errContext string,
) ([]complexityFileContent, error) {
	rows, err := conn.Query(ctx, query, namedArguments(arguments)...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errContext, err)
	}
	defer rows.Close()

	var files []complexityFileContent
	for rows.Next() {
		var file complexityFileContent
		if err := rows.Scan(&file.Path, &file.Contents); err != nil {
			return nil, fmt.Errorf("scan %s: %w", errContext, err)
		}
		files = append(files, file)
	}
	return files, rows.Err()
}

// hasComplexityBlameLineText ports _has_blame_line_text
// (job_complexity_db.py:152-175): a cheap existence probe for usable
// (non-null, non-empty) line text, since GitHub's Blame API never populates
// it (git_blame.line is NULL for every GitHub-synced repo -- CHAOS-2861).
func hasComplexityBlameLineText(
	ctx context.Context, conn driver.Conn, repoID uuid.UUID, orgID string,
) (bool, error) {
	rows, err := conn.Query(ctx, `
		SELECT 1
		FROM git_blame
		WHERE repo_id = {repo_id:UUID}
		  AND org_id = {org_id:String}
		  AND line IS NOT NULL
		  AND line != ''
		LIMIT 1
	`, namedArguments(map[string]any{"repo_id": repoID, "org_id": orgID})...)
	if err != nil {
		return false, fmt.Errorf("has blame line text: %w", err)
	}
	defer rows.Close()
	return rows.Next(), rows.Err()
}

// maxLastSyncedComplexity ports _max_last_synced (job_complexity_db.py:178-190).
func maxLastSyncedComplexity(
	ctx context.Context, conn driver.Conn, table string, repoID uuid.UUID, orgID string,
) (*time.Time, error) {
	// table is one of exactly two caller-supplied literals ("git_files",
	// "git_blame") -- never request-controlled -- matching the Python
	// f-string's own trust boundary.
	query := fmt.Sprintf(`
		SELECT maxOrNull(last_synced)
		FROM %s
		WHERE repo_id = {repo_id:UUID}
		  AND org_id = {org_id:String}
	`, table)
	rows, err := conn.Query(ctx, query, namedArguments(map[string]any{"repo_id": repoID, "org_id": orgID})...)
	if err != nil {
		return nil, fmt.Errorf("max last synced (%s): %w", table, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, rows.Err()
	}
	var lastSynced *time.Time
	if err := rows.Scan(&lastSynced); err != nil {
		return nil, fmt.Errorf("scan max last synced (%s): %w", table, err)
	}
	return lastSynced, rows.Err()
}

// buildComplexityRef ports _build_ref (job_complexity_db.py:193-203): the
// stored `ref` value is the later of git_files'/git_blame's own
// last_synced watermark, formatted "db_last_synced:<iso8601 UTC>", or the
// literal "db_last_synced:unknown" when neither table has ever synced this
// repo.
func buildComplexityRef(
	ctx context.Context, conn driver.Conn, repoID uuid.UUID, orgID string,
) (string, error) {
	filesSynced, err := maxLastSyncedComplexity(ctx, conn, "git_files", repoID, orgID)
	if err != nil {
		return "", err
	}
	blameSynced, err := maxLastSyncedComplexity(ctx, conn, "git_blame", repoID, orgID)
	if err != nil {
		return "", err
	}
	var latest *time.Time
	for _, candidate := range []*time.Time{filesSynced, blameSynced} {
		if candidate == nil {
			continue
		}
		if latest == nil || candidate.After(*latest) {
			latest = candidate
		}
	}
	if latest == nil {
		return "db_last_synced:unknown", nil
	}
	return "db_last_synced:" + latest.UTC().Format(time.RFC3339Nano), nil
}

// writeFileComplexitySnapshots batch-inserts file_complexity_snapshots rows,
// column order matching the CREATE TABLE in
// src/dev_health_ops/migrations/clickhouse/007_complexity_investment_issues.sql
// (+ org_id from 024_add_org_id.sql).
func writeFileComplexitySnapshots(
	ctx context.Context, conn driver.Conn, rows []complexity.FileSnapshot,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO file_complexity_snapshots (
		repo_id, as_of_day, ref, file_path, language, loc, functions_count,
		cyclomatic_total, cyclomatic_avg, high_complexity_functions,
		very_high_complexity_functions, computed_at, org_id
	)`)
	if err != nil {
		return fmt.Errorf("prepare file_complexity_snapshots batch: %w", err)
	}
	for _, row := range rows {
		repoID, parseErr := uuid.Parse(row.RepoID)
		if parseErr != nil {
			return fmt.Errorf("file complexity snapshot has an unparseable repo id: %w", parseErr)
		}
		if err := batch.Append(
			repoID, row.AsOfDay, row.Ref, row.FilePath, row.Language, row.LOC,
			row.FunctionsCount, row.CyclomaticTotal, row.CyclomaticAvg,
			row.HighComplexityFunctions, row.VeryHighComplexityFunctions,
			row.ComputedAt, row.OrgID,
		); err != nil {
			return fmt.Errorf("append file complexity snapshot: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send file_complexity_snapshots batch: %w", err)
	}
	return nil
}

// writeRepoComplexityDaily batch-inserts the single repo_complexity_daily row
// for the day (always exactly one row per call -- see BuildSnapshots' own
// one-day-only doc).
func writeRepoComplexityDaily(
	ctx context.Context, conn driver.Conn, row complexity.RepoDaily,
) error {
	repoID, err := uuid.Parse(row.RepoID)
	if err != nil {
		return fmt.Errorf("repo complexity daily row has an unparseable repo id: %w", err)
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO repo_complexity_daily (
		repo_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc,
		high_complexity_functions, very_high_complexity_functions, computed_at, org_id
	)`)
	if err != nil {
		return fmt.Errorf("prepare repo_complexity_daily batch: %w", err)
	}
	if err := batch.Append(
		repoID, row.Day, row.LOCTotal, row.CyclomaticTotal, row.CyclomaticPerKLOC,
		row.HighComplexityFunctions, row.VeryHighComplexityFunctions, row.ComputedAt, row.OrgID,
	); err != nil {
		return fmt.Errorf("append repo complexity daily: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send repo_complexity_daily batch: %w", err)
	}
	return nil
}
