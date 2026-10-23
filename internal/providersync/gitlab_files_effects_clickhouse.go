package providersync

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitLabFilesClickHouseEffects persists gitlab/files rows to the existing
// tenant-qualified git_files ReplacingMergeTree. The schema and readback
// comparison intentionally match GitHub files; the provider guard remains
// local so a GitLab effect can never be accepted under the GitHub claim.
type GitLabFilesClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitLabFilesClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "files" || effect.Destination != "git_files" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[gitFileRow](effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := row.validate(claim); err != nil {
			return err
		}
	}
	if len(rows) == 0 {
		return nil
	}
	if sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.preserveExistingContents(ctx, claim, rows); err != nil {
		return err
	}
	batch, err := sink.Conn.PrepareBatch(ctx, gitFilesInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.RepoID, row.Path, row.Executable, row.Contents, row.LastSynced, row.OrgID); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

// preserveExistingContents mirrors base_git.backfill_file_records: a
// paths-only rewrite must not replace known text with NULL in the shared
// ReplacingMergeTree. Freshly fetched content remains authoritative; only
// missing content is hydrated from the tenant-qualified winning row.
func (sink GitLabFilesClickHouseEffects) preserveExistingContents(
	ctx context.Context, claim Claim, rows []gitFileRow,
) error {
	pathsByRepo := map[string][]string{}
	repoOrder := []string{}
	seen := map[[2]string]struct{}{}
	for index := range rows {
		if rows[index].Contents != nil {
			continue
		}
		key := [2]string{rows[index].RepoID, rows[index].Path}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		if _, ok := pathsByRepo[rows[index].RepoID]; !ok {
			repoOrder = append(repoOrder, rows[index].RepoID)
		}
		pathsByRepo[rows[index].RepoID] = append(pathsByRepo[rows[index].RepoID], rows[index].Path)
	}
	existing := map[[2]string]*string{}
	for _, repoID := range repoOrder {
		for _, chunk := range chunkPathsByRenderedBytes(pathsByRepo[repoID], gitLabFilesPathsPerStatementBytes) {
			if err := sink.readExistingContents(ctx, claim.OrgID, repoID, chunk, existing); err != nil {
				return err
			}
		}
	}
	for index := range rows {
		if rows[index].Contents != nil {
			continue
		}
		if found := existing[[2]string{rows[index].RepoID, rows[index].Path}]; found != nil {
			rows[index].Contents = found
		}
	}
	return nil
}

// gitLabFilesPathsPerStatementBytes bounds the rendered path array of one
// readback statement, well under ClickHouse's max_query_size.
const gitLabFilesPathsPerStatementBytes = 128 * 1024

func (sink GitLabFilesClickHouseEffects) readExistingContents(
	ctx context.Context, orgID, repoID string, paths []string, existing map[[2]string]*string,
) error {
	result, err := sink.Conn.Query(ctx, `
SELECT path, contents FROM git_files FINAL
WHERE org_id = ? AND repo_id = ? AND path IN ?`,
		orgID, repoID, paths)
	if err != nil {
		return err
	}
	defer result.Close()
	for result.Next() {
		var path string
		var contents *string
		if err := result.Scan(&path, &contents); err != nil {
			return err
		}
		existing[[2]string{repoID, path}] = contents
	}
	return result.Err()
}

// chunkPathsByRenderedBytes splits paths into consecutive chunks whose
// quoted array literal stays within maxBytes (each byte may render as two).
// A path that alone exceeds maxBytes gets its own chunk.
func chunkPathsByRenderedBytes(paths []string, maxBytes int) [][]string {
	var chunks [][]string
	start, size := 0, 2
	for i, path := range paths {
		add := 2*len(path) + 4
		if i > start && size+add > maxBytes {
			chunks = append(chunks, paths[start:i])
			start, size = i, 2
		}
		size += add
	}
	if start < len(paths) {
		chunks = append(chunks, paths[start:])
	}
	return chunks
}

func (sink GitLabFilesClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "files" || effect.Destination != "git_files" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[gitFileRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	for _, row := range expected {
		if err := row.validate(claim); err != nil {
			return EffectConflict, err
		}
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	exact, absent := 0, 0
	for _, row := range expected {
		inspection, err := sink.inspectGitLabFile(ctx, row)
		if err != nil {
			return EffectConflict, err
		}
		switch inspection {
		case EffectExact:
			exact++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	switch {
	case exact == len(expected):
		return EffectExact, nil
	case absent == len(expected):
		return EffectAbsent, nil
	default:
		return EffectConflict, nil
	}
}

func (sink GitLabFilesClickHouseEffects) inspectGitLabFile(ctx context.Context, expected gitFileRow) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `
SELECT repo_id, path, executable, contents, last_synced, org_id
FROM git_files FINAL
WHERE org_id = ? AND repo_id = ? AND path = ?`, expected.OrgID, expected.RepoID, expected.Path)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual gitFileVersion
	for rows.Next() {
		if err := rows.Scan(
			&actual.Row.RepoID, &actual.Row.Path, &actual.Row.Executable, &actual.Row.Contents,
			&actual.LastSynced, &actual.Row.OrgID,
		); err != nil {
			return EffectConflict, err
		}
		actual.Found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitLabFileVersion(expected, actual), nil
}

func compareGitLabFileVersion(expected gitFileRow, actual gitFileVersion) EffectInspection {
	if !actual.Found || actual.LastSynced.IsZero() || actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().After(expected.LastSynced.UTC()) || actual.Row.RepoID != expected.RepoID ||
		actual.Row.Path != expected.Path || actual.Row.Executable != expected.Executable ||
		actual.Row.OrgID != expected.OrgID ||
		(expected.Contents != nil && !stringPointersEqual(actual.Row.Contents, expected.Contents)) {
		return EffectConflict
	}
	return EffectExact
}

var _ EffectSink = GitLabFilesClickHouseEffects{}
var _ EffectReadback = GitLabFilesClickHouseEffects{}
