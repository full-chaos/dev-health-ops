package localgit

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Writer writes the rows of a local sync the way ClickHouseStore does
// (storage/clickhouse.py): plain inserts, one batch per call, the column order
// of the Python insert with org_id appended only when the store has one
// (`_insert_rows` adds it when truthy; otherwise the column default applies),
// and last_synced the time of the call. There is no stored-version contract:
// ReplacingMergeTree(last_synced) reconciles duplicates, as it does for Python.
type Writer struct {
	Conn  driver.Conn
	OrgID string
	Now   func() time.Time
}

func (w Writer) now() time.Time {
	if w.Now != nil {
		return w.Now().UTC()
	}
	return time.Now().UTC()
}

// Statements of the tables the stored-version invariant does not cover:
// plain inserts, with org_id appended only when there is one (`_insert_rows`),
// so a run with no organization gets the column default.
const (
	commitStatsInsert = `INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, old_file_mode, new_file_mode, last_synced)`
	commitStatsOrgIn  = `INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, old_file_mode, new_file_mode, last_synced, org_id)`
)

// insert writes rows with the statement that fits: plain, or with org_id.
func (w Writer) insert(ctx context.Context, table, plain, withOrg string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	statement := plain
	if w.OrgID != "" {
		statement = withOrg
		for i := range rows {
			rows[i] = append(rows[i], w.OrgID)
		}
	}
	batch, err := w.Conn.PrepareBatch(ctx, statement)
	if err != nil {
		return fmt.Errorf("prepare %s: %w", table, err)
	}
	defer func() { _ = batch.Abort() }()
	for _, row := range rows {
		if err := batch.Append(row...); err != nil {
			return fmt.Errorf("append %s: %w", table, err)
		}
	}
	return batch.Send()
}

// InsertRepo is insert_repo(Repo(repo_path=..., repo=<dir name>, provider="local")):
// a Repo that was never flushed has no ref, settings or tags, and created_at
// is the sync time (build_repository_insert_row); tags are the repository's tag names (TagsJSON).
func (w Writer) InsertRepo(ctx context.Context, id uuid.UUID, name, tagsJSON string) error {
	synced := w.now()
	return w.writeContracted(ctx, repositoryContract, repoInsert,
		[][]any{{id, w.effectiveOrg(), name, nil, synced, nil, tagsJSON, "local", synced}})
}

func nullable(text *string) any {
	if text == nil {
		return nil
	}
	return *text
}

// InsertCommits is insert_git_commit_data. author_when is the committed time,
// as _extract_commit_info sets both.
func (w Writer) InsertCommits(ctx context.Context, repoID uuid.UUID, commits []Commit) error {
	synced := w.now()
	rows := make([][]any, 0, len(commits))
	for _, c := range commits {
		rows = append(rows, []any{w.effectiveOrg(), repoID, c.Hash, c.Message, nullable(c.AuthorName), nullable(c.AuthorEmail), c.CommittedAt,
			nullable(c.CommitterName), nullable(c.CommitterEmail), c.CommittedAt, uint32(len(c.Parents)), synced})
	}
	return w.writeContracted(ctx, commitContract, commitInsert, rows)
}

// InsertCommitStats is insert_git_commit_stats.
func (w Writer) InsertCommitStats(ctx context.Context, repoID uuid.UUID, stats []CommitStat) error {
	synced := w.now()
	rows := make([][]any, 0, len(stats))
	for _, s := range stats {
		rows = append(rows, []any{repoID, s.CommitHash, s.FilePath, int32(s.Additions), int32(s.Deletions), s.OldFileMode, s.NewFileMode, synced})
	}
	return w.insert(ctx, "git_commit_stats", commitStatsInsert, commitStatsOrgIn, rows)
}

// InsertPullRequests is insert_git_pull_requests for the inferred rows: no
// body, review or size data, zero counts.
func (w Writer) InsertPullRequests(ctx context.Context, repoID uuid.UUID, prs []PullRequest) error {
	synced := w.now()
	rows := make([][]any, 0, len(prs))
	for _, p := range prs {
		var merged any
		if p.MergedAt != nil {
			merged = *p.MergedAt
		}
		rows = append(rows, []any{repoID, uint32(p.Number), nullable(p.Title), nil, p.State, nullable(p.AuthorName), nullable(p.AuthorEmail),
			p.CreatedAt, merged, nil, nullable(p.HeadBranch), nil, nil, nil, nil, nil, nil, uint32(0), uint32(0), uint32(0), synced, nil, w.effectiveOrg()})
	}
	return w.writeContracted(ctx, pullRequestContract, pullRequestInsert, rows)
}
