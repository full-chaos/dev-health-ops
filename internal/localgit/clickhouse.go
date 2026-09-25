package localgit

import (
	"context"
	"fmt"
	"strings"
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

func (w Writer) insert(ctx context.Context, table string, columns []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	if w.OrgID != "" {
		columns = append(append([]string{}, columns...), "org_id")
		for i := range rows {
			rows[i] = append(rows[i], w.OrgID)
		}
	}
	batch, err := w.Conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(columns, ", ")))
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
	return w.insert(ctx, "repos",
		[]string{"id", "repo", "ref", "created_at", "settings", "tags", "provider", "last_synced", "source_id"},
		[][]any{{id, name, nil, synced, nil, tagsJSON, "local", synced, nil}})
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
		rows = append(rows, []any{repoID, c.Hash, c.Message, nullable(c.AuthorName), nullable(c.AuthorEmail), c.CommittedAt,
			nullable(c.CommitterName), nullable(c.CommitterEmail), c.CommittedAt, uint32(len(c.Parents)), synced, nil})
	}
	return w.insert(ctx, "git_commits",
		[]string{"repo_id", "hash", "message", "author_name", "author_email", "author_when", "committer_name", "committer_email", "committer_when", "parents", "last_synced", "source_id"},
		rows)
}

// InsertCommitStats is insert_git_commit_stats.
func (w Writer) InsertCommitStats(ctx context.Context, repoID uuid.UUID, stats []CommitStat) error {
	synced := w.now()
	rows := make([][]any, 0, len(stats))
	for _, s := range stats {
		rows = append(rows, []any{repoID, s.CommitHash, s.FilePath, int32(s.Additions), int32(s.Deletions), s.OldFileMode, s.NewFileMode, synced})
	}
	return w.insert(ctx, "git_commit_stats",
		[]string{"repo_id", "commit_hash", "file_path", "additions", "deletions", "old_file_mode", "new_file_mode", "last_synced"},
		rows)
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
			p.CreatedAt, merged, nil, nullable(p.HeadBranch), nil, nil, nil, nil, nil, nil, uint32(0), uint32(0), uint32(0), synced, nil})
	}
	return w.insert(ctx, "git_pull_requests",
		[]string{"repo_id", "number", "title", "body", "state", "author_name", "author_email", "created_at", "merged_at", "closed_at", "head_branch", "base_branch",
			"additions", "deletions", "changed_files", "first_review_at", "first_comment_at", "changes_requested_count", "reviews_count", "comments_count", "last_synced", "source_id"},
		rows)
}
