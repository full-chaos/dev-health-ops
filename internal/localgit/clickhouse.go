package localgit

import (
	"context"
	"fmt"
	"math"
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
	filesInsert       = `INSERT INTO git_files (repo_id, path, executable, contents, last_synced)`
	filesOrgIn        = `INSERT INTO git_files (repo_id, path, executable, contents, last_synced, org_id)`
	blameInsert       = `INSERT INTO git_blame (repo_id, path, line_no, author_email, author_name, author_when, commit_hash, line, last_synced)`
	blameOrgIn        = `INSERT INTO git_blame (repo_id, path, line_no, author_email, author_name, author_when, commit_hash, line, last_synced, org_id)`
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

// representable reports whether the ClickHouse client can write the instant as a
// DateTime64: the driver scales it through int64 nanoseconds, which wraps for a
// year before 1678 or after 2262. Python writes the raw tick count and a year the
// column does not document (2299 and after) comes back as another date; the port
// refuses instead of writing a wrapped timestamp.
func representable(t time.Time) bool {
	return !t.Before(time.Unix(0, math.MinInt64)) && !t.After(time.Unix(0, math.MaxInt64))
}

func checkRepresentable(what string, times ...time.Time) error {
	for _, t := range times {
		if !representable(t) {
			return fmt.Errorf("%s %s is outside the range the ClickHouse client can write (1678 to 2262)", what, t.UTC().Format(time.RFC3339))
		}
	}
	return nil
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
		if err := checkRepresentable("commit "+c.Hash+" committed at", c.CommittedAt); err != nil {
			return err
		}
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
		if s.Additions > math.MaxInt32 || s.Deletions > math.MaxInt32 {
			// Int32 columns: ClickHouse rejects the batch (a DataError in Python).
			return fmt.Errorf("commit %s: a line count past the Int32 column for %s", s.CommitHash, s.FilePath)
		}
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
		row, err := pullRequestRow(repoID, w.effectiveOrg(), p, synced)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	return w.writeContracted(ctx, pullRequestContract, pullRequestInsert, rows)
}

// pullRequestRow is one git_pull_requests insert row in pullRequestInsert's column
// order: an inferred pull request has no body, size, review or comment data, and
// the number must fit the UInt32 column and the times the DateTime64 client.
func pullRequestRow(repoID uuid.UUID, org string, p PullRequest, synced time.Time) ([]any, error) {
	if p.Number > math.MaxUint32 {
		// ClickHouse rejects the batch (a DataError in Python).
		return nil, fmt.Errorf("pull request number %d does not fit the UInt32 column", p.Number)
	}
	if err := checkRepresentable(fmt.Sprintf("pull request %d created at", p.Number), p.CreatedAt); err != nil {
		return nil, err
	}
	var merged any
	if p.MergedAt != nil {
		merged = *p.MergedAt
	}
	return []any{repoID, uint32(p.Number), nullable(p.Title), nil, p.State, nullable(p.AuthorName), nullable(p.AuthorEmail),
		p.CreatedAt, merged, nil, nullable(p.HeadBranch), nil, nil, nil, nil, nil, nil, uint32(0), uint32(0), uint32(0), synced, nil, org}, nil
}

// InsertFiles is insert_git_file_data (executable is 1 or 0).
func (w Writer) InsertFiles(ctx context.Context, repoID uuid.UUID, files []File) error {
	synced := w.now()
	rows := make([][]any, 0, len(files))
	for _, f := range files {
		executable := uint8(0)
		if f.Executable {
			executable = 1
		}
		rows = append(rows, []any{repoID, f.Path, executable, nullable(f.Contents), synced})
	}
	return w.insert(ctx, "git_files", filesInsert, filesOrgIn, rows)
}

// InsertBlame is insert_blame_data.
func (w Writer) InsertBlame(ctx context.Context, repoID uuid.UUID, lines []BlameLine) error {
	synced := w.now()
	rows := make([][]any, 0, len(lines))
	for _, l := range lines {
		rows = append(rows, []any{repoID, l.Path, uint32(l.LineNo), nullable(l.AuthorEmail), nullable(l.AuthorName), l.AuthorWhen, l.CommitHash, l.Line, synced})
	}
	return w.insert(ctx, "git_blame", blameInsert, blameOrgIn, rows)
}
