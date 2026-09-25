package localgit

import (
	"context"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

// The local sync writes three tables the stored-version invariant covers
// (repos, git_commits, git_pull_requests), each under a contract table
// (storedversion.Contract), like every other Go writer of those tables. A local
// run states every column its source has (a commit or a pull request read from
// the working tree) and keeps what it has no field for, so a rerun never
// blanks a value another version established. `dev-hops` inserts blindly; the
// rows a run writes into an EMPTY table are the same, which the differential
// oracle pins.

// org_id is written explicitly: a run with no organization writes the tables'
// column default, "default" (migration 024), as Python's omitted column does.
const defaultOrgID = "default"

const (
	repoInsert = `
INSERT INTO repos (
  id, org_id, repo, ref, created_at, settings, tags, provider, last_synced
)`
	commitInsert = `
INSERT INTO git_commits (
  org_id, repo_id, hash, message, author_name, author_email, author_when,
  committer_name, committer_email, committer_when, parents, last_synced
)`
	pullRequestInsert = `
INSERT INTO git_pull_requests (
  repo_id, number, title, body, state, author_name, author_email,
  created_at, merged_at, closed_at, head_branch, base_branch,
  additions, deletions, changed_files, first_review_at, first_comment_at,
  changes_requested_count, reviews_count, comments_count, last_synced,
  source_id, org_id
)`
)

func contractColumns(rule storedversion.Rule, names ...string) []storedversion.Column {
	columns := make([]storedversion.Column, 0, len(names))
	for _, name := range names {
		columns = append(columns, storedversion.Column{Name: name, Rule: rule, Fields: []string{name}})
	}
	return columns
}

func joinColumns(groups ...[]storedversion.Column) []storedversion.Column {
	var out []storedversion.Column
	for _, group := range groups {
		out = append(out, group...)
	}
	return out
}

// repositoryContract: a local repository has no default-branch field (ref is
// R1, kept); every other column is stated.
var repositoryContract = storedversion.Contract{Writer: "local_sync", Table: "repos", Columns: joinColumns(
	contractColumns(storedversion.Identity, "id"),
	contractColumns(storedversion.Writer, "org_id"),
	contractColumns(storedversion.Stated, "repo"),
	[]storedversion.Column{{Name: "ref", Rule: storedversion.NoField}},
	contractColumns(storedversion.Stated, "created_at", "settings", "tags"),
	contractColumns(storedversion.Writer, "provider", "last_synced"),
)}

// commitContract: a commit is immutable and the read states every column.
var commitContract = storedversion.Contract{Writer: "local_sync", Table: "git_commits", Columns: joinColumns(
	contractColumns(storedversion.Writer, "org_id"),
	contractColumns(storedversion.Identity, "repo_id", "hash"),
	contractColumns(storedversion.Stated, "message", "author_name", "author_email", "author_when",
		"committer_name", "committer_email", "committer_when", "parents"),
	contractColumns(storedversion.Writer, "last_synced"),
)}

// pullRequestContract: an inferred pull request states its title, state,
// author, created_at, merged_at (terminal: an open ref never un-merges a held
// merge), closed_at and head branch, and has no field for the review, size,
// body and comment columns, which keep their held values (R1).
var pullRequestContract = storedversion.Contract{Writer: "local_sync", Table: "git_pull_requests", Columns: joinColumns(
	contractColumns(storedversion.Identity, "repo_id", "number"),
	contractColumns(storedversion.Stated, "title", "state", "author_name", "author_email", "created_at"),
	[]storedversion.Column{
		{Name: "merged_at", Rule: storedversion.Stated, Fields: []string{"merged_at"}, Terminal: true},
		{Name: "closed_at", Rule: storedversion.StateCoupled, Fields: []string{"closed_at"}},
	},
	contractColumns(storedversion.Stated, "head_branch", "base_branch"),
	[]storedversion.Column{
		{Name: "body", Rule: storedversion.NoField}, {Name: "additions", Rule: storedversion.NoField},
		{Name: "deletions", Rule: storedversion.NoField}, {Name: "changed_files", Rule: storedversion.NoField},
		{Name: "first_review_at", Rule: storedversion.NoField}, {Name: "first_comment_at", Rule: storedversion.NoField},
		{Name: "changes_requested_count", Rule: storedversion.NoField}, {Name: "reviews_count", Rule: storedversion.NoField},
		{Name: "comments_count", Rule: storedversion.NoField},
	},
	contractColumns(storedversion.Writer, "last_synced", "source_id", "org_id"),
)}

// StoredVersionSpecs lists the local-sync writers under a stored-version
// contract, keyed by table, for the invariant enumeration.
func StoredVersionSpecs() map[string][]storedversion.Spec {
	carry := func(contract storedversion.Contract) func(map[string]any) map[string]bool {
		return func(map[string]any) map[string]bool { return contract.Carry(func(string) bool { return false }) }
	}
	return map[string][]storedversion.Spec{
		"repos":             {{Name: "local sync repositories", Contract: repositoryContract, Insert: repoInsert, Carry: carry(repositoryContract)}},
		"git_commits":       {{Name: "local sync commits", Contract: commitContract, Insert: commitInsert, Carry: carry(commitContract)}},
		"git_pull_requests": {{Name: "local sync pull requests", Contract: pullRequestContract, Insert: pullRequestInsert, Carry: carry(pullRequestContract)}},
	}
}

// writeContracted applies the contract to the rows (one FINAL read of the keys
// it keeps a column of; a failed read fails the write, so nothing blanks a held
// value) and inserts them.
func (w Writer) writeContracted(ctx context.Context, contract storedversion.Contract, insert string, values [][]any) error {
	if len(values) == 0 {
		return nil
	}
	rows := make([]storedversion.Row, len(values))
	for i, row := range values {
		rows[i] = storedversion.Row{Values: row, Carry: contract.Carry(func(string) bool { return false })}
	}
	if _, err := contract.Apply(ctx, w.Conn, w.effectiveOrg(), insert, rows); err != nil {
		return err
	}
	batch, err := w.Conn.PrepareBatch(ctx, insert)
	if err != nil {
		return fmt.Errorf("prepare %s: %w", contract.Table, err)
	}
	defer func() { _ = batch.Abort() }()
	for _, row := range rows {
		if err := batch.Append(row.Values...); err != nil {
			return fmt.Errorf("append %s: %w", contract.Table, err)
		}
	}
	return batch.Send()
}

func (w Writer) effectiveOrg() string {
	if w.OrgID != "" {
		return w.OrgID
	}
	return defaultOrgID
}
