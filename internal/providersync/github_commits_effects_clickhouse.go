package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type GitHubCommitsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type GitLabCommitsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitHubCommitsClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	return writeGitCommitsEffect(ctx, sink.Conn, sink.Lease, "github", claim, effect)
}

func (sink GitLabCommitsClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	return writeGitCommitsEffect(ctx, sink.Conn, sink.Lease, "gitlab", claim, effect)
}

func writeGitCommitsEffect(ctx context.Context, conn driver.Conn, lease providerfoundation.LeaseGuard, provider string, claim Claim, effect EffectBatch) error {
	if ctx == nil || lease == nil || claim.Validate() != nil || claim.Provider != provider || claim.Dataset != "commits" || effect.Destination != "git_commits" {
		return ErrInvalidConfiguration
	}
	if err := lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[gitCommitRow](effect)
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
	if conn == nil {
		return ErrInvalidConfiguration
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO git_commits (org_id, repo_id, hash, message, author_name, author_email, author_when, committer_name, committer_email, committer_when, parents, last_synced)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.OrgID, row.RepoID, row.Hash, row.Message, row.AuthorName, row.AuthorEmail, row.AuthorWhen, row.CommitterName, row.CommitterEmail, row.CommitterWhen, row.Parents, row.LastSynced); err != nil {
			return err
		}
	}
	if err := lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubCommitsClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	return inspectGitCommitsEffect(ctx, sink.Conn, sink.Lease, "github", claim, effect)
}

func (sink GitLabCommitsClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	return inspectGitCommitsEffect(ctx, sink.Conn, sink.Lease, "gitlab", claim, effect)
}

func inspectGitCommitsEffect(ctx context.Context, conn driver.Conn, lease providerfoundation.LeaseGuard, provider string, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if ctx == nil || lease == nil || claim.Validate() != nil || claim.Provider != provider || claim.Dataset != "commits" || effect.Destination != "git_commits" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[gitCommitRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	if conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	exact, absent := 0, 0
	for _, row := range expected {
		if err := row.validate(claim); err != nil {
			return EffectConflict, err
		}
		inspected, inspectErr := inspectGitCommit(ctx, conn, row)
		if inspectErr != nil {
			return EffectConflict, inspectErr
		}
		if inspected == EffectExact {
			exact++
		} else if inspected == EffectAbsent {
			absent++
		} else {
			return EffectConflict, nil
		}
	}
	if exact == len(expected) {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func inspectGitCommit(ctx context.Context, conn driver.Conn, expected gitCommitRow) (EffectInspection, error) {
	rows, err := conn.Query(ctx, `SELECT org_id, repo_id, hash, message, author_name, author_email, author_when, committer_name, committer_email, committer_when, parents, last_synced FROM git_commits FINAL WHERE org_id = ? AND repo_id = ? AND hash = ?`, expected.OrgID, expected.RepoID, expected.Hash)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	actual := gitCommitVersion{}
	for rows.Next() {
		if err := rows.Scan(&actual.Row.OrgID, &actual.Row.RepoID, &actual.Row.Hash, &actual.Row.Message, &actual.Row.AuthorName, &actual.Row.AuthorEmail, &actual.Row.AuthorWhen, &actual.Row.CommitterName, &actual.Row.CommitterEmail, &actual.Row.CommitterWhen, &actual.Row.Parents, &actual.LastSynced); err != nil {
			return EffectConflict, err
		}
		actual.Found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareCommitVersion(expected, actual), nil
}

type gitCommitVersion struct {
	Row        gitCommitRow
	LastSynced time.Time
	Found      bool
}

func compareCommitVersion(expected gitCommitRow, actual gitCommitVersion) EffectInspection {
	if !actual.Found || actual.LastSynced.IsZero() || actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().After(expected.LastSynced.UTC()) ||
		actual.Row.OrgID != expected.OrgID || actual.Row.RepoID != expected.RepoID ||
		actual.Row.Hash != expected.Hash ||
		!stringPointersEqual(actual.Row.Message, expected.Message) ||
		actual.Row.AuthorName != expected.AuthorName ||
		!stringPointersEqual(actual.Row.AuthorEmail, expected.AuthorEmail) ||
		!actual.Row.AuthorWhen.UTC().Equal(expected.AuthorWhen.UTC()) ||
		actual.Row.CommitterName != expected.CommitterName ||
		!stringPointersEqual(actual.Row.CommitterEmail, expected.CommitterEmail) ||
		!actual.Row.CommitterWhen.UTC().Equal(expected.CommitterWhen.UTC()) ||
		actual.Row.Parents != expected.Parents {
		return EffectConflict
	}
	return EffectExact
}

var _ EffectSink = GitHubCommitsClickHouseEffects{}
var _ EffectReadback = GitHubCommitsClickHouseEffects{}
var _ EffectSink = GitLabCommitsClickHouseEffects{}
var _ EffectReadback = GitLabCommitsClickHouseEffects{}
