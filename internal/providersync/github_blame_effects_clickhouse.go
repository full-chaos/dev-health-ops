package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type GitHubBlameClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitHubBlameClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "blame" ||
		(effect.Destination != "github_blame_path_progress" && effect.Destination != "git_blame") {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if effect.Destination == "github_blame_path_progress" {
		return sink.writeProgressEffect(ctx, claim, effect)
	}
	return sink.writeBlameEffect(ctx, claim, effect)
}

func (sink GitHubBlameClickHouseEffects) writeBlameEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	rows, err := decodeEffectRows[gitBlameRow](effect)
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
	batch, err := sink.Conn.PrepareBatch(ctx, `
INSERT INTO git_blame (
  repo_id, path, line_no, author_email, author_name, author_when,
  commit_hash, line, last_synced, org_id
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.RepoID, row.Path, row.LineNo, row.AuthorEmail, row.AuthorName,
			row.AuthorWhen, row.CommitHash, row.Line, row.LastSynced, row.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubBlameClickHouseEffects) writeProgressEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	rows, err := decodeEffectRows[gitHubBlamePathProgressRow](effect)
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
	batch, err := sink.Conn.PrepareBatch(ctx, `
INSERT INTO github_blame_path_progress (
  org_id, repo_id, tree_ref, path, generation, outcome, attempted_at
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.OrgID, row.RepoID, row.TreeRef, row.Path, row.Generation,
			row.Outcome, row.AttemptedAt,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubBlameClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "blame" ||
		(effect.Destination != "github_blame_path_progress" && effect.Destination != "git_blame") {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	if effect.Destination == "github_blame_path_progress" {
		return sink.inspectProgressEffect(ctx, claim, effect)
	}
	return sink.inspectBlameEffect(ctx, claim, effect)
}

func (sink GitHubBlameClickHouseEffects) inspectBlameEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	expected, err := decodeEffectRows[gitBlameRow](effect)
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
		inspection, err := sink.inspectBlame(ctx, row)
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
	if exact == len(expected) {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func (sink GitHubBlameClickHouseEffects) inspectProgressEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	expected, err := decodeEffectRows[gitHubBlamePathProgressRow](effect)
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
		inspection, inspectErr := sink.inspectProgress(ctx, row)
		if inspectErr != nil {
			return EffectConflict, inspectErr
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
	if exact == len(expected) {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func (sink GitHubBlameClickHouseEffects) inspectProgress(
	ctx context.Context,
	expected gitHubBlamePathProgressRow,
) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `
SELECT org_id, repo_id, tree_ref, path, generation, outcome, attempted_at
FROM github_blame_path_progress FINAL
WHERE org_id = ? AND repo_id = ? AND tree_ref = ? AND path = ? AND generation = ?`,
		expected.OrgID, expected.RepoID, expected.TreeRef, expected.Path,
		expected.Generation,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual gitHubBlamePathProgressRow
	found := false
	for rows.Next() {
		if err := rows.Scan(
			&actual.OrgID, &actual.RepoID, &actual.TreeRef, &actual.Path,
			&actual.Generation, &actual.Outcome, &actual.AttemptedAt,
		); err != nil {
			return EffectConflict, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	if !found || actual.AttemptedAt.UTC().Before(expected.AttemptedAt.UTC()) {
		return EffectAbsent, nil
	}
	if actual.OrgID != expected.OrgID || actual.RepoID != expected.RepoID ||
		actual.TreeRef != expected.TreeRef || actual.Path != expected.Path ||
		actual.Generation != expected.Generation || actual.Outcome != expected.Outcome ||
		!actual.AttemptedAt.UTC().Equal(expected.AttemptedAt.UTC()) {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

func (sink GitHubBlameClickHouseEffects) inspectBlame(ctx context.Context, expected gitBlameRow) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `
SELECT repo_id, path, line_no, author_email, author_name, author_when,
       commit_hash, line, last_synced, org_id
FROM git_blame FINAL
WHERE org_id = ? AND repo_id = ? AND path = ? AND line_no = ?`,
		expected.OrgID, expected.RepoID, expected.Path, expected.LineNo,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual gitBlameVersion
	for rows.Next() {
		if err := rows.Scan(
			&actual.Row.RepoID, &actual.Row.Path, &actual.Row.LineNo,
			&actual.Row.AuthorEmail, &actual.Row.AuthorName, &actual.Row.AuthorWhen,
			&actual.Row.CommitHash, &actual.Row.Line, &actual.LastSynced, &actual.Row.OrgID,
		); err != nil {
			return EffectConflict, err
		}
		actual.Found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitBlameVersion(expected, actual), nil
}

type gitBlameVersion struct {
	Row        gitBlameRow
	LastSynced time.Time
	Found      bool
}

func compareGitBlameVersion(expected gitBlameRow, actual gitBlameVersion) EffectInspection {
	if !actual.Found || actual.LastSynced.IsZero() || actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().After(expected.LastSynced.UTC()) || actual.Row.RepoID != expected.RepoID ||
		actual.Row.Path != expected.Path || actual.Row.LineNo != expected.LineNo ||
		actual.Row.OrgID != expected.OrgID ||
		!stringPointersEqual(actual.Row.AuthorEmail, expected.AuthorEmail) ||
		!stringPointersEqual(actual.Row.AuthorName, expected.AuthorName) ||
		!timePointersEqual(actual.Row.AuthorWhen, expected.AuthorWhen) ||
		!stringPointersEqual(actual.Row.CommitHash, expected.CommitHash) ||
		!stringPointersEqual(actual.Row.Line, expected.Line) {
		return EffectConflict
	}
	return EffectExact
}

var _ EffectSink = GitHubBlameClickHouseEffects{}
var _ EffectReadback = GitHubBlameClickHouseEffects{}
