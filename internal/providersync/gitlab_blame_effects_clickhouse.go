package providersync

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitLabBlameClickHouseEffects persists the same git_blame projection as the
// GitHub provider while keeping the provider guard local to the GitLab claim.
// The exact point lookup is the durable readback boundary after a crash or
// lease handoff; no successful unit may rely on an in-memory batch alone.
type GitLabBlameClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitLabBlameClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "blame" || effect.Destination != "git_blame" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[gitBlameRow](effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := validateGitLabBlameRow(row, claim); err != nil {
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

func (sink GitLabBlameClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "blame" || effect.Destination != "git_blame" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[gitBlameRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	for _, row := range expected {
		if err := validateGitLabBlameRow(row, claim); err != nil {
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
		inspection, inspectErr := sink.inspectGitLabBlame(ctx, row)
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
	switch {
	case exact == len(expected):
		return EffectExact, nil
	case absent == len(expected):
		return EffectAbsent, nil
	default:
		return EffectConflict, nil
	}
}

func (sink GitLabBlameClickHouseEffects) inspectGitLabBlame(
	ctx context.Context,
	expected gitBlameRow,
) (EffectInspection, error) {
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

var _ EffectSink = GitLabBlameClickHouseEffects{}
var _ EffectReadback = GitLabBlameClickHouseEffects{}
