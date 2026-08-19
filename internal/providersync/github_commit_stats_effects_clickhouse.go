package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitHubCommitStatsClickHouseEffects writes git_commit_stats rows and reads
// the winning ReplacingMergeTree version through a tenant-scoped FINAL lookup.
type GitHubCommitStatsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type GitLabCommitStatsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitHubCommitStatsClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	return writeCommitStatsEffect(ctx, sink.Conn, sink.Lease, "github", claim, effect)
}

func (sink GitLabCommitStatsClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	return writeCommitStatsEffect(ctx, sink.Conn, sink.Lease, "gitlab", claim, effect)
}

func writeCommitStatsEffect(
	ctx context.Context,
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
	provider string,
	claim Claim,
	effect EffectBatch,
) error {
	if ctx == nil || lease == nil || claim.Validate() != nil ||
		claim.Provider != provider || claim.Dataset != "commit-stats" ||
		effect.Destination != "git_commit_stats" {
		return ErrInvalidConfiguration
	}
	if err := lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[commitStatsRow](effect)
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
	batch, err := conn.PrepareBatch(ctx, `
INSERT INTO git_commit_stats (
  repo_id, commit_hash, file_path, additions, deletions, old_file_mode,
  new_file_mode, last_synced, org_id
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.RepoID, row.CommitHash, row.FilePath, row.Additions, row.Deletions,
			row.OldFileMode, row.NewFileMode, row.LastSynced, row.OrgID,
		); err != nil {
			return err
		}
	}
	if err := lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubCommitStatsClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	return inspectCommitStatsEffect(ctx, sink.Conn, sink.Lease, "github", claim, effect)
}

func (sink GitLabCommitStatsClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	return inspectCommitStatsEffect(ctx, sink.Conn, sink.Lease, "gitlab", claim, effect)
}

func inspectCommitStatsEffect(
	ctx context.Context,
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
	provider string,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	if ctx == nil || lease == nil || claim.Validate() != nil ||
		claim.Provider != provider || claim.Dataset != "commit-stats" ||
		effect.Destination != "git_commit_stats" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[commitStatsRow](effect)
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
	if conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	exact, absent := 0, 0
	for _, row := range expected {
		inspection, err := inspectCommitStats(ctx, conn, row)
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

func inspectCommitStats(
	ctx context.Context,
	conn driver.Conn,
	expected commitStatsRow,
) (EffectInspection, error) {
	rows, err := conn.Query(ctx, `
SELECT org_id, commit_hash, file_path, additions, deletions, old_file_mode,
       new_file_mode, last_synced
FROM git_commit_stats FINAL
WHERE org_id = ? AND repo_id = ? AND commit_hash = ? AND file_path = ?`,
		expected.OrgID, expected.RepoID, expected.CommitHash, expected.FilePath)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var version commitStatsVersion
	for rows.Next() {
		if err := rows.Scan(
			&version.Row.OrgID, &version.Row.CommitHash, &version.Row.FilePath,
			&version.Row.Additions, &version.Row.Deletions, &version.Row.OldFileMode,
			&version.Row.NewFileMode, &version.LastSynced,
		); err != nil {
			return EffectConflict, err
		}
		version.Found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareCommitStatsVersion(expected, version), nil
}

type commitStatsVersion struct {
	Row        commitStatsRow
	LastSynced time.Time
	Found      bool
}

func compareCommitStatsVersion(expected commitStatsRow, actual commitStatsVersion) EffectInspection {
	if !actual.Found || actual.LastSynced.IsZero() || actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().After(expected.LastSynced.UTC()) ||
		actual.Row.OrgID != expected.OrgID || actual.Row.CommitHash != expected.CommitHash ||
		actual.Row.FilePath != expected.FilePath || actual.Row.Additions != expected.Additions ||
		actual.Row.Deletions != expected.Deletions || actual.Row.OldFileMode != expected.OldFileMode ||
		actual.Row.NewFileMode != expected.NewFileMode {
		return EffectConflict
	}
	return EffectExact
}

var _ EffectSink = GitHubCommitStatsClickHouseEffects{}
var _ EffectReadback = GitHubCommitStatsClickHouseEffects{}
var _ EffectSink = GitLabCommitStatsClickHouseEffects{}
var _ EffectReadback = GitLabCommitStatsClickHouseEffects{}
