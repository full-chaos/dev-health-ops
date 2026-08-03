package providersync

import (
	"context"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitLabCICDClickHouseEffects persists the isolated Python-owned pipeline row
// through one prepared batch and reads it back through the complete
// tenant-prefixed ReplacingMergeTree key.
type GitLabCICDClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitLabCICDClickHouseEffects) valid(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "gitlab" || claim.Dataset != "cicd" ||
		effect.Destination != "ci_pipeline_runs" {
		return ErrInvalidConfiguration
	}
	return sink.Lease.Assert(ctx)
}

func (sink GitLabCICDClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if err := sink.valid(ctx, claim, effect); err != nil {
		return err
	}
	rows, err := decodeEffectRows[gitLabCICDPipelineRow](effect)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if err := row.validate(claim); err != nil {
			return err
		}
		key := strings.Join([]string{row.OrgID, row.RepoID, row.RunID}, "\x00")
		if _, duplicate := seen[key]; duplicate {
			return ErrInvalidConfiguration
		}
		seen[key] = struct{}{}
	}
	if len(rows) == 0 {
		return sink.Lease.Assert(ctx)
	}
	if sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `
INSERT INTO ci_pipeline_runs (
  org_id, repo_id, run_id, status, queued_at, started_at, finished_at,
  retry_count, last_synced
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.OrgID, row.RepoID, row.RunID, row.Status, row.QueuedAt,
			row.StartedAt, row.FinishedAt, row.RetryCount, row.LastSynced,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitLabCICDClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if err := sink.valid(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[gitLabCICDPipelineRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	exact, absent := 0, 0
	for _, row := range expected {
		if err := row.validate(claim); err != nil {
			return EffectConflict, err
		}
		inspection, err := inspectGitLabCICDPipeline(ctx, sink.Conn, row)
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

func inspectGitLabCICDPipeline(
	ctx context.Context, conn driver.Conn, expected gitLabCICDPipelineRow,
) (EffectInspection, error) {
	rows, err := conn.Query(ctx, `
SELECT org_id, repo_id, run_id, status, queued_at, started_at, finished_at,
       retry_count, last_synced
FROM ci_pipeline_runs FINAL
WHERE org_id = ? AND repo_id = ? AND run_id = ?`,
		expected.OrgID, expected.RepoID, expected.RunID,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual gitLabCICDPipelineRow
	found := false
	for rows.Next() {
		if err := rows.Scan(
			&actual.OrgID, &actual.RepoID, &actual.RunID, &actual.Status,
			&actual.QueuedAt, &actual.StartedAt, &actual.FinishedAt,
			&actual.RetryCount, &actual.LastSynced,
		); err != nil {
			return EffectConflict, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitLabCICDPipelineVersion(expected, actual, found), nil
}

func compareGitLabCICDPipelineVersion(
	expected, actual gitLabCICDPipelineRow, found bool,
) EffectInspection {
	if !found || actual.LastSynced.IsZero() || actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().After(expected.LastSynced.UTC()) ||
		actual.OrgID != expected.OrgID || actual.RepoID != expected.RepoID ||
		actual.RunID != expected.RunID || !stringPointersEqual(actual.Status, expected.Status) ||
		!timePointersEqual(actual.QueuedAt, expected.QueuedAt) ||
		!actual.StartedAt.UTC().Equal(expected.StartedAt.UTC()) ||
		!timePointersEqual(actual.FinishedAt, expected.FinishedAt) ||
		actual.RetryCount != expected.RetryCount {
		return EffectConflict
	}
	return EffectExact
}

var _ EffectSink = GitLabCICDClickHouseEffects{}
var _ EffectReadback = GitLabCICDClickHouseEffects{}
