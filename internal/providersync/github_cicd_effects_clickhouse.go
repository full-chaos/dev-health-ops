package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitHubCICDClickHouseEffects persists the ci_pipeline_runs effect emitted by
// GitHubCICDRouteHandler and uses a FINAL point lookup to fence recovery.
type GitHubCICDClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitHubCICDClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "github" || claim.Dataset != "cicd" ||
		effect.Destination != "ci_pipeline_runs" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[ciPipelineRunRow](effect)
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
INSERT INTO ci_pipeline_runs (
  org_id, repo_id, run_id, status, queued_at, started_at, finished_at, retry_count, last_synced
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.OrgID, row.RepoID, row.RunID, row.Status, row.QueuedAt, row.StartedAt,
			row.FinishedAt, row.RetryCount, row.LastSynced,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubCICDClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "github" || claim.Dataset != "cicd" ||
		effect.Destination != "ci_pipeline_runs" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[ciPipelineRunRow](effect)
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
		inspection, err := sink.inspectPipelineRun(ctx, row)
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

func (sink GitHubCICDClickHouseEffects) inspectPipelineRun(
	ctx context.Context,
	expected ciPipelineRunRow,
) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `
SELECT org_id, run_id, status, queued_at, started_at, finished_at, retry_count, last_synced
FROM ci_pipeline_runs FINAL
WHERE org_id = ? AND repo_id = ? AND run_id = ?`, expected.OrgID, expected.RepoID, expected.RunID)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var version ciPipelineRunVersion
	for rows.Next() {
		if err := rows.Scan(
			&version.Row.OrgID, &version.Row.RunID, &version.Row.Status, &version.Row.QueuedAt,
			&version.Row.StartedAt, &version.Row.FinishedAt, &version.Row.RetryCount,
			&version.LastSynced,
		); err != nil {
			return EffectConflict, err
		}
		version.Found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return comparePipelineRunVersion(expected, version), nil
}

type ciPipelineRunVersion struct {
	Row        ciPipelineRunRow
	LastSynced time.Time
	Found      bool
}

func comparePipelineRunVersion(
	expected ciPipelineRunRow,
	actual ciPipelineRunVersion,
) EffectInspection {
	if !actual.Found || actual.LastSynced.IsZero() {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().After(expected.LastSynced.UTC()) {
		return EffectConflict
	}
	if actual.Row.RunID != expected.RunID {
		return EffectConflict
	}
	if actual.Row.OrgID != expected.OrgID {
		return EffectConflict
	}
	if !stringPointersEqual(actual.Row.Status, expected.Status) {
		return EffectConflict
	}
	if !timePointersEqual(actual.Row.QueuedAt, expected.QueuedAt) {
		return EffectConflict
	}
	if !actual.Row.StartedAt.UTC().Equal(expected.StartedAt.UTC()) {
		return EffectConflict
	}
	if !timePointersEqual(actual.Row.FinishedAt, expected.FinishedAt) {
		return EffectConflict
	}
	if actual.Row.RetryCount != expected.RetryCount {
		return EffectConflict
	}
	return EffectExact
}

var _ EffectSink = GitHubCICDClickHouseEffects{}
var _ EffectReadback = GitHubCICDClickHouseEffects{}
