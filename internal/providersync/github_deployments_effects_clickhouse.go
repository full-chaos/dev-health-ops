package providersync

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type GitHubDeploymentsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitHubDeploymentsClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil || claim.Provider != "github" || claim.Dataset != "deployments" || effect.Destination != "deployments" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[deploymentRow](effect)
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
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO deployments (repo_id, deployment_id, status, environment, started_at, finished_at, deployed_at, merged_at, pull_request_number, release_ref, release_ref_confidence, org_id, last_synced)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.RepoID, row.DeploymentID, row.Status, row.Environment, row.StartedAt, row.FinishedAt, row.DeployedAt, row.MergedAt, nullableInt32(row.PullRequestNumber), row.ReleaseRef, row.ReleaseRefConfidence, row.OrgID, row.LastSynced); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubDeploymentsClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil || claim.Provider != "github" || claim.Dataset != "deployments" || effect.Destination != "deployments" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[deploymentRow](effect)
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
		inspection, err := sink.inspectDeployment(ctx, row)
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

func (sink GitHubDeploymentsClickHouseEffects) inspectDeployment(ctx context.Context, expected deploymentRow) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `SELECT repo_id, deployment_id, status, environment, started_at, finished_at, deployed_at, merged_at, pull_request_number, release_ref, release_ref_confidence, org_id, last_synced FROM deployments FINAL WHERE org_id = ? AND repo_id = ? AND deployment_id = ?`, expected.OrgID, expected.RepoID, expected.DeploymentID)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual deploymentRow
	found := false
	for rows.Next() {
		if err := rows.Scan(&actual.RepoID, &actual.DeploymentID, &actual.Status, &actual.Environment, &actual.StartedAt, &actual.FinishedAt, &actual.DeployedAt, &actual.MergedAt, &actual.PullRequestNumber, &actual.ReleaseRef, &actual.ReleaseRefConfidence, &actual.OrgID, &actual.LastSynced); err != nil {
			return EffectConflict, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareDeploymentVersion(expected, actual, found), nil
}

func compareDeploymentVersion(expected, actual deploymentRow, found bool) EffectInspection {
	if !found || actual.LastSynced.IsZero() {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().After(expected.LastSynced.UTC()) {
		return EffectConflict
	}
	if actual.RepoID != expected.RepoID || actual.DeploymentID != expected.DeploymentID || actual.OrgID != expected.OrgID || !stringPointersEqual(actual.Status, expected.Status) || !stringPointersEqual(actual.Environment, expected.Environment) || !timePointersEqual(actual.StartedAt, expected.StartedAt) || !timePointersEqual(actual.FinishedAt, expected.FinishedAt) || !timePointersEqual(actual.DeployedAt, expected.DeployedAt) || !timePointersEqual(actual.MergedAt, expected.MergedAt) || !intPointersEqual(actual.PullRequestNumber, expected.PullRequestNumber) || actual.ReleaseRef != expected.ReleaseRef || actual.ReleaseRefConfidence != expected.ReleaseRefConfidence {
		return EffectConflict
	}
	return EffectExact
}

func intPointersEqual(left, right *int) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func nullableInt32(value *int) any {
	if value == nil {
		return nil
	}
	return int32(*value)
}

var _ EffectSink = GitHubDeploymentsClickHouseEffects{}
var _ EffectReadback = GitHubDeploymentsClickHouseEffects{}
