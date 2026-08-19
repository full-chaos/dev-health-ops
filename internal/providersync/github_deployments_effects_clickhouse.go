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

// GitLabDeploymentsClickHouseEffects applies the same deployment-row
// persistence/readback contract without adding a provider column to the
// shared deployments table. Its fixed provider binding prevents a caller from
// using this sink for an unrelated provider just because the row shape matches.
type GitLabDeploymentsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type deploymentsClickHouseEffects struct {
	Conn     driver.Conn
	Lease    providerfoundation.LeaseGuard
	Provider string
}

func (sink GitHubDeploymentsClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	return sink.shared().WriteEffect(ctx, claim, effect)
}

func (sink GitHubDeploymentsClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	return sink.shared().InspectEffect(ctx, claim, effect)
}

func (sink GitHubDeploymentsClickHouseEffects) shared() deploymentsClickHouseEffects {
	return deploymentsClickHouseEffects{Conn: sink.Conn, Lease: sink.Lease, Provider: "github"}
}

func (sink GitLabDeploymentsClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	return sink.shared().WriteEffect(ctx, claim, effect)
}

func (sink GitLabDeploymentsClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	return sink.shared().InspectEffect(ctx, claim, effect)
}

func (sink GitLabDeploymentsClickHouseEffects) shared() deploymentsClickHouseEffects {
	return deploymentsClickHouseEffects{Conn: sink.Conn, Lease: sink.Lease, Provider: "gitlab"}
}

func (sink deploymentsClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if ctx == nil || sink.Lease == nil || (sink.Provider != "github" && sink.Provider != "gitlab") || claim.Validate() != nil || claim.Provider != sink.Provider || claim.Dataset != "deployments" || effect.Destination != "deployments" {
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
		if err := batch.Append(row.RepoID, row.DeploymentID, row.Status, row.Environment, row.StartedAt, row.FinishedAt, row.DeployedAt, row.MergedAt, nullableUInt32(row.PullRequestNumber), row.ReleaseRef, row.ReleaseRefConfidence, row.OrgID, row.LastSynced); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink deploymentsClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || (sink.Provider != "github" && sink.Provider != "gitlab") || claim.Validate() != nil || claim.Provider != sink.Provider || claim.Dataset != "deployments" || effect.Destination != "deployments" {
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

func (sink deploymentsClickHouseEffects) inspectDeployment(ctx context.Context, expected deploymentRow) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `SELECT repo_id, deployment_id, status, environment, started_at, finished_at, deployed_at, merged_at, pull_request_number, release_ref, release_ref_confidence, org_id, last_synced FROM deployments FINAL WHERE org_id = ? AND repo_id = ? AND deployment_id = ?`, expected.OrgID, expected.RepoID, expected.DeploymentID)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var (
		actual            deploymentRow
		pullRequestNumber *uint32
	)
	found := false
	for rows.Next() {
		if err := rows.Scan(&actual.RepoID, &actual.DeploymentID, &actual.Status, &actual.Environment, &actual.StartedAt, &actual.FinishedAt, &actual.DeployedAt, &actual.MergedAt, &pullRequestNumber, &actual.ReleaseRef, &actual.ReleaseRefConfidence, &actual.OrgID, &actual.LastSynced); err != nil {
			return EffectConflict, err
		}
		actual.PullRequestNumber = uint32PointerAsInt(pullRequestNumber)
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

func uint32PointerAsInt(value *uint32) *int {
	if value == nil {
		return nil
	}
	converted := int(*value)
	return &converted
}

func nullableUInt32(value *int) *uint32 {
	if value == nil {
		return nil
	}
	converted := uint32(*value)
	return &converted
}

var _ EffectSink = GitHubDeploymentsClickHouseEffects{}
var _ EffectReadback = GitHubDeploymentsClickHouseEffects{}
var _ EffectSink = GitLabDeploymentsClickHouseEffects{}
var _ EffectReadback = GitLabDeploymentsClickHouseEffects{}
