package providersync

import (
	"context"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
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
	// Every write reads the held version of its keys (the terminal merged_at
	// pair needs it even when no lookup failed). A read failure fails the
	// write, and the unit retries, instead of inserting uncorrected values.
	if err := applyDeploymentContract(ctx, sink.Conn, claim, rows, true); err != nil {
		slog.Warn("providersync.deployment.carry_forward_guard_read_failed", "org_id", claim.OrgID, "provider", claim.Provider, "unit_id", claim.ID, "phase", "write", "cause", err.Error())
		return err
	}
	batch, err := sink.Conn.PrepareBatch(ctx, deploymentsInsert)
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
	// WriteEffect carried held columns into the rows it inserted. Recovery
	// rebuilds `expected` from a fresh Collect() pass that knows nothing of
	// that correction, so the same contract runs here against the same
	// stored state; otherwise a row the contract wrote reads back as
	// EffectConflict. A read failure fails the inspection, the same as it
	// fails WriteEffect.
	if err := applyDeploymentContract(ctx, sink.Conn, claim, expected, false); err != nil {
		slog.Warn("providersync.deployment.carry_forward_guard_read_failed", "org_id", claim.OrgID, "provider", claim.Provider, "unit_id", claim.ID, "phase", "inspect", "cause", err.Error())
		return EffectConflict, err
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

// deploymentLifecycleCarriedForward is one row where a stored started_at/
// finished_at/status was carried forward over a nil this pass's statuses
// lookup produced by failing (never over an honest, successful-but-empty
// nil). It carries only the row's own key -- never environment or any
// other row content -- the same minimal shape
// pullRequestMergedAtRegressionGuarded already uses for its own write-once
// guard.
type deploymentLifecycleCarriedForward struct {
	RepoID       string
	DeploymentID string
}

// deploymentLifecycleRegressionGuardedEvent is this guard's stable log
// event name, the same substitute-counter convention
// pullRequestMergedAtRegressionGuardedEvent already uses.
const deploymentLifecycleRegressionGuardedEvent = "providersync.deployment.lifecycle_regression_guarded"

// logDeploymentLifecycleRegressionGuarded emits one WARN line per carried
// row, never a batch aggregate -- this is the one place in the whole
// system where the stored value and the incoming nil are both in the same
// process at the same time.
func logDeploymentLifecycleRegressionGuarded(
	ctx context.Context, claim Claim, carried []deploymentLifecycleCarriedForward,
) {
	for _, event := range carried {
		slog.Default().LogAttrs(ctx, slog.LevelWarn, deploymentLifecycleRegressionGuardedEvent,
			slog.String("org_id", claim.OrgID),
			slog.String("provider", claim.Provider),
			slog.String("dataset", claim.Dataset),
			slog.String("unit_id", claim.ID),
			slog.String("repo_id", event.RepoID),
			logging.ProviderIDAttr("deployment_id", event.DeploymentID),
		)
	}
}

// deploymentPullRequestCarriedForward is one row where a stored
// merged_at/pull_request_number was carried forward over the nils a
// failed per-SHA pull request lookup produced this pass.
type deploymentPullRequestCarriedForward struct {
	RepoID       string
	DeploymentID string
}

const deploymentPullRequestRegressionGuardedEvent = "providersync.deployment.pull_request_regression_guarded"

// logDeploymentPullRequestRegressionGuarded emits one WARN line per
// carried row, keyed by the row's own identity only.
func logDeploymentPullRequestRegressionGuarded(
	ctx context.Context, claim Claim, carried []deploymentPullRequestCarriedForward,
) {
	for _, event := range carried {
		slog.Default().LogAttrs(ctx, slog.LevelWarn, deploymentPullRequestRegressionGuardedEvent,
			slog.String("org_id", claim.OrgID),
			slog.String("provider", claim.Provider),
			slog.String("dataset", claim.Dataset),
			slog.String("unit_id", claim.ID),
			slog.String("repo_id", event.RepoID),
			logging.ProviderIDAttr("deployment_id", event.DeploymentID),
		)
	}
}

var _ EffectSink = GitHubDeploymentsClickHouseEffects{}
var _ EffectReadback = GitHubDeploymentsClickHouseEffects{}
var _ EffectSink = GitLabDeploymentsClickHouseEffects{}
var _ EffectReadback = GitLabDeploymentsClickHouseEffects{}
